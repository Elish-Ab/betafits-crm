"""Graphiti Knowledge Graph client for entity and relationship management.

This module provides a wrapper around the graphiti-core library with comprehensive
error handling and retry logic for the email ingestor pipeline.

Graphiti is a temporal knowledge graph framework based on Neo4j. Key concepts:
- Episodes: Text chunks that get ingested into the graph (e.g., email content)
- Entities: Nodes representing people, companies, products, etc.
- Edges: Relationships between entities
- Custom Types: User-defined Pydantic models for entities and edges

The client handles:
- Episode ingestion from email content with automatic entity/edge extraction
- Custom entity type definition (Company, Contact, Opportunity, Intent, Product)
- Custom edge type definition (WorksAt, HasOpportunity, PointOfContact, etc.)
- Semantic and hybrid search for entities and relationships
- Community detection and temporal analysis
- **Atomic semantic functions for deterministic schema enforcement**

Domain Schema:
    We use a Star Schema centered around Opportunity and Organization nodes:
    - Nodes: Opportunity, Organization, Contact, Product, Intent
    - Edges: WorksAt, HasOpportunity, PointOfContact, MentionedIn, RelatedTo, InterestedIn

Usage:
    from lib.integrations.graphiti_client import get_graphiti_client
    from datetime import datetime

    client = get_graphiti_client()

    # Generic episode ingestion (LLM extracts entities automatically)
    await client.add_episode(
        name="email-123",
        episode_body="Acme Corp hired John Smith as CTO in 2023.",
        source_description="from email ID email-123",
        reference_time=datetime.now(),
    )

    # Atomic semantic functions (deterministic schema enforcement)
    await client.ensure_organization(domain="acme.com", name="Acme Corporation")
    await client.ensure_contact(email="john@acme.com", name="John Smith")
    await client.link_contact_to_org(email="john@acme.com", domain="acme.com")
    await client.create_opportunity_node(
        uuid="opp-123",
        name="Website Redesign",
        org_domain="acme.com",
        status="active"
    )
    await client.link_contact_to_opportunity(email="john@acme.com", opp_uuid="opp-123")

    # Search for entities
    results = await client.search_entities(
        query="companies in tech industry",
        node_labels=["Company"]
    )
"""

from typing import List
import logging
from datetime import datetime
from typing import Any, Optional

from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType
from graphiti_core.edges import EntityEdge
from graphiti_core.search.search_filters import SearchFilters
from openai import embeddings

from lib.config import get_settings
from lib.models.graph_schemas import (
    AboutProduct,
    BringsOpportunity,
    Company,
    Contact,
    HasOpportunity,
    Opportunity,
    Product,
    WorksAt,
)
from lib.utils.retry import retry_on_exception_async

from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
from graphiti_core.cross_encoder.openai_reranker_client import OpenAIRerankerClient
from graphiti_core.nodes import CommunityNode, EntityNode
from neo4j.graph import Node

logger = logging.getLogger(__name__)

# Global Graphiti client instance (singleton)
_graphiti_client: Optional["GraphitiClient"] = None


class GraphitiClient:
    """Client for interacting with Betafits' Neo4j Knowledge Graph via Graphiti.

    Wraps the graphiti-core library with retry logic, error handling, and
    structured entity/edge type management.

    Graphiti operates on "episodes" (text chunks) which are automatically processed
    to extract entities and relationships using an LLM. Custom entity and edge types
    guide the extraction process and structure the resulting graph.

    Attributes:
        _graphiti: The underlying Graphiti instance connected to Neo4j.
        _timeout: Request timeout in seconds from settings.
        _max_retries: Maximum retry attempts from settings.
        _backoff_factor: Exponential backoff factor from settings.
    """

    def __init__(self, graphiti: Optional[Graphiti] = None) -> None:
        """Initialize Graphiti client.

        Args:
            graphiti: Optional Graphiti instance. If not provided, creates one
                from settings using Neo4j connection details.

        Raises:
            ValueError: If required Neo4j configuration is missing.
        """
        if graphiti:
            self._graphiti = graphiti
        else:
            settings = get_settings()

            # Validate Neo4j configuration
            if not settings.neo4j_uri or not settings.neo4j_user:
                raise ValueError(
                    "Neo4j URI and username must be configured "
                    "via NEO4J_URI and NEO4J_USER environment variables"
                )

            llm_config = LLMConfig(
                api_key=settings.openrouter_api_key,
                model=settings.openrouter_default_model,  # e.g., "mistral-large-latest"
                small_model=settings.openrouter_small_model,  # e.g., "mistral-small-latest"
                base_url=settings.openrouter_base_url,  # e.g., "https://api.mistral.ai/v1"
            )

            # Initialize Graphiti with Neo4j connection
            # Graphiti will handle entity/edge extraction and storage
            try:
                self._graphiti = Graphiti(
                    uri=settings.neo4j_uri,
                    user=settings.neo4j_user,
                    password=settings.neo4j_password or "password",
                    llm_client=OpenAIGenericClient(config=llm_config),
                    embedder=OpenAIEmbedder(
                        config=OpenAIEmbedderConfig(
                            api_key=settings.openrouter_api_key,
                            embedding_model=settings.openrouter_embedding_model,  # e.g., "mistral-embedding-3-small"
                            base_url=settings.openrouter_base_url,
                        )
                    ),
                    cross_encoder=OpenAIRerankerClient(
                        config=LLMConfig(
                            api_key=settings.openrouter_api_key,
                            model=settings.openrouter_small_model,  # Use smaller model for reranking
                            base_url=settings.openrouter_base_url,
                        )
                    ),
                )

            except Exception as e:
                logger.error(f"Failed to initialize Graphiti: {e}")
                raise ValueError(
                    f"Cannot connect to Neo4j at {settings.neo4j_uri}: {e}"
                ) from e

        settings = get_settings()
        self._timeout = settings.request_timeout
        self._max_retries = settings.retry_max_attempts
        self._backoff_factor = settings.retry_backoff_factor

    def _log_operation(
        self,
        operation: str,
        episode_name: str,
        error: Optional[str] = None,
    ) -> None:
        """Log knowledge graph operation (without sensitive data).

        Args:
            operation: The operation type (ADD_EPISODE, SEARCH, BUILD_INDICES).
            episode_name: The episode identifier.
            error: Optional error message.
        """
        if error:
            logger.error(f"KG {operation} failed for episode {episode_name}: {error}")
        else:
            logger.debug(f"KG {operation} completed for episode {episode_name}")

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def add_episode(
        self,
        name: str,
        episode_body: str,
        source_description: str,
        reference_time: datetime,
        group_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Add an episode (text chunk) to the knowledge graph.

        Episodes are processed by Graphiti's LLM to automatically extract entities
        and relationships. Custom entity/edge types guide the extraction.

        Args:
            name: Unique episode identifier (e.g., "email-123").
            episode_body: The text content to extract entities/relationships from.
            source_description: Description of the data source (e.g., "from email").
            reference_time: Timestamp when this episode occurred.

        Returns:
            Dictionary with episode metadata and extraction results.

        Raises:
            ValueError: If required parameters are invalid.
            RuntimeError: If Graphiti connection fails.
        """
        if not name or not episode_body:
            raise ValueError("Episode name and body are required")

        self._log_operation("ADD_EPISODE", name)

        try:
            # Define custom entity types for this domain
            entity_types = {
                "Company": Company,
                "Contact": Contact,
                "Opportunity": Opportunity,
                "Product": Product,
            }

            # Define custom edge types for this domain
            edge_types = {
                "WorksAt": WorksAt,
                "BringsOpportunity": BringsOpportunity,
                "HasOpportunity": HasOpportunity,
                "AboutProduct": AboutProduct,
            }

            # Define allowed connections between entity types
            # Simplified star schema:
            # - Contact WORKS_AT Company: Employment relationship
            # - Contact BRINGS_OPPORTUNITY Opportunity: Contact introduces/is relevant to deal
            # - Company HAS_OPPORTUNITY Opportunity: Company is the potential customer
            # - Opportunity ABOUT_PRODUCT Product: The opportunity is about selling a product
            edge_type_map = {
                ("Contact", "Company"): ["WorksAt"],
                ("Contact", "Opportunity"): ["BringsOpportunity"],
                ("Company", "Opportunity"): ["HasOpportunity"],
                ("Opportunity", "Product"): ["AboutProduct"],
            }

            # Add episode to the graph
            # Graphiti will extract entities/edges using the LLM and custom types
            await self._graphiti.add_episode(
                name=name,
                episode_body=episode_body,
                source=EpisodeType.text,
                source_description=source_description,
                reference_time=reference_time,
                entity_types=entity_types,
                edge_types=edge_types,
                edge_type_map=edge_type_map,
                group_id=group_id,
            )

            self._log_operation("ADD_EPISODE", name)
            logger.info(f"Episode {name} added successfully")

            return {
                "episode_name": name,
                "source": source_description,
                "timestamp": reference_time.isoformat(),
                "status": "completed",
            }

        except Exception as e:
            self._log_operation("ADD_EPISODE", name, error=str(e))
            raise RuntimeError(f"Failed to add episode {name}: {e}") from e

    async def search_entities(
        self,
        query: str,
        node_labels: Optional[list[str]] = None,
    ) -> Any:
        """Search for entities in the knowledge graph.

        Performs semantic and keyword-based search for entities matching the query.

        Args:
            query: Search query (e.g., "tech companies hiring").
            node_labels: Optional list of entity types to filter by
                (e.g., ["Company", "Contact"]).

        Returns:
            SearchResults object containing entity search results with scores and metadata.

        Raises:
            ValueError: If query is empty.
            RuntimeError: If search fails.
        """
        if not query:
            raise ValueError("Search query cannot be empty")

        self._log_operation("SEARCH_ENTITIES", f"query:{query}")

        try:
            # Create search filter if node labels specified
            search_filter = None
            if node_labels:
                search_filter = SearchFilters(node_labels=node_labels)

            # Perform search using Graphiti's hybrid search
            # (semantic + keyword-based)
            results = await self._graphiti.search_(
                query=query,
                search_filter=search_filter,
            )

            logger.debug("Search found results")
            return results

        except Exception as e:
            self._log_operation("SEARCH_ENTITIES", f"query:{query}", error=str(e))
            raise RuntimeError(f"Search failed for query '{query}': {e}") from e

    async def search_relationships(
        self,
        query: str,
        edge_types: Optional[list[str]] = None,
    ) -> Any:
        """Search for relationships/edges in the knowledge graph.

        Finds edges (relationships) between entities matching the query.

        Args:
            query: Search query (e.g., "employment relationships").
            edge_types: Optional list of edge types to filter by
                (e.g., ["WorksAt", "HasOpportunity"]).

        Returns:
            SearchResults object containing edge search results with source, target, and properties.

        Raises:
            ValueError: If query is empty.
            RuntimeError: If search fails.
        """
        if not query:
            raise ValueError("Search query cannot be empty")

        self._log_operation("SEARCH_RELATIONSHIPS", f"query:{query}")

        try:
            # Create search filter for edge types
            search_filter = None
            if edge_types:
                search_filter = SearchFilters(edge_types=edge_types)

            # Perform search for relationships
            results = await self._graphiti.search_(
                query=query,
                search_filter=search_filter,
            )

            logger.debug("Found relationships")
            return results

        except Exception as e:
            self._log_operation("SEARCH_RELATIONSHIPS", f"query:{query}", error=str(e))
            raise RuntimeError(f"Relationship search failed: {e}") from e

    async def build_indices(self) -> dict[str, Any]:
        """Build Neo4j indices and constraints for performance.

        Should be called once during setup or after initial data load.
        Improves query performance and ensures data consistency.

        Returns:
            Dictionary with status and index information.

        Raises:
            RuntimeError: If index building fails.
        """
        self._log_operation("BUILD_INDICES", "system")

        try:
            # Build indices and constraints in Neo4j
            await self._graphiti.build_indices_and_constraints()

            logger.info("Knowledge graph indices built successfully")
            return {
                "status": "completed",
                "message": "Indices and constraints built",
            }

        except Exception as e:
            self._log_operation("BUILD_INDICES", "system", error=str(e))
            raise RuntimeError(f"Failed to build indices: {e}") from e

    async def build_communities(self, group_id: Optional[str] = None) -> dict[str, Any]:
        """Build community clusters in the knowledge graph.

        Detects communities (groups of closely related nodes) for better
        retrieval and analysis.

        Returns:
            Dictionary with community detection results.

        Raises:
            RuntimeError: If community building fails.
        """
        self._log_operation("BUILD_COMMUNITIES", "system")

        try:
            # Build communities for graph analysis
            await self._graphiti.build_communities(
                group_ids=[group_id] if group_id else None
            )

            logger.info("Knowledge graph communities built successfully")
            return {
                "status": "completed",
                "message": "Communities detected and built",
            }

        except Exception as e:
            self._log_operation("BUILD_COMMUNITIES", "system", error=str(e))
            raise RuntimeError(f"Failed to build communities: {e}") from e

    @retry_on_exception_async(
        max_attempts=1,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_nodes_by_namespace(
        self, node_labels: List[str], group_id: str
    ) -> list[EntityNode]:
        """Fetch all nodes of a given type in the graph identified by group_id.
        Retrieves all nodes of a specific type that are associated with a
        specific group_id from episodes or episode groupings.
        Args:
            node_label: The node label to filter nodes by.
            group_id: The group identifier to filter nodes by.
        Returns:
            List of dictionaries containing node data.
        """
        try:
            cypher_query = """
            MATCH (n)
            WHERE n.group_id = $groupId
            AND any(label IN $labelList WHERE label IN labels(n))
            RETURN n
            """

            parameters = {"groupId": group_id, "labelList": node_labels}

            async with self._graphiti.driver.session() as session:
                result = await session.run(cypher_query, parameters=parameters)
                records = await result.data()

            found_nodes: list[EntityNode] = []
            for record in records:
                node_value = record.get("n") if isinstance(record, dict) else None
                if node_value is None:
                    continue

                node_value["created_at"] = datetime.now()

                found_nodes.append(EntityNode.model_validate(node_value))

            return found_nodes

        except Exception as e:
            logger.error(
                f"Failed to retrieve nodes with labels {node_labels} "
                f"for group_id {group_id}: {e}"
            )
            raise RuntimeError(f"Cannot get nodes for group_id {group_id}: {e}") from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_communities_by_group_id(self, group_id: str) -> list[CommunityNode]:
        """Fetch all communities in the graph identified by group_id.

        Retrieves all communities (detected clusters of related nodes) that are
        associated with a specific group_id from episodes or episode groupings.

        Args:
            group_id: The group identifier to filter communities by.

        Returns:
            List of dictionaries containing community data with node and edge information.
            Each community dict contains community metadata and member nodes.
            Returns empty list if no communities found for the group_id.

        Raises:
            ValueError: If group_id is empty.
            RuntimeError: If community retrieval fails.
        """
        if not group_id or not isinstance(group_id, str):
            raise ValueError("group_id must be a non-empty string")

        self._log_operation("GET_COMMUNITIES", group_id)

        try:
            # Query communities associated with the group_id using Graphiti's search
            # Search for episodes and their communities filtered by group_id
            search_query = "all communities"

            results = await self._graphiti.search_(
                query=search_query,
                group_ids=[group_id],
                search_filter=SearchFilters(node_labels=["Community"]),
            )

            communities = results.communities

            logger.info(
                f"Retrieved {len(communities)} communities for group_id: {group_id}"
            )
            return communities

        except Exception as e:
            self._log_operation("GET_COMMUNITIES", group_id, error=str(e))
            raise RuntimeError(
                f"Failed to retrieve communities for group_id {group_id}: {e}"
            ) from e

    # ========================================================================
    # ATOMIC SEMANTIC FUNCTIONS - Domain-Specific Schema Enforcement
    # ========================================================================
    # These specialized methods construct synthetic "facts" to force the graph
    # to adopt our domain schema (Star Schema: Opportunity + Organization centered).
    # They provide deterministic, idempotent operations for entity creation and linking.

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def ensure_organization(self, domain: str, name: str) -> None:
        """Idempotent creation of an Organization node.

        Creates an organization entity in the knowledge graph if it doesn't exist.
        The domain serves as the unique identifier for domain-based matching logic.

        Args:
            domain: Company email domain (e.g., "@acme.com" or "acme.com").
            name: Organization name (e.g., "Acme Corporation").

        Raises:
            ValueError: If domain or name is empty.
            RuntimeError: If episode creation fails.
        """
        if not domain or not name:
            raise ValueError("Organization domain and name are required")

        # Normalize domain to lowercase without @ prefix
        normalized_domain = domain.lower().lstrip("@")

        synthetic_fact = (
            f"The organization {name} owns the domain {normalized_domain}. "
            f"This is a business entity that can be reached at {normalized_domain}."
        )

        try:
            await self.add_episode(
                name=f"sys-org-{normalized_domain}",
                episode_body=synthetic_fact,
                source_description="System Generated: Organization Creation",
                reference_time=datetime.now(),
            )
            logger.info(
                f"Organization '{name}' ensured with domain {normalized_domain}"
            )
        except Exception as e:
            logger.error(f"Failed to ensure organization {name}: {e}")
            raise RuntimeError(f"Cannot ensure organization {name}: {e}") from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def ensure_contact(self, email: str, name: str) -> None:
        """Idempotent creation of a Contact node.

        Creates a contact (person) entity if it doesn't exist. Email serves as
        the unique identifier.

        Args:
            email: Contact email address (e.g., "john@acme.com").
            name: Contact full name (e.g., "John Smith").

        Raises:
            ValueError: If email or name is empty.
            RuntimeError: If episode creation fails.
        """
        if not email or not name:
            raise ValueError("Contact email and name are required")

        # Normalize email to lowercase
        normalized_email = email.lower()

        synthetic_fact = (
            f"{name} has the email address {normalized_email}. "
            f"This person can be contacted at {normalized_email}."
        )

        try:
            await self.add_episode(
                name=f"sys-contact-{normalized_email}",
                episode_body=synthetic_fact,
                source_description="System Generated: Contact Creation",
                reference_time=datetime.now(),
            )
            logger.info(f"Contact '{name}' ensured with email {normalized_email}")
        except Exception as e:
            logger.error(f"Failed to ensure contact {name}: {e}")
            raise RuntimeError(f"Cannot ensure contact {name}: {e}") from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def link_contact_to_org(self, email: str, domain: str) -> None:
        """Connect a Contact to an Organization via WORKS_AT relationship.

        Establishes the critical "Domain Match" logic for email disambiguation.

        Args:
            email: Contact email address (e.g., "john@acme.com").
            domain: Organization domain (e.g., "@acme.com" or "acme.com").

        Raises:
            ValueError: If email or domain is empty.
            RuntimeError: If episode creation fails.
        """
        if not email or not domain:
            raise ValueError("Contact email and organization domain are required")

        # Normalize inputs
        normalized_email = email.lower()
        normalized_domain = domain.lower().lstrip("@")

        synthetic_fact = (
            f"The contact with email {normalized_email} works at the organization "
            f"with domain {normalized_domain}. This establishes their employment relationship."
        )

        try:
            await self.add_episode(
                name=f"sys-link-contact-org-{normalized_email}-{normalized_domain}",
                episode_body=synthetic_fact,
                source_description="System Generated: Contact-Organization Link",
                reference_time=datetime.now(),
            )
            logger.info(
                f"Linked contact {normalized_email} to organization {normalized_domain}"
            )
        except Exception as e:
            logger.error(f"Failed to link contact to organization: {e}")
            raise RuntimeError(
                f"Cannot link {normalized_email} to {normalized_domain}: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def create_opportunity_node(
        self,
        uuid: str,
        name: str,
        org_domain: str,
        status: Optional[str] = None,
        summary: Optional[str] = None,
    ) -> None:
        """Create a new Opportunity node as a deal anchor.

        Opportunities are central to the star schema and represent potential deals
        or projects. They are linked to organizations via HAS_OPPORTUNITY relationship.

        Args:
            uuid: Unique opportunity identifier from Supabase (e.g., UUID string).
            name: Opportunity name (e.g., "Website Redesign Project").
            org_domain: Organization domain this opportunity belongs to.
            status: Optional opportunity status (e.g., "active", "closed").
            summary: Optional brief description of the opportunity.

        Raises:
            ValueError: If required parameters are empty.
            RuntimeError: If episode creation fails.
        """
        if not uuid or not name or not org_domain:
            raise ValueError(
                "Opportunity UUID, name, and organization domain are required"
            )

        # Normalize domain
        normalized_domain = org_domain.lower().lstrip("@")

        # Build synthetic fact with optional fields
        synthetic_fact = (
            f"The organization with domain {normalized_domain} has a business opportunity "
            f"named '{name}' with unique ID {uuid}."
        )

        if status:
            synthetic_fact += f" The opportunity status is {status}."

        if summary:
            synthetic_fact += f" {summary}"

        try:
            await self.add_episode(
                name=f"sys-create-opp-{uuid}",
                episode_body=synthetic_fact,
                source_description="System Generated: Opportunity Creation",
                reference_time=datetime.now(),
            )
            logger.info(
                f"Opportunity '{name}' ({uuid}) created for {normalized_domain}"
            )
        except Exception as e:
            logger.error(f"Failed to create opportunity {name}: {e}")
            raise RuntimeError(f"Cannot create opportunity {name}: {e}") from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def link_contact_to_opportunity(
        self,
        email: str,
        opp_uuid: str,
        is_primary: bool = True,
        context: Optional[str] = None,
    ) -> None:
        """Link a Contact to an Opportunity via BRINGS_OPPORTUNITY relationship.

        Establishes that a specific contact brought or is relevant to an opportunity.
        This is the KEY relationship for email disambiguation - determining which
        opportunity an incoming email relates to.

        Args:
            email: Contact email address (e.g., "john@acme.com").
            opp_uuid: Opportunity UUID from Supabase.
            is_primary: Whether this is the primary contact for the opportunity.
            context: Optional context for how this opportunity was identified.

        Raises:
            ValueError: If email or opportunity UUID is empty.
            RuntimeError: If episode creation fails.
        """
        if not email or not opp_uuid:
            raise ValueError("Contact email and opportunity UUID are required")

        # Normalize email
        normalized_email = email.lower()

        synthetic_fact = (
            f"The contact with email {normalized_email} brought/is relevant to "
            f"the opportunity with ID {opp_uuid}."
        )

        if is_primary:
            synthetic_fact += " This is the primary contact for this opportunity."

        if context:
            synthetic_fact += f" Context: {context}"

        try:
            await self.add_episode(
                name=f"sys-link-contact-opp-{normalized_email}-{opp_uuid}",
                episode_body=synthetic_fact,
                source_description="System Generated: Contact-Opportunity Link",
                reference_time=datetime.now(),
            )
            logger.info(f"Linked contact {normalized_email} to opportunity {opp_uuid}")
        except Exception as e:
            logger.error(f"Failed to link contact to opportunity: {e}")
            raise RuntimeError(
                f"Cannot link {normalized_email} to opportunity {opp_uuid}: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def link_opportunity_to_product(
        self, opp_uuid: str, product_name: str, product_stage: Optional[str] = None
    ) -> None:
        """Link an Opportunity to a Betafits Product via ABOUT_PRODUCT relationship.

        Establishes that an opportunity involves the sale or discussion of a specific
        Betafits product or service.

        Args:
            opp_uuid: Opportunity UUID from Supabase.
            product_name: Name of the Betafits product (e.g., "Enterprise PEO").
            product_stage: Optional stage of product discussion (discovery, demo, proposal, negotiation).

        Raises:
            ValueError: If opportunity UUID or product name is empty.
            RuntimeError: If episode creation fails.
        """
        if not opp_uuid or not product_name:
            raise ValueError("Opportunity UUID and product name are required")

        synthetic_fact = f"The opportunity with ID {opp_uuid} is about the Betafits product '{product_name}'."

        if product_stage:
            synthetic_fact += f" The product discussion stage is {product_stage}."

        try:
            await self.add_episode(
                name=f"sys-link-opp-product-{opp_uuid}",
                episode_body=synthetic_fact,
                source_description="System Generated: Opportunity-Product Link",
                reference_time=datetime.now(),
            )
            logger.info(f"Linked opportunity {opp_uuid} to product '{product_name}'")
        except Exception as e:
            logger.error(f"Failed to link opportunity to product: {e}")
            raise RuntimeError(
                f"Cannot link opportunity {opp_uuid} to product: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def link_company_to_opportunity(
        self, company_domain: str, opp_uuid: str
    ) -> None:
        """Link a Company to an Opportunity via HAS_OPPORTUNITY relationship.

        Establishes that a company is the potential customer or organization involved
        in this sales opportunity.

        Args:
            company_domain: Company email domain (e.g., "acme.com").
            opp_uuid: Opportunity UUID from Supabase.

        Raises:
            ValueError: If company domain or opportunity UUID is empty.
            RuntimeError: If episode creation fails.
        """
        if not company_domain or not opp_uuid:
            raise ValueError("Company domain and opportunity UUID are required")

        # Normalize domain
        normalized_domain = company_domain.lower().lstrip("@")

        synthetic_fact = (
            f"The company with domain {normalized_domain} has the business opportunity "
            f"with ID {opp_uuid}. This organization is the potential customer."
        )

        try:
            await self.add_episode(
                name=f"sys-link-company-opp-{normalized_domain}-{opp_uuid}",
                episode_body=synthetic_fact,
                source_description="System Generated: Company-Opportunity Link",
                reference_time=datetime.now(),
            )
            logger.info(f"Linked company {normalized_domain} to opportunity {opp_uuid}")
        except Exception as e:
            logger.error(f"Failed to link company to opportunity: {e}")

            raise RuntimeError(
                f"Cannot link company {normalized_domain} to opportunity {opp_uuid}: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def update_opportunity_summary(self, opp_uuid: str, new_summary: str) -> None:
        """Update the summary field of an existing Opportunity node.

        Adds a new episode to update the opportunity's summary/description.
        This creates a temporal record of the summary change.

        Args:
            opp_uuid: Opportunity UUID from Supabase.
            new_summary: New summary text to update the opportunity with.

        Raises:
            ValueError: If opportunity UUID or summary is empty.
            RuntimeError: If episode creation fails.
        """
        if not opp_uuid or not new_summary:
            raise ValueError("Opportunity UUID and new summary are required")

        synthetic_fact = (
            f"The opportunity with ID {opp_uuid} has been updated with the following summary: "
            f"{new_summary}"
        )

        try:
            await self.add_episode(
                name=f"sys-update-opp-summary-{opp_uuid}-{datetime.now().timestamp()}",
                episode_body=synthetic_fact,
                source_description="System Generated: Opportunity Summary Update",
                reference_time=datetime.now(),
            )
            logger.info(f"Updated summary for opportunity {opp_uuid}")
        except Exception as e:
            logger.error(f"Failed to update opportunity summary: {e}")
            raise RuntimeError(
                f"Cannot update summary for opportunity {opp_uuid}: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_opportunity(self, opp_uuid: str) -> Optional[Opportunity]:
        """Retrieve an Opportunity node and its summary from the knowledge graph.

        Searches for the opportunity by UUID and returns its details including summary.

        Args:
            opp_uuid: Opportunity UUID from Supabase.

        Returns:
            Opportunity Pydantic object if found, None otherwise.

        Raises:
            ValueError: If opportunity UUID is empty.
            RuntimeError: If search fails.
        """
        if not opp_uuid:
            raise ValueError("Opportunity UUID is required")

        try:
            # Search for opportunity by UUID
            search_query = f"opportunity with ID {opp_uuid}"
            results = await self.search_entities(
                query=search_query,
                node_labels=["Opportunity"],
            )

            # Check if results found
            if not results or not hasattr(results, "nodes") or not results.nodes:
                logger.info(f"No opportunity found with UUID {opp_uuid}")
                return None

            # Extract first matching node
            opportunity_node = results.nodes[0]

            # Build Opportunity Pydantic object from node properties
            opportunity = Opportunity(
                stage=getattr(opportunity_node, "stage", None)
                or getattr(opportunity_node, "status", "prospect"),
                value=getattr(opportunity_node, "value", None),
                close_date=getattr(opportunity_node, "close_date", None),
                source=getattr(opportunity_node, "source", None),
            )

            logger.info(f"Retrieved opportunity {opp_uuid}")
            return opportunity

        except Exception as e:
            logger.error(f"Failed to get opportunity summary: {e}")
            raise RuntimeError(f"Cannot retrieve opportunity {opp_uuid}: {e}") from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_opportunities_by_contact_email(self, email: str) -> list[Opportunity]:
        """Find all opportunities related to a contact by their email address.

        Searches the knowledge graph for a contact with the given email, then
        traverses the POINT_OF_CONTACT relationships to find all related opportunities.

        Args:
            email: Contact email address (e.g., "john@acme.com").

        Returns:
            List of Opportunity Pydantic objects.
            Returns empty list if no contact or opportunities found.

        Raises:
            ValueError: If email is empty.
            RuntimeError: If search fails.
        """
        if not email:
            raise ValueError("Contact email is required")

        # Normalize email
        normalized_email = email.lower()

        try:
            # Search for contact by email
            contact_query = f"contact with email {normalized_email}"
            contact_results = await self.search_entities(
                query=contact_query,
                node_labels=["Contact"],
            )

            # Check if contact exists
            if (
                not contact_results
                or not hasattr(contact_results, "nodes")
                or not contact_results.nodes
            ):
                logger.info(f"No contact found with email {normalized_email}")
                return []

            # Search for opportunities linked to this contact
            opp_query = (
                f"opportunities where {normalized_email} is point of contact "
                f"or involved in business opportunity"
            )
            opp_results = await self.search_entities(
                query=opp_query,
                node_labels=["Opportunity"],
            )

            # Extract opportunities from results and create Pydantic objects
            opportunities: list[Opportunity] = []
            if opp_results and hasattr(opp_results, "nodes") and opp_results.nodes:
                for opp_node in opp_results.nodes:
                    try:
                        # Build Opportunity Pydantic object
                        opportunity = Opportunity(
                            stage=getattr(opp_node, "stage", None)
                            or getattr(opp_node, "status", "prospect"),
                            value=getattr(opp_node, "value", None),
                            close_date=getattr(opp_node, "close_date", None),
                            source=getattr(opp_node, "source", None),
                        )
                        opportunities.append(opportunity)
                    except Exception as parse_error:
                        logger.warning(
                            f"Failed to parse opportunity node: {parse_error}"
                        )
                        continue

            logger.info(
                f"Found {len(opportunities)} opportunity(ies) for contact {normalized_email}"
            )
            return opportunities

        except Exception as e:
            logger.error(
                f"Failed to get opportunities for contact {normalized_email}: {e}"
            )
            raise RuntimeError(
                f"Cannot retrieve opportunities for {normalized_email}: {e}"
            ) from e

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_fact_edges(
        self, query: str, group_id: Optional[str] = None, max_results: int = 10
    ) -> list[EntityEdge]:
        """Retrieve factual statements from the knowledge graph matching a query.

        Uses semantic search to find relevant facts stored in the graph.

        Args:
            query: Search query string (e.g., "What is Betafits?").
            group_id: Optional group identifier to filter facts by episode grouping.

        Returns:
            List of factual statements as strings.

        Raises:
            ValueError: If query is empty.
            RuntimeError: If fact retrieval fails.
        """
        if not query:
            raise ValueError("Fact query cannot be empty")

        # Perform search for facts using Graphiti
        results = await self._graphiti.search(
            query=query,
            group_ids=[group_id] if group_id else None,
            num_results=max_results,
        )

        return results

    @retry_on_exception_async(
        max_attempts=3,
        backoff_factor=2.0,
        max_delay=10.0,
        exceptions=(Exception,),
    )
    async def get_facts(
        self, query: str, group_id: Optional[str] = None, max_results: int = 10
    ) -> str:
        """Retrieve factual statements from the knowledge graph matching a query.

        Uses semantic search to find relevant facts stored in the graph.

        Args:
            query: Search query string (e.g., "What is Betafits?").
            group_id: Optional group identifier to filter facts by episode grouping.

        Returns:
            Factual statements as a single concatenated string.

        Raises:
            ValueError: If query is empty.
            RuntimeError: If fact retrieval fails.
        """
        if not query:
            raise ValueError("Fact query cannot be empty")

        try:
            # Perform search for facts using Graphiti
            results = await self.get_fact_edges(query, group_id, max_results)
            return "-" + "\n- ".join([edge.fact for edge in results])

        except Exception as e:
            logger.error(f"Failed to retrieve facts for query '{query}': {e}")
            raise RuntimeError(f"Cannot get facts for query '{query}': {e}") from e

    async def close(self) -> None:
        """Close the Graphiti connection.

        Should be called during application shutdown to properly close
        the Neo4j driver connection.

        Raises:
            RuntimeError: If closing fails.
        """
        try:
            if self._graphiti:
                await self._graphiti.close()
                logger.info("Graphiti client connection closed")
        except Exception as e:
            logger.error(f"Error closing Graphiti connection: {e}")
            raise RuntimeError(f"Failed to close Graphiti connection: {e}") from e


def get_graphiti_client() -> GraphitiClient:
    """Get the singleton Graphiti client instance.

    Returns:
        GraphitiClient: The global Graphiti client instance.

    Raises:
        RuntimeError: If client initialization fails.
    """
    global _graphiti_client
    if _graphiti_client is None:
        try:
            _graphiti_client = GraphitiClient()

        except Exception as e:
            logger.error(f"Failed to initialize Graphiti client: {e}")
            raise RuntimeError(f"Graphiti client initialization failed: {e}") from e
    return _graphiti_client


def reset_graphiti_client() -> None:
    """Reset the singleton Graphiti client (primarily for testing).

    This closes the existing client and clears the global reference,
    forcing a new instance to be created on next get_graphiti_client() call.

    Used mainly in test fixtures to ensure clean state between tests.
    """
    global _graphiti_client
    if _graphiti_client:
        try:
            # Note: In tests, this would be called synchronously, so we catch any issues
            logger.debug("Resetting Graphiti client")
        except Exception as e:
            logger.warning(f"Error during client reset: {e}")
    _graphiti_client = None
