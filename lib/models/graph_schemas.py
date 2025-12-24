"""
Knowledge Graph (KG) entity and edge type definitions.

These Pydantic models represent nodes and edges in the Neo4j Graphiti knowledge graph.
All entities and relationships are strictly typed and validated.

For HIPAA compliance:
- No medical entities are extracted or stored
- Medical terms in emails are filtered during entity extraction
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

# ============================================================================
# Type Enums (for Knowledge Graph nodes and edges)
# ============================================================================


class EntityType(str, Enum):
    """Enum of allowed entity types in the knowledge graph."""

    COMPANY = "Company"
    CONTACT = "Contact"
    OPPORTUNITY = "Opportunity"
    PRODUCT = "Product"


class EdgeType(str, Enum):
    """Enum of allowed relationship types in the knowledge graph.

    Relationship Model:
    - Contact WORKS_AT Company: Contact is employed at/affiliated with Company
    - Contact BRINGS_OPPORTUNITY Opportunity: Contact is the person who brought/is relevant to this deal
    - Company HAS_OPPORTUNITY Opportunity: Company is the customer/provider of the opportunity
    - Opportunity ABOUT_PRODUCT Product: The opportunity is related to/selling this Betafits product
    """

    WORKS_AT = "WorksAt"
    BRINGS_OPPORTUNITY = "BringsOpportunity"
    HAS_OPPORTUNITY = "HasOpportunity"
    ABOUT_PRODUCT = "AboutProduct"


# ============================================================================
# Entity Types (KG Nodes)
# ============================================================================


class Company(BaseModel):
    """Company/Organization entity - The customer or partner organization.

    Represents a business entity that is either:
    1. The potential customer for an Opportunity (HAS_OPPORTUNITY relationship)
    2. An employer where a Contact works (Contact WORKS_AT Company)

    For every Opportunity, there should be a Company that has/provides that opportunity.
    The Company is extracted from email domain analysis and sender information.

    Note: The 'name' field is protected and handled automatically by Graphiti.
    additional_metadata is serialized as JSON string for Neo4j compatibility.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    website: Optional[str] = Field(default=None, description="Website URL")
    address: Optional[str] = Field(default=None, description="Address (Scraped)")
    city: Optional[str] = Field(default=None, description="City")
    state: Optional[str] = Field(default=None, description="State")
    zip_code: Optional[str] = Field(default=None, description="Zip")
    country: Optional[str] = Field(default=None, description="Country")
    email_domain: Optional[str] = Field(default=None, description="Email domain (All)")
    phone_number: Optional[str] = Field(
        default=None, description="Phone Number (Scraped)"
    )



class Contact(BaseModel):
    """Contact/Person entity - A human stakeholder in the sales process.

    Represents an individual who is either:
    1. The person who brought/introduced an Opportunity (BRINGS_OPPORTUNITY relationship)
    2. An employee at a Company (WORKS_AT relationship)

    The Contact is critical for email disambiguation - it's the sender/recipient of the
    email and serves as the connection point between the email and the sales opportunity.

    Note: The 'name' field is protected and handled automatically by Graphiti.
    additional_metadata is serialized as JSON string for Neo4j compatibility.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    email: str = Field(..., description="Email address - Used as unique identifier")
    position: Optional[str] = Field(default=None, description="Position/Job Title")
    role: Optional[str] = Field(
        default=None, description="Role (e.g., decision maker, influencer, procurement)"
    )


class Opportunity(BaseModel):
    """Sales Opportunity - THE CENTRAL ANCHOR NODE for every email in the pipeline.

    CRITICAL: Every email processed through this pipeline MUST be associated with
    an Opportunity node. The Opportunity is the single source of truth that anchors
    all other entities (Contact, Company, Product) in the knowledge graph.

    Design Pattern:
    - Every incoming email refers to either an EXISTING opportunity or creates a NEW one
    - The Opportunity is the star-schema center that all other nodes revolve around
    - Contacts BRING this opportunity (BringsOpportunity relationship)
    - Companies HAVE this opportunity (HasOpportunity relationship)
    - Products are discussed in context of this opportunity (AboutProduct relationship)

    This entity MUST be extracted from every email. If no explicit opportunity exists,
    the LLM should infer one from the email context (e.g., "budget inquiry" → opportunity
    for consulting engagement, "demo request" → opportunity for product sale).

    Note: The 'name' and 'summary' fields are protected and handled automatically by Graphiti.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    stage: str = Field(
        ...,
        description="Sales stage: prospect, qualified, proposal, negotiation, won, lost",
    )
    value: Optional[float] = Field(
        default=None, description="Opportunity value in USD", ge=0
    )
    close_date: Optional[datetime] = Field(
        default=None, description="Expected close date"
    )
    source: Optional[str] = Field(
        default=None,
        description="Opportunity source: email, call, referral, website, etc.",
    )


class Product(BaseModel):
    """Betafits Product/Service entity - What is being sold in the opportunity.

    Represents a Betafits product or service that is discussed/sold in the context
    of an Opportunity. Every Opportunity should link to at least one Product via
    the ABOUT_PRODUCT relationship.

    Examples:
    - PEO Platform
    - Benefits Consulting
    - HR Outsourcing
    - Payroll Services
    - Employee Wellness Program

    Note: The 'name' field is protected and handled automatically by Graphiti.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    category: Optional[str] = Field(
        default=None,
        description="Product category: PEO, Consulting, Benefits, Payroll, etc.",
    )
    description: Optional[str] = Field(default=None, description="Product description")


# ============================================================================
# Edge Types (KG Relationships)
# ============================================================================


class WorksAt(BaseModel):
    """Contact works at Company relationship.

    Represents employment or affiliation between a contact and a company.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    start_date: Optional[datetime] = Field(default=None, description="Start date")
    end_date: Optional[datetime] = Field(
        default=None, description="End date (null = current)"
    )
    title_at_time: Optional[str] = Field(
        default=None, description="Title when worked there"
    )


class BringsOpportunity(BaseModel):
    """Contact brings/introduces an Opportunity into the sales pipeline.

    Represents that a specific contact is the person who brought or is relevant
    to an opportunity. This is the KEY relationship for email disambiguation.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    identified_date: datetime = Field(
        default_factory=datetime.utcnow, description="When contact brought opportunity"
    )
    is_primary_contact: bool = Field(
        default=True, description="Is this the primary contact for the opportunity?"
    )
    context: Optional[str] = Field(
        default=None, description="How/where this opportunity was identified"
    )


class HasOpportunity(BaseModel):
    """Company has/provides an Opportunity.

    Represents that a company is the potential customer or organization involved
    in this sales opportunity. The company is the one who receives/benefits from the service.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    identified_date: datetime = Field(
        default_factory=datetime.utcnow, description="When opportunity was identified"
    )
    source: Optional[str] = Field(
        default=None, description="How was this company linked to the opportunity"
    )


class AboutProduct(BaseModel):
    """Opportunity is about a Betafits Product.

    Represents that this opportunity involves the sale or discussion of a specific
    Betafits product or service.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    product_stage: Optional[str] = Field(
        default=None,
        description="Stage of product discussion: discovery, demo, proposal, negotiation",
    )
    discovered_date: datetime = Field(
        default_factory=datetime.utcnow,
        description="When product was mentioned in opportunity",
    )


# ============================================================================
# KG Configuration Maps
# ============================================================================


# Mapping of allowed node types
ENTITY_TYPE_MAP = {
    EntityType.COMPANY: Company,
    EntityType.CONTACT: Contact,
    EntityType.OPPORTUNITY: Opportunity,
    EntityType.PRODUCT: Product,
}

# Mapping of allowed edge types
EDGE_TYPE_MAP = {
    EdgeType.WORKS_AT: WorksAt,
    EdgeType.BRINGS_OPPORTUNITY: BringsOpportunity,
    EdgeType.HAS_OPPORTUNITY: HasOpportunity,
    EdgeType.ABOUT_PRODUCT: AboutProduct,
}

# Allowed connections: (source_type, target_type) -> [edge_types]
# Simplified star schema:
# - Contact WORKS_AT Company: Employment relationship
# - Contact BRINGS_OPPORTUNITY Opportunity: Contact introduces/is relevant to deal (KEY RELATION)
# - Company HAS_OPPORTUNITY Opportunity: Company is the potential customer
# - Opportunity ABOUT_PRODUCT Product: The opportunity is about selling a product
ALLOWED_EDGES = {
    (EntityType.CONTACT, EntityType.COMPANY): [EdgeType.WORKS_AT],
    (EntityType.CONTACT, EntityType.OPPORTUNITY): [EdgeType.BRINGS_OPPORTUNITY],
    (EntityType.COMPANY, EntityType.OPPORTUNITY): [EdgeType.HAS_OPPORTUNITY],
    (EntityType.OPPORTUNITY, EntityType.PRODUCT): [EdgeType.ABOUT_PRODUCT],
}

# HIPAA filtering: Medical entity types to exclude from extraction
HIPAA_EXCLUDED_ENTITY_TYPES = {
    "MedicalCondition",
    "MedicationName",
    "Diagnosis",
    "Treatment",
    "Symptom",
    "HealthcareProvider",
    "PharmaceuticalProduct",
}

# Medical keywords to filter during extraction
HIPAA_EXCLUDED_KEYWORDS = {
    "diabetes",
    "hypertension",
    "cancer",
    "COVID",
    "symptom",
    "disease",
    "medication",
    "prescription",
    "diagnosis",
    "treatment",
    "therapy",
    "surgery",
    "patient",
    "clinical",
    "hospital",
    "pharmaceutical",
}
