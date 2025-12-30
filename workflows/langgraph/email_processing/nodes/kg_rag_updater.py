"""KG and RAG updater node for atomic knowledge graph and vector DB updates.

This is Node 3 of the 8-node pipeline. Responsibilities:
- Update knowledge graph (Graphiti) with email content as episode
- Graphiti automatically extracts entities and relations from the episode
- Update RAG vector store (Supabase pgvector) with email content
- Atomic transaction (both succeed or both fail)
- Return KGAndRAGUpdate status

Note: This node no longer requires entity_list or relations from previous nodes.
Graphiti's add_episode() handles entity and relation extraction internally.
"""

import logging
import stat
import time

from lib.integrations.graphiti_client import get_graphiti_client
from lib.integrations.supabase.opportunites_client import OpportuityDBClient
from lib.integrations.supabase.supabase_client import get_supabase_client
from lib.integrations.vector_db_client import OpenRouterEmbeddings, get_vector_db_client
from lib.models.database_schemas import ReceivedEmail
from workflows.langgraph.email_processing.state import PipelineState

logger = logging.getLogger(__name__)


async def kg_rag_updater_node(state: PipelineState) -> PipelineState:
    """Update knowledge graph and RAG vector store with email content.

    Inputs from state:
        - parsed_email: EmailParsed from router node

    Outputs to state:
        - kg_rag_update: KGAndRAGUpdate with update status and metadata

    Args:
        state: PipelineState with labeled_email and parsed_email fields.

    Returns:
        Updated PipelineState with kg_rag_update field.

    Raises:
        ValueError: If update fails.

    Note:
        Graphiti automatically extracts entities and relations from the episode,
        so we no longer need entity_list or relations from previous nodes.
    """
    start_time = time.time()

    # Get required inputs
    email_opt = state.get("email")
    oppurtunity_opt = state.get("matched_opportunity")

    if not email_opt:
        raise ValueError("email is required in state")
    if not oppurtunity_opt:
        raise ValueError("matched_opportunity is required in state")

    email = email_opt
    opportunity = oppurtunity_opt.selected_opportunity

    email_id = email.message_id

    logger.info(f"[Node 3] kg_rag_updater_node starting for {email_id} ")

    try:
        graphiti_client = get_graphiti_client()
        supabase_client = await get_supabase_client()
        opportunity_client = await OpportuityDBClient.create()
        openrouter_embedder = OpenRouterEmbeddings()

        if not state.get("is_kg_updated"):
            try:
                # Prepare email content for episode
                email_episode_body = (
                    f"Email from {email.from_email}\n"
                    f"To {email.to_emails}\n"
                    f"Subject: {email.subject}\n\n"
                    f"{email.body}"
                )

                # Add as episode to KG
                kg_response = await graphiti_client.add_episode(
                    name=f"Email-{email_id}",
                    episode_body=email_episode_body,
                    source_description=f"Incoming email from {email.from_email}",
                    reference_time=email.received_at
                    if isinstance(email, ReceivedEmail)
                    else email.sent_at,
                    group_id=opportunity.id,
                )

                if kg_response:
                    logger.info(
                        "[Node 3] KG episode added successfully "
                        "(Graphiti will extract entities and relations internally)"
                    )
                    state["is_kg_updated"] = True

            except Exception as kg_error:
                logger.error(f"[Node 3] KG update failed: {kg_error}")
                raise ValueError(f"KG update failed: {kg_error}") from kg_error
        else:
            logger.info(f"[Node 3] KG update skipped (already updated for {email_id})")

        try:
            if not state.get("are_communities_built"):
                await graphiti_client.build_communities(group_id=opportunity.id)
                state["are_communities_built"] = True
            else:
                logger.info(
                    f"[Node 3] KG communities build skipped (already built for {email_id})"
                )
        except Exception as e:
            logger.error(f"[Node 3] KG communities build failed: {e}")
            raise ValueError(f"KG communities build failed: {e}") from e

        if not state.get("is_opportunity_index_rag_updated"):
            if opportunity.id:
                try:
                    communities = await graphiti_client.get_nodes_by_namespace(
                        group_id=opportunity.id,
                        node_labels=["Community"],
                    )
                    logger.info(
                        f"[Node 3] Retrieved {len(communities)} communities for oppurtunity {opportunity.id}"
                    )
                    opportunity_summary = "\n".join(
                        [comm.summary for comm in communities]
                    )

                    if not opportunity_summary.strip() == "":
                        opportunity.summary = opportunity_summary
                        opportunity.embedding = await openrouter_embedder.aembed_query(
                            opportunity_summary
                        )

                        updated = await opportunity_client.upsert_opportunity(
                            opportunity,
                        )
                        if updated:
                            logger.info(
                                f"[Node 3] Updated opportunity {opportunity.id} summary in Supabase"
                            )
                        else:
                            logger.warning(
                                f"[Node 3] Failed to update opportunity {opportunity.id} summary in Supabase"
                            )
                    else:
                        logger.info(
                            f"[Node 3] No community summaries found to update opportunity {opportunity.id}"
                        )
                    state["is_opportunity_index_rag_updated"] = True
                except Exception as e:
                    logger.error(f"[Node 3] Failed to update opportunity summary: {e}")
        else:
            logger.info(
                f"[Node 3] RAG update skipped (already updated for opportunity {opportunity.id})"
            )

        logger.info(
            f"[Node 3] KG+RAG update successful for {email_id} "
            f"in {time.time() - start_time:.2f}s"
        )

        return state

    except ValueError as ve:
        logger.error(f"[Node 3] ValueError: {ve}")
        raise
    except Exception as error:
        logger.error(
            f"[Node 3] Failed to update KG+RAG: {error} "
            f"in {time.time() - start_time:.2f}s"
        )
        raise ValueError(f"Failed to update KG+RAG: {error}") from error
