"""
Initialize Supabase database tables for email ingestor pipeline.

This script creates all required tables with proper schemas, indexes, and constraints.
Run this once during initial setup or when resetting the database.

Usage:
    python -m scripts.init_database

Tables created:
- received_emails: Inbound emails from Gmail
- received_email_attachments: Email attachments
- drafted_emails: AI-generated drafts awaiting approval
- sent_emails: Confirmed sent emails
- validation_logs: Pipeline execution audit trail
- kg_entities: Cached knowledge graph entities (optional)
- rag_vectors: Vector embeddings for semantic search (pgvector)

Requirements:
- SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables
- pgvector extension enabled in Supabase (for rag_vectors table)
"""

import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv

from lib.integrations.supabase.supabase_client import get_supabase_client

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# SQL migration for all tables
MIGRATION_SQL = """
-- ============================================================================
-- Create CRM schema
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS crm;

-- Set search path to use crm schema by default
SET search_path TO crm, public;

-- ============================================================================
-- Enable required extensions
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Note: pgvector should already be enabled via Supabase dashboard
-- If not enabled, go to: Database → Extensions → Enable pgvector

-- ============================================================================
-- Table: received_emails
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.received_emails (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_id TEXT UNIQUE NOT NULL,
    from_email TEXT NOT NULL,
    to_emails JSONB NOT NULL DEFAULT '[]',
    cc_emails JSONB DEFAULT '[]',
    bcc_emails JSONB DEFAULT '[]',
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    received_by TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    thread_id TEXT,
    labels JSONB DEFAULT '[]',
    is_read BOOLEAN DEFAULT FALSE,
    classification TEXT,
    classification_confidence NUMERIC(3,2) CHECK (classification_confidence >= 0 AND classification_confidence <= 1),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for received_emails
CREATE INDEX IF NOT EXISTS idx_received_emails_email_id ON crm.received_emails(email_id);
CREATE INDEX IF NOT EXISTS idx_received_emails_thread_id ON crm.received_emails(thread_id);
CREATE INDEX IF NOT EXISTS idx_received_emails_received_at ON crm.received_emails(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_received_emails_classification ON crm.received_emails(classification);
CREATE INDEX IF NOT EXISTS idx_received_emails_from_email ON crm.received_emails(from_email);

-- ============================================================================
-- Table: received_email_attachments
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.received_email_attachments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    mime_type TEXT,
    sha256 TEXT,
    metadata JSONB DEFAULT '{}',
    storage_path TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_email FOREIGN KEY (email_id) REFERENCES crm.received_emails(email_id) ON DELETE CASCADE
);

-- Indexes for received_email_attachments
CREATE INDEX IF NOT EXISTS idx_attachments_email_id ON crm.received_email_attachments(email_id);
CREATE INDEX IF NOT EXISTS idx_attachments_filename ON crm.received_email_attachments(filename);

-- ============================================================================
-- Table: drafted_emails
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.drafted_emails (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_id TEXT NOT NULL,
    draft_subject TEXT NOT NULL,
    draft_body TEXT NOT NULL,
    to_emails JSONB NOT NULL,
    cc_emails JSONB DEFAULT '[]',
    bcc_emails JSONB DEFAULT '[]',
    model_used TEXT,
    tokens_used INTEGER CHECK (tokens_used >= 0),
    draft_metadata JSONB DEFAULT '{}',
    confidence NUMERIC(3,2) DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    approval_status TEXT DEFAULT 'pending' CHECK (approval_status IN ('pending', 'approved', 'rejected', 'sent')),
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    rejection_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_original_email FOREIGN KEY (email_id) REFERENCES crm.received_emails(email_id) ON DELETE CASCADE
);

-- Indexes for drafted_emails
CREATE INDEX IF NOT EXISTS idx_drafted_emails_email_id ON crm.drafted_emails(email_id);
CREATE INDEX IF NOT EXISTS idx_drafted_emails_approval_status ON crm.drafted_emails(approval_status);
CREATE INDEX IF NOT EXISTS idx_drafted_emails_created_at ON crm.drafted_emails(created_at DESC);

-- ============================================================================
-- Table: sent_emails
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.sent_emails (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    original_email_id TEXT NOT NULL,
    drafted_email_id UUID,
    sent_subject TEXT NOT NULL,
    sent_body TEXT NOT NULL,
    to_emails JSONB NOT NULL,
    cc_emails JSONB DEFAULT '[]',
    bcc_emails JSONB DEFAULT '[]',
    sent_message_id TEXT,
    sent_status TEXT NOT NULL CHECK (sent_status IN ('sent', 'bounced', 'delivery_failed')),
    sent_at TIMESTAMPTZ NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_original_email_sent FOREIGN KEY (original_email_id) REFERENCES crm.received_emails(email_id) ON DELETE CASCADE,
    CONSTRAINT fk_drafted_email FOREIGN KEY (drafted_email_id) REFERENCES crm.drafted_emails(id) ON DELETE SET NULL
);

-- Indexes for sent_emails
CREATE INDEX IF NOT EXISTS idx_sent_emails_original_email_id ON crm.sent_emails(original_email_id);
CREATE INDEX IF NOT EXISTS idx_sent_emails_sent_message_id ON crm.sent_emails(sent_message_id);
CREATE INDEX IF NOT EXISTS idx_sent_emails_sent_at ON crm.sent_emails(sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_sent_emails_sent_status ON crm.sent_emails(sent_status);

-- ============================================================================
-- Table: validation_logs
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.validation_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_id TEXT NOT NULL,
    pipeline_execution_id TEXT NOT NULL,
    total_duration_seconds NUMERIC(10,3) NOT NULL CHECK (total_duration_seconds >= 0),
    final_status TEXT NOT NULL CHECK (final_status IN ('success', 'partial_success', 'failure')),
    stage_logs JSONB DEFAULT '[]',
    errors JSONB DEFAULT '[]',
    warnings JSONB DEFAULT '[]',
    summary TEXT NOT NULL,
    kg_nodes_created INTEGER DEFAULT 0 CHECK (kg_nodes_created >= 0),
    kg_edges_created INTEGER DEFAULT 0 CHECK (kg_edges_created >= 0),
    rag_vectors_upserted INTEGER DEFAULT 0 CHECK (rag_vectors_upserted >= 0),
    model_config_used JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_email_validation FOREIGN KEY (email_id) REFERENCES crm.received_emails(email_id) ON DELETE CASCADE
);

-- Indexes for validation_logs
CREATE INDEX IF NOT EXISTS idx_validation_logs_email_id ON crm.validation_logs(email_id);
CREATE INDEX IF NOT EXISTS idx_validation_logs_execution_id ON crm.validation_logs(pipeline_execution_id);
CREATE INDEX IF NOT EXISTS idx_validation_logs_final_status ON crm.validation_logs(final_status);
CREATE INDEX IF NOT EXISTS idx_validation_logs_created_at ON crm.validation_logs(created_at DESC);

-- ============================================================================
-- Table: lg_runs (Universal LangGraph Run Tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.lg_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id TEXT UNIQUE NOT NULL,  -- Format: LG-RUN-YYYY-MM-DD-XXXX
    workflow TEXT NOT NULL,        -- e.g., "Email Processing", "Form 5500 Ingestion"
    triggered_by TEXT NOT NULL,    -- e.g., "Gmail API", "CLI", "API Request"
    environment TEXT NOT NULL,     -- e.g., "production", "development", "staging"
    status TEXT NOT NULL CHECK (status IN ('started', 'running', 'completed', 'failed', 'cancelled')),
    step_log JSONB DEFAULT '[]',   -- Array of step executions with timestamps
    error_details JSONB,           -- Error information if failed
    metadata JSONB DEFAULT '{}',   -- Additional context (user, trigger details, etc.)
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for lg_runs
CREATE INDEX IF NOT EXISTS idx_lg_runs_run_id ON crm.lg_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_lg_runs_workflow ON crm.lg_runs(workflow);
CREATE INDEX IF NOT EXISTS idx_lg_runs_status ON crm.lg_runs(status);
CREATE INDEX IF NOT EXISTS idx_lg_runs_triggered_by ON crm.lg_runs(triggered_by);
CREATE INDEX IF NOT EXISTS idx_lg_runs_environment ON crm.lg_runs(environment);
CREATE INDEX IF NOT EXISTS idx_lg_runs_started_at ON crm.lg_runs(started_at DESC);

-- ============================================================================
-- Table: state_snapshots (Universal State Management)
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.state_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    snapshot_id TEXT UNIQUE NOT NULL,        -- Unique identifier for this snapshot
    run_id TEXT NOT NULL,                     -- Links to lg_runs.run_id
    state_envelope JSONB NOT NULL,            -- Universal state envelope with schema_version, workflow_id, actor, phase, payload
    checkpoint_id TEXT,                       -- Optional LangGraph checkpoint ID if using checkpointer
    phase TEXT NOT NULL,                      -- Current workflow phase (e.g., "classify", "extract", "validate")
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_lg_run FOREIGN KEY (run_id) REFERENCES crm.lg_runs(run_id) ON DELETE CASCADE
);

-- Indexes for state_snapshots
CREATE INDEX IF NOT EXISTS idx_state_snapshots_snapshot_id ON crm.state_snapshots(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_state_snapshots_run_id ON crm.state_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_state_snapshots_phase ON crm.state_snapshots(phase);
CREATE INDEX IF NOT EXISTS idx_state_snapshots_created_at ON crm.state_snapshots(created_at DESC);

-- ============================================================================
-- Table: kg_entities (Optional - for caching KG nodes)
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.kg_entities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id TEXT UNIQUE NOT NULL,
    entity_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_data JSONB DEFAULT '{}',
    last_seen_in_email TEXT,
    mention_count INTEGER DEFAULT 0 CHECK (mention_count >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_last_seen_email FOREIGN KEY (last_seen_in_email) REFERENCES crm.received_emails(email_id) ON DELETE SET NULL
);

-- Indexes for kg_entities
CREATE INDEX IF NOT EXISTS idx_kg_entities_entity_id ON crm.kg_entities(entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_entities_entity_type ON crm.kg_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_kg_entities_entity_name ON crm.kg_entities(entity_name);

-- ============================================================================
-- Table: rag_vectors (pgvector for semantic search)
-- ============================================================================
CREATE TABLE IF NOT EXISTS crm.rag_vectors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email_id TEXT NOT NULL,
    chunk_index INTEGER DEFAULT 0 CHECK (chunk_index >= 0),
    content TEXT NOT NULL,
    embedding vector(1536) NOT NULL,
    content_type TEXT DEFAULT 'email_body' CHECK (content_type IN ('email_body', 'email_subject', 'context')),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_email_vector FOREIGN KEY (email_id) REFERENCES crm.received_emails(email_id) ON DELETE CASCADE
);

-- Indexes for rag_vectors
CREATE INDEX IF NOT EXISTS idx_rag_vectors_email_id ON crm.rag_vectors(email_id);
CREATE INDEX IF NOT EXISTS idx_rag_vectors_content_type ON crm.rag_vectors(content_type);
-- Vector similarity index (HNSW for fast approximate nearest neighbor search)
CREATE INDEX IF NOT EXISTS idx_rag_vectors_embedding ON crm.rag_vectors USING hnsw (embedding vector_cosine_ops);

-- ============================================================================
-- Trigger: Update updated_at timestamp automatically
-- ============================================================================
CREATE OR REPLACE FUNCTION crm.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables with updated_at
DROP TRIGGER IF EXISTS update_received_emails_updated_at ON crm.received_emails;
CREATE TRIGGER update_received_emails_updated_at
    BEFORE UPDATE ON crm.received_emails
    FOR EACH ROW
    EXECUTE FUNCTION crm.update_updated_at_column();

DROP TRIGGER IF EXISTS update_drafted_emails_updated_at ON crm.drafted_emails;
CREATE TRIGGER update_drafted_emails_updated_at
    BEFORE UPDATE ON crm.drafted_emails
    FOR EACH ROW
    EXECUTE FUNCTION crm.update_updated_at_column();

DROP TRIGGER IF EXISTS update_kg_entities_updated_at ON crm.kg_entities;
CREATE TRIGGER update_kg_entities_updated_at
    BEFORE UPDATE ON crm.kg_entities
    FOR EACH ROW
    EXECUTE FUNCTION crm.update_updated_at_column();

DROP TRIGGER IF EXISTS update_lg_runs_updated_at ON crm.lg_runs;
CREATE TRIGGER update_lg_runs_updated_at
    BEFORE UPDATE ON crm.lg_runs
    FOR EACH ROW
    EXECUTE FUNCTION crm.update_updated_at_column();
    FOR EACH ROW
    EXECUTE FUNCTION crm.update_updated_at_column();

-- ============================================================================
-- Success message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Database schema initialization completed successfully!';
END $$;
"""


def execute_sql_via_rpc(sql: str) -> dict[str, Any]:
    """Execute raw SQL via Supabase RPC function.

    Note: This requires a custom RPC function in Supabase.
    Alternative: Use direct PostgreSQL connection with psycopg2.

    Args:
        sql: SQL statements to execute.

    Returns:
        Response from RPC call.

    Raises:
        Exception: If SQL execution fails.
    """
    client = get_supabase_client()
    try:
        # Try using Supabase RPC (requires custom function in Supabase)
        response = client._client.rpc("exec_sql", {"query": sql}).execute()
        return response.data if hasattr(response, "data") else {}  # type: ignore
    except Exception as e:
        logger.error(f"RPC execution failed: {e}")
        raise


def execute_sql_via_psycopg2(sql: str) -> None:
    """Execute raw SQL via direct PostgreSQL connection.

    Requires psycopg2 library and direct database credentials.

    Args:
        sql: SQL statements to execute.

    Raises:
        ImportError: If psycopg2 is not installed.
        Exception: If SQL execution fails.
    """
    try:
        import psycopg2
    except ImportError:
        logger.error(
            "psycopg2 not installed. Install with: pip install psycopg2-binary"
        )
        raise

    from lib.config import get_settings

    settings = get_settings()

    # Parse Supabase URL to get database connection details
    # Supabase URL format: https://<project-ref>.supabase.co
    # Database host: db.<project-ref>.supabase.co
    supabase_url = settings.supabase_url
    if not supabase_url:
        raise ValueError("SUPABASE_URL not configured")

    # Extract project reference from URL
    import re

    match = re.search(r"https://([^.]+)\.supabase\.co", supabase_url)
    if not match:
        raise ValueError(f"Invalid Supabase URL format: {supabase_url}")

    project_ref = match.group(1)
    db_host = f"db.{project_ref}.supabase.co"

    # Get database password from environment
    import os

    db_password = os.getenv("SUPABASE_DB_PASSWORD")
    if not db_password:
        raise ValueError(
            "SUPABASE_DB_PASSWORD environment variable required for direct database connection"
        )

    # Connect to database
    logger.info(f"Connecting to database: {db_host}")
    conn = psycopg2.connect(
        host=db_host,
        port=5432,
        database="postgres",
        user="postgres",
        password=db_password,
    )

    try:
        with conn.cursor() as cursor:
            logger.info("Executing migration SQL...")
            cursor.execute(sql)
            conn.commit()
            logger.info("Migration completed successfully!")
    finally:
        conn.close()


def main() -> None:
    """Initialize database tables."""
    logger.info("=" * 80)
    logger.info("Betafits Email Ingestor - Database Initialization")
    logger.info("=" * 80)

    try:
        # Try psycopg2 method (recommended for complex migrations)
        logger.info("Attempting direct PostgreSQL connection...")
        execute_sql_via_psycopg2(MIGRATION_SQL)

    except ImportError:
        logger.warning(
            "psycopg2 not installed. Install with: pip install psycopg2-binary"
        )
        logger.info("Falling back to RPC method (requires custom Supabase function)...")

        try:
            execute_sql_via_rpc(MIGRATION_SQL)
        except Exception as e:
            logger.error(f"RPC method failed: {e}")
            logger.error(
                "\nTo run migrations, you need either:\n"
                "1. Install psycopg2: pip install psycopg2-binary\n"
                "   Set SUPABASE_DB_PASSWORD environment variable\n"
                "2. Create a custom RPC function in Supabase:\n"
                "   CREATE OR REPLACE FUNCTION exec_sql(query text)\n"
                "   RETURNS void AS $$\n"
                "   BEGIN\n"
                "       EXECUTE query;\n"
                "   END;\n"
                "   $$ LANGUAGE plpgsql SECURITY DEFINER;"
            )
            sys.exit(1)

    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        sys.exit(1)

    logger.info("=" * 80)
    logger.info("Database initialization completed successfully!")
    logger.info("=" * 80)
    logger.info("\nTables created:")
    logger.info("  - received_emails")
    logger.info("  - received_email_attachments")
    logger.info("  - drafted_emails")
    logger.info("  - sent_emails")
    logger.info("  - validation_logs")
    logger.info("  - kg_entities (optional cache)")
    logger.info("  - rag_vectors (pgvector)")
    logger.info("\nNext steps:")
    logger.info("  1. Verify tables in Supabase dashboard")
    logger.info("  2. Test with: python -m apps.test_trigger.main")


if __name__ == "__main__":
    main()
