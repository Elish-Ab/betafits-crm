# Scripts

Database migrations, backfills, and administrative tasks for the email ingestor pipeline.

## Database Initialization

### Prerequisites

1. **Install psycopg2** (recommended):
   ```bash
   pip install psycopg2-binary
   ```

2. **Get your Supabase database password**:
   - Go to Supabase Dashboard → Project Settings → Database
   - Copy the database password
   - Add to `.env`:
     ```
     SUPABASE_DB_PASSWORD=your_database_password_here
     ```

### Running the Migration

```bash
python -m scripts.init_database
```

This will create all required tables:
- `received_emails` - Inbound emails from Gmail
- `received_email_attachments` - Email attachments
- `drafted_emails` - AI-generated drafts awaiting approval
- `sent_emails` - Confirmed sent emails
- `validation_logs` - Pipeline execution audit trail
- `kg_entities` - Cached knowledge graph entities (optional)
- `rag_vectors` - Vector embeddings for semantic search (pgvector)

### What It Does

1. Enables required PostgreSQL extensions (`uuid-ossp`, `pgvector`)
2. Creates all tables with proper schemas
3. Sets up indexes for query performance
4. Creates foreign key constraints for data integrity
5. Adds triggers for automatic `updated_at` timestamps
6. Creates vector similarity index for semantic search

### Verification

After running the migration:

1. Check Supabase Dashboard → Table Editor
2. Verify all 7 tables are created
3. Check that indexes are present
4. Test with sample data insertion

### Troubleshooting

**Error: psycopg2 not installed**
```bash
pip install psycopg2-binary
```

**Error: SUPABASE_DB_PASSWORD not set**
- Get password from Supabase Dashboard → Project Settings → Database
- Add to `.env` file

**Error: Connection refused**
- Verify Supabase project is active
- Check network/firewall settings
- Ensure database URL is correct

### Alternative: Manual SQL Execution

If the script fails, you can manually execute the SQL in Supabase:

1. Open Supabase Dashboard → SQL Editor
2. Copy the SQL from `scripts/init_database.py` (the `MIGRATION_SQL` variable)
3. Paste and run in SQL Editor

## Future Scripts

Add additional scripts here for:
- Data backfills
- Database cleanup
- Performance optimization
- Data exports/imports
