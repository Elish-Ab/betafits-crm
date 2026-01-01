# Audit Reports Sync Pipeline

This project provides a streamlined workflow for importing 2023 audit report data into Supabase/Postgres and synchronizing missing document URLs into Airtable. It ensures consistent and reliable audit metadata across systems through two coordinated Python scripts.

## Project Structure

```
audit_reports_sync_pipeline/
    upload_audit_report_csv_to_supabase.py
    update_airtable_audit_report_urls.py
    README.md
```

## Overview of Scripts

### upload_audit_report_csv_to_supabase.py
Loads the pipe-delimited CSV file (`2023_audit.csv`) into the `audit_reports_2023` table in Supabase/Postgres.
- Connects to the database
- Creates the table if missing
- Reads and processes the CSV with pandas
- Inserts rows, skipping duplicates

### update_airtable_audit_report_urls.py
Backfills missing Airtable URL fields using corresponding entries in the Supabase table.
- Connects to Airtable
- Fetches records missing “Filing Document URL”
- Matches via `ACK ID 2023`
- Updates URL fields in Airtable

## Workflow

1. Place `2023_audit.csv` in the project folder.
2. Load data into Supabase:
   ```bash
   python upload_audit_report_csv_to_supabase.py
   ```
3. Synchronize Airtable URLs:
   ```bash
   python update_airtable_audit_report_urls.py
   ```
4. Confirm data is fully populated in both systems.

## Environment Variables

### Supabase / Postgres
- DB_HOST
- DB_PORT
- DB_NAME
- DB_USER
- DB_PASSWORD

### Airtable
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID
- AIRTABLE_TABLE_NAME

## Dependencies

Install required packages:
```
pip install psycopg2-binary pandas python-dotenv airtable-python-wrapper tqdm
```

## Database Table Schema

| Column         | Type              | Description                   |
|----------------|-------------------|-------------------------------|
| id             | TEXT PRIMARY KEY  | Unique report identifier      |
| form_type      | TEXT              | Filing form type              |
| filing_year    | INTEGER           | Filing year                   |
| sponsor_name   | TEXT              | Sponsor name                  |
| ein            | TEXT              | Employer Identification No.   |
| plan_number    | TEXT              | Plan number                   |
| link           | TEXT              | Audit report URL              |
| facsimile_link | TEXT              | Filing document URL           |

## Error Handling & Safeguards

- Duplicate rows in the CSV do not overwrite existing data
- Only empty Airtable URL fields are updated
- Parameterized SQL queries
- Missing Supabase matches are skipped safely

## Notes for Maintainers

- Update scripts if CSV format or Airtable schema changes
- Extend for new years by duplicating scripts and updating names
- Consider adding logging, retries, tests, and Dockerization

## License
Internal use only.
