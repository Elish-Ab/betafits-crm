# Airtable Commissions Schema Export

This project provides a Python script that exports the full schema of the Airtable **Commissions** base using the Airtable Meta API.  
The script produces a normalized CSV containing tables, fields, field types, linked-record references, formulas (with field names), and selectable option lists.  
This is useful for documentation, migrations, auditing, or tracking structural changes to the Airtable base over time.

## File Overview

### `airtable_commissions_schema_export.py`
Fetches the Airtable schema and generates a flattened CSV export.  
The script:

1. Calls the Airtable Meta API to retrieve the base schema.  
2. Creates lookup maps for table IDs and field IDs.  
3. Expands linked-record references and choice lists into readable columns.  
4. Converts formula fields so they reference field names instead of field IDs.  
5. Outputs a single CSV containing the full enriched schema.

## Requirements

- Python 3.9+
- requests
- pandas
