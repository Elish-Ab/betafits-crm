# Form 5500 Matching Utilities
This project contains a set of Python scripts that match Airtable data to Supabase tables containing Form 5500 information (main filings and Schedule SF).
The scripts fill in ACK IDs, EINs, entity names, and “Match Type” values such as Retirement_5500, Retirement_SF, and H&W.

## What this project does
Reads records from Airtable:
- Prospects
- 5500 Data 2023
Looks up matching records in Supabase tables:
- Main Form 5500 data (e.g. f_5500_2023)
- Schedule SF data (e.g. f_5500_sf_2023)
Updates Airtable with:
- ACK ID 2023
- Entity Name
- DBA
- EIN
- Match Type
Uses a consistent matching strategy:
- Match by EIN (if available)
- Match by normalized entity name
- If multiple candidates remain, optionally ask the user to choose

## Files in this repository
1. prospects_main_5500_matcher.py
Matches Airtable Prospects to the main 2023 Form 5500 table.
Input:
- Airtable table: Prospects
- Supabase table: main Form 5500 data (e.g. f_5500_2023)
Logic:
- Skips records that already have ACK ID 2023 or are marked No Match Found in Main 5500
- Tries to match each prospect by:
  - EIN
  - Normalized entity name
  - Company name with optional manual selection if there are multiple matches
Output to Airtable:
- ACK ID 2023
- Entity Name
- DBA
- EIN
- Clears No Match Found in Main 5500 when a match is found
- Sets No Match Found in Main 5500 when no match is found

2. prospects_main_5500_matcher_with_type.py
Second-pass matcher for Prospects that focuses on records with name issues and sets Match Type.
Input:
- Airtable table: Prospects
- Supabase table: main Form 5500 data (e.g. f_5500_2023)
Logic:
- Primarily processes records flagged as name mismatches (e.g. Name Mismatch)
Matching order:
- EIN
- Normalized entity name
- Company name with optional manual selection
For each selected Supabase record, determines Match Type:
- H&W if welfare benefit code is present
- Retirement_5500 (or similar variant) if pension/retirement code is present
Output to Airtable:
- ACK ID 2023
- Entity Name
- DBA
- EIN
- Match Type as a list (e.g. ["H&W"], ["Retirement_5500"], or a combination)
- Match Basis (e.g. EIN / entity name / manual selection)
- Clears No Match Found in Main 5500 when a match is found
- Sets No Match Found in Main 5500 when no match is found

3. prospects_sf_2023_matcher.py
Matches Prospects to Schedule SF data and labels those matches as Retirement_SF.
Input:
- Airtable table: Prospects
- Supabase table: Schedule SF data (e.g. f_5500_sf_2023)
Logic:
- Typically processes prospects that may have SF-related matches (e.g. name mismatch / special cases)
Matching order:
- EIN
- Normalized entity name
- Company name with optional manual selection
Output to Airtable:
- ACK ID 2023
- Entity Name
- DBA
- EIN
- Match Type set to include Retirement_SF
- Match Basis
- Clears No Match Found in SF when a match is found
- Sets No Match Found in SF when no match is found

4. airtable_5500_match_type_backfill.py
Backfills Match Type for 5500 Data 2023 records that already have an ACK ID.
Input:
- Airtable table: 5500 Data 2023
- Supabase table: main Form 5500 data (e.g. f_5500_2023)
Logic:
- Selects Airtable records where Match Type is empty or missing
- Uses ACK ID 2023 to look up the corresponding record in Supabase
- Reads benefit-type fields in Supabase (e.g. pension vs welfare)
- Sets Match Type based on those codes:
  - Retirement_5500 when pension/retirement codes exist
  - H&W when only welfare codes exist
Output to Airtable:
- Updated Match Type on 5500 Data 2023 rows via Airtable REST API

## Typical workflow
Run the scripts in this order for a full refresh:
1. Match Prospects to main 5500 data
```bash
python prospects_main_5500_matcher.py
```
Populates base fields (ACK ID 2023, Entity Name, DBA, EIN)
Marks clear non-matches in Prospects
2. Handle difficult prospects and set Match Type
```bash
python prospects_main_5500_matcher_with_type.py
```
Focuses on Prospects with unresolved or mismatched names
Assigns Match Type (e.g. H&W, Retirement_5500)
3. Match to Schedule SF
```bash
python prospects_sf_2023_matcher.py
```
Adds Retirement_SF matches for Prospects where applicable
4. Backfill Match Type on 5500 Data 2023
```bash
python airtable_5500_match_type_backfill.py
```
Ensures all relevant 5500 Data 2023 rows have a Match Type

## Requirements and configuration
Python
- Python 3.x
- Typical dependencies (exact list may vary slightly by script):
  - requests
  - psycopg2 or another PostgreSQL client (for Supabase, if used directly)
  - python-dotenv (if environment variables are loaded from a .env file)
  - Any Airtable client library used in the scripts (or direct HTTP via requests)
- Install dependencies (example):
```bash
pip install -r requirements.txt
```
(if you don’t have a requirements.txt, install the libraries used in the imports of each script.)

Environment variables
- The scripts expect credentials and connection details in environment variables (often via .env).
- Typical values you will need (names may differ based on your code):
  - Airtable:
    - AIRTABLE_API_KEY
    - AIRTABLE_BASE_ID
  - Supabase / database:
    - SUPABASE_URL and SUPABASE_KEY
    - or DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
Create a .env file (example):
```
AIRTABLE_API_KEY=your_airtable_api_key
AIRTABLE_BASE_ID=your_airtable_base_id

SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_service_key
# or direct DB credentials if used:
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```
Then load it in your shell or rely on python-dotenv inside the scripts.

## How to run safely
- Test on a small subset of records first (e.g. filter in Airtable or DB, or modify queries in code if needed).
- Consider duplicating Airtable tables (or using a test base) before running bulk updates.
- When prompted for manual match selection, choose carefully: your choice will be written back to Airtable and may be reused on future runs.

## Summary
Use the prospects_* scripts to match and classify Prospects.
Use airtable_5500_match_type_backfill.py to complete Match Type values in 5500 Data 2023.
All scripts coordinate Airtable and Supabase data to keep Form 5500–related records consistent and enriched with ACK IDs, EINs, and clear benefit-type classifications.

