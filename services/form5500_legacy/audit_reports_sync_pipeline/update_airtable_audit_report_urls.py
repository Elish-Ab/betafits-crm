import requests
import psycopg2

# Airtable Credentials
AIRTABLE_BASE_ID = "appjvhsxUUz6o0dzo"
AIRTABLE_API_KEY = "pataexwS1dNvKkmVk.b01f01a400ccf38c96e31b35db5974122438c536a8592b50a1564de4c35e67c3"
TABLE_NAME = "Audit Reports 2023"
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_NAME}"

# Supabase Database Credentials
DB_USER = "postgres.usjxjbglawbxronycthr"
DB_PASSWORD = "hEzcYRz5ktANIdaX"
DB_HOST = "aws-0-us-west-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

# Fetch records from Airtable
headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
params = {"filterByFormula": "{Filing Document URL} = ''"}
response = requests.get(AIRTABLE_URL, headers=headers, params=params)
data = response.json()
records = data.get("records", [])

# Connect to Supabase Database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

for record in records:
    ack_id = record["fields"].get("ACK ID 2023")
    record_id = record["id"]

    if ack_id:
        cursor.execute("SELECT link, facsimile_link FROM audit_reports_2023 WHERE id = %s", (ack_id,))
        result = cursor.fetchone()

        if result:
            audit_report_url, filing_document_url = result

            update_data = {
                "fields": {
                    "Audit Report URL": audit_report_url,
                    "Filing Document URL": filing_document_url
                }
            }

            update_response = requests.patch(f"{AIRTABLE_URL}/{record_id}", headers=headers, json=update_data)
            if update_response.status_code == 200:
                print(f"Updated record {record_id} successfully")
            else:
                print(f"Failed to update record {record_id}")

# Close database connection
cursor.close()
conn.close()
