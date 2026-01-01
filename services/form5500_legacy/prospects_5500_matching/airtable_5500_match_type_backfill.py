import requests
import psycopg2

# Airtable Credentials
AIRTABLE_BASE_ID = "appjvhsxUUz6o0dzo"
AIRTABLE_API_KEY = "pataexwS1dNvKkmVk.b01f01a400ccf38c96e31b35db5974122438c536a8592b50a1564de4c35e67c3"
TABLE_NAME = "5500 Data 2023"
AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_NAME}"
HEADERS = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

# Supabase Database Credentials
DB_USER = "postgres.usjxjbglawbxronycthr"
DB_PASSWORD = "hEzcYRz5ktANIdaX"
DB_HOST = "aws-0-us-west-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

# Connect to Supabase Database
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("Connected to Supabase successfully.")
except Exception as e:
    print("Database connection failed:", str(e))
    exit()

# Function to fetch records from Airtable
def fetch_airtable_records():
    records = []
    offset = None

    while True:
        params = {
            "filterByFormula": "{Match Type} = ''"  # Only fetch records where Match Type is empty
        }
        if offset:
            params["offset"] = offset

        response = requests.get(AIRTABLE_URL, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")  # Get next page offset
            if not offset:
                break  # Exit loop when all pages are fetched
        else:
            print("Failed to fetch data from Airtable:", response.text)
            break

    return records

# Fetch records from Airtable
airtable_records = fetch_airtable_records()
print(f"Fetched {len(airtable_records)} records from Airtable.")

# Process each record
for record in airtable_records:
    ack_id = record["fields"].get("ACK ID 2023")
    record_id = record["id"]

    if ack_id:
        # Query Supabase for a match
        cursor.execute("SELECT TYPE_PENSION_BNFT_CODE FROM f_5500_2023 WHERE ack_id = %s", (ack_id,))
        result = cursor.fetchone()

        if result:
            type_pension_benefit_code = result[0]
            match_type = "Retirement_5500" if type_pension_benefit_code else "H&W"

            # Update the record in Airtable
            update_data = {"fields": {"Match Type": match_type}}
            update_response = requests.patch(f"{AIRTABLE_URL}/{record_id}", headers=HEADERS, json=update_data)

            if update_response.status_code == 200:
                print(f"Updated {record_id} with Match Type: {match_type}")
            else:
                print(f"Failed to update {record_id}: {update_response.text}")

# Close database connection
cursor.close()
conn.close()
print("Database connection closed.")
