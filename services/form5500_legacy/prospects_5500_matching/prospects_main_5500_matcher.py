import re
import time
import psycopg2
from psycopg2 import OperationalError
from airtable import Airtable

# Airtable Credentials
AIRTABLE_BASE_ID = "appjvhsxUUz6o0dzo"
AIRTABLE_API_KEY = "pataexwS1dNvKkmVk.b01f01a400ccf38c96e31b35db5974122438c536a8592b50a1564de4c35e67c3"
TABLE_NAME = "Prospects"

airtable = Airtable(AIRTABLE_BASE_ID, TABLE_NAME, AIRTABLE_API_KEY)

# Supabase Database Credentials
DB_USER = "postgres.usjxjbglawbxronycthr"
DB_PASSWORD = "hEzcYRz5ktANIdaX"
DB_HOST = "aws-0-us-west-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"


# Function to establish database connection
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10  # Increase timeout
        )
        return conn, conn.cursor()
    except OperationalError as e:
        print("Database connection failed:", e)
        return None, None


# Initial connection
conn, cursor = connect_to_db()


# Function to clean and normalize strings
def clean_string(value):
    """Remove punctuation (commas, full stops) and normalize spaces."""
    return re.sub(r'[,.]', '', value).strip().lower() if value else ""


# Function to normalize EIN by removing dashes
def normalize_ein(ein):
    return ein.replace('-', '').strip() if ein else ""


# Function to reconnect if connection is lost
def ensure_connection():
    global conn, cursor
    if conn is None or conn.closed != 0:
        print("Reconnecting to the database...")
        conn, cursor = connect_to_db()


# Fetch a record from Supabase by EIN
def fetch_supabase_record_by_ein(ein):
    ensure_connection()
    cursor.execute("""
        SELECT ack_id, sponsor_dfe_name, spons_dfe_dba_name, spons_dfe_ein 
        FROM f_5500_2023 
        WHERE spons_dfe_ein = %s 
        LIMIT 1
    """, (ein,))
    return cursor.fetchone()


# Fetch records from Supabase by sponsor or DBA name
def fetch_supabase_records_by_name(name):
    ensure_connection()
    cursor.execute("""
        SELECT ack_id, sponsor_dfe_name, spons_dfe_dba_name, spons_dfe_ein, spons_dfe_mail_us_city, 
               spons_dfe_loc_us_city, business_code, type_pension_bnft_code, type_welfare_bnft_code 
        FROM f_5500_2023 
        WHERE sponsor_dfe_name ILIKE %s OR spons_dfe_dba_name ILIKE %s
    """, (f"%{name}%", f"%{name}%"))
    return cursor.fetchall()


# Function to process and match prospects
def process_prospects():
    """Process each record in Airtable and match against Supabase."""
    prospects = airtable.get_all()

    for record in prospects:
        fields = record.get("fields", {})
        record_id = record["id"]

        # Skip records where "ACK ID 2023" is not empty
        if fields.get("ACK ID 2023") or fields.get("No Match Found in Main 5500"):
            continue

        company_name = fields.get("Company Name", "").strip()
        ein = normalize_ein(fields.get("EIN", ""))  # Normalize EIN from Prospects
        entity_name = clean_string(fields.get("Entity Name", ""))

        matched_record = None
        match_basis = ""

        # Match by EIN
        if ein:
            matched_record = fetch_supabase_record_by_ein(ein)
            if matched_record:
                match_basis = "EIN"
            else:
                print(f"No match found for {company_name} based on EIN. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Match by Entity Name (if EIN not found)
        if not matched_record and entity_name:
            possible_matches = fetch_supabase_records_by_name(entity_name)
            matched_record = next((rec for rec in possible_matches if clean_string(rec[1]) == entity_name), None)
            if matched_record:
                match_basis = "Entity Name"
            else:
                print(
                    f"No match found for {company_name} based on Entity Name. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Match by Company Name (if no exact match)
        if not matched_record and company_name:
            possible_matches = fetch_supabase_records_by_name(company_name)
            if possible_matches:
                print(f"Company: {company_name}")
                for i, rec in enumerate(possible_matches, 1):
                    print(f"{i}. {rec[1]} ({rec[2]})")

                existing_selection = fields.get("Match Selection")
                if existing_selection and existing_selection.isdigit():
                    selected_index = int(existing_selection) - 1
                    if 0 <= selected_index < len(possible_matches):
                        matched_record = possible_matches[selected_index]
                        match_basis = "User Selection"
                else:
                    user_choice = input("Select a number (or 'n' for no match): ")
                    if user_choice.lower() == 'n':
                        print(
                            f"No match found for {company_name} based on Company Name. Marking as 'No Match Found in Main 5500'.")
                        airtable.update(record_id, {"No Match Found in Main 5500": True})
                        continue
                    else:
                        try:
                            selected_index = int(user_choice) - 1
                            matched_record = possible_matches[selected_index]
                            #airtable.update(record_id, {"Match Selection": user_choice})
                            match_basis = "User Selection"
                        except (ValueError, IndexError):
                            print("Invalid choice. Skipping this record.")
                            continue
            else:
                print(
                    f"No match found for {company_name} based on Company Name. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Update Airtable with matched record or no match flag
        if matched_record:
            update_fields = {
                "ACK ID 2023": matched_record[0],  # ack_id
                "Entity Name": matched_record[1],  # sponsor_dfe_name
                "DBA": matched_record[2],  # spons_dfe_dba_name
                "EIN": matched_record[3],  # spons_dfe_ein
                #"Match Type": "Main 5500",
                "No Match Found in Main 5500": False  # Ensure the field is set to False when a match is found
            }
            print(f"Match found for {company_name} based on {match_basis}. Updating Airtable fields:")
            for field, value in update_fields.items():
                print(f"  - {field}: {value}")
            airtable.update(record_id, update_fields)
        else:
            print(f"No match found for {company_name}. Marking as 'No Match Found in Main 5500'.")
            airtable.update(record_id, {"No Match Found in Main 5500": True})

        #time.sleep(1)  # Avoid hitting API rate limits


if __name__ == "__main__":
    process_prospects()

# Close the database connection
if cursor: cursor.close()
if conn: conn.close()
