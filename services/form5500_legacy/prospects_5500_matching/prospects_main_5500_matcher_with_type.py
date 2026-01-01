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
        SELECT 
            ack_id, 
            sponsor_dfe_name, 
            spons_dfe_dba_name, 
            spons_dfe_ein, 
            CASE 
                WHEN TYPE_WELFARE_BNFT_CODE IS NOT NULL THEN 'H&W' 
                ELSE 'Retirement_F5500' 
            END AS Benefit_Type
        FROM f_5500_2023 
        WHERE spons_dfe_ein = %s
    """, (ein,))
    return cursor.fetchall()


# Fetch records from Supabase by sponsor or DBA name
def fetch_supabase_records_by_name(name):
    ensure_connection()
    cursor.execute("""
        SELECT ack_id, 
               sponsor_dfe_name, 
               spons_dfe_dba_name, 
               spons_dfe_ein, 
               CASE 
                   WHEN TYPE_WELFARE_BNFT_CODE IS NOT NULL THEN 'H&W' 
                   ELSE 'Retirement_F5500' 
               END AS Benefit_Type,
               spons_dfe_mail_us_city, 
               spons_dfe_loc_us_city, 
               fct_naics_codes.Industry_Title AS industry_title  -- Replace business_code with Industry Title
        FROM f_5500_2023
        LEFT JOIN fct_naics_codes 
            ON f_5500_2023.business_code = fct_naics_codes.NAICS_Code
        WHERE sponsor_dfe_name ILIKE %s 
           OR spons_dfe_dba_name ILIKE %s
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
        if not fields.get("Name Mismatch"):
            continue

        company_name = fields.get("Company Name", "").strip()
        ein = normalize_ein(fields.get("EIN", ""))  # Normalize EIN from Prospects
        entity_name = clean_string(fields.get("Entity Name", ""))
        HQ1= fields.get("HQ City (Scraped)", "")
        HQ2= fields.get("HQ Scrape", "")
        HQ3= fields.get("HQ", "").strip()
        Industry= fields.get("Industry (Scraped)", "").strip()

        matched_records = None
        match_basis = ""

        # Match by EIN
        if ein:
            matched_records = fetch_supabase_record_by_ein(ein)
            if matched_records:
                match_basis = "EIN"
            else:
                print(f"No match found for {company_name} based on EIN. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Match by Entity Name (if EIN not found)
        if not matched_records and entity_name:
            possible_matches = fetch_supabase_records_by_name(entity_name)
            matched_records = [rec for rec in possible_matches if clean_string(rec[1]) == clean_string(entity_name)]
            if matched_records:
                match_basis = "Entity Name"
            else:
                print(
                    f"No match found for {company_name} based on Entity Name. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Match by Company Name (if no exact match)
        if not matched_records and company_name:
            possible_matches = fetch_supabase_records_by_name(company_name)
            if possible_matches:
                print(f"Company: {company_name} {HQ1} {HQ2} {HQ3} {Industry}")
                for i, rec in enumerate(possible_matches, 1):
                    print(f"{i}. {rec[1]} DBA:{rec[2]} HQ1:{rec[5]} HQ2:{rec[6]} Industry:{rec[7]}")
                print("\a") #sound for user input
                user_choice = input("Select numbers separated by commas (or 'n' for no match): ").strip()
                if user_choice.lower() == 'n':
                    print(
                        f"No match found for {company_name} based on Company Name. Marking as 'No Match Found in Main 5500'.")
                    airtable.update(record_id, {"No Match Found in Main 5500": True})
                    continue
                else:
                    try:
                        selected_indices = [int(choice.strip()) - 1 for choice in user_choice.split(',')]
                        matched_records = [possible_matches[i] for i in selected_indices if
                                           0 <= i < len(possible_matches)]

                        if matched_records:
                            match_basis = "User Selection"
                        else:
                            print("Invalid selections. Skipping this record.")
                            continue
                    except ValueError:
                        print("Invalid input. Please enter numbers separated by commas or 'n' for no match.")
                        continue
            else:
                print(
                    f"No match found for {company_name} based on Company Name. Marking as 'No Match Found in Main 5500'.")
                airtable.update(record_id, {"No Match Found in Main 5500": True})
                continue

        # Update Airtable with matched record or no match flag
        if matched_records:
            ack_ids = ", ".join(record[0] for record in matched_records)  # Join ACK IDs as a comma-separated string
            entity_name = matched_records[0][1]  # Entity Name (same for all)
            dba = matched_records[0][2]  # DBA (same for all)
            ein = matched_records[0][3]  # EIN (same for all)

            # Determine the Match Type based on Benefit_Type in matched records
            match_types = set()
            for record in matched_records:
                benefit_type = record[4]  # Assuming Benefit_Type is at index 4
                if benefit_type in ["H&W", "Retirement_F5500"]:
                    match_types.add(benefit_type)

            update_fields = {
                "ACK ID 2023": ack_ids,
                "Entity Name": entity_name,
                "DBA": dba,
                "EIN": ein,
                "Match Type": list(match_types),  # Convert to list for multiple select field
                "No Match Found in Main 5500": False,  # Ensure the field is set to False when a match is found
                "Match Basis": match_basis
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
