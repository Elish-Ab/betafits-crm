import pandas as pd
import psycopg2
import requests
import json
from PyPDF2 import PdfReader, PdfWriter
import io
import base64
import os
from typing import Dict, Any, Optional

# Database configuration
DB_CONFIG = {
    "host": "aws-0-us-west-1.pooler.supabase.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres.usjxjbglawbxronycthr",
    "password": "hEzcYRz5ktANIdaX"
}

# Airtable configuration
AIRTABLE_CONFIG = {
    "base_id": "appjvhsxUUz6o0dzo",
    "table_id": "tblf4Ed9PaDo76QHH",
    "pat": "patORcFPSvwabTvGV.ff78ce60800192e321417836e531ab24ff6e1a2ae634546e2c15c6a8ddfe9a57"
}

# PDF form file path
PDF_FORM_PATH = "401k_at_a_glance_acroform.pdf"


def connect_to_supabase():
    """Connect to Supabase database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to Supabase")
        return conn
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        return None


def get_prospects_data(conn):
    """Get prospects data from Supabase where contains_401k is true"""
    query = """
    SELECT 
        record_id,
        ack_id,
        avg_account_balance,
        current_participating,
        eligible_participants,
        with_balances,
        separated,
        participation_rate,
        ee_contrib_per_eligible_ee,
        er_contrib_per_eligible_ee,
        ee_contrib_per_particip_ee,
        er_contrib_per_particip_ee,
        contains_automatic_enrollment,
        sf_entity,
        form_plan_year,
        partcp_loans_ind,
        generosity_index_value,
        contributions_index_value,
        total_fees_pct,
        total_fees_pepm
    FROM f_5500.dim_prospects_sf 
    WHERE contains_401k = true
    """

    try:
        df = pd.read_sql_query(query, conn)
        print(f"Retrieved {len(df)} records from prospects table")
        return df
    except Exception as e:
        print(f"Error retrieving prospects data: {e}")
        return pd.DataFrame()


def get_airtable_data(record_ids):
    """Get data from Airtable Prospects table"""
    base_url = f"https://api.airtable.com/v0/{AIRTABLE_CONFIG['base_id']}/{AIRTABLE_CONFIG['table_id']}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_CONFIG['pat']}",
        "Content-Type": "application/json"
    }

    all_records = []
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            all_records.extend(data.get("records", []))

            offset = data.get("offset")
            if not offset:
                break

        except Exception as e:
            print(f"Error retrieving Airtable data: {e}")
            break

    # Filter records that match our record_ids and extract relevant fields
    filtered_data = []
    for record in all_records:
        fields = record.get("fields", {})
        record_id = fields.get("Record ID")

        if record_id in record_ids:
            filtered_data.append({
                "record_id": record_id,
                "airtable_record_id": record["id"],
                "Company Name": fields.get("Company Name"),
                "Year Founded (Scraped)": fields.get("Year Founded (Scraped)"),
                "Industry (Scraped)": fields.get("Industry (Scraped)"),
                "HQ City (Scraped)": fields.get("HQ City (Scraped)")
            })

    df_airtable = pd.DataFrame(filtered_data)
    print(f"Retrieved {len(df_airtable)} matching records from Airtable")
    return df_airtable


def get_f5500_data(conn, ack_ids):
    """Get data from f_5500_sf_2023 table"""
    if not ack_ids:
        return pd.DataFrame()

    # Create placeholders for the IN clause
    placeholders = ','.join(['%s'] * len(ack_ids))

    query = f"""
    SELECT 
        ack_id,
        SF_NET_ASSETS_BOY_AMT,
        SF_NET_ASSETS_EOY_AMT,
        SF_EMPLR_CONTRIB_INCOME_AMT,
        SF_PARTICIP_CONTRIB_INCOME_AMT,
        SF_401K_DESIGN_BASED_SAFE_IND,
        SF_401K_DESIGN_BASED_SAFE_HARBOR_IND
    FROM f_5500.f_5500_sf_2023 
    WHERE ack_id IN ({placeholders})
    """

    try:
        df = pd.read_sql_query(query, conn, params=list(ack_ids))
        print(f"Retrieved {len(df)} records from f_5500_sf_2023 table")
        return df
    except Exception as e:
        print(f"Error retrieving f_5500 data: {e}")
        return pd.DataFrame()


def safe_value(value):
    """Convert null, empty, or zero values to 'N/A'"""
    if value is None or value == '' or value == 0:
        return 'N/A'
    return str(value)


def calculate_growth_rate(boy_assets, eoy_assets):
    """Calculate growth rate between beginning and end of year assets"""
    try:
        if boy_assets and eoy_assets and boy_assets != 0:
            growth_rate = ((eoy_assets - boy_assets) / boy_assets) * 100
            return f"{growth_rate:.2f}%"
    except:
        pass
    return 'N/A'


def fill_pdf_form(pdf_path: str, form_data: Dict[str, Any]) -> bytes:
    """Fill PDF form with data and return as bytes"""
    try:
        # Read the PDF
        with open(pdf_path, 'rb') as file:
            reader = PdfReader(file)
            writer = PdfWriter()

            # Copy all pages
            for page in reader.pages:
                writer.add_page(page)

            # Fill form fields
            if "/AcroForm" in reader.trailer["/Root"]:
                writer.update_page_form_field_values(writer.pages[0], form_data)

            # Save to bytes
            output_buffer = io.BytesIO()
            writer.write(output_buffer)
            output_buffer.seek(0)
            return output_buffer.getvalue()

    except Exception as e:
        print(f"Error filling PDF form: {e}")
        return None


def upload_to_airtable(airtable_record_id: str, pdf_bytes: bytes, filename: str):
    """Upload PDF to Airtable attachment field"""
    try:
        # First, upload the file to get the attachment URL
        base_url = f"https://api.airtable.com/v0/{AIRTABLE_CONFIG['base_id']}/{AIRTABLE_CONFIG['table_id']}"
        headers = {
            "Authorization": f"Bearer {AIRTABLE_CONFIG['pat']}",
            "Content-Type": "application/json"
        }

        # Convert PDF to base64 for upload
        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')

        # Create attachment data
        attachment_data = {
            "url": f"data:application/pdf;base64,{pdf_base64}",
            "filename": filename
        }

        # Update the record with the attachment
        update_data = {
            "fields": {
                "401k at a glance": [attachment_data]
            }
        }

        response = requests.patch(
            f"{base_url}/{airtable_record_id}",
            headers=headers,
            json=update_data
        )

        if response.status_code == 200:
            print(f"Successfully uploaded PDF for record {airtable_record_id}")
            return True
        else:
            print(f"Error uploading PDF for record {airtable_record_id}: {response.text}")
            return False

    except Exception as e:
        print(f"Error uploading to Airtable: {e}")
        return False


def process_records(merged_df):
    """Process each record to fill PDF and upload to Airtable"""
    if not os.path.exists(PDF_FORM_PATH):
        print(f"Error: PDF form file '{PDF_FORM_PATH}' not found")
        return

    successful_uploads = 0

    for index, row in merged_df.iterrows():
        try:
            # Calculate growth rate
            growth_rate = calculate_growth_rate(
                row.get('SF_NET_ASSETS_BOY_AMT'),
                row.get('SF_NET_ASSETS_EOY_AMT')
            )

            # Prepare form data mapping
            form_data = {
                'plan_assets_boy': safe_value(row.get('SF_NET_ASSETS_BOY_AMT')),
                'plan_assets_eoy': safe_value(row.get('SF_NET_ASSETS_EOY_AMT')),
                'growth_rate': growth_rate,
                'avg_account_balance': safe_value(row.get('avg_account_balance')),
                'company_name': safe_value(row.get('Company Name')),
                'founded_on': safe_value(row.get('Year Founded (Scraped)')),
                'industry': safe_value(row.get('Industry (Scraped)')),
                'headquarters': safe_value(row.get('HQ City (Scraped)')),
                'current_particip': safe_value(row.get('current_participating')),
                'eligible_particip': safe_value(row.get('eligible_participants')),
                'with_bal': safe_value(row.get('with_balances')),
                'seperated': safe_value(row.get('separated')),
                'generosity_index_rank': safe_value(row.get('generosity_index_value')),
                'fees_index_rank': safe_value(row.get('total_fees_pct')),
                'contributions_index_rank': safe_value(row.get('contributions_index_value')),
                'total_contributions': safe_value(row.get('SF_EMPLR_CONTRIB_INCOME_AMT')),
                'contributions_per_participating': safe_value(row.get('ee_contrib_per_particip_ee')),
                'contributions_per_eligible': safe_value(row.get('ee_contrib_per_eligible_ee')),
                'er_contrib': safe_value(row.get('SF_EMPLR_CONTRIB_INCOME_AMT')),
                'er_pct': 'N/A',  # Not directly available in data
                'er_per_particip': safe_value(row.get('er_contrib_per_particip_ee')),
                'er_per_eligible': safe_value(row.get('er_contrib_per_eligible_ee')),
                'ee_total': safe_value(row.get('SF_PARTICIP_CONTRIB_INCOME_AMT')),
                'ee_pct': 'N/A',  # Not directly available in data
                'ee_per_particip': safe_value(row.get('ee_contrib_per_particip_ee')),
                'ee_per_eligible': safe_value(row.get('ee_contrib_per_eligible_ee')),
                'safe_harbor': safe_value(row.get('SF_401K_DESIGN_BASED_SAFE_HARBOR_IND')),
                'automatic_enrollment': safe_value(row.get('contains_automatic_enrollment')),
                'participation_rate': safe_value(row.get('participation_rate')),
                'loans': safe_value(row.get('partcp_loans_ind')),
                'sf_entity': safe_value(row.get('sf_entity')),
                'form_plan_year': safe_value(row.get('form_plan_year'))
            }

            # Fill PDF form
            pdf_bytes = fill_pdf_form(PDF_FORM_PATH, form_data)

            if pdf_bytes:
                # Upload to Airtable
                filename = f"401k_at_a_glance_{row['record_id']}.pdf"
                success = upload_to_airtable(row['airtable_record_id'], pdf_bytes, filename)

                if success:
                    successful_uploads += 1
                    print(f"Processed record {row['record_id']} successfully")
                else:
                    print(f"Failed to upload PDF for record {row['record_id']}")
            else:
                print(f"Failed to fill PDF for record {row['record_id']}")

        except Exception as e:
            print(f"Error processing record {row['record_id']}: {e}")

    print(f"\nProcessing complete. Successfully uploaded {successful_uploads} PDFs out of {len(merged_df)} records.")


def main():
    """Main execution function"""
    print("Starting 401k at a Glance PDF generation process...")

    # Connect to Supabase
    conn = connect_to_supabase()
    if not conn:
        return

    try:
        # Step 1: Get prospects data from Supabase
        print("\n1. Retrieving prospects data from Supabase...")
        d1 = get_prospects_data(conn)

        if d1.empty:
            print("No prospects data found. Exiting.")
            return

        # Step 2: Get Airtable data
        print("\n2. Retrieving data from Airtable...")
        record_ids = d1['record_id'].tolist()
        airtable_df = get_airtable_data(record_ids)

        # Step 3: Merge with Airtable data
        print("\n3. Merging with Airtable data...")
        d1 = d1.merge(airtable_df, on='record_id', how='inner')
        print(f"After Airtable merge: {len(d1)} records")

        # Step 4: Get f_5500 data
        print("\n4. Retrieving f_5500 data...")
        ack_ids = d1['ack_id'].dropna().tolist()
        f5500_df = get_f5500_data(conn, ack_ids)

        # Step 5: Merge with f_5500 data
        print("\n5. Merging with f_5500 data...")
        d1 = d1.merge(f5500_df, on='ack_id', how='inner')
        print(f"Final dataset: {len(d1)} records")

        if d1.empty:
            print("No records remain after all merges. Exiting.")
            return

        # Step 6: Process each record to fill PDF and upload
        print("\n6. Processing records and uploading PDFs...")
        process_records(d1)

    except Exception as e:
        print(f"Error in main execution: {e}")

    finally:
        # Close database connection
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
