import pandas as pd
import psycopg2
import requests
import json
from typing import Dict, List, Any

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


def connect_to_supabase():
    """Connect to Supabase PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to Supabase")
        return conn
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        return None


def get_prospects_data(conn):
    """Get data from f_5500.dim_prospects_sf where contains_401k is true"""
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
        print(f"Retrieved {len(df)} records from dim_prospects_sf")
        return df
    except Exception as e:
        print(f"Error retrieving prospects data: {e}")
        return None


def get_airtable_data(record_ids: List[str]) -> pd.DataFrame:
    """Get data from Airtable Prospects table"""
    base_url = f"https://api.airtable.com/v0/{AIRTABLE_CONFIG['base_id']}/{AIRTABLE_CONFIG['table_id']}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_CONFIG['pat']}",
        "Content-Type": "application/json"
    }

    all_records = []
    offset = None

    try:
        while True:
            # Build URL with offset if needed
            url = base_url
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            records = data.get("records", [])

            # Filter records that match our record_ids
            for record in records:
                fields = record.get("fields", {})
                if fields.get("Record ID") in record_ids:
                    all_records.append({
                        "record_id": fields.get("Record ID"),
                        "Company Name": fields.get("Company Name"),
                        "Year Founded (Scraped)": fields.get("Year Founded (Scraped)"),
                        "Industry (Scraped)": fields.get("Industry (Scraped)"),
                        "HQ City (Scraped)": fields.get("HQ City (Scraped)")
                    })

            # Check if there are more pages
            offset = data.get("offset")
            if not offset:
                break

        df = pd.DataFrame(all_records)
        print(f"Retrieved {len(df)} matching records from Airtable")
        return df

    except Exception as e:
        print(f"Error retrieving Airtable data: {e}")
        return pd.DataFrame()


def get_f5500_data(conn, ack_ids: List[str]):
    """Get data from f_5500.f_5500_sf_2023 for matching ack_ids"""
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
        df = pd.read_sql_query(query, conn, params=ack_ids)
        print(f"Retrieved {len(df)} records from f_5500_sf_2023")
        return df
    except Exception as e:
        print(f"Error retrieving f5500 data: {e}")
        return pd.DataFrame()


def main():
    """Main function to orchestrate the data processing"""
    # Connect to Supabase
    conn = connect_to_supabase()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return

    try:
        # Step 1: Get prospects data
        print("Step 1: Getting prospects data...")
        d1 = get_prospects_data(conn)
        if d1 is None or d1.empty:
            print("No prospects data retrieved. Exiting.")
            return

        # Step 2: Get Airtable data and join
        print("Step 2: Getting Airtable data...")
        record_ids = d1['record_id'].tolist()
        airtable_df = get_airtable_data(record_ids)

        if not airtable_df.empty:
            # Inner join with Airtable data
            d1 = d1.merge(airtable_df, on='record_id', how='inner')
            print(f"After Airtable join: {len(d1)} records")
        else:
            print("No matching Airtable data found")

        # Step 3: Get f5500 data and join
        print("Step 3: Getting f5500 data...")
        ack_ids = d1['ack_id'].dropna().tolist()
        f5500_df = get_f5500_data(conn, ack_ids)

        if not f5500_df.empty:
            # Inner join with f5500 data
            d1 = d1.merge(f5500_df, on='ack_id', how='inner')
            print(f"After f5500 join: {len(d1)} records")
        else:
            print("No matching f5500 data found")

        # Step 4: Save to CSV
        print("Step 4: Saving to CSV...")
        d1.to_csv('401k_data.csv', index=False)
        print(f"Data saved to 401k_data.csv with {len(d1)} records and {len(d1.columns)} columns")

        # Display summary
        print("\nFinal dataset summary:")
        print(f"Shape: {d1.shape}")
        print(f"Columns: {list(d1.columns)}")

    except Exception as e:
        print(f"Error in main processing: {e}")

    finally:
        # Close database connection
        if conn:
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()
