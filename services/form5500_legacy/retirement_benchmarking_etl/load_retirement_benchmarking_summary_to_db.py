import json
import psycopg2

# Load the local JSON file
with open('industry_summary1.json', 'r') as file:
    data = json.load(file)

# Database configuration
DB_CONFIG = {
    "host": "aws-0-us-west-1.pooler.supabase.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres.usjxjbglawbxronycthr",
    "password": "hEzcYRz5ktANIdaX"
}

# Connect to the database
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create the table
cursor.execute("""
CREATE SCHEMA IF NOT EXISTS f_5500;

CREATE TABLE IF NOT EXISTS f_5500.retirement_benchmarking_summary (
    industry TEXT PRIMARY KEY,
    generosity_index JSONB,
    fees_pct_index JSONB,
    fees_pepm_index JSONB,
    contributions_index JSONB
);
""")

# Insert the data
for item in data:
    industry = item.get("Industry")
    generosity_index = json.dumps(item.get("generosity_index"))
    fees_pct_index = json.dumps(item.get("fees_pct_index"))
    fees_pepm_index = json.dumps(item.get("fees_pepm_index"))
    contributions_index = json.dumps(item.get("contributions_index"))

    cursor.execute("""
        INSERT INTO f_5500.retirement_benchmarking_summary (
            industry,
            generosity_index,
            fees_pct_index,
            fees_pepm_index,
            contributions_index
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (industry) DO UPDATE SET
            generosity_index = EXCLUDED.generosity_index,
            fees_pct_index = EXCLUDED.fees_pct_index,
            fees_pepm_index = EXCLUDED.fees_pepm_index,
            contributions_index = EXCLUDED.contributions_index;
    """, (
        industry,
        generosity_index,
        fees_pct_index,
        fees_pepm_index,
        contributions_index
    ))

# Commit and close connection
conn.commit()
cursor.close()
conn.close()
