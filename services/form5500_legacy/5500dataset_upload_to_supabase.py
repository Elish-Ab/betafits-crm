import psycopg2
import pandas as pd
from tqdm import tqdm

# Database credentials
DB_USER = "postgres.usjxjbglawbxronycthr"
DB_PASSWORD = "hEzcYRz5ktANIdaX"
DB_HOST = "aws-0-us-west-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

# CSV file path
csv_file = r"C:\Users\navee\Downloads\2023_audit_reports_data\2023_audit.csv"

# Connect to the database
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# Create table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS audit_reports_2023 (
    id TEXT PRIMARY KEY,
    form_type TEXT,
    filing_year INT,
    sponsor_name TEXT,
    ein BIGINT,
    plan_number INT,
    link TEXT,
    facsimile_link TEXT
);
"""
cur.execute(create_table_query)
conn.commit()

# Load CSV file
df = pd.read_csv(csv_file, sep="|", dtype=str)

# Replace NaN values with None
df = df.where(pd.notna(df), None)

# Prepare SQL query
insert_query = """
INSERT INTO audit_reports_2023 (id, form_type, filing_year, sponsor_name, ein, plan_number, link, facsimile_link)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

# Insert data with progress bar
print("Uploading data to the database...")
for row in tqdm(df.itertuples(index=False, name=None), total=len(df)):
    cur.execute(insert_query, row)

# Commit and close
conn.commit()
cur.close()
conn.close()

print("Data upload completed successfully!")
