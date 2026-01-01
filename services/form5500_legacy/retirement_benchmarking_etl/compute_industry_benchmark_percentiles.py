import psycopg2
import pandas as pd
import json
from tqdm import tqdm

# DB Configuration
DB_CONFIG = {
    "host": "aws-0-us-west-1.pooler.supabase.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres.usjxjbglawbxronycthr",
    "password": "hEzcYRz5ktANIdaX"
}

QUERY = """
SELECT top_level_industry, generosity_index_value, total_fees_pct, total_fees_pepm, contributions_index_value
FROM f_5500.fx_sf_2023
"""

PERCENTILE_COLUMNS = {
    "generosity_index_value": "generosity_index_percentile",
    "total_fees_pct": "total_fees_pct_percentile",
    "total_fees_pepm": "total_fees_pepm_percentile",
    "contributions_index_value": "contributions_index_percentile"
}

# ⏱️ Connect to DB with timeout
def connect_to_db():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '20min';")
    return conn

# 📥 Fetch data
def fetch_data():
    print("🔄 Fetching data from Supabase...")
    with connect_to_db() as conn:
        df = pd.read_sql(QUERY, conn)
    print(f"✅ Fetched {len(df)} rows.")
    return df

# 📊 Compute percentiles for one group
def compute_percentiles(df, group_name):
    print(f"📊 Computing percentiles for '{group_name}'...")
    for col, percentile_col in PERCENTILE_COLUMNS.items():
        non_null_count = df[col].notna().sum()
        print(f"   🔍 {col}: {non_null_count} non-null values")

        if non_null_count < 3:
            print(f"   ⚠️ Skipping {col} for '{group_name}' — insufficient data")
            df[percentile_col] = None
            continue

        ranks = df[col].rank(pct=True)
        df[percentile_col] = ranks.apply(
            lambda x: None if pd.isna(x) else (0 if x == 0 else int(min(100, -(-x * 100 // 1))))
        )
    return df

# 📈 Summarize min/max per percentile bucket
def summarize_percentile_ranges(df, group_name):
    summary = {}
    for col, percentile_col in PERCENTILE_COLUMNS.items():
        result = {}
        for rank in range(0, 101):
            group = df[df[percentile_col] == rank]
            if not group.empty:
                result[rank] = {
                    "min": float(group[col].min()),
                    "max": float(group[col].max())
                }
        # Adjust naming for final output
        summary[col.replace("_value", "").replace("total_", "")] = result
    return {
        "Industry": group_name,
        "generosity_index": summary.get("generosity_index"),
        "fees_pct_index": summary.get("fees_pct"),
        "fees_pepm_index": summary.get("fees_pepm"),
        "contributions_index": summary.get("contributions_index")
    }

# 🔁 Run summary for overall + each industry
def create_summary_table(df):
    summary_records = []

    # Overall
    df_overall = compute_percentiles(df.copy(), "Overall")
    summary_records.append(summarize_percentile_ranges(df_overall, "Overall"))

    # By Industry
    industries = df["top_level_industry"].dropna().unique()
    for industry in tqdm(industries, desc="🏭 Processing industries"):
        subset = df[df["top_level_industry"] == industry].copy()
        subset = compute_percentiles(subset, industry)
        summary_records.append(summarize_percentile_ranges(subset, industry))

    return summary_records

# 🚀 Main entry
def main():
    df = fetch_data()
    summary = create_summary_table(df)

    with open("industry_summary1.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("\n✅ Saved summary to industry_summary1.json")

if __name__ == "__main__":
    main()
