# Retirement Plan Benchmarking Summary

This repository contains a small pipeline that computes industry-level retirement plan benchmarking percentiles from a 5500-based dataset and loads the results into a Postgres database (Supabase-hosted).

## Files

- `compute_industry_benchmark_percentiles.py`  
  Connects to the Supabase Postgres database, queries `f_5500.fx_sf_2023`, and computes percentile buckets for:
  - `generosity_index_value`
  - `total_fees_pct`
  - `total_fees_pepm`
  - `contributions_index_value`  
  It produces a JSON summary file called `industry_summary1.json`.

- `R_benchmarking_summary.py`  
  Reads `industry_summary1.json` and upserts the data into the Postgres table `f_5500.retirement_benchmarking_summary` (creating the schema/table if needed).  
  (Optionally, you can rename this file to `load_retirement_benchmarking_summary_to_db.py` for extra clarity.)

- `industry_summary1.json`  
  JSON file that stores, for each industry (and overall), the min/max value ranges for the four metrics at each percentile bucket.

## Data Flow

1. `load_retirement_benchmarking_summary_to_db.py`  
   - Query `f_5500.fx_sf_2023` from Supabase Postgres.  
   - Compute percentile ranges by industry and overall.  
   - Write the results to `industry_summary1.json`.

2. `load_retirement_benchmarking_summary_to_db.py` 
   - Read `industry_summary1.json`.  
   - Ensure the `f_5500.retirement_benchmarking_summary` table exists (with JSONB columns).  
   - Upsert each industry’s percentile summary into the table.

