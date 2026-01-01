import pandas as pd
import psycopg2
import json
from psycopg2.extras import RealDictCursor

# Database configuration
DB_CONFIG = {
    "host": "aws-0-us-west-1.pooler.supabase.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres.usjxjbglawbxronycthr",
    "password": "hEzcYRz5ktANIdaX"
}


def connect_to_supabase():
    """Connect to Supabase database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def get_benchmarking_data():
    """Get benchmarking JSON data from Supabase"""
    conn = connect_to_supabase()
    if not conn:
        return None

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT generosity_index, fees_pct_index, fees_pepm_index, contributions_index
        FROM f_5500.retirement_benchmarking_summary 
        WHERE industry = 'Overall'
        """
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            return {
                'generosity_index': result['generosity_index'],
                'fees_pct_index': result['fees_pct_index'],
                'fees_pepm_index': result['fees_pepm_index'],
                'contributions_index': result['contributions_index']
            }
        else:
            print("No data found for 'Overall' industry")
            return None

    except Exception as e:
        print(f"Error fetching benchmarking data: {e}")
        return None
    finally:
        conn.close()


def find_rank(value, json_data):
    """Find the rank for a given value based on min/max ranges in JSON data"""
    if pd.isna(value) or value is None:
        return None

    for rank_str, range_data in json_data.items():
        rank = int(rank_str)
        min_val = range_data['min']
        max_val = range_data['max']

        # Check if value falls within this rank's range
        if min_val <= value <= max_val:
            return rank

    # If no exact match, find the closest rank
    # This handles edge cases where value might be slightly outside ranges
    best_rank = None
    min_distance = float('inf')

    for rank_str, range_data in json_data.items():
        rank = int(rank_str)
        min_val = range_data['min']
        max_val = range_data['max']

        # Calculate distance to range
        if value < min_val:
            distance = min_val - value
        elif value > max_val:
            distance = value - max_val
        else:
            distance = 0

        if distance < min_distance:
            min_distance = distance
            best_rank = rank

    return best_rank


def process_dataframe():
    """Main function to process the 401k data"""
    # Load the CSV data
    try:
        d1 = pd.read_csv('401k_data.csv')
        print(f"Loaded dataframe with {len(d1)} records")
        print(f"Columns: {list(d1.columns)}")
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return

    # Get benchmarking data from Supabase
    benchmarking_data = get_benchmarking_data()
    if not benchmarking_data:
        print("Failed to get benchmarking data")
        return

    print("Successfully retrieved benchmarking data from Supabase")

    # Initialize new columns
    d1['generosity_index_rank'] = None
    d1['contributions_index_rank'] = None
    d1['fees_index_rank'] = None

    # Process each record
    for index, row in d1.iterrows():
        # Calculate generosity_index_rank
        if 'generosity_index_value' in d1.columns:
            generosity_rank = find_rank(row['generosity_index_value'],
                                        benchmarking_data['generosity_index'])
            d1.at[index, 'generosity_index_rank'] = generosity_rank

        # Calculate contributions_index_rank
        if 'contributions_index_value' in d1.columns:
            contributions_rank = find_rank(row['contributions_index_value'],
                                           benchmarking_data['contributions_index'])
            d1.at[index, 'contributions_index_rank'] = contributions_rank

        # Calculate fees_index_rank (average of two fee rankings)
        rank1 = None
        rank2 = None

        if 'total_fees_pct' in d1.columns:
            rank1 = find_rank(row['total_fees_pct'],
                              benchmarking_data['fees_pct_index'])

        if 'total_fees_pepm' in d1.columns:
            rank2 = find_rank(row['total_fees_pepm'],
                              benchmarking_data['fees_pepm_index'])

        # Calculate average rank and round to whole number
        if rank1 is not None and rank2 is not None:
            avg_rank = round((rank1 + rank2) / 2)
            d1.at[index, 'fees_index_rank'] = avg_rank
        elif rank1 is not None:
            d1.at[index, 'fees_index_rank'] = rank1
        elif rank2 is not None:
            d1.at[index, 'fees_index_rank'] = rank2

    # Save the updated dataframe
    try:
        d1.to_csv('401k_data.csv', index=False)
        print(f"Successfully saved updated dataframe with {len(d1)} records")
        print("New columns added: generosity_index_rank, contributions_index_rank, fees_index_rank")

        # Display summary statistics
        print("\nSummary of new ranking columns:")
        for col in ['generosity_index_rank', 'contributions_index_rank', 'fees_index_rank']:
            if col in d1.columns:
                non_null_count = d1[col].notna().sum()
                print(f"{col}: {non_null_count} non-null values out of {len(d1)} records")
                if non_null_count > 0:
                    print(f"  Range: {d1[col].min():.0f} - {d1[col].max():.0f}")

    except Exception as e:
        print(f"Error saving CSV file: {e}")


if __name__ == "__main__":
    # Install required packages if not already installed
    # pip install pandas psycopg2-binary

    process_dataframe()
