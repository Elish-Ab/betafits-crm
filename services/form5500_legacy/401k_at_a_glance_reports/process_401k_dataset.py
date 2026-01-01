import pandas as pd
import numpy as np
import re


def clean_city_field(city_value):
    """Clean city field by removing brackets and quotes"""
    try:
        if pd.isna(city_value):
            return city_value

        city_str = str(city_value)
        # Remove brackets and quotes
        cleaned = re.sub(r"[\[\]']", "", city_str)
        cleaned = cleaned.strip()

        if cleaned == "" or cleaned.lower() == "nan":
            return np.nan

        return cleaned
    except:
        return np.nan


# Load the data
print("Loading 401k_data.csv...")
d1 = pd.read_csv('401k_data.csv')

print("Processing data transformations...")

# Create growth_rate field (keep as numeric for now)
print("Creating growth_rate field...")
d1['growth_rate'] = np.where(
    (pd.notna(d1['sf_net_assets_boy_amt'])) & (d1['sf_net_assets_boy_amt'] != 0),
    (d1['sf_net_assets_eoy_amt'] - d1['sf_net_assets_boy_amt']) * 100 / d1['sf_net_assets_boy_amt'],
    np.nan
)

# Rename fields (keep original column names for calculations)
print("Renaming fields...")
d1 = d1.rename(columns={
    'Company Name': 'company_name',
    'Year Founded (Scraped)': 'founded_on',
    'Industry (Scraped)': 'industry',
    'HQ City (Scraped)': 'headquarters',
    'current_participating': 'current_particip',
    'eligible_participants': 'eligible_particip',
    'with_balances': 'with_bal'
})

# Clean headquarters field
print("Processing headquarters...")
d1['headquarters'] = d1['headquarters'].apply(clean_city_field)

# Create total_contributions field (keep as numeric)
print("Creating total_contributions field...")
d1['total_contributions'] = np.where(
    (pd.notna(d1['sf_emplr_contrib_income_amt'])) & (pd.notna(d1['sf_particip_contrib_income_amt'])),
    d1['sf_emplr_contrib_income_amt'] + d1['sf_particip_contrib_income_amt'],
    np.nan
)

# Create contributions_per_participating field
print("Creating contributions_per_participating field...")
d1['contributions_per_participating'] = np.where(
    (pd.notna(d1['total_contributions'])) & (pd.notna(d1['current_particip'])) & (d1['current_particip'] != 0),
    d1['total_contributions'] / d1['current_particip'],
    np.nan
)

# Create contributions_per_eligible field
print("Creating contributions_per_eligible field...")
d1['contributions_per_eligible'] = np.where(
    (pd.notna(d1['total_contributions'])) & (pd.notna(d1['eligible_particip'])) & (d1['eligible_particip'] != 0),
    d1['total_contributions'] / d1['eligible_particip'],
    np.nan
)

# Create er_pct field
print("Creating er_pct field...")
total_contrib = d1['sf_emplr_contrib_income_amt'] + d1['sf_particip_contrib_income_amt']
d1['er_pct'] = np.where(
    (pd.notna(d1['sf_emplr_contrib_income_amt'])) & (pd.notna(total_contrib)) & (total_contrib != 0),
    d1['sf_emplr_contrib_income_amt'] * 100 / total_contrib,
    np.nan
)

# Create ee_pct field
print("Creating ee_pct field...")
d1['ee_pct'] = np.where(
    (pd.notna(d1['sf_particip_contrib_income_amt'])) & (pd.notna(total_contrib)) & (total_contrib != 0),
    d1['sf_particip_contrib_income_amt'] * 100 / total_contrib,
    np.nan
)

# Create er_per_particip field
print("Creating er_per_particip field...")
d1['er_per_particip'] = np.where(
    (pd.notna(d1['sf_emplr_contrib_income_amt'])) & (pd.notna(d1['current_particip'])) & (d1['current_particip'] != 0),
    d1['sf_emplr_contrib_income_amt'] / d1['current_particip'],
    np.nan
)

# Create ee_per_particip field
print("Creating ee_per_particip field...")
d1['ee_per_particip'] = np.where(
    (pd.notna(d1['sf_particip_contrib_income_amt'])) & (pd.notna(d1['current_particip'])) & (
            d1['current_particip'] != 0),
    d1['sf_particip_contrib_income_amt'] / d1['current_particip'],
    np.nan
)

# Create er_per_eligible field
print("Creating er_per_eligible field...")
d1['er_per_eligible'] = np.where(
    (pd.notna(d1['sf_emplr_contrib_income_amt'])) & (pd.notna(d1['eligible_particip'])) & (
            d1['eligible_particip'] != 0),
    d1['sf_emplr_contrib_income_amt'] / d1['eligible_particip'],
    np.nan
)

# Create ee_per_eligible field
print("Creating ee_per_eligible field...")
d1['ee_per_eligible'] = np.where(
    (pd.notna(d1['sf_particip_contrib_income_amt'])) & (pd.notna(d1['eligible_particip'])) & (
            d1['eligible_particip'] != 0),
    d1['sf_particip_contrib_income_amt'] / d1['eligible_particip'],
    np.nan
)

# Create safe_harbor field
print("Creating safe_harbor field...")


def create_safe_harbor(value):
    if pd.isna(value):
        return "N/A"
    str_val = str(value).strip()
    if str_val == '1' or str_val == '1.0':
        return "Yes"
    elif str_val == '0' or str_val == '0.0':
        return "No"
    else:
        return "N/A"


d1['safe_harbor'] = d1['sf_401k_design_based_safe_ind'].apply(create_safe_harbor)

# Create automatic_enrollment field
print("Creating automatic_enrollment field...")


def create_automatic_enrollment(value):
    if pd.isna(value):
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    str_val = str(value).lower().strip()
    if str_val in ['true', '1', 'yes']:
        return "Yes"
    elif str_val in ['false', '0', 'no']:
        return "No"
    else:
        return "N/A"


d1['automatic_enrollment'] = d1['contains_automatic_enrollment'].apply(create_automatic_enrollment)

# Create loans field
print("Creating loans field...")


def create_loans(value):
    if pd.isna(value):
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    str_val = str(value).lower().strip()
    if str_val in ['true', '1', 'yes']:
        return "Yes"
    elif str_val in ['false', '0', 'no']:
        return "No"
    else:
        return "N/A"


d1['loans'] = d1['partcp_loans_ind'].apply(create_loans)

# Process participation_rate (multiply by 100)
print("Processing participation_rate...")
d1['participation_rate'] = np.where(
    pd.notna(d1['participation_rate']),
    d1['participation_rate'] * 100,
    np.nan
)

# Rename contribution amount fields (keep original values for final formatting)
print("Preparing contribution amount fields...")
d1['er_contrib'] = d1['sf_emplr_contrib_income_amt']
d1['ee_total'] = d1['sf_particip_contrib_income_amt']

# Rename asset fields
d1['plan_assets_boy'] = d1['sf_net_assets_boy_amt']
d1['plan_assets_eoy'] = d1['sf_net_assets_eoy_amt']

print("All calculations completed. Now formatting fields...")


# Define formatting functions
def format_currency_string(value, decimal_places=0):
    """Format number as currency string with commas and specified decimal places"""
    try:
        if pd.isna(value):
            return "N/A"
        if decimal_places == 0:
            formatted = f"${int(round(float(value))):,}"
        else:
            formatted = f"${float(value):,.{decimal_places}f}"
        return formatted
    except:
        return "N/A"


def format_percentage_string(value, decimal_places=2):
    """Format number as percentage string with specified decimal places"""
    try:
        if pd.isna(value):
            return "N/A"
        formatted = f"{float(value):.{decimal_places}f}%"
        return formatted
    except:
        return "N/A"


def format_integer_string(value):
    """Format number as integer string"""
    try:
        if pd.isna(value):
            return "N/A"
        return str(int(float(value)))
    except:
        return "N/A"


def format_whole_number_to_string(value):
    """Convert to whole number (no decimals) then to string"""
    try:
        if pd.isna(value) or value is None or str(value).strip() == '' or str(value).lower() in ['nan', 'none', 'null']:
            return "N/A"
        # Convert to float first, then to int to remove decimals
        num_val = float(value)
        if num_val == 0:
            return "0"
        return str(int(num_val))
    except:
        return "N/A"


# STEP 1: Convert specified fields to whole numbers without decimals, then to text
print("STEP 1: Converting specified fields to whole numbers then to text...")
whole_number_fields = ['founded_on', 'current_particip', 'eligible_particip', 'with_bal',
                       'separated', 'generosity_index_rank', 'fees_index_rank']

for field in whole_number_fields:
    if field in d1.columns:
        print(f"Processing {field} to whole numbers...")
        d1[field] = d1[field].apply(format_whole_number_to_string)

# Special handling for founded_on field: convert 0 to "N/A"
if 'founded_on' in d1.columns:
    d1['founded_on'] = d1['founded_on'].apply(lambda x: "N/A" if x == "0" else x)

# STEP 2: Format other specific fields with their required formatting
print("STEP 2: Formatting currency and percentage fields...")

# Currency fields with no decimals
currency_fields_no_decimal = [
    'plan_assets_boy', 'plan_assets_eoy', 'total_contributions',
    'contributions_per_participating', 'contributions_per_eligible',
    'er_per_particip', 'ee_per_particip', 'er_per_eligible', 'ee_per_eligible',
    'er_contrib', 'ee_total'
]

for field in currency_fields_no_decimal:
    if field in d1.columns:
        d1[field] = d1[field].apply(lambda x: format_currency_string(x, 0))

# Currency field with decimals
if 'avg_account_balance' in d1.columns:
    d1['avg_account_balance'] = d1['avg_account_balance'].apply(lambda x: format_currency_string(x, 2))

# Percentage fields
percentage_fields = ['growth_rate', 'er_pct', 'ee_pct', 'participation_rate']
for field in percentage_fields:
    if field in d1.columns:
        d1[field] = d1[field].apply(lambda x: format_percentage_string(x, 2))

# Integer fields
integer_fields = ['form_plan_year', 'contributions_index_rank']
for field in integer_fields:
    if field in d1.columns:
        d1[field] = d1[field].apply(format_integer_string)

# STEP 3: Convert ALL fields to text strings and standardize N/A values
print("STEP 3: Converting all fields to text and standardizing N/A values...")


def convert_to_string_with_na(value):
    """Convert any value to string, replacing invalid values with 'N/A'"""
    try:
        # Handle pandas NA, None, and NaN values
        if pd.isna(value) or value is None:
            return "N/A"

        # Convert to string
        str_val = str(value).strip()

        # Handle empty strings and various null representations
        if str_val == '' or str_val.lower() in ['nan', 'none', 'null']:
            return "N/A"

        return str_val
    except:
        return "N/A"


# Convert ALL columns to text strings with proper N/A handling
for column in d1.columns:
    print(f"Converting column '{column}' to text...")
    d1[column] = d1[column].apply(convert_to_string_with_na)

# Final cleanup: ensure no pandas NA values remain and all columns are string type
print("Final cleanup...")
d1 = d1.fillna("N/A")
for column in d1.columns:
    d1[column] = d1[column].astype(str)

# STEP 4: Save the updated CSV with the same filename
print("Saving processed data to 401k_data.csv...")
d1.to_csv('401k_data.csv', index=False)

print("Data processing completed successfully!")
print(f"Final dataframe shape: {d1.shape}")
print(f"Sample of processed data:")
print(d1.head())

# Verify data types
print("\nData types after processing:")
print(d1.dtypes.value_counts())

# Final verification
print("\nFinal verification of specified whole number fields:")
for field in whole_number_fields:
    if field in d1.columns:
        sample_values = d1[field].head(10).tolist()
        print(f"{field}: {sample_values}")

print("All processing completed!")
