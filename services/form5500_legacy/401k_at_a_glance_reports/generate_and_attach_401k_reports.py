import csv

# Path to the CSV file
filename = "401k_data.csv"

# Fields to convert to whole numbers and then to string
fields_to_whole_numbers = [
    "founded_on",
    "current_particip",
    "eligible_particip",
    "with_bal",
    "separated",
    "generosity_index_rank",
    "fees_index_rank",
]

def convert_to_whole_string(value):
    try:
        # Remove formatting like commas or dollar signs
        cleaned = value.replace(",", "").replace("$", "")
        num = float(cleaned)
        return str(int(round(num)))
    except:
        return "N/A" if value.strip() == "N/A" else value

def ensure_string(value):
    value = value.strip()
    if value.upper() == "N/A":
        return '"N/A"'
    if not (value.startswith('"') and value.endswith('"')):
        return f'"{value}"'
    return value

# Read, transform, and write
with open(filename, mode="r", newline='', encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Transform rows
for row in rows:
    for field in fieldnames:
        value = row[field].strip()
        if field in fields_to_whole_numbers:
            row[field] = ensure_string(convert_to_whole_string(value))
        else:
            row[field] = ensure_string(value)

# Write back to same file
with open(filename, mode="w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
