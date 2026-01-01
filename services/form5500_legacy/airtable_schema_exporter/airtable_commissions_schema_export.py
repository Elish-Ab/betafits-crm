import requests
import pandas as pd
import re

# Airtable credentials
base_id = 'appI7SJCSBcTKcjUI'
api_key = 'pat2YkO9CbuzW6Sbe.4591bd645a2414ca7aaddf7a03376621e3884509849c6f345cbdd9a467a8d113'
url = f'https://api.airtable.com/v0/meta/bases/{base_id}/tables'

headers = {
    'Authorization': f'Bearer {api_key}'
}

# Fetch schema
response = requests.get(url, headers=headers)
data = response.json()

# Build lookup dictionaries
table_id_to_name = {}
field_id_to_name = {}

for table in data.get('tables', []):
    table_id_to_name[table['id']] = table['name']
    for field in table['fields']:
        field_id_to_name[field['id']] = field['name']

# Build records
records = []

for table in data.get('tables', []):
    table_name = table['name']
    table_id = table['id']

    for field in table.get('fields', []):
        combined = {
            'table_name': table_name,
            'table_id': table_id,
            **field
        }

        options = field.get('options', {})

        # ➤ options_list from options.choices
        choices = options.get('choices')
        if isinstance(choices, list):
            combined['options_list'] = ', '.join(choice.get('name', '') for choice in choices)
        else:
            combined['options_list'] = ''

        # ➤ linked_table from options.linkedTableId
        linked_table_id = options.get('linkedTableId')
        combined['linked_table'] = table_id_to_name.get(linked_table_id, '') if linked_table_id else ''

        # ➤ record_link_field from options.recordLinkFieldId
        link_field_id = options.get('recordLinkFieldId')
        combined['record_link_field'] = field_id_to_name.get(link_field_id, '') if link_field_id else ''

        # ➤ field_in_linked from options.fieldIdInLinkedTable
        field_in_linked_id = options.get('fieldIdInLinkedTable')
        combined['field_in_linked'] = field_id_to_name.get(field_in_linked_id, '') if field_in_linked_id else ''

        # ➤ formula with field IDs replaced by names
        formula_str = options.get('formula', '')
        if isinstance(formula_str, str):
            def replace_field_id(match):
                field_id = match.group(1)
                return f"{{{field_id_to_name.get(field_id, field_id)}}}"
            combined['formula'] = re.sub(r'{(fld\w+)}', replace_field_id, formula_str)
        else:
            combined['formula'] = ''

        # ➤ result_options_choices_list from options.result.options.choices
        result_choices = options.get('result', {}).get('options', {}).get('choices')
        if isinstance(result_choices, list):
            combined['result_options_choices_list'] = ', '.join(choice.get('name', '') for choice in result_choices)
        else:
            combined['result_options_choices_list'] = ''

        # ➤ inverse_link_field from options.inverseLinkFieldId
        inverse_link_id = options.get('inverseLinkFieldId')
        combined['inverse_link_field'] = field_id_to_name.get(inverse_link_id, '') if inverse_link_id else ''

        # ➤ referenced_fields from options.referencedFieldIds (list of field IDs)
        referenced_ids = options.get('referencedFieldIds')
        if isinstance(referenced_ids, list):
            combined['referenced_fields'] = ', '.join(field_id_to_name.get(fid, fid) for fid in referenced_ids)
        else:
            combined['referenced_fields'] = ''

        records.append(combined)

# Normalize and export
df = pd.json_normalize(records)
df.to_csv('airtable_schema_full.csv', index=False)

print("Schema exported as 'airtable_schema_full.csv'")