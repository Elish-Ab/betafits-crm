# import librarires
import json 
import os 

import gdown # Google Drive library
import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials

# load the url of the file
file_url = 'https://drive.google.com/file/d/1fdlJtBfxMts9UFJtYID16ZVWjf3SOBTa/view?usp=share_link'

# store file id
file_id = file_url.split('/')[-2]

# assign the prefix value
prefix = 'https://drive.google.com/uc?/export=download&id='

# download the data
gdown.download(prefix + file_id)

# assing the data frame to variable
csv_file_path = 'f_5500_sf_2021_latest'

# get the columns to be matched
SELECTED_COLUMNS_FROM_CSV = ['SF_SPONSOR_NAME', 'SF_SPONS_US_CITY', 'ACK_ID']

EQUIVALENT_AIRTABLE_COLUMN_NAMES = ['SPONSOR_NAME', 'SPONS_US_CITY', 'ID']

# assign the ids
AIRTABLE_API_KEY = "keyaIUEIxy6mUelF9"
AIRTABLE_BASE_ID = "appQfs70fHCsFgeUe"
AIRTABLE_TABLE_ID = "tblMY35I9egPXI0bA"
AIRTABLE_VIEW_ID = "viwigFQvfedA3pOJm"

# storing information to be used with the API
headers = {
    'Authorization': f'Bearer {AIRTABLE_API_KEY}',
    'Content-Type': 'application/json'
}

# create an empty to store matching records
all_records = []


def get_data_from_csv_for_selected_columns(file_path, selected_columns):
    '''
    The function will use the path to the CSV file and based on the selected column names
    will trim the data to only the desired columns.
    
    Input:   file_path - file path to the CSV file
             selected_columns - the list of the columns from the CSV file
    Reuturn:  the data with only the selcted columns
    
    '''
    selected_columns_df = pd.read_csv(f'{file_path}.csv', low_memory=False)

    selected_columns_df = selected_columns_df[selected_columns]

    return selected_columns_df


def search_and_match_records(base_id, table_id, view_id, search_fields, remaining_data_to_post):
    for record in all_records:
        match_count = 0
        for field, value in search_fields.items():
            airtable_record_value = record['fields'][field].strip()
            excel_record_to_match = value.strip()

            if field == 'SPONSOR_NAME' and is_spnosor_name_match(airtable_record_value, excel_record_to_match):
                print({
                    'Match found:': {
                        'AirTable record': airtable_record_value,
                        'CSV record': excel_record_to_match
                    }
                })
                record_id = record['id']
                return make_resulting_dict_object(remaining_data_to_post, "True")

    create_new_record(base_id, table_id, view_id, remaining_data_to_post)
    return make_resulting_dict_object(remaining_data_to_post, "False")


def is_spnosor_name_match(record_value, search_value):
    parsed_record_code = record_value
    parsed_search_code = search_value

    if parsed_record_code == parsed_search_code:
        return True
    return False


def make_resulting_dict_object(remaining_data_to_post, match_value):
    return {
        **remaining_data_to_post,
        'MATCH': match_value}


def create_new_record(base_id, table_id, view_id, fields):
    data = {
        'fields': {
            **fields,
        }
    }
    url = f'https://api.airtable.com/v0/{base_id}/{table_id}'
    response = requests.post(
        f'{url}?view={view_id}', headers=headers, data=json.dumps(data))
    print("Post:"+str(response.json()))


def loop_all_records(selected_columns_df):
    all_records_to_add_in_output_csv = []
    for index, row in selected_columns_df.iterrows():
        record_dict = row.to_dict()
        record_dict[EQUIVALENT_AIRTABLE_COLUMN_NAMES[0]] = str(
            record_dict[EQUIVALENT_AIRTABLE_COLUMN_NAMES[0]])
        records_to_match = {k: record_dict[k]
                            for k in EQUIVALENT_AIRTABLE_COLUMN_NAMES}
        data_to_post = record_dict.copy()
        record_to_add_in_output_csv = search_and_match_records(
            AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, AIRTABLE_VIEW_ID, records_to_match, data_to_post)
        all_records_to_add_in_output_csv.append(record_to_add_in_output_csv)
        print("_"*80)
    return all_records_to_add_in_output_csv


def output_results_to_excel(all_records_to_add_in_output_csv):
    output_file_path = os.path.join(os.getcwd(), 'output.xlsx')

    if os.path.exists(output_file_path):
        existing_file = pd.read_excel('output.xlsx')
        output_excel_dataframe = pd.DataFrame(all_records_to_add_in_output_csv)
        df_concatenated = pd.concat(
            [existing_file, output_excel_dataframe], axis=0)
        df_concatenated.to_excel('output.xlsx', index=False)
    else:
        output_excel_dataframe = pd.DataFrame(all_records_to_add_in_output_csv)
        # Write the DataFrame to an Excel file
        output_excel_dataframe.to_excel('output.xlsx', index=False)


def update_column_names_and_drop_nan_records(selected_columns_df):
    selected_columns_df = selected_columns_df.dropna(
        thresh=selected_columns_df.shape[1]-2)
    for i in range(len(SELECTED_COLUMNS_FROM_CSV)):  # Updating names wrt. airtable names
        selected_columns_df = selected_columns_df.rename(
            columns={SELECTED_COLUMNS_FROM_CSV[i]: EQUIVALENT_AIRTABLE_COLUMN_NAMES[i]})
    return selected_columns_df


def main(base_id, table_id, view_id):
    url = f'https://api.airtable.com/v0/{base_id}/{table_id}'
    print(url)

    response = requests.get(f'{url}/?view={view_id}', headers=headers)
    data = json.loads(response.text)
    all_records.extend(data['records'])

    while True:
        url = url
        if 'offset' in data:
            offset_url = f'{url}?offset={data["offset"]}&view={view_id}'
            print('offset url:', offset_url)

            response = requests.get(offset_url, headers=headers)
            data = json.loads(response.text)
            all_records.extend(data['records'])
        else:
            break

    selected_columns_df = get_data_from_csv_for_selected_columns(
        csv_file_path, SELECTED_COLUMNS_FROM_CSV)

    selected_columns_df = update_column_names_and_drop_nan_records(
        selected_columns_df)

    print('column names:', selected_columns_df.columns)

    all_records_to_add_in_output_csv = loop_all_records(selected_columns_df)


if __name__ == '__main__':
    main(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, AIRTABLE_VIEW_ID)
