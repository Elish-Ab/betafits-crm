#!/usr/bin/env python3
"""
Complete 401k PDF Form Filler and Airtable Uploader
Fills acroform PDFs with data from CSV, flattens them, and uploads to Airtable.
"""

import pandas as pd
import requests
import fitz  # PyMuPDF
import os
import time
import sys


def fill_and_flatten_pdf_with_data(input_pdf_path, output_pdf_path, record_data, acroform_fields):
    """
    Fill PDF form fields with actual data and flatten using rasterization.

    Args:
        input_pdf_path (str): Path to the input PDF file
        output_pdf_path (str): Path to save the flattened output PDF
        record_data (dict): Dictionary containing the data to fill
        acroform_fields (list): List of field names to fill

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Open the PDF document
        doc = fitz.open(input_pdf_path)
        print(f"  📄 Opened PDF: {input_pdf_path}")

        # Fill form fields with actual data
        filled_fields = 0
        for page_num in range(doc.page_count):
            page = doc[page_num]

            # Get all widgets (form fields) on the page
            widgets = list(page.widgets())

            if widgets:
                for widget in widgets:
                    field_name = widget.field_name

                    # Check if this field should be filled and we have data for it
                    if field_name in acroform_fields and field_name in record_data:
                        # Get the value from record_data
                        field_value = record_data[field_name]

                        # Simply convert to string as-is (data is already properly formatted in CSV)
                        field_value = str(record_data[field_name]).strip('"')

                        # Check if it's a fillable field type
                        if widget.field_type in [fitz.PDF_WIDGET_TYPE_TEXT,
                                                 fitz.PDF_WIDGET_TYPE_COMBOBOX,
                                                 fitz.PDF_WIDGET_TYPE_LISTBOX]:
                            widget.field_value = field_value
                            widget.update()
                            filled_fields += 1
                            print(f"    ✓ Filled field '{field_name}' with: {field_value}")

        print(f"  📊 Total fields filled: {filled_fields}")

        # Create a new document for the flattened version
        flattened_doc = fitz.open()

        # Rasterize each page to flatten the form
        for page_num in range(doc.page_count):
            page = doc[page_num]

            # Render the page as a moderate resolution image (150 DPI for faster processing)
            mat = fitz.Matrix(150 / 72, 150 / 72)  # 150 DPI scaling matrix
            pix = page.get_pixmap(matrix=mat, alpha=False)

            # Create a new page in the flattened document
            rect = page.rect
            new_page = flattened_doc.new_page(width=rect.width, height=rect.height)

            # Insert the rasterized image into the new page
            new_page.insert_image(rect, pixmap=pix)

        # Save the flattened document
        flattened_doc.save(output_pdf_path)
        print(f"  🎯 Flattened PDF saved as: {output_pdf_path} (150 DPI)")

        # Close documents
        doc.close()
        flattened_doc.close()

        return True

    except Exception as e:
        print(f"  ❌ Error processing PDF: {str(e)}")
        return False


def fill_and_upload_pdf_to_airtable(record_data, airtable_pat, airtable_base_id, airtable_table_id,
                                    acroform_path, acroform_fields, tmp_dir="tmpfiles",
                                    delay_after_tmpfiles_upload=1, delay_before_next_record=0.5):
    """
    Fills an acroform PDF with data, flattens it, uploads to tmpfiles.org, and attaches to Airtable.

    Args:
        record_data (dict): A dictionary containing data for filling the PDF and the 'record_id'.
        airtable_pat (str): Your Airtable Personal Access Token.
        airtable_base_id (str): The ID of your Airtable base.
        airtable_table_id (str): The ID of your Airtable table.
        acroform_path (str): The path to the acroform PDF template.
        acroform_fields (list): List of field names to fill in the PDF.
        tmp_dir (str): Directory to temporarily save the filled PDFs.
        delay_after_tmpfiles_upload (int/float): Delay in seconds after uploading to tmpfiles.org.
        delay_before_next_record (int/float): Delay in seconds before processing the next record.
    """
    record_id = record_data.get('record_id', '').strip('"')
    if not record_id:
        print("❌ Error: 'record_id' not found or is empty in record_data. Skipping this record.")
        return

    os.makedirs(tmp_dir, exist_ok=True)
    filled_pdf_filename = os.path.join(tmp_dir, f"{record_id}.pdf")
    pdf_filename_for_airtable = f"{record_id}.pdf"

    try:
        # Step 1: Fill and flatten the Acroform PDF
        success = fill_and_flatten_pdf_with_data(
            acroform_path,
            filled_pdf_filename,
            record_data,
            acroform_fields
        )

        if not success:
            print(f"❌ Failed to fill and flatten PDF for record ID: {record_id}")
            return

        print(f"✅ Successfully filled and flattened PDF for record ID: {record_id}")

        # Step 2: Upload the filled PDF to tmpfiles.org
        with open(filled_pdf_filename, "rb") as f:
            res = requests.post("https://tmpfiles.org/api/v1/upload", files={"file": f})

        if res.ok:
            # Extract the correct download URL from tmpfiles.org response
            full_api_url_returned = res.json()["data"]["url"]
            path_segment = full_api_url_returned.split('tmpfiles.org', 1)[-1]
            file_url = f"https://tmpfiles.org/dl{path_segment}"

            print(f"📎 Uploaded PDF URL for {record_id}: {file_url}")
            time.sleep(delay_after_tmpfiles_upload)
        else:
            print(f"❌ Failed to upload PDF for {record_id} to tmpfiles.org: {res.text}")
            return

        # Step 3: Attach the file to Airtable
        airtable_url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_id}/{record_id}"
        headers = {
            "Authorization": f"Bearer {airtable_pat}",
            "Content-Type": "application/json"
        }

        data = {
            "fields": {
                "401k at a glance": [
                    {"url": file_url, "filename": pdf_filename_for_airtable}
                ]
            }
        }

        airtable_response = requests.patch(airtable_url, json=data, headers=headers)

        # Step 4: Handle Airtable response
        if airtable_response.ok:
            print(f"✅ PDF for {record_id} successfully attached to Airtable.")
        else:
            print(f"❌ Failed to attach PDF for {record_id} to Airtable: {airtable_response.text}")

    except Exception as e:
        print(f"❌ An error occurred for record ID {record_id}: {e}")
    finally:
        # Clean up temporary file
        if os.path.exists(filled_pdf_filename):
            os.remove(filled_pdf_filename)
            print(f"🗑️ Cleaned up temporary PDF file: {filled_pdf_filename}")

    time.sleep(delay_before_next_record)


# --- Configuration ---
CSV_FILE = '401k_data.csv'
ACROFORM_TEMPLATE = '401k_at_a_glance_acroform.pdf'

AIRTABLE_BASE_ID = "appjvhsxUUz6o0dzo"
AIRTABLE_TABLE_ID = "tblf4Ed9PaDo76QHH"
AIRTABLE_PAT = "patORcFPSvwabTvGV.ff78ce60800192e321417836e531ab24ff6e1a2ae634546e2c15c6a8ddfe9a57"

ACROFORM_FIELDS = [
    'plan_assets_boy', 'plan_assets_eoy', 'growth_rate', 'avg_account_balance',
    'company_name', 'founded_on', 'industry', 'headquarters', 'current_particip',
    'eligible_particip', 'with_bal', 'separated', 'generosity_index_rank',
    'fees_index_rank', 'contributions_index_rank', 'total_contributions',
    'contributions_per_participating', 'contributions_per_eligible', 'er_contrib',
    'er_pct', 'er_per_particip', 'er_per_eligible', 'ee_total', 'ee_pct',
    'ee_per_particip', 'ee_per_eligible', 'safe_harbor', 'automatic_enrollment',
    'participation_rate', 'loans', 'sf_entity', 'form_plan_year'
]

# --- Main Script Execution ---
if __name__ == "__main__":
    UPLOAD_DELAY_TMPFILES = 2  # Delay after tmpfiles.org upload (seconds)
    DELAY_BETWEEN_RECORDS = 1  # Delay between processing each record (seconds)

    print("🚀 401k PDF Form Filler and Airtable Uploader")
    print("=" * 60)

    # Check if required files exist
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: CSV file '{CSV_FILE}' not found!")
        print("Please make sure the CSV file is in the same directory as this script.")
        sys.exit(1)

    if not os.path.exists(ACROFORM_TEMPLATE):
        print(f"❌ Error: PDF template '{ACROFORM_TEMPLATE}' not found!")
        print("Please make sure the PDF template is in the same directory as this script.")
        sys.exit(1)

    try:
        # Load the CSV data
        d1 = pd.read_csv(CSV_FILE)
        print(f"📊 Loaded {len(d1)} records from {CSV_FILE}")
        print(f"📋 Columns in CSV: {list(d1.columns)}")
        print()

        # Check if record_id column exists
        if 'record_id' not in d1.columns:
            print("❌ Error: 'record_id' column not found in CSV file!")
            print("Please make sure your CSV file has a 'record_id' column.")
            sys.exit(1)

        # Process each record
        for index, row in d1.iterrows():
            record_data = row.to_dict()
            current_record_id = record_data.get('record_id', 'N/A')

            print(f"🔄 Processing row {index + 1}/{len(d1)} - Record ID: {current_record_id}")
            print("-" * 50)

            fill_and_upload_pdf_to_airtable(
                record_data,
                AIRTABLE_PAT,
                AIRTABLE_BASE_ID,
                AIRTABLE_TABLE_ID,
                ACROFORM_TEMPLATE,
                ACROFORM_FIELDS,
                delay_after_tmpfiles_upload=UPLOAD_DELAY_TMPFILES,
                delay_before_next_record=DELAY_BETWEEN_RECORDS
            )
            print()  # Add spacing between records

        print("=" * 60)
        print("🎉 All records processed successfully!")

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print(f"Make sure '{CSV_FILE}' and '{ACROFORM_TEMPLATE}' are in the same directory as the script.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        sys.exit(1)