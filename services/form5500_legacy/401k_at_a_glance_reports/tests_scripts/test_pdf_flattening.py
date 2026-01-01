#!/usr/bin/env python3
"""
PDF AcroForm Filler Script
Fills all fillable text fields in a PDF form with "Dummy Data"
and saves as a flattened PDF using rasterization.
"""

import fitz  # PyMuPDF
import os
import sys


def fill_and_flatten_pdf(input_pdf_path, output_pdf_path):
    """
    Fill all text fields in a PDF form with "Dummy Data" and flatten using rasterization.

    Args:
        input_pdf_path (str): Path to the input PDF file
        output_pdf_path (str): Path to save the flattened output PDF
    """

    try:
        # Open the PDF document
        doc = fitz.open(input_pdf_path)
        print(f"Opened PDF: {input_pdf_path}")
        print(f"Number of pages: {doc.page_count}")

        # Fill form fields with dummy data
        filled_fields = 0
        for page_num in range(doc.page_count):
            page = doc[page_num]

            # Get all widgets (form fields) on the page
            widgets = list(page.widgets())  # Convert generator to list

            if widgets:
                print(f"Found {len(widgets)} form fields on page {page_num + 1}")

                for widget in widgets:
                    # Check if it's a text field
                    if widget.field_type == fitz.PDF_WIDGET_TYPE_TEXT:
                        # Fill the field with "Dummy Data"
                        widget.field_value = "Dummy Data"
                        widget.update()
                        filled_fields += 1
                        print(f"  Filled field: {widget.field_name or 'Unnamed'}")

                    # Handle other field types that might accept text
                    elif widget.field_type in [fitz.PDF_WIDGET_TYPE_COMBOBOX,
                                               fitz.PDF_WIDGET_TYPE_LISTBOX]:
                        widget.field_value = "Dummy Data"
                        widget.update()
                        filled_fields += 1
                        print(f"  Filled field: {widget.field_name or 'Unnamed'}")

        print(f"\nTotal fields filled: {filled_fields}")

        # Create a new document for the flattened version
        flattened_doc = fitz.open()

        # Rasterize each page to flatten the form
        for page_num in range(doc.page_count):
            page = doc[page_num]

            # Render the page as a high-resolution image
            # Use a very high DPI for maximum quality (600 DPI)
            mat = fitz.Matrix(600 / 72, 600 / 72)  # 600 DPI scaling matrix
            # Get additional rendering options for best quality
            pix = page.get_pixmap(matrix=mat, alpha=False)  # alpha=False for smaller file size

            # Create a new page in the flattened document
            # Get the original page dimensions
            rect = page.rect
            new_page = flattened_doc.new_page(width=rect.width, height=rect.height)

            # Insert the rasterized image into the new page
            new_page.insert_image(rect, pixmap=pix)

            print(f"Rasterized page {page_num + 1}")

        # Save the flattened document
        flattened_doc.save(output_pdf_path)
        print(f"\nFlattened PDF saved as: {output_pdf_path}")

        # Close documents
        doc.close()
        flattened_doc.close()

        return True

    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return False


def main():
    """Main function to execute the PDF form filling and flattening."""

    input_pdf = "401k_at_a_glance_acroform.pdf"
    output_pdf = "401k_at_a_glance_acroform_filled.pdf"

    # Check if input file exists
    if not os.path.exists(input_pdf):
        print(f"Error: Input file '{input_pdf}' not found!")
        print("Please make sure the PDF file is in the same directory as this script.")
        sys.exit(1)

    print("PDF AcroForm Filler and Flattener")
    print("=" * 40)
    print(f"Input file: {input_pdf}")
    print(f"Output file: {output_pdf}")
    print()

    # Process the PDF
    success = fill_and_flatten_pdf(input_pdf, output_pdf)

    if success:
        print("\n" + "=" * 40)
        print("SUCCESS: PDF processing completed!")
        print(f"- All text fields filled with 'Dummy Data'")
        print(f"- Form flattened using rasterization at 600 DPI")
        print(f"- Output saved as: {output_pdf}")
    else:
        print("\n" + "=" * 40)
        print("ERROR: PDF processing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
