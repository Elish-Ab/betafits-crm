from pypdf import PdfReader

# Load the PDF file
reader = PdfReader("401k_at_a_glance_acroform.pdf")

# Get the form fields
fields = reader.get_fields()

# Extract field names
field_names = list(fields.keys())

print(field_names)
