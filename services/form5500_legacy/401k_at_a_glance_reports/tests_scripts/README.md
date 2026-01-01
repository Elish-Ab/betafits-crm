# Test and Utility Scripts

This folder contains helper scripts used for testing, debugging, and verification.  
These scripts are **not** part of the production 401k report pipeline.

---

## 📜 Scripts Overview

### `acroform_fields.py`
Prints all acroform field names from the PDF template.  
Useful for ensuring CSV columns match PDF field names.

### `flatten_acroform.py`
Fills the PDF with placeholder values and flattens it.  
Used to inspect field alignment, text positioning, and flattening quality.

### `attach_file_to_airtable.py`
Creates a small test PDF and attaches it to a test Airtable record.  
Validates Airtable connectivity and attachment logic.

### `attach_file_to_airtable1.py`
Variation of the above that extracts tmpfiles URLs using regex before attaching to Airtable.

---

## 🧪 When to Use These Scripts

Use the scripts in this folder when:

- Testing PDF acroform field behavior  
- Debugging PDF flattening or rendering  
- Verifying Airtable attachment logic  
- Experimenting without running the full production pipeline  

These scripts are optional and do **not** affect the main report generation workflow.
