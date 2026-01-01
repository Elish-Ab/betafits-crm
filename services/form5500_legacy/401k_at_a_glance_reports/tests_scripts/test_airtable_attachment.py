import requests
from fpdf import FPDF
import os

# ---------- Step 1: Create a dummy PDF that says "Love" ----------
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", size=24)
pdf.cell(200, 100, txt="Love", ln=True, align="C")

pdf_filename = "love.pdf"
pdf.output(pdf_filename)

# ---------- Step 2: Upload the PDF to file.io ----------
with open("love.pdf", "rb") as f:
    res = requests.post("https://tmpfiles.org/api/v1/upload", files={"file": f})

file_url = "https://tmpfiles.org" + res.json()["data"]["url"]
print("📎 Uploaded PDF URL:", file_url)


# ---------- Step 3: Attach the file to Airtable ----------
airtable_base_id = "appjvhsxUUz6o0dzo"
airtable_table_id = "tblf4Ed9PaDo76QHH"
record_id = "recRWkxFKVghkfpQc"
airtable_pat = "patORcFPSvwabTvGV.ff78ce60800192e321417836e531ab24ff6e1a2ae634546e2c15c6a8ddfe9a57"

airtable_url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_id}/{record_id}"

headers = {
    "Authorization": f"Bearer {airtable_pat}",
    "Content-Type": "application/json"
}

data = {
    "fields": {
        "test attach": [
            {"url": file_url}
        ]
    }
}

airtable_response = requests.patch(airtable_url, json=data, headers=headers)

# ---------- Step 4: Handle Airtable response ----------
if airtable_response.ok:
    print("✅ PDF successfully attached to Airtable.")
else:
    print("❌ Failed to attach PDF to Airtable:", airtable_response.text)

# ---------- Optional: Clean up the local PDF file ----------
os.remove(pdf_filename)

