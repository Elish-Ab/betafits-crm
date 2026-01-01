from fpdf import FPDF
import requests
import json

# Step 1: Create PDF
pdf_filename = "love.pdf"
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", size=36)
pdf.cell(200, 100, txt="Love", ln=True, align='C')
pdf.output(pdf_filename)

# Step 2: Upload PDF to tmpfiles.org
# Step 2: Upload PDF to tmpfiles.org
with open(pdf_filename, "rb") as f:
    res = requests.post("https://tmpfiles.org/api/v1/upload", files={"file": f})
    response_data = res.json()["data"]

    # Extract numeric ID safely
    import re
    match = re.search(r"tmpfiles\.org/(\d+)/", response_data["url"])
    if match:
        file_id = match.group(1)
        file_url = f"https://tmpfiles.org/dl/{file_id}/love.pdf"
    else:
        raise ValueError("❌ Failed to extract file ID from tmpfiles.org URL.")

print("📎 Direct PDF URL:", file_url)

# Step 3: Attach to Airtable
airtable_url = "https://api.airtable.com/v0/appjvhsxUUz6o0dzo/tblf4Ed9PaDo76QHH/recRWkxFKVghkfpQc"
headers = {
    "Authorization": "Bearer patORcFPSvwabTvGV.ff78ce60800192e321417836e531ab24ff6e1a2ae634546e2c15c6a8ddfe9a57",
    "Content-Type": "application/json"
}
payload = {
    "fields": {
        "test attach": [{"url": file_url}]
    }
}

response = requests.patch(airtable_url, headers=headers, data=json.dumps(payload))
print("✅ Airtable PATCH status:", response.status_code)

# Step 4: Confirm attachment
if response.status_code == 200:
    updated = response.json()
    attachments = updated.get("fields", {}).get("test attach", [])
    if not attachments:
        print("⚠️ Attachment field is still empty.")
    else:
        print("✅ Attachment added:")
        for item in attachments:
            print("   -", item.get("filename"), "->", item.get("url"))
else:
    print("❌ Airtable error:", response.text)
