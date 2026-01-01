import time
from io import BytesIO
import requests
from pyairtable import Api
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By

# ✅ Airtable details
AIRTABLE_API_KEY = "patORcFPSvwabTvGV.ff78ce60800192e321417836e531ab24ff6e1a2ae634546e2c15c6a8ddfe9a57"
BASE_ID = "appjvhsxUUz6o0dzo"
TABLE_NAME = "PEO 5500s"
DOC_LINK_FIELD = "doc link"
ATTACHMENT_FIELD = "Form 5500"

# ✅ Airtable connection
api = Api(AIRTABLE_API_KEY)
table = api.base(BASE_ID).table(TABLE_NAME)

# ✅ Edge browser configuration
EDGE_PROFILE_PATH = r"C:\Users\navee\AppData\Local\Microsoft\Edge\User Data"
EDGE_PROFILE_DIR = "Default"
EDGE_DRIVER_PATH = r"C:\Users\navee\Downloads\edgedriver\msedgedriver.exe"


options = Options()
options.add_argument(f"--user-data-dir={EDGE_PROFILE_PATH}")
options.add_argument(f"--profile-directory={EDGE_PROFILE_DIR}")
options.add_argument(f"--profile-directory=Default")
options.add_argument("--start-maximized")

driver_service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=driver_service, options=options)

# ✅ Download and upload process
records = table.all(max_records=100)

for record in records:
    record_id = record["id"]
    doc_url = record["fields"].get(DOC_LINK_FIELD)

    if not doc_url:
        print(f"⏭️ No download link for record {record_id}, skipping.")
        continue

    print(f"🌐 Navigating to {doc_url}")
    driver.get(doc_url)
    time.sleep(7)  # Adjust delay depending on site load/download

    # Attempt to fetch file with existing browser session cookies
    session_cookies = driver.get_cookies()
    cookie_dict = {cookie['name']: cookie['value'] for cookie in session_cookies}

    try:
        response = requests.get(doc_url, cookies=cookie_dict)
        if response.status_code != 200:
            print(f"❌ Could not download file for {record_id} (status {response.status_code})")
            continue

        file_bytes = BytesIO(response.content)

        # Upload to file.io
        print("📤 Uploading to file.io...")
        upload_response = requests.post("https://file.io", files={"file": ("form5500.pdf", file_bytes)})
        upload_json = upload_response.json()
        file_url = upload_json.get("link")

        if not file_url:
            print(f"❌ Upload failed: {upload_json}")
            continue

        # Update Airtable
        table.update(record_id, {ATTACHMENT_FIELD: [{"url": file_url}]})
        print(f"✅ Attached file to record {record_id}")

    except Exception as e:
        print(f"❌ Error handling record {record_id}: {e}")
        continue

driver.quit()
print("🎉 Done!")
