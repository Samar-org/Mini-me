import csv
from pyairtable import Api
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

SOURCE_TABLE = "x-Pay4more-Fetch_me_url"
TARGET_TABLE = "Items-Pay4more"
SKU_FIELD = "SKU"

# Fields allowed in Items-Pay4more table
ALLOWED_FIELDS = [
    "Product Name", "Item Type", "SKU", "Description", "Photo Files",
    "Quantity", "Unit Retail Price", "Total Price", "Warehouse",
    "Shelf Location", "Dimensions", "Weight", "Condition", "Inspection Photos",
    "Optimized-Item Photos", "Item Photos", "Item Featured Photo",
    "Marketplaces", "Category", "Subcategory", "Brand", "Model", "Tags",
    "Buy Price", "Price Notes", "Item Notes"
]

# Fields that are Airtable attachments
ATTACHMENT_FIELDS = [
    "Photo Files", "Item Photos", "Inspection Photos",
    "Optimized-Item Photos", "Item Featured Photo"
]

# Airtable client
api = Api(AIRTABLE_API_KEY)
source_table = api.table(BASE_ID, SOURCE_TABLE)
target_table = api.table(BASE_ID, TARGET_TABLE)

# Normalize Airtable attachment fields
def clean_attachments(fields: dict) -> dict:
    cleaned = {}
    for key, value in fields.items():
        if key in ATTACHMENT_FIELDS:
            if isinstance(value, list):
                cleaned[key] = [{"url": item["url"]} for item in value if isinstance(item, dict) and "url" in item]
        else:
            cleaned[key] = value
    return cleaned

# Read SKU list from CSV and sync records
with open("skus.csv", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sku = row.get("SKU")
        if not sku:
            continue

        # Skip if SKU already exists
        existing = target_table.all(formula=f"{{{SKU_FIELD}}} = '{sku}'")
        if existing:
            print(f"⏩ SKU {sku} already exists in target — skipping.")
            continue

        # Fetch source record
        matches = source_table.all(formula=f"{{{SKU_FIELD}}} = '{sku}'")
        if not matches:
            print(f"⚠️ No record found in source for SKU: {sku}")
            continue

        raw_fields = matches[0]['fields']
        filtered = {k: v for k, v in raw_fields.items() if k in ALLOWED_FIELDS}
        cleaned = clean_attachments(filtered)

        try:
            target_table.create(cleaned)
            print(f"✅ Created item for SKU: {sku}")
        except Exception as e:
            print(f"❌ Error creating SKU {sku}: {e}")
