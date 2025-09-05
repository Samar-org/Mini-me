import os
from dotenv import load_dotenv
from pyairtable import Api
import requests

# Load environment
load_dotenv()
API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

SOURCE_TABLE = "x-Pay4more-Fetch_me_url"
DEST_TABLE = "Items-Pay4more"
SKU_FIELD = "SKU"

# Initialize Airtable API
api = Api(API_KEY)
source_table = api.table(BASE_ID, SOURCE_TABLE)
dest_table = api.table(BASE_ID, DEST_TABLE)

def fetch_all_skus(table, field):
    """Returns a set of all SKU values in the specified table."""
    records = table.all(fields=[field])
    return set(r['fields'].get(field, '').strip() for r in records if field in r['fields'])

def replicate_images(image_list):
    """Creates a new image attachment list (used to duplicate Airtable-hosted images)."""
    new_images = []
    for img in image_list:
        if isinstance(img, dict) and 'url' in img:
            new_images.append({'url': img['url']})
    return new_images

def replicate_record_data(source_fields):
    """Prepare field data for new record, including handling images."""
    new_fields = {}
    for key, value in source_fields.items():
        if isinstance(value, list) and all(isinstance(v, dict) and 'url' in v for v in value):
            # Assume it's an attachment/image list
            new_fields[key] = replicate_images(value)
        else:
            new_fields[key] = value
    return new_fields

def main():
    print("🔍 Fetching SKUs from destination table...")
    existing_skus = fetch_all_skus(dest_table, SKU_FIELD)
    print(f"✅ Found {len(existing_skus)} existing SKUs in {DEST_TABLE}")

    print("📦 Fetching records from source table...")
    source_records = source_table.all()
    created_count = 0

    for record in source_records:
        fields = record.get("fields", {})
        sku = fields.get(SKU_FIELD, "").strip()

        if not sku:
            print(f"⚠️ No SKU found in record {record['id']}, skipping.")
            continue
        if sku in existing_skus:
            print(f"⏩ SKU {sku} already exists, skipping.")
            continue

        new_fields = replicate_record_data(fields)
        dest_table.create(new_fields)
        created_count += 1
        print(f"✅ Created new record for SKU: {sku}")

    print(f"🎉 Done! {created_count} new records created.")

if __name__ == "__main__":
    main()
