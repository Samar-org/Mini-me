import os
from dotenv import load_dotenv
from pyairtable import Api
from pyairtable.formulas import match

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Airtable config
SOURCE_TABLE = "x-Pay4more-Fetch_me_url"
DEST_TABLE = "Items-Pay4more"
SOURCE_PHOTO_FIELD = "Photo Files"
DEST_PHOTO_FIELD = "Photo Files"  # Updated here
SKU_FIELD = "SKU"

# Connect to Airtable
api = Api(API_KEY)
source_table = api.table(BASE_ID, SOURCE_TABLE)
dest_table = api.table(BASE_ID, DEST_TABLE)

# Get all source records
source_records = source_table.all()
print(f"Found {len(source_records)} source records.")

updated_count = 0
skipped = 0

def to_url_list(attachments):
    return [p["url"] for p in attachments if "url" in p]

for record in source_records:
    sku = record["fields"].get(SKU_FIELD)
    photos = record["fields"].get(SOURCE_PHOTO_FIELD)

    if not sku or not photos:
        skipped += 1
        continue

    # Match destination record by SKU
    formula = match({SKU_FIELD: sku})
    dest_records = dest_table.all(formula=formula)

    if not dest_records:
        print(f"❌ SKU '{sku}' not found in destination table.")
        skipped += 1
        continue

    dest_record = dest_records[0]
    dest_record_id = dest_record["id"]

    # Extract existing photo URLs
    existing_photos = dest_record["fields"].get(DEST_PHOTO_FIELD, [])
    existing_urls = to_url_list(existing_photos)
    new_urls = to_url_list(photos)

    # Merge and deduplicate
    all_urls = list(dict.fromkeys(existing_urls + new_urls))
    combined_photos = [{"url": url} for url in all_urls]

    # Update destination record
    dest_table.update(dest_record_id, {DEST_PHOTO_FIELD: combined_photos})
    updated_count += 1
    print(f"✅ Updated SKU '{sku}' with {len(new_urls)} new photos.")

print(f"\n✅ Done. {updated_count} records updated, {skipped} skipped.")
