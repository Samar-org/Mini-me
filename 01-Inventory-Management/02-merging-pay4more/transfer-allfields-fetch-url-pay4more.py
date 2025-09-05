import os
from pyairtable import Api
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Airtable table names
SOURCE_TABLE = "x-Pay4more-Fetch_me_url"
DEST_TABLE = "Items-Pay4more"

# Connect to Airtable
api = Api(AIRTABLE_API_KEY)
source_table = api.table(AIRTABLE_BASE_ID, SOURCE_TABLE)
dest_table = api.table(AIRTABLE_BASE_ID, DEST_TABLE)

# Clean attachment field for Airtable (keep only url and optional filename)
def clean_attachments(attachments):
    return [{"url": att["url"], "filename": att.get("filename")} for att in attachments if "url" in att]

# Load all destination records once for fast SKU matching
print("🔄 Loading destination records...")
dest_records = dest_table.all()
sku_to_dest = {rec["fields"].get("SKU"): rec for rec in dest_records if "SKU" in rec["fields"]}

# Collect all destination field names
print("🔍 Collecting valid destination fields...")
dest_table_fields = set()
for rec in dest_records:
    dest_table_fields.update(rec.get("fields", {}).keys())

# Full list of fields to transfer (updated)
full_transfer_fields = [
    "Product URL", "Product Name", "Shelf Location", "Category", "Special Suppliers",
    "Scraping Status", "Unit Retail Price", "Currency", "4more Unit Price (CAD)", "Quantity",
    "Description", "Dimensions", "Weight", "Photos URL", "Photo Files", "Scraping Website", "Error",
    "Inspection Condition", "Inspection Notes", "Inspection Photos", "Box Condition",
    "Color", "Model", "Pallet", "Status", "Sale Price", "Original Price", "Processed By",
    "4more Store"
]

# Fetch source records
print("📦 Fetching source records...")
source_records = source_table.all()

copied = 0
skipped = 0
moved_only = 0

for record in source_records:
    fields = record.get("fields", {})
    sku = fields.get("SKU")

    if not sku:
        print(f"⚠️ No SKU in source record {record['id']}")
        skipped += 1
        continue

    if sku not in sku_to_dest:
        print(f"❌ SKU '{sku}' not found in destination table")
        skipped += 1
        continue

    is_moved = fields.get("Status", "").strip().lower() == "moved"

    # Case: Only update Product URL for moved items
    if is_moved:
        update_fields = {}
        if "Product URL" in fields:
            update_fields["Product URL"] = fields["Product URL"]
        print(f"↪️ Only updated 'Product URL' for moved item SKU: {sku}")
        moved_only += 1
    else:
        # Transfer full set of fields that exist and are valid
        update_fields = {}
        for f in full_transfer_fields:
            if f not in fields or f not in dest_table_fields:
                continue
            value = fields[f]
            if isinstance(value, list) and all(isinstance(x, dict) and "url" in x for x in value):
                update_fields[f] = clean_attachments(value)
            else:
                update_fields[f] = value
        print(f"✅ Full update for SKU: {sku}")

    # Update the destination record
    dest_table.update(sku_to_dest[sku]["id"], update_fields)
    copied += 1

print(f"\n✅ Sync complete. {copied} updated, {moved_only} were 'moved', {skipped} skipped.")
