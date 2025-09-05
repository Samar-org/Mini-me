import os
import csv
from datetime import date
from pyairtable import Api
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

def get_table_structure_csv(table_name, sample_size=3):
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, table_name)

    # Fetch sample records
    records = table.all(max_records=sample_size)
    if not records:
        print("No records found in the table.")
        return

    # Infer field types
    fields = {}
    for record in records:
        for key, value in record["fields"].items():
            current_type = type(value).__name__
            if key not in fields:
                fields[key] = current_type
            elif fields[key] != current_type:
                fields[key] = "Mixed"

    # Prepare filename
    today = date.today().isoformat()
    filename = f"{table_name}_{today}.csv"

    # Write to CSV
    with open(filename, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Field Name", "Inferred Type"])
        for field, field_type in fields.items():
            writer.writerow([field, field_type])

    print(f"Structure saved to: {filename}")

if __name__ == "__main__":
    table_to_inspect = input("Enter the table name: ").strip()
    get_table_structure_csv(table_to_inspect)
