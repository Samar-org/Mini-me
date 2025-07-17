import os
import requests
from pyairtable import Api
from dotenv import load_dotenv

# --- ⚙️ CONFIGURATION ---
# Load variables from the .env file into the environment
load_dotenv()

# Get credentials from environment variables
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# --- Field Names (Must match your Airtable column headers exactly) ---
TABLE_NAME = "Product Catalogue"
PRODUCT_CODE_FIELD = "4more-Product-Code"  # <-- This line has been updated
PHOTOS_FIELD = "Photo Files"

# --- Output Folder ---
OUTPUT_FOLDER = "unprocessed images"


def download_airtable_images():
    """
    Connects to Airtable, fetches records, and downloads images,
    renaming them based on a product code.
    """
    # --- 1. Validate Configuration ---
    if not AIRTABLE_TOKEN or not BASE_ID:
        print(
            "❌ Error: Make sure AIRTABLE_API_KEY and AIRTABLE_BASE_ID are set in your .env file."
        )
        return

    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    print(f"✅ Images will be saved to the '{OUTPUT_FOLDER}' folder.")

    # --- 2. Connect to Airtable ---
    try:
        api = Api(AIRTABLE_TOKEN)
        table = api.table(BASE_ID, TABLE_NAME)
        print(
            f"🔗 Successfully connected to Airtable base '{BASE_ID}', table '{TABLE_NAME}'."
        )
    except Exception as e:
        print(f"❌ Error connecting to Airtable: {e}")
        return

    # --- 3. Fetch and Process Records ---
    print("\n⏳ Fetching all records... this may take a moment.")
    try:
        all_records = table.all()
        print(f"👍 Found {len(all_records)} records to process.")

        for record in all_records:
            fields = record.get("fields", {})
            # Get the product code using the updated field name
            product_code = fields.get(PRODUCT_CODE_FIELD)
            photos = fields.get(PHOTOS_FIELD)

            if not product_code:
                print(
                    f"⚠️ Skipping record ID '{record['id']}' (missing '{PRODUCT_CODE_FIELD}')."
                )
                continue
            if not photos:
                continue

            print(f"\nProcessing product: '{product_code}'")
            image_counter = 1

            for attachment in photos:
                image_url = attachment.get("url")
                original_filename = attachment.get("filename")

                if not image_url or not original_filename:
                    print(
                        f"  - ⚠️ Skipping an attachment for '{product_code}' due to missing data."
                    )
                    continue

                # --- 4. Download and Save Image ---
                try:
                    # Get the file extension (e.g., .jpg, .png)
                    _, file_extension = os.path.splitext(original_filename)

                    # Create the new filename
                    new_filename = (
                        f"4more-{product_code}-{image_counter}{file_extension}"
                    )
                    file_path = os.path.join(OUTPUT_FOLDER, new_filename)

                    print(f"  - Downloading '{original_filename}' -> '{new_filename}'")
                    response = requests.get(image_url, stream=True)
                    response.raise_for_status()  # Raise an error for bad responses

                    # Save the image file
                    with open(file_path, "wb") as f:
                        f.write(response.content)

                    image_counter += 1

                except requests.exceptions.RequestException as e:
                    print(f"  - ❌ Error downloading image for '{product_code}': {e}")
                except IOError as e:
                    print(f"  - ❌ Error saving file for '{product_code}': {e}")

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

    print("\n🎉 Script finished.")


# --- Run the main function ---
if __name__ == "__main__":
    download_airtable_images()
