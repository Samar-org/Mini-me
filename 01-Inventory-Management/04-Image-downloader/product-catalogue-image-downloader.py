import os
import requests
from datetime import datetime
from pyairtable import Api
from dotenv import load_dotenv

# --- ⚙️ CONFIGURATION ---
# Load variables from the .env file into the environment
load_dotenv()

# Get credentials from environment variables
AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# --- Airtable & Field Names (Must match your Airtable setup exactly) ---
TABLE_NAME = "Product Catalogue"
VIEW_NAME = "Image-Optimization"          # Fetch records only from this view
PRODUCT_CODE_FIELD = "4more-Product-Code"
PRIMARY_PHOTOS_FIELD = "Photo Files"      # First photo field
FEATURED_PHOTOS_FIELD = "Featured Photos" # Second photo field


def download_airtable_images():
    """
    Connects to a specific Airtable view, fetches records, and downloads images
    from two separate attachment fields into a timestamped folder.
    """
    # --- 1. Validate Configuration & Setup ---
    if not AIRTABLE_TOKEN or not BASE_ID:
        print("❌ Error: Make sure AIRTABLE_API_KEY and AIRTABLE_BASE_ID are set in your .env file.")
        return

    # Create a unique, timestamped folder name for this run
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_folder = f"Unprocessed image-{timestamp}"
    os.makedirs(output_folder, exist_ok=True)
    print(f"✅ Images will be saved to the new folder: '{output_folder}'")

    # --- 2. Connect to Airtable ---
    try:
        api = Api(AIRTABLE_TOKEN)
        table = api.table(BASE_ID, TABLE_NAME)
        print(f"🔗 Successfully connected to Airtable base '{BASE_ID}', table '{TABLE_NAME}'.")
    except Exception as e:
        print(f"❌ Error connecting to Airtable: {e}")
        return

    # --- 3. Fetch and Process Records from the specific view ---
    print(f"\n⏳ Fetching records from the '{VIEW_NAME}' view...")
    try:
        # Use the `view` parameter to fetch only records from the specified view
        records_in_view = table.all(view=VIEW_NAME)
        print(f"👍 Found {len(records_in_view)} records in the view to process.")

        for record in records_in_view:
            fields = record.get('fields', {})
            product_code = fields.get(PRODUCT_CODE_FIELD)

            if not product_code:
                print(f"⚠️ Skipping record ID '{record['id']}' (missing '{PRODUCT_CODE_FIELD}').")
                continue

            # Get images from both "Photo Files" and "Featured Photos" and combine them
            primary_photos = fields.get(PRIMARY_PHOTOS_FIELD) or []
            featured_photos = fields.get(FEATURED_PHOTOS_FIELD) or []
            all_photos = primary_photos + featured_photos

            if not all_photos:
                continue

            print(f"\nProcessing product: '{product_code}' ({len(all_photos)} total images)")
            image_counter = 1

            for attachment in all_photos:
                image_url = attachment.get('url')
                original_filename = attachment.get('filename')

                if not image_url or not original_filename:
                    print(f"  - ⚠️ Skipping an attachment for '{product_code}' due to missing data.")
                    continue

                # --- 4. Download and Save Image ---
                try:
                    _, file_extension = os.path.splitext(original_filename)

                    new_filename = f"4more-{product_code}-{image_counter}{file_extension}"
                    file_path = os.path.join(output_folder, new_filename)

                    print(f"  - Downloading '{original_filename}' -> '{new_filename}'")
                    response = requests.get(image_url, stream=True)
                    response.raise_for_status()

                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    image_counter += 1

                except requests.exceptions.RequestException as e:
                    print(f"  - ❌ Error downloading image for '{product_code}': {e}")
                except IOError as e:
                    print(f"  - ❌ Error saving file for '{product_code}': {e}")

    except Exception as e:
        print(f"❌ An unexpected error occurred while fetching or processing: {e}")

    print("\n🎉 Script finished.")


# --- Run the main function ---
if __name__ == "__main__":
    download_airtable_images()