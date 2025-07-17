#!/usr/bin/env python3
import os
import sys
import logging
import requests
import re
import time
from io import BytesIO
from PIL import Image, ImageEnhance
from woocommerce import API as WCAPI
from pyairtable import Api
from typing import Dict, List, Optional, Any, Tuple

# Load .env if you use one
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("product_sync.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Airtable Configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
# WooCommerce Configuration
WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WC_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")
# WordPress Application Password for Media Upload
WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")


# E-commerce Optimization Settings
class SyncConfig:
    IMAGE_SIZE: Tuple[int, int] = (1080, 1080)
    IMAGE_QUALITY: int = 85  # For JPEG fallback
    WEBP_QUALITY: int = 80
    UPLOAD_DELAY: float = 0.5  # Delay between image uploads
    WC_API_TIMEOUT: int = 90


config = SyncConfig()

# --- Validate Environment Variables ---
REQUIRED_VARS = [
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
    "WC_URL",
    "WC_CONSUMER_KEY",
    "WC_CONSUMER_SECRET",
    "WP_USER",
    "WP_PASS",
]
missing_vars = [var for var in REQUIRED_VARS if not globals().get(var)]
if missing_vars:
    logger.error(
        f"FATAL: Missing required environment variables: {', '.join(missing_vars)}"
    )
    sys.exit(1)

# --- API Clients ---
# Airtable
airtable_api = Api(AIRTABLE_API_KEY)
items_table = airtable_api.table(AIRTABLE_BASE_ID, "Items-Pay4more")
catalogue_table = airtable_api.table(AIRTABLE_BASE_ID, "Product Catalogue")

# WooCommerce
wcapi = WCAPI(
    url=WC_URL,
    consumer_key=WC_CONSUMER_KEY,
    consumer_secret=WC_CONSUMER_SECRET,
    wp_api=True,
    version="wc/v3",
    timeout=config.WC_API_TIMEOUT,
)

# --- Field & Meta Mappings ---
# Maps WooCommerce fields to Airtable field names
FIELD_MAPPINGS = {
    "name": "Product Name",
    "description": "Description",
    "short_description": "Meta Description",
    "regular_price": "Unit Retail Price",
    "sale_price": "4more Price",
    "sku": "4more-Product-Code",
    "stock_quantity": "Quantity",
    "weight": "Weight",
}

# Maps WooCommerce meta keys to Airtable field names for SEO plugins
META_MAPPINGS = {
    "rank_math_title": "Meta Title",
    "rank_math_focus_keyword": "Focus Keyword",
    "rank_math_description": "Meta Description",
}

# Image fields in the order of priority
IMAGE_FIELDS_ORDER = ["Featured Photos", "Photo Files"]


# --- Helper Functions ---
def first(x: Any) -> Any:
    """Returns the first element of a list, or the item itself if not a list."""
    if isinstance(x, list) and x:
        return x[0]
    return x


def find_catalogue_by_code(code: str) -> Optional[Dict]:
    """Finds a record in the 'Product Catalogue' table by its product code."""
    try:
        formula = f'{{4more-Product-Code}}="{code}"'
        records = catalogue_table.all(formula=formula)
        if records:
            logger.info(f"Found matching record in Product Catalogue for SKU: {code}")
            return records[0]
        logger.warning(f"No matching record in Product Catalogue for SKU: {code}")
        return None
    except Exception as e:
        logger.error(f"Error searching Product Catalogue for SKU {code}: {e}")
        return None


def generate_seo_filename(product_name: str, original_filename: str, index: int) -> str:
    """Generates an SEO-friendly filename."""
    clean_name = re.sub(r"[^\w\s-]", "", product_name.lower()).strip()
    clean_name = re.sub(r"[-\s]+", "-", clean_name)[:50]
    # Use original extension, but default to jpg if unknown
    ext = "jpg"
    if "." in original_filename:
        ext = original_filename.rsplit(".", 1)[-1].lower()
        if ext not in ["jpeg", "jpg", "png", "webp"]:
            ext = "jpg"

    return f"{clean_name}-{index + 1}.{ext}"


def optimize_and_upload_image(
    image_url: str, product_name: str, index: int
) -> Optional[int]:
    """Downloads, optimizes, and uploads an image to WordPress, returning the media ID."""
    try:
        logger.info(f"Processing image {index + 1}: {image_url}")
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content))

        # Convert to RGB if it has an alpha channel (like PNG) to avoid black backgrounds
        if img.mode in ("RGBA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3] if img.mode == "RGBA" else None)
            img = background

        # Resize to fit within the target dimensions while maintaining aspect ratio
        img.thumbnail(config.IMAGE_SIZE, Image.Resampling.LANCZOS)

        # Sharpen slightly for better detail
        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.1)

        # Prepare to save to buffer
        buffer = BytesIO()
        content_type = "image/jpeg"
        final_filename = generate_seo_filename(
            product_name, image_url.split("/")[-1], index
        )

        # Try to save as WebP for best performance
        try:
            img.save(buffer, format="WEBP", quality=config.WEBP_QUALITY)
            content_type = "image/webp"
            final_filename = final_filename.rsplit(".", 1)[0] + ".webp"
            logger.info("Image optimized to WebP format.")
        except Exception:
            logger.warning("WebP conversion failed, falling back to JPEG.")
            buffer.seek(0)  # Reset buffer
            img.save(
                buffer,
                format="JPEG",
                quality=config.IMAGE_QUALITY,
                optimize=True,
                progressive=True,
            )
            content_type = "image/jpeg"
            final_filename = final_filename.rsplit(".", 1)[0] + ".jpg"

        buffer.seek(0)

        # Upload to WordPress Media Library
        media_url = f"{WC_URL}/wp-json/wp/v2/media"
        headers = {
            "Content-Disposition": f'attachment; filename="{final_filename}"',
            "Content-Type": content_type,
        }
        auth = (WP_USER, WP_PASS)

        upload_response = requests.post(
            media_url, headers=headers, data=buffer, auth=auth, timeout=60
        )
        upload_response.raise_for_status()

        media_data = upload_response.json()
        media_id = media_data["id"]
        logger.info(f"Successfully uploaded image, Media ID: {media_id}")

        # Update media metadata for SEO
        metadata_payload = {
            "alt_text": f"{product_name} - view {index + 1}",
            "title": f"{product_name} Image {index + 1}",
        }
        meta_update_url = f"{WC_URL}/wp-json/wp/v2/media/{media_id}"
        requests.post(meta_update_url, json=metadata_payload, auth=auth, timeout=30)

        return media_id

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download or upload image {image_url}: {e}")
    except Exception as e:
        logger.error(
            f"An error occurred during image optimization for {image_url}: {e}"
        )

    return None


def upload_and_attach_images(fields: Dict, product_name: str) -> List[Dict[str, int]]:
    """
    Processes images from specified Airtable fields in order, optimizes them,
    and returns a list of WooCommerce image dictionaries.
    """
    wc_images = []
    image_urls = []

    # Collect all image URLs from the specified fields in order
    for field_name in IMAGE_FIELDS_ORDER:
        if field_name in fields and isinstance(fields[field_name], list):
            for attachment in fields[field_name]:
                if "url" in attachment:
                    image_urls.append(attachment["url"])

    if not image_urls:
        logger.info("No images found for this product.")
        return []

    logger.info(
        f"Found {len(image_urls)} total images to process for '{product_name}'."
    )

    for i, url in enumerate(image_urls):
        media_id = optimize_and_upload_image(url, product_name, i)
        if media_id:
            wc_images.append({"id": media_id})
        time.sleep(config.UPLOAD_DELAY)  # Be kind to the server

    return wc_images


def process_single_product(record: Dict):
    """Fetches, merges, and syncs a single product record to WooCommerce."""
    item_fields = record.get("fields", {})
    sku = first(item_fields.get(FIELD_MAPPINGS["sku"]))

    if not sku:
        logger.warning(f"Skipping record {record['id']} due to missing SKU.")
        return

    logger.info(f"--- Processing SKU: {sku} ---")

    # Fetch and merge data from Product Catalogue
    catalogue_record = find_catalogue_by_code(sku)
    catalogue_fields = catalogue_record.get("fields", {}) if catalogue_record else {}
    merged_fields = {
        **catalogue_fields,
        **item_fields,
    }  # Item fields will overwrite catalogue fields

    product_name = merged_fields.get(FIELD_MAPPINGS["name"], sku)

    # Construct the WooCommerce payload
    payload = {
        "type": "simple",
        "name": product_name,
        "sku": sku,
        "status": "publish",
        "manage_stock": True,
    }

    # Map standard fields
    for wc_field, at_field in FIELD_MAPPINGS.items():
        val = first(merged_fields.get(at_field))
        if val is not None and val != "":
            payload[wc_field] = (
                str(val)
                if wc_field in ["regular_price", "sale_price", "weight"]
                else val
            )

    # Map meta fields for SEO
    meta_data = []
    for meta_key, at_field in META_MAPPINGS.items():
        val = first(merged_fields.get(at_field))
        if val:
            meta_data.append({"key": meta_key, "value": val})
    if meta_data:
        payload["meta_data"] = meta_data

    # Process and upload images
    wc_images = upload_and_attach_images(merged_fields, product_name)
    if wc_images:
        payload["images"] = wc_images

    # Check for existing product and either update or create
    try:
        existing_check = wcapi.get("products", params={"sku": sku})
        existing_check.raise_for_status()
        existing = existing_check.json()

        if existing and isinstance(existing, list):
            product_id = existing[0]["id"]
            logger.info(
                f"Product with SKU {sku} exists (ID: {product_id}). Updating..."
            )
            response = wcapi.put(f"products/{product_id}", payload)
        else:
            logger.info(
                f"Product with SKU {sku} does not exist. Creating new product..."
            )
            response = wcapi.post("products", payload)

        response.raise_for_status()
        logger.info(
            f"Successfully synced product SKU: {sku}. Response Code: {response.status_code}"
        )

    except requests.exceptions.RequestException as e:
        logger.error(
            f"API Error processing SKU {sku}: {e.response.text if e.response else e}"
        )
    except Exception as e:
        logger.error(f"An unexpected error occurred for SKU {sku}: {e}")


def main():
    """Main function to run the sync process."""
    logger.info("================================================")
    logger.info("Starting Pay4more Product Sync...")
    logger.info(f"Source Airtable View: Pay4more Sync View")
    logger.info("================================================")

    try:
        # Fetch records ONLY from the specified view
        records_to_sync = items_table.all(view="Pay4more Sync View")

        if not records_to_sync:
            logger.info("No products found in the 'Pay4more Sync View'. Sync complete.")
            return

        logger.info(f"Found {len(records_to_sync)} products to sync from the view.")

        for i, record in enumerate(records_to_sync):
            logger.info(f"Processing record {i + 1} of {len(records_to_sync)}...")
            process_single_product(record)

        logger.info("================================================")
        logger.info("Product sync process finished.")
        logger.info("================================================")

    except Exception as e:
        logger.error(f"A fatal error occurred during the sync process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
