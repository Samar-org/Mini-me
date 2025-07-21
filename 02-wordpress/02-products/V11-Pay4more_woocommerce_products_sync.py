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
from typing import Dict, List, Optional, Any

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
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WC_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")
WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")


class SyncConfig:
    IMAGE_QUALITY: int = 85
    WEBP_QUALITY: int = 80
    UPLOAD_DELAY: float = 0.5
    WC_API_TIMEOUT: int = 90


config = SyncConfig()

# --- Validate Environment ---
REQUIRED_VARS = [
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
    "WC_URL",
    "WC_CONSUMER_KEY",
    "WC_CONSUMER_SECRET",
    "WP_USER",
    "WP_PASS",
]
if any(not globals().get(var) for var in REQUIRED_VARS):
    logger.error("FATAL: Missing one or more required environment variables.")
    sys.exit(1)

# --- API Clients ---
airtable_api = Api(AIRTABLE_API_KEY)
items_table = airtable_api.table(AIRTABLE_BASE_ID, "Items-Pay4more")
catalogue_table = airtable_api.table(AIRTABLE_BASE_ID, "Product Catalogue")
wcapi = WCAPI(
    url=WC_URL,
    consumer_key=WC_CONSUMER_KEY,
    consumer_secret=WC_CONSUMER_SECRET,
    version="wc/v3",
    timeout=config.WC_API_TIMEOUT,
)

# --- Field Mappings ---
ITEM_FIELD_MAPPINGS = {"sku": "SKU", "stock_quantity": "Quantity"}
CATALOGUE_FIELD_MAPPINGS = {
    "name": "Product Name",
    "description": "Description",
    "short_description": "Meta Description",
    "regular_price": "Unit Retail Price",
    "sale_price": "4more Unit Price",
    "weight": "Weight",
    "tags": "Product Tags",
    "category": "Category Name",
    "parent_category": "Parent Category Rollup (from Category)",
    "brand": "Brand",
}
SEO_FIELD_MAPPINGS = {
    "title": "Meta Title",
    "keywords": "Focus Keyword",
    "description": "Meta Description",
}
CUSTOM_FIELD_MAPPINGS = {
    "color": "Color",
    "model": "Model",
    "heavy": "Heavy",
    "fragile": "Fragile",
    "big": "Big",
    "condition": "Condition",
    "box_condition": "Box Condition",
}
# MODIFICATION: Only use the "Photo Files" field for images.
IMAGE_FIELDS_ORDER = ["Photo Files"]


# --- Helper Functions ---
def first(x: Any) -> Any:
    return x[0] if isinstance(x, list) and x else x


def find_catalogue_by_code(code: str) -> Optional[Dict]:
    try:
        formula = f"{{4more-Product-Code}} = '{code}'"
        records = catalogue_table.all(formula=formula, max_records=1)
        if records:
            return records[0]
        logger.error(f"No catalogue record found for code: {code}")
    except Exception as e:
        logger.error(f"Error searching catalogue for code {code}: {e}")
    return None


def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    if not isinstance(dimensions_str, str):
        return {}
    match = re.search(
        r"(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)", dimensions_str
    )
    return (
        {"length": match.group(1), "width": match.group(2), "height": match.group(3)}
        if match
        else {}
    )


def generate_seo_filename(product_name: str, original_filename: str, index: int) -> str:
    clean_name = re.sub(r"[^\w\s-]", "", product_name.lower()).strip()
    clean_name = re.sub(r"[-\s]+", "-", clean_name)[:50]
    ext = (
        original_filename.rsplit(".", 1)[-1].lower()
        if "." in original_filename
        else "jpg"
    )
    return f"{clean_name}-{index + 1}.{ext if ext in ['jpeg', 'jpg', 'png', 'webp'] else 'jpg'}"


def optimize_and_upload_image(
    image_url: str, product_name: str, alt_text: str, index: int
) -> Optional[int]:
    try:
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))
        if img.mode in ("RGBA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3] if img.mode == "RGBA" else None)
            img = background

        # MODIFICATION: Removed image resizing line `img.thumbnail(...)`

        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.1)
        buffer = BytesIO()
        final_filename = generate_seo_filename(
            product_name, image_url.split("/")[-1], index
        )
        try:
            img.save(buffer, format="WEBP", quality=config.WEBP_QUALITY)
            content_type, final_filename = (
                "image/webp",
                final_filename.rsplit(".", 1)[0] + ".webp",
            )
        except Exception:
            buffer.seek(0)
            img.save(
                buffer,
                format="JPEG",
                quality=config.IMAGE_QUALITY,
                optimize=True,
                progressive=True,
            )
            content_type, final_filename = (
                "image/jpeg",
                final_filename.rsplit(".", 1)[0] + ".jpg",
            )
        buffer.seek(0)
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
        media_id = upload_response.json()["id"]
        final_alt_text = alt_text if alt_text else f"{product_name} - view {index + 1}"
        metadata_payload = {
            "alt_text": final_alt_text,
            "title": f"{product_name} - Image {index + 1}",
            "caption": f"View of {product_name}",
            "description": f"Product image of {product_name} available at Pay4More.",
        }
        requests.post(
            f"{WC_URL}/wp-json/wp/v2/media/{media_id}",
            json=metadata_payload,
            auth=auth,
            timeout=30,
        )
        logger.info(
            f"✅ Uploaded image and full metadata: '{final_filename}' (ID: {media_id})"
        )
        return media_id
    except Exception as e:
        logger.error(f"Error processing image {image_url}: {e}")
        return None


def process_product_images(
    catalogue_fields: Dict, product_name: str, catalogue_code: str, image_cache: Dict
) -> List[Dict[str, int]]:
    if catalogue_code in image_cache:
        logger.info(f"Using cached images for catalogue code: {catalogue_code}")
        return image_cache[catalogue_code]
    image_urls = [
        att["url"]
        for field in IMAGE_FIELDS_ORDER
        if field in catalogue_fields and isinstance(catalogue_fields[field], list)
        for att in catalogue_fields[field]
        if "url" in att
    ]
    if not image_urls:
        return []
    alt_text = first(catalogue_fields.get("Image Alt Text", ""))
    wc_images = []
    for i, url in enumerate(image_urls):
        media_id = optimize_and_upload_image(url, product_name, alt_text, i)
        if media_id:
            wc_images.append({"id": media_id})
        time.sleep(config.UPLOAD_DELAY)
    image_cache[catalogue_code] = wc_images
    logger.info(
        f"Cached {len(wc_images)} new images for catalogue code: {catalogue_code}"
    )
    return wc_images


def get_or_create_term(
    name: str, taxonomy_slug: str, existing_terms: Dict[str, int]
) -> Optional[int]:
    if not isinstance(name, str) or not name.strip() or name.startswith("rec"):
        return None
    name_lower = name.lower().strip()
    if name_lower in existing_terms:
        return existing_terms[name_lower]

    endpoint = f"products/{taxonomy_slug}"
    try:
        logger.info(f"Creating new {taxonomy_slug}: '{name}'")
        response = wcapi.post(endpoint, {"name": name})
        response_data = response.json()
        if response.status_code == 201:
            term_id = response_data["id"]
            logger.info(f"✅ Created new {taxonomy_slug}: '{name}' (ID: {term_id})")
            existing_terms[name_lower] = term_id
            return term_id
        elif response.status_code == 400 and response_data.get("code") == "term_exists":
            existing_id = response_data.get("data", {}).get("resource_id")
            if existing_id:
                logger.warning(
                    f"Term '{name}' already exists with ID: {existing_id}. Using existing ID."
                )
                existing_terms[name_lower] = existing_id
                return existing_id
        else:
            logger.error(f"Failed to create {taxonomy_slug} '{name}': {response.text}")
    except Exception as e:
        logger.error(f"Exception creating {taxonomy_slug} '{name}': {e}")
    return None


def update_product_seo(product_id: int, seo_data: Dict):
    if not seo_data:
        return
    seo_endpoint_url = f"{WC_URL}/wp-json/pay4more/v1/update_seo"
    payload = {"product_id": product_id, **seo_data}
    logger.info(f"Updating SEO for product {product_id} via custom endpoint...")
    try:
        response = requests.post(
            seo_endpoint_url, json=payload, auth=(WP_USER, WP_PASS), timeout=30
        )
        response.raise_for_status()
        logger.info(f"✅ Successfully updated SEO for product {product_id}.")
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Failed to update SEO for product {product_id}: {e.response.text if e.response else e}"
        )


# --- Core Logic ---
def process_single_product(record: Dict, tax_terms: Dict, image_cache: Dict) -> bool:
    item_fields = record.get("fields", {})
    item_sku = first(item_fields.get(ITEM_FIELD_MAPPINGS["sku"]))
    catalogue_code = first(item_fields.get("4more-Product-Code-linked"))
    if not item_sku or not catalogue_code:
        logger.warning(
            f"Skipping record {record.get('id')}: Missing SKU or linked catalogue code."
        )
        return False

    catalogue_record = find_catalogue_by_code(catalogue_code)
    if not catalogue_record:
        logger.error(
            f"Could not find catalogue '{catalogue_code}' linked from item {item_sku}. Skipping."
        )
        return False

    catalogue_fields = catalogue_record.get("fields", {})
    product_name = first(catalogue_fields.get(CATALOGUE_FIELD_MAPPINGS["name"]))
    if not product_name:
        logger.warning(
            f"Catalogue {catalogue_code} is missing 'Product Name'. Skipping item {item_sku}."
        )
        return False

    logger.info(
        f"Processing Item SKU: {item_sku} -> Catalogue: {product_name} ({catalogue_code})"
    )

    payload = {"type": "simple", "status": "publish", "manage_stock": True}
    for wc_field, at_field in {
        **ITEM_FIELD_MAPPINGS,
        **CATALOGUE_FIELD_MAPPINGS,
    }.items():
        source_fields = (
            item_fields if wc_field in ITEM_FIELD_MAPPINGS else catalogue_fields
        )
        if wc_field in ["tags", "category", "parent_category", "brand"]:
            continue
        val = first(source_fields.get(at_field))
        if val is not None and val != "":
            if wc_field in ["regular_price", "sale_price"]:
                try:
                    val = f"{float(val):.2f}"
                except:
                    val = "0.00"
                payload[wc_field] = val
            elif wc_field == "weight":
                payload[wc_field] = str(val)
            else:
                payload[wc_field] = val
    # --- Process Taxonomies ---
    all_cat_names = set()
    if child_cats := catalogue_fields.get(CATALOGUE_FIELD_MAPPINGS["category"], []):
        all_cat_names.update(child_cats)
    if parent_cats := catalogue_fields.get(
        CATALOGUE_FIELD_MAPPINGS["parent_category"], []
    ):
        all_cat_names.update(parent_cats)
    if all_cat_names:
        payload["categories"] = [
            {"id": cat_id}
            for name in all_cat_names
            if (
                cat_id := get_or_create_term(
                    name, "categories", tax_terms["categories"]
                )
            )
        ]

    if tag_names := catalogue_fields.get("Product Tags", []):
        payload["tags"] = [
            {"id": tid}
            for name in tag_names
            if (tid := get_or_create_term(name, "tags", tax_terms["tags"]))
        ]

    if brand_name := first(catalogue_fields.get("brand")):
        if brand_id := get_or_create_term(brand_name, "brands", tax_terms["brands"]):
            payload["brands"] = [{"id": brand_id}]

    # --- Process Custom Fields with Yes/No logic ---
    meta_data = []
    boolean_to_text_fields = ["heavy", "fragile", "big"]
    for meta_key, at_field in CUSTOM_FIELD_MAPPINGS.items():
        val = first(catalogue_fields.get(at_field))

        if meta_key in boolean_to_text_fields:
            # Always add these fields, with a value of "Yes" or "No"
            meta_data.append({"key": meta_key, "value": "Yes" if val is True else "No"})
        elif val is not None and val != "":
            # For other fields, only add them if they have a value
            meta_data.append({"key": meta_key, "value": val})

    if meta_data:
        payload["meta_data"] = meta_data

    payload["dimensions"] = parse_dimensions(
        first(catalogue_fields.get("Dimensions", ""))
    )
    payload["images"] = process_product_images(
        catalogue_fields, product_name, catalogue_code, image_cache
    )

    # --- Create or Update Product ---
    product_id = None
    try:
        existing = wcapi.get("products", params={"sku": item_sku}).json()
        if existing:
            product_id = existing[0]["id"]
            logger.info(f"Updating existing product (ID: {product_id})...")
            response = wcapi.put(f"products/{product_id}", payload)
        else:
            logger.info(
                f"Product with SKU '{item_sku}' not found. Attempting to create new product..."
            )
            response = wcapi.post("products", payload)
        response.raise_for_status()
        product_id = response.json().get("id", product_id)
        logger.info(
            f"✅ Successfully synced product SKU: {item_sku} (ID: {product_id})"
        )
    except requests.exceptions.RequestException as e:
        error_data = {"code": "unknown", "message": str(e)}
        error_code = error_data.get("code")
        error_message = error_data.get("message")
        try:
            if e.response is not None:
                error_data = e.response.json()
        except ValueError:
            error_data = {"code": "invalid_json", "message": e.response.text}
        logger.error(
            f"❌ API Error while syncing SKU '{item_sku}': {error_code} - {error_message}"
        )
        if error_code == "product_invalid_sku":
            logger.error(
                "    ↳ This SKU might already be in use by a product in your WooCommerce trash."
            )
        logger.error(f"    ↳ Data payload that failed: {payload}")
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred for SKU {item_sku}: {e}", exc_info=True
        )
        return False

    if product_id:
        seo_data = {}
        for key, at_field in SEO_FIELD_MAPPINGS.items():
            val = catalogue_fields.get(at_field)
            if val:
                seo_data[key] = (
                    ", ".join(val)
                    if key == "keywords" and isinstance(val, list)
                    else first(val)
                )
        update_product_seo(product_id, seo_data)

    return True


def get_all_paginated_items(endpoint: str) -> List[Dict]:
    items, page = [], 1
    full_endpoint = f"products/{endpoint}"
    while True:
        try:
            response = wcapi.get(full_endpoint, params={"per_page": 100, "page": page})
            response.raise_for_status()
            page_items = response.json()
            if not page_items:
                break
            items.extend(page_items)
            page += 1
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 404:
                logger.warning(
                    f"Endpoint '{full_endpoint}' not found (404). This may be expected if a related plugin is not active."
                )
            break
        except Exception:
            break
    return items


def main():
    logger.info("🚀 Starting product sync from Airtable to WooCommerce...")
    try:
        tax_terms = {
            "categories": {
                term["name"].lower().strip(): term["id"]
                for term in get_all_paginated_items("categories")
            },
            "tags": {
                term["name"].lower().strip(): term["id"]
                for term in get_all_paginated_items("tags")
            },
            "brands": {
                term["name"].lower().strip(): term["id"]
                for term in get_all_paginated_items("brands")
            },
        }
        logger.info(
            f"Pre-fetched {len(tax_terms['categories'])} categories, {len(tax_terms['tags'])} tags, and {len(tax_terms['brands'])} brands."
        )

        logger.info("Fetching products from Airtable view 'Pay4more Sync View'...")
        records_to_sync = items_table.all(view="Pay4more Sync View")
        if not records_to_sync:
            logger.info("No products found to sync.")
            return

        logger.info(f"Found {len(records_to_sync)} products to sync.")
        image_cache, success_count, error_count = {}, 0, 0

        for i, record in enumerate(records_to_sync):
            logger.info(f"[{i + 1}/{len(records_to_sync)}] -------------------------")
            if process_single_product(record, tax_terms, image_cache):
                success_count += 1
            else:
                error_count += 1

        logger.info(
            "=" * 60
            + f"\n🏁 SYNC COMPLETE! ✅ Success: {success_count}, ❌ Errors: {error_count}\n"
            + "=" * 60
        )
    except Exception as e:
        logger.error(f"A fatal error occurred during sync: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
