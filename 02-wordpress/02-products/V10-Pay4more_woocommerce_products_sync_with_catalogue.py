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
    IMAGE_SIZE: tuple[int, int] = (1080, 1080)
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

# --- Field Mappings (Aligned with V7 for Categories) ---
ITEM_FIELD_MAPPINGS = {
    "sku": "SKU",
    "sale_price": "4more Price",
    "stock_quantity": "Quantity",
}
CATALOGUE_FIELD_MAPPINGS = {
    "name": "Product Name",
    "description": "Description",
    "short_description": "Meta Description",
    "regular_price": "Unit Retail Price",
    "weight": "Weight",
    "tags": "Product Tags",
    # CORRECTED to use the exact field names from your working V7 script
    "category": "Category Name",
    "parent_category": "Parent Category Rollup (from Category)",
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
IMAGE_FIELDS_ORDER = ["Featured Photos", "Photo Files"]


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
        img.thumbnail(config.IMAGE_SIZE, Image.Resampling.LANCZOS)
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
            "title": f"{product_name} Image {index + 1}",
        }
        requests.post(
            f"{WC_URL}/wp-json/wp/v2/media/{media_id}",
            json=metadata_payload,
            auth=auth,
            timeout=30,
        )
        logger.info(f"✅ Uploaded image: '{final_filename}' (ID: {media_id})")
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
        if field in catalogue_fields
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


def get_term_id(
    name: str,
    taxonomy: str,
    existing_terms: Dict[str, int],
    create_if_not_found: bool = True,
) -> Optional[int]:
    if not isinstance(name, str) or not name.strip():
        return None
    name_lower = name.lower()
    if name_lower in existing_terms:
        return existing_terms[name_lower]

    if not create_if_not_found:
        return None

    try:
        logger.info(f"Creating new {taxonomy[:-1]}: '{name}'")
        response = wcapi.post(f"products/{taxonomy}", {"name": name})
        if response.status_code == 201:
            new_term = response.json()
            existing_terms[name_lower] = new_term["id"]
            return new_term["id"]
        if response.status_code == 400 and "term_exists" in response.text:
            logger.warning(
                f"Term '{name}' already exists in WooCommerce. Consider re-fetching terms."
            )
            return None
        logger.error(f"Failed to create {taxonomy[:-1]} '{name}': {response.text}")
    except Exception as e:
        logger.error(f"Exception creating {taxonomy[:-1]} '{name}': {e}")
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
def process_single_product(record: Dict, tax_terms: Dict, image_cache: Dict):
    item_fields = record.get("fields", {})
    item_sku = first(item_fields.get(ITEM_FIELD_MAPPINGS["sku"]))
    catalogue_code = first(item_fields.get("4more-Product-Code-linked"))
    if not item_sku or not catalogue_code:
        logger.warning(
            f"Skipping record {record.get('id')}: Missing SKU or linked catalogue code."
        )
        return

    catalogue_record = find_catalogue_by_code(catalogue_code)
    if not catalogue_record:
        logger.error(
            f"Could not find catalogue '{catalogue_code}' linked from item {item_sku}. Skipping."
        )
        return

    catalogue_fields = catalogue_record.get("fields", {})
    product_name = first(catalogue_fields.get(CATALOGUE_FIELD_MAPPINGS["name"]))
    if not product_name:
        logger.warning(
            f"Catalogue {catalogue_code} is missing 'Product Name'. Skipping item {item_sku}."
        )
        return

    logger.info(
        f"Processing Item SKU: {item_sku} -> Catalogue: {product_name} ({catalogue_code})"
    )

    # 1. Prepare Main Product Payload
    payload = {"type": "simple", "status": "publish", "manage_stock": True}
    for wc_field, at_field in {
        **ITEM_FIELD_MAPPINGS,
        **CATALOGUE_FIELD_MAPPINGS,
    }.items():
        source_fields = (
            item_fields if wc_field in ITEM_FIELD_MAPPINGS else catalogue_fields
        )
        if at_field in [
            CATALOGUE_FIELD_MAPPINGS["category"],
            CATALOGUE_FIELD_MAPPINGS["parent_category"],
            "tags",
        ]:
            continue
        val = first(source_fields.get(at_field))
        if val is not None and val != "":
            payload[wc_field] = (
                str(val)
                if wc_field in ["regular_price", "sale_price", "weight"]
                else val
            )

    # 2. Process Taxonomies (V7 Logic)
    all_cat_names = set()
    child_cats = catalogue_fields.get(CATALOGUE_FIELD_MAPPINGS["category"], [])
    parent_cats = catalogue_fields.get(CATALOGUE_FIELD_MAPPINGS["parent_category"], [])
    if child_cats:
        all_cat_names.update(child_cats)
    if parent_cats:
        all_cat_names.update(parent_cats)

    if all_cat_names:
        logger.info(f"Found Airtable category names: {list(all_cat_names)}")
        wc_category_map = tax_terms["categories"]
        category_ids = {
            wc_category_map[name.lower()]
            for name in all_cat_names
            if isinstance(name, str) and name.lower() in wc_category_map
        }

        if category_ids:
            payload["categories"] = [{"id": cid} for cid in category_ids]
            logger.info(
                f"Assigning to existing WooCommerce category IDs: {list(category_ids)}"
            )
        else:
            logger.warning(
                f"Could not find any matching pre-fetched WooCommerce categories for: {list(all_cat_names)}"
            )

    tag_names = catalogue_fields.get("tags", [])
    if tag_names:
        payload["tags"] = [
            {"id": tid}
            for name in tag_names
            if (
                tid := get_term_id(
                    name, "tags", tax_terms["tags"], create_if_not_found=True
                )
            )
        ]

    # 3. Process Custom Fields
    meta_data = []
    for meta_key, at_field in CUSTOM_FIELD_MAPPINGS.items():
        val = first(catalogue_fields.get(at_field))
        if val is not None and val != "":
            meta_data.append({"key": meta_key, "value": val})
    if meta_data:
        payload["meta_data"] = meta_data
        logger.info(f"Adding Custom Fields to product: {meta_data}")

    # 4. Process Dimensions and Images
    payload["dimensions"] = parse_dimensions(
        first(catalogue_fields.get("Dimensions", ""))
    )
    payload["images"] = process_product_images(
        catalogue_fields, product_name, catalogue_code, image_cache
    )

    # 5. Create or Update Product
    product_id = None
    try:
        existing = wcapi.get("products", params={"sku": item_sku}).json()
        if existing:
            product_id = existing[0]["id"]
            logger.info(f"Updating existing product (ID: {product_id})...")
            response = wcapi.put(f"products/{product_id}", payload)
        else:
            logger.info(f"Creating new product...")
            response = wcapi.post("products", payload)
        response.raise_for_status()
        product_id = response.json().get("id", product_id)
        logger.info(
            f"✅ Successfully synced product SKU: {item_sku} (ID: {product_id})"
        )
    except Exception as e:
        logger.error(
            f"API Error syncing product {item_sku}: {getattr(e, 'response', e)}"
        )
        return

    # 6. Update SEO Data via Custom Endpoint
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


def get_all_paginated_items(endpoint: str) -> List[Dict]:
    items, page = [], 1
    while True:
        try:
            response = wcapi.get(endpoint, params={"per_page": 100, "page": page})
            response.raise_for_status()
            page_items = response.json()
            if not page_items:
                break
            items.extend(page_items)
            page += 1
        except Exception:
            break
    return items


def main():
    logger.info("🚀 Starting product sync from Airtable to WooCommerce...")
    try:
        tax_terms = {
            "categories": {
                term["name"].lower(): term["id"]
                for term in get_all_paginated_items("products/categories")
            },
            "tags": {
                term["name"].lower(): term["id"]
                for term in get_all_paginated_items("products/tags")
            },
        }
        logger.info("Fetching products from Airtable view 'Pay4more Sync View'...")
        records_to_sync = items_table.all(view="Pay4more Sync View")
        if not records_to_sync:
            logger.info("No products found to sync.")
            return

        logger.info(f"Found {len(records_to_sync)} products to sync.")
        image_cache, success_count, error_count = {}, 0, 0

        for i, record in enumerate(records_to_sync):
            logger.info(f"[{i + 1}/{len(records_to_sync)}] -------------------------")
            try:
                process_single_product(record, tax_terms, image_cache)
                success_count += 1
            except Exception as e:
                logger.error(
                    f"Critical error on record {record.get('id', 'N/A')}: {e}",
                    exc_info=True,
                )
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
