import os
import sys
import requests
import base64
import time
import logging
import re
import html
from pathlib import Path
from io import BytesIO
from PIL import Image
from dotenv import load_dotenv
from woocommerce import API
from pyairtable import Api
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple
from functools import wraps

# Handle Windows console encoding
if sys.platform.startswith("win"):
    try:
        import codecs

        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
    except:
        pass

# Load environment variables
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Setup logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("sync.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
except:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

# Global cache
_tag_cache = {}
_brand_cache = {}


@dataclass
class SyncConfig:
    """Configuration for the sync process."""

    image_size: tuple = (1200, 1200)
    image_quality: int = 85
    webp_quality: int = 80
    upload_delay: float = 0.5
    timeout: int = 30
    per_page: int = 100
    max_retries: int = 3


config = SyncConfig()

# --- Field Mappings ---
# IMPORTANT: Ensure these Airtable field names exactly match your base.
FIELD_MAPPINGS = {
    "name": "Product Name",
    "slug": "Product Slug",
    "type": "WC-Type",
    "featured": "Featured",
    "description": "Description",
    "short_description": "Meta Description",
    "sku": "SKU",
    "regular_price": "Unit Retail Price",
    "sale_price": "4more Price",
    "weight": "Weight",
    "dimensions": "Dimensions",
    "stock_quantity": "Quantity",
    "tags": "Product Tags",
    "brand": "Brand",
    "parent_category": "Parent Category Rollup (from Category)",
    "category": "Category Rollup (from Category)",
}

ACF_MAPPINGS = {
    "inspected": "Inspected",
    "condition": "Inspection Condition",
    "box_condition": "Box Condition",
    "color": "Color",
    "model": "Model",
    "location": "Warehouse",
    "aisle_bin": "Shelf Location",
    "heavy": "Heavy",
    "fragile": "Fragile",
    "big": "Big",
}

# The order of these fields determines the processing order.
# FIX: Removed trailing space from "Photo Files "
IMAGE_FIELDS = ["Featured Photos", "Photo Files"]

DEFAULT_VALUES = {
    "status": "publish",
    "catalog_visibility": "visible",
    "tax_status": "taxable",
    "stock_status": "instock",
    "backorders": "no",
    "type": "simple",
    "manage_stock": True,
}


# --- Environment and API Setup ---
def validate_environment():
    required_vars = [
        "PAY4MORE_WOOCOMMERCE_STORE_URL",
        "PAY4MORE_WOOCOMMERCE_CONSUMER_KEY",
        "PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET",
        "WORDPRESS_USERNAME",
        "WORDPRESS_APPLICATION_PASSWORD",
        "AIRTABLE_API_KEY",
        "AIRTABLE_BASE_ID",
    ]
    if any(not os.getenv(var) for var in required_vars):
        raise ValueError("Missing one or more required environment variables.")


validate_environment()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appI0EDHQVVZCxVZ9")
WOOCOMMERCE_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
BASE_URL = f"{urlparse(WOOCOMMERCE_URL).scheme}://{urlparse(WOOCOMMERCE_URL).netloc}"

api = Api(AIRTABLE_API_KEY)
items_table = api.table(AIRTABLE_BASE_ID, "Items-Pay4more")
wcapi = API(
    url=BASE_URL,
    consumer_key=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY"),
    consumer_secret=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET"),
    version="wc/v3",
    timeout=config.timeout,
)

WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
WP_AUTH_HEADER = "Basic " + base64.b64encode(f"{WP_USER}:{WP_PASS}".encode()).decode(
    "utf-8"
)


# --- Helper Functions ---
def retry_on_failure(max_retries: int = 3, delay: float = 1):
    """Decorator to retry failed API calls."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} attempts."
                        )
                        raise
                    logger.warning(
                        f"Attempt {attempt + 1} for {func.__name__} failed: {e}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
            return None

        return wrapper

    return decorator


def clean_and_normalize(name: str) -> str:
    """A more robust function to clean and normalize category names."""
    if not isinstance(name, str):
        return ""
    unescaped_name = html.unescape(name)
    cleaned_name = re.sub(r"[\u200B-\u200D\uFEFF\s]+", " ", unescaped_name).strip()
    normalized = cleaned_name.lower().replace(" & ", " and ").replace("&", "and")
    return re.sub(r"\s+", " ", normalized)


def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    """Parse dimension string into a WooCommerce dimension object."""
    if not dimensions_str or not isinstance(dimensions_str, str):
        return {"length": "", "width": "", "height": ""}
    clean_str = dimensions_str.replace('"', "").replace("'", "")
    pattern = (
        r"(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)"
    )
    match = re.search(pattern, clean_str)
    if match:
        try:
            return {
                "length": match.group(1),
                "width": match.group(2),
                "height": match.group(3),
            }
        except IndexError:
            pass  # Fall through if regex matches but groups are wrong
    logger.warning(f"Could not parse dimensions from string: '{dimensions_str}'")
    return {"length": "", "width": "", "height": ""}


@retry_on_failure()
def fetch_all_wc_data(endpoint: str) -> List[Dict[str, Any]]:
    """Fetch paginated data from a WooCommerce endpoint."""
    logger.info(f"Fetching all {endpoint} from WooCommerce...")
    data = []
    page = 1
    while True:
        response = wcapi.get(
            endpoint, params={"per_page": config.per_page, "page": page}
        )
        response.raise_for_status()
        page_data = response.json()
        if not page_data:
            break
        data.extend(page_data)
        page += 1
    logger.info(f"Fetched {len(data)} items from {endpoint}.")
    return data


def generate_seo_filename(product_name: str, original_filename: str, index: int) -> str:
    """Generates an SEO-friendly filename for a product image."""
    clean_name = re.sub(r"[^\w\s-]", "", product_name.lower())
    clean_name = re.sub(r"[-\s]+", "-", clean_name).strip("-")
    clean_name = (
        clean_name[:50].rsplit("-", 1)[0] if len(clean_name) > 50 else clean_name
    )
    ext = (
        original_filename.split(".")[-1].lower() if "." in original_filename else "jpg"
    )
    filename = f"{clean_name}.{ext}"
    if index > 0:
        filename = f"{clean_name}-{index + 1}.{ext}"
    return filename


def optimize_image(img: Image.Image) -> Tuple[BytesIO, str]:
    """Optimizes a single image for e-commerce."""
    if img.mode not in ("RGB", "L"):
        background = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "RGBA" and img.getbands() == ("R", "G", "B", "A"):
            background.paste(img, mask=img.split()[3])
        else:
            background.paste(img)
        img = background
    img.thumbnail(config.image_size, Image.Resampling.LANCZOS)
    buffer = BytesIO()
    try:
        img.save(buffer, format="WEBP", quality=config.webp_quality)
        return buffer, "webp"
    except Exception:
        img.save(buffer, format="JPEG", quality=config.image_quality, optimize=True)
        return buffer, "jpeg"


@retry_on_failure()
def upload_image(image_url: str, product_name: str, index: int) -> Optional[int]:
    """Downloads, optimizes, and uploads a single image."""
    image_data = requests.get(image_url, timeout=20)
    image_data.raise_for_status()
    img = Image.open(BytesIO(image_data.content))
    buffer, img_format = optimize_image(img)

    filename = generate_seo_filename(product_name, image_url.split("/")[-1], index)
    filename = filename.rsplit(".", 1)[0] + f".{img_format}"

    headers = {
        "Authorization": WP_AUTH_HEADER,
        "Content-Disposition": f"attachment; filename={filename}",
        "Content-Type": f"image/{img_format}",
    }
    response = requests.post(
        f"{BASE_URL}/wp-json/wp/v2/media", headers=headers, data=buffer.getvalue()
    )
    response.raise_for_status()

    media_id = response.json()["id"]
    logger.info(f"✅ Uploaded image '{filename}' (ID: {media_id})")
    return media_id


def get_or_create_term(name: str, taxonomy: str, cache: dict) -> Optional[int]:
    """Gets or creates a taxonomy term (tag, brand)."""
    if not name or len(name.strip()) < 2:
        return None
    clean_name = name.strip()
    normalized_name = clean_and_normalize(clean_name)
    if normalized_name in cache:
        return cache[normalized_name]

    endpoint = f"products/{taxonomy}s"
    response = wcapi.post(endpoint, {"name": clean_name})
    if response.status_code == 201:
        term = response.json()
        logger.info(f"✅ Created {taxonomy}: '{clean_name}' (ID: {term['id']})")
        cache[normalized_name] = term["id"]
        return term["id"]
    elif response.status_code == 400 and "term_exists" in response.json().get(
        "code", ""
    ):
        existing_id = response.json().get("data", {}).get("resource_id")
        if existing_id:
            logger.warning(f"Term '{clean_name}' already existed. Updating cache.")
            cache[normalized_name] = existing_id
            return existing_id
    logger.error(f"Failed to create term '{clean_name}': {response.text}")
    return None


@retry_on_failure(max_retries=1)
def get_existing_product(sku: str) -> Optional[int]:
    """Checks for an existing product by SKU."""
    response = wcapi.get("products", params={"sku": sku})
    response.raise_for_status()
    products = response.json()
    return products[0]["id"] if products else None


# --- Main Processing Functions ---
def process_images(fields: dict, product_name: str) -> list:
    """Processes all images for a product in the specified order."""
    image_ids = []
    logger.info("--- Starting Image Processing ---")
    logger.debug(f"Available fields for record: {list(fields.keys())}")
    for field_name in IMAGE_FIELDS:
        images = fields.get(field_name)
        if not images:
            logger.debug(f"Image field '{field_name}' is empty or not found.")
            continue
        if not isinstance(images, list):
            logger.warning(f"Image field '{field_name}' is not a list. Skipping.")
            continue

        logger.info(f"Found {len(images)} file(s) in '{field_name}'.")
        for img_data in images:
            if url := img_data.get("url"):
                try:
                    if img_id := upload_image(url, product_name, len(image_ids)):
                        image_ids.append({"id": img_id})
                    time.sleep(config.upload_delay)
                except Exception as e:
                    logger.error(f"Failed to process image from URL {url}: {e}")
            else:
                logger.warning(f"No 'url' key found in image object: {img_data}")

    logger.info(f"--- Finished Image Processing. Uploaded {len(image_ids)} images. ---")
    return image_ids


def process_taxonomies(
    fields: dict, wc_cats: list, wc_tags: dict, wc_brands: dict
) -> dict:
    """Processes all taxonomies (categories, tags, brands)."""
    data = {}

    # --- Tags ---
    if tag_names := fields.get(FIELD_MAPPINGS["tags"], []):
        data["tags"] = [
            {"id": tid}
            for name in tag_names
            if (tid := get_or_create_term(name, "tag", wc_tags))
        ]

    # --- Brands ---
    if brand_name := (fields.get(FIELD_MAPPINGS["brand"], []) or [None])[0]:
        if brand_id := get_or_create_term(brand_name, "brand", wc_brands):
            data["brands"] = [{"id": brand_id}]

    # --- Categories (Updated Logic) ---
    logger.info("--- Starting Category Processing ---")
    category_ids = set()

    # Get field names from mappings to check them in Airtable data.
    # IMPORTANT: Ensure these field names in FIELD_MAPPINGS are correct for your Airtable base.
    parent_cat_field = FIELD_MAPPINGS.get("parent_category")
    child_cat_field = FIELD_MAPPINGS.get("category")

    parent_cat_names = fields.get(parent_cat_field, [])
    child_cat_names = fields.get(child_cat_field, [])

    # Airtable rollups should be lists. Add warning if they are not.
    if not isinstance(parent_cat_names, list):
        logger.warning(f"Parent category field '{parent_cat_field}' was not a list. Value: {parent_cat_names}")
        parent_cat_names = []
    if not isinstance(child_cat_names, list):
        logger.warning(f"Child category field '{child_cat_field}' was not a list. Value: {child_cat_names}")
        child_cat_names = []
        
    logger.info(f"Found Parent Categories from Airtable ('{parent_cat_field}'): {parent_cat_names}")
    logger.info(f"Found Child Categories from Airtable ('{child_cat_field}'): {child_cat_names}")

    # Combine parent and child category names, then find their IDs.
    airtable_cat_names = set(parent_cat_names + child_cat_names)
    logger.info(f"Combined unique category names for processing: {airtable_cat_names}")

    if not airtable_cat_names:
        logger.warning("No category names found in this Airtable record.")

    for name in airtable_cat_names:
        normalized_airtable_name = clean_and_normalize(name)
        if not normalized_airtable_name:
            continue

        match_found = False
        for wc_cat in wc_cats:
            if normalized_airtable_name == clean_and_normalize(wc_cat.get("name")):
                logger.info(
                    f"  -> SUCCESS: Matched '{name}' with WooCommerce category '{wc_cat['name']}' (ID: {wc_cat['id']})"
                )
                category_ids.add(wc_cat["id"])
                match_found = True
                break
        if not match_found:
            logger.warning(
                f"  -> FAILED: Could not find a match for '{name}' in WooCommerce."
            )

    if category_ids:
        data["categories"] = [{"id": cid} for cid in category_ids]
        logger.info(f"Final category IDs to be assigned: {[c['id'] for c in data['categories']]}")
    else:
        logger.warning("No categories matched. Assigning to 'Uncategorized'.")
        for wc_cat in wc_cats:
            if clean_and_normalize(wc_cat.get("name")) == "uncategorized":
                data["categories"] = [{"id": wc_cat["id"]}]
                break

    logger.info("--- Finished Category Processing ---")
    return data


def process_single_product(record: dict, wc_cats: list, wc_tags: dict, wc_brands: dict):
    """Processes a single Airtable record and syncs it to WooCommerce."""
    fields = record.get("fields", {})
    sku = fields.get(FIELD_MAPPINGS["sku"])
    name = fields.get(FIELD_MAPPINGS["name"], "Unknown Product")
    logger.info(f"Processing: {name} (SKU: {sku})")

    if not sku:
        logger.warning("Skipping product with no SKU.")
        return False

    try:
        data = {**DEFAULT_VALUES}
        all_mappings = {**FIELD_MAPPINGS, **ACF_MAPPINGS}
        for key, at_field in all_mappings.items():
            if (value := fields.get(at_field)) is not None:
                # Handle lookups/rollups that return lists
                if isinstance(value, list) and len(value) > 0:
                    value = value[0]

                if key in ["regular_price", "sale_price"]:
                    data[key] = str(value)
                elif key == "dimensions":
                    data[key] = parse_dimensions(str(value))
                elif key in ACF_MAPPINGS:
                    data.setdefault("meta_data", []).append(
                        {"key": key, "value": value}
                    )
                elif value is not None:
                    data[key] = value

        data.update(process_taxonomies(fields, wc_cats, wc_tags, wc_brands))
        if images := process_images(fields, name):
            data["images"] = images

        if existing_id := get_existing_product(sku):
            logger.info(f"Updating existing product (ID: {existing_id})...")
            response = wcapi.put(f"products/{existing_id}", data)
        else:
            logger.info("Creating new product...")
            response = wcapi.post("products", data)

        response.raise_for_status()
        logger.info(f"✅ Successfully synced product SKU: {sku}")
        return True

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"❌ HTTP Error for SKU {sku}: {e.response.status_code} - {e.response.text}"
        )
        logger.debug(
            f"--- Data payload that caused the error for SKU {sku} ---\n{data}"
        )
        return False
    except Exception as e:
        logger.error(
            f"❌ An unexpected exception occurred while syncing SKU {sku}: {e}",
            exc_info=True,
        )
        return False


# --- Main Sync Execution ---
def sync_products():
    """Main function to run the sync process."""
    logger.info("🚀 Starting product sync from Airtable to WooCommerce...")
    try:
        wc_tags = {
            clean_and_normalize(t["name"]): t["id"]
            for t in fetch_all_wc_data("products/tags")
        }
        wc_brands = {
            clean_and_normalize(b["name"]): b["id"]
            for b in fetch_all_wc_data("products/brands")
        }
        wc_cats = fetch_all_wc_data("products/categories")

        logger.info("Fetching products from Airtable...")
        records = items_table.all(view="Pay4more Sync View")
        logger.info(f"Found {len(records)} products to sync.")

        success, errors = 0, 0
        for i, rec in enumerate(records, 1):
            logger.info(f"[{i}/{len(records)}] -------------------------")
            if process_single_product(rec, wc_cats, wc_tags, wc_brands):
                success += 1
            else:
                errors += 1

        logger.info("=" * 60)
        logger.info("🏁 SYNC COMPLETE!")
        logger.info(f"✅ Success: {success}, ❌ Errors: {errors}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"A fatal error occurred during setup: {e}", exc_info=True)


if __name__ == "__main__":
    sync_products()