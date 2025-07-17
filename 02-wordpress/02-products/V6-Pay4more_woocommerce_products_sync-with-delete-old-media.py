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

# --- Windows Console Encoding ---
# Handles potential output errors on Windows environments
if sys.platform.startswith("win"):
    try:
        import codecs

        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
    except:
        pass

# --- Environment and Logging Setup ---
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

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

# --- Global Caches & Configuration ---
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
    # "type": "WC-Type",
    "featured": "Featured",
    "description": "Description",
    "short_description": "Meta Description",  # Also used for Rank Math if its field is empty
    "sku": "SKU",
    "regular_price": "Unit Retail Price",
    "sale_price": "4more Price",
    "weight": "Weight",
    "dimensions": "Dimensions",
    "stock_quantity": "Quantity",
    "tags": "Product Tags",
    "brand": "Brand",
    "parent_category": "Parent Category Rollup (from Category)",
    "category": "Category Name",
}

# Advanced Custom Fields (ACF) Mappings
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

# Rank Math SEO Mappings
META_MAPPINGS = {
    "rank_math_title": "Meta Title",
    "rank_math_description": "Meta Description",
    "rank_math_focus_keyword": "Focus Keyword",
}

# The order determines the processing sequence for images.
IMAGE_FIELDS = ["Featured Photos", "Photo Files"]

# Default values for new WooCommerce products
DEFAULT_VALUES = {
    "status": "publish",
    "catalog_visibility": "visible",
    "tax_status": "taxable",
    "stock_status": "instock",
    "backorders": "no",
    "type": "simple",
    "manage_stock": True,
}


# --- Environment and API Initialization ---
def validate_environment():
    """Ensures all required environment variables are set."""
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

# API Clients
api = Api(os.getenv("AIRTABLE_API_KEY"))
items_table = api.table(os.getenv("AIRTABLE_BASE_ID"), "Items-Pay4more")

WOOCOMMERCE_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
BASE_URL = f"{urlparse(WOOCOMMERCE_URL).scheme}://{urlparse(WOOCOMMERCE_URL).netloc}"

wcapi = API(
    url=BASE_URL,
    consumer_key=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY"),
    consumer_secret=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET"),
    version="wc/v3",
    timeout=config.timeout,
)

# WordPress Authentication
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
    """Cleans and normalizes a string for comparison."""
    if not isinstance(name, str):
        return ""
    unescaped_name = html.unescape(name)
    cleaned_name = re.sub(r"[\u200B-\u200D\uFEFF\s]+", " ", unescaped_name).strip()
    normalized = cleaned_name.lower().replace(" & ", " and ").replace("&", "and")
    return re.sub(r"\s+", " ", normalized)


def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    """Parses a dimension string into a WooCommerce dimension object."""
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
            pass
    logger.warning(f"Could not parse dimensions from string: '{dimensions_str}'")
    return {"length": "", "width": "", "height": ""}


@retry_on_failure()
def fetch_all_wc_data(endpoint: str) -> List[Dict[str, Any]]:
    """Fetches all paginated data from a WooCommerce endpoint."""
    logger.info(f"Fetching all {endpoint} from WooCommerce...")
    data = []
    page = 1
    while True:
        try:
            response = wcapi.get(
                endpoint, params={"per_page": config.per_page, "page": page}
            )
            response.raise_for_status()
            page_data = response.json()
            if not page_data:
                break
            data.extend(page_data)
            page += 1
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"Endpoint not found: {endpoint}. This might be expected if a plugin is not active."
                )
                return []
            raise
    logger.info(f"Fetched {len(data)} items from {endpoint}.")
    return data


def generate_seo_filename(product_name: str, original_filename: str, index: int) -> str:
    """Generates an SEO-friendly filename for a product image."""
    clean_name = re.sub(r"[^\w\s-]", "", product_name.lower()).strip()
    clean_name = re.sub(r"[-\s]+", "-", clean_name)
    clean_name = (
        clean_name[:50].rsplit("-", 1)[0] if len(clean_name) > 50 else clean_name
    )
    ext = (
        original_filename.split(".")[-1].lower() if "." in original_filename else "jpg"
    )

    if index > 0:
        return f"{clean_name}-{index + 1}.{ext}"
    return f"{clean_name}.{ext}"


def optimize_image(img: Image.Image) -> Tuple[BytesIO, str]:
    """
    Resizes, pads with a white background to make it square,
    and optimizes an image for e-commerce.
    """
    target_size = config.image_size

    # Create a new square image with a white background
    background = Image.new("RGB", target_size, (255, 255, 255))

    # Make a copy to avoid modifying the original image object
    img_copy = img.copy()

    # Convert RGBA to RGB if necessary (removes transparency)
    if img_copy.mode in ("RGBA", "P"):
        img_copy = img_copy.convert("RGB")

    # Resize the image while maintaining aspect ratio
    img_copy.thumbnail(target_size, Image.Resampling.LANCZOS)

    # Calculate position to paste the resized image onto the center of the background
    paste_x = (target_size[0] - img_copy.width) // 2
    paste_y = (target_size[1] - img_copy.height) // 2

    # Paste the resized image
    background.paste(img_copy, (paste_x, paste_y))

    # Save the final square image to a buffer
    buffer = BytesIO()
    try:
        # Prefer WEBP for its superior compression
        background.save(buffer, format="WEBP", quality=config.webp_quality)
        return buffer, "webp"
    except Exception:
        # Fallback to JPEG if WEBP is not supported
        background.save(
            buffer, format="JPEG", quality=config.image_quality, optimize=True
        )
        return buffer, "jpeg"


@retry_on_failure()
def upload_image(image_url: str, product_name: str, index: int) -> Optional[int]:
    """Downloads, optimizes, and uploads a single image to WordPress."""
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


def get_or_create_term(name: str, taxonomy: str) -> Optional[int]:
    """Gets or creates a taxonomy term (tag, brand) and uses a cache."""
    if not name or len(name.strip()) < 2:
        return None

    clean_name = name.strip()
    normalized_name = clean_and_normalize(clean_name)

    cache, endpoint_slug = None, None
    if taxonomy == "product_tag":
        cache, endpoint_slug = _tag_cache, "tags"
    elif taxonomy == "product_brand":
        cache, endpoint_slug = _brand_cache, "brands"
    else:
        logger.error(f"Unknown taxonomy '{taxonomy}' in get_or_create_term.")
        return None

    if normalized_name in cache:
        return cache[normalized_name]

    endpoint = f"products/{endpoint_slug}"

    logger.info(
        f"Attempting to create {taxonomy}: '{clean_name}' via POST to {endpoint}"
    )
    response = wcapi.post(endpoint, {"name": clean_name})

    if response.status_code == 201:
        term = response.json()
        logger.info(f"✅ Created {taxonomy}: '{clean_name}' (ID: {term['id']})")
        cache[normalized_name] = term["id"]
        return term["id"]

    # --- ENHANCED ERROR LOGGING FOR BRANDS ---
    if taxonomy == "product_brand":
        logger.error(f"--- BRAND CREATION FAILED ---")
        logger.error(
            f"Failed to create brand '{clean_name}'. Status Code: {response.status_code}"
        )
        logger.error(f"Server Response: {response.text}")
        logger.error(f"-----------------------------")
    # -----------------------------------------

    elif response.status_code == 400 and "term_exists" in response.json().get(
        "code", ""
    ):
        existing_id = response.json().get("data", {}).get("resource_id")
        if existing_id:
            logger.warning(f"Term '{clean_name}' already existed. Updating cache.")
            cache[normalized_name] = existing_id
            return existing_id
    else:
        logger.error(f"Failed to create term '{clean_name}': {response.text}")

    return None


@retry_on_failure(max_retries=1)
def get_existing_product(sku: str) -> Optional[int]:
    """Checks for an existing product by SKU."""
    response = wcapi.get("products", params={"sku": sku})
    response.raise_for_status()
    products = response.json()
    return products[0]["id"] if products else None


def test_brand_endpoints() -> Optional[str]:
    """Tests which brand endpoint is available in WooCommerce."""
    logger.info("Testing brand endpoints to determine WooCommerce setup...")
    try:
        response = wcapi.get("products/brands", params={"per_page": 1})
        # --- ADD THIS LINE FOR DEBUGGING ---
        logger.info(
            f"DEBUG: Received status code '{response.status_code}' from /products/brands endpoint."
        )
        # ------------------------------------
        if response.status_code == 200:
            logger.info(
                "✅ WooCommerce Brands plugin is active ('products/brands' endpoint)."
            )
            return "brands_plugin"
    except Exception as e:
        logger.error(f"❌ An exception occurred while testing brand endpoints: {e}")

    logger.error("❌ No working brand endpoints found. Brands will not be processed.")
    return None


# --- Main Processing Functions ---
def process_images(fields: dict, product_name: str) -> List[Dict[str, int]]:
    """Processes all images for a product in the specified order."""
    image_ids = []
    logger.info("--- Starting Image Processing ---")

    for field_name in IMAGE_FIELDS:
        images = fields.get(field_name)
        if not isinstance(images, list):
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


def process_single_product(
    record: Dict, wc_cats: List[Dict], brand_method: Optional[str]
):
    """Processes one Airtable record and syncs it to WooCommerce."""
    fields = record.get("fields", {})
    sku = fields.get(FIELD_MAPPINGS["sku"])
    name = fields.get(FIELD_MAPPINGS["name"], "Unknown Product")
    logger.info(f"Processing: {name} (SKU: {sku})")

    if not sku:
        logger.warning("Skipping product with no SKU.")
        return False

    try:
        data = {**DEFAULT_VALUES}
        data.setdefault("meta_data", [])

        # --- Consolidate All Mappings ---
        all_mappings = {**FIELD_MAPPINGS, **ACF_MAPPINGS, **META_MAPPINGS}
        for key, at_field in all_mappings.items():
            value = fields.get(at_field)
            if value is None:
                continue

            # Handle lookups/rollups that return lists
            if isinstance(value, list) and len(value) > 0:
                value = value[0]

            if key in ["regular_price", "sale_price"]:
                data[key] = str(value)
            elif key == "dimensions":
                data[key] = parse_dimensions(str(value))
            elif key in ACF_MAPPINGS or key in META_MAPPINGS:
                data["meta_data"].append({"key": key, "value": value})
            elif key not in ["tags", "brand", "category", "parent_category"]:
                data[key] = value

        # --- Categories ---
        parent_cats = fields.get(FIELD_MAPPINGS["parent_category"], [])
        child_cats = fields.get(FIELD_MAPPINGS["category"], [])
        airtable_cat_names = set(parent_cats + child_cats)
        category_ids = set()
        for cat_name in airtable_cat_names:
            norm_name = clean_and_normalize(cat_name)
            for wc_cat in wc_cats:
                if norm_name == clean_and_normalize(wc_cat.get("name")):
                    category_ids.add(wc_cat["id"])
                    break
        if category_ids:
            data["categories"] = [{"id": cid} for cid in category_ids]

        # --- Tags ---
        if tag_names := fields.get(FIELD_MAPPINGS["tags"], []):
            data["tags"] = [
                {"id": tid}
                for name in tag_names
                if (tid := get_or_create_term(name, "product_tag"))
            ]

        # --- Brands ---
        if brand_name := (fields.get(FIELD_MAPPINGS["brand"], []) or [None])[0]:
            if brand_method == "brands_plugin":
                if brand_id := get_or_create_term(brand_name, "product_brand"):
                    data["brands"] = [{"id": brand_id}]
                    logger.info(f"Assigned brand via plugin: {brand_name}")

        # --- Images ---
        if images := process_images(fields, name):
            data["images"] = images

        # --- Create or Update Product ---
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
            f"❌ An unexpected exception occurred for SKU {sku}: {e}", exc_info=True
        )
        return False


# --- Main Sync Execution ---
def sync_products():
    """Main function to run the entire sync process."""
    logger.info("🚀 Starting product sync from Airtable to WooCommerce...")
    try:
        # --- Pre-computation and Caching ---
        brand_method = "brands_plugin"  # test_brand_endpoints()
        if brand_method == "brands_plugin":
            brands_data = fetch_all_wc_data("products/brands")
            global _brand_cache
            _brand_cache = {
                clean_and_normalize(b["name"]): b["id"] for b in brands_data
            }
            logger.info(f"Cached {len(_brand_cache)} brands.")

        tags_data = fetch_all_wc_data("products/tags")
        global _tag_cache
        _tag_cache = {clean_and_normalize(t["name"]): t["id"] for t in tags_data}
        logger.info(f"Cached {len(_tag_cache)} tags.")

        wc_cats = fetch_all_wc_data("products/categories")

        # --- Fetch Airtable Records ---
        logger.info("Fetching products from Airtable...")
        records = items_table.all(view="Pay4more Sync View")
        logger.info(f"Found {len(records)} products to sync.")

        # --- Process Records ---
        success, errors = 0, 0
        for i, rec in enumerate(records, 1):
            logger.info(f"[{i}/{len(records)}] -------------------------")
            if process_single_product(rec, wc_cats, brand_method):
                success += 1
            else:
                errors += 1

        logger.info("=" * 60)
        logger.info("🏁 SYNC COMPLETE!")
        logger.info(f"✅ Success: {success}, ❌ Errors: {errors}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(
            f"A fatal error occurred during the sync process: {e}", exc_info=True
        )


if __name__ == "__main__":
    sync_products()
