import os
import sys
import requests
import base64
import time
import logging
import re
from pathlib import Path
from io import BytesIO
from PIL import Image
from dotenv import load_dotenv
from woocommerce import API
from pyairtable import Api
from urllib.parse import urlparse, unquote_plus
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
        pass  # Fallback to default encoding

# Load environment variables
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Setup logging with Windows-compatible encoding
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
    # Fallback logging setup for encoding issues
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

# Global cache for tags and brands to avoid repeated API calls
_tag_cache = {}
_brand_cache = {}


@dataclass
class SyncConfig:
    """Configuration for the sync process."""

    max_pages: int = 100
    image_size: tuple = (1200, 1200)  # Larger size for better quality
    thumbnail_size: tuple = (300, 300)  # For gallery thumbnails
    image_quality: int = 85  # JPEG quality
    webp_quality: int = 80  # WebP quality for modern browsers
    upload_delay: float = 0.5
    timeout: int = 30
    per_page: int = 100
    max_retries: int = 3
    optimize_images: bool = True
    generate_webp: bool = True  # Generate WebP versions for better performance


# Configuration
config = SyncConfig()

# Field mappings from your CSV
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
    "parent_category": "Category Parent",
    "category": "Category Name",
}

# Meta data mappings
META_MAPPINGS = {
    "rank_math_title": "Meta Title",
    "rank_math_description": "Meta Description",
    "rank_math_focus_keyword": "Focus Keyword",
}

# ACF (Advanced Custom Fields) mappings
ACF_MAPPINGS = {
    # Inspection Details
    "inspected": "Inspected",
    "condition": "Inspection Condition",  # Updated from "Condition" to "Inspection Condition"
    "box_condition": "Box Condition",  # Added Box Condition field
    # Product Details
    "color": "Color",  # Added Color field
    "model": "Model",  # Added Model field
    # Product Location
    "location": "Warehouse",
    "aisle_bin": "Shelf Location",
    # Product Shipping/Pickup Specs (checkboxes)
    "heavy": "Heavy",
    "fragile": "Fragile",
    "big": "Big",
}

# Image field mappings
# The first image processed from "Item Featured Photo" will become the product's featured image.
# The rest of the images will be added to the product gallery.
IMAGE_FIELDS = ["Item Featured Photo", "Item Photos"]

# Default values from your CSV
DEFAULT_VALUES = {
    "status": "publish",
    "catalog_visibility": "visible",
    "tax_status": "taxable",
    "stock_status": "instock",
    "backorders": "no",
    "type": "simple",
    "manage_stock": True,
}


def validate_environment():
    """Validate all required environment variables are set."""
    required_vars = [
        "PAY4MORE_WOOCOMMERCE_STORE_URL",
        "PAY4MORE_WOOCOMMERCE_CONSUMER_KEY",
        "PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET",
        "WORDPRESS_USERNAME",
        "WORDPRESS_APPLICATION_PASSWORD",
        "AIRTABLE_API_KEY",
        "AIRTABLE_BASE_ID",
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")


# Validate environment on import
validate_environment()

# Environment variables setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "appI0EDHQVVZCxVZ9")
AIRTABLE_ITEMS_TABLE = "Items-Pay4more"
AIRTABLE_VIEW_NAME = "Pay4more Sync View"
AIRTABLE_CATEGORIES_TABLE = "Categories"

WOOCOMMERCE_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WOOCOMMERCE_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WOOCOMMERCE_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")
WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
BASE_URL = f"{urlparse(WOOCOMMERCE_URL).scheme}://{urlparse(WOOCOMMERCE_URL).netloc}"

# Airtable clients
api = Api(AIRTABLE_API_KEY)
items_table = api.table(AIRTABLE_BASE_ID, AIRTABLE_ITEMS_TABLE)
categories_table = api.table(AIRTABLE_BASE_ID, AIRTABLE_CATEGORIES_TABLE)

# WooCommerce API
wcapi = API(
    url=BASE_URL,
    consumer_key=WOOCOMMERCE_CONSUMER_KEY,
    consumer_secret=WOOCOMMERCE_CONSUMER_SECRET,
    version="wc/v3",
    timeout=config.timeout,
)


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
                        raise
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
            return None

        return wrapper

    return decorator


def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    """Parse dimension string into WooCommerce format.

    Handles formats like:
    - "30.5" H x 47.2" W x 15.7" D"
    - "12 x 10 x 8"
    - "12in x 10in x 8in"
    """
    if not dimensions_str or not isinstance(dimensions_str, str):
        return {"length": "", "width": "", "height": ""}

    # Remove quotes and standardize separators
    clean_str = dimensions_str.replace('"', "").replace("'", "")

    # Try to extract numbers with various patterns
    # Pattern 1: "30.5 H x 47.2 W x 15.7 D"
    pattern1 = r"(\d+\.?\d*)\s*[HhLl]?\s*[xX]\s*(\d+\.?\d*)\s*[WwBb]?\s*[xX]\s*(\d+\.?\d*)\s*[DdTt]?"
    # Pattern 2: Simple "12 x 10 x 8" or "12x10x8"
    pattern2 = r"(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)"

    match = re.search(pattern1, clean_str) or re.search(pattern2, clean_str)

    if match:
        try:
            # WooCommerce expects length, width, height
            values = [match.group(1), match.group(2), match.group(3)]
            return {"length": values[0], "width": values[1], "height": values[2]}
        except:
            pass

    # If parsing fails, return empty dimensions
    logger.warning(f"Could not parse dimensions: {dimensions_str}")
    return {"length": "", "width": "", "height": ""}


def optimize_image_for_ecommerce(
    img: Image.Image, size: Tuple[int, int], quality: int = 85
) -> Tuple[BytesIO, str]:
    """Optimize image for ecommerce with proper compression and format.

    Returns:
        Tuple of (image buffer, format)
    """
    # Convert to RGB if necessary (handles RGBA, P mode, etc.)
    if img.mode not in ("RGB", "L"):
        # Create white background for transparency
        background = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "RGBA":
            background.paste(img, mask=img.split()[3])
        else:
            background.paste(img)
        img = background

    # Calculate aspect ratio and resize
    img.thumbnail(size, Image.Resampling.LANCZOS)

    # Apply slight sharpening for better product visibility
    try:
        from PIL import ImageEnhance

        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(1.1)  # Slight sharpening
    except:
        pass

    # Save optimized image
    buffer = BytesIO()

    # Try WebP first if supported (better compression)
    if config.generate_webp:
        try:
            img.save(buffer, format="WEBP", quality=config.webp_quality, method=6)
            buffer.seek(0)
            return buffer, "webp"
        except:
            # Fallback to JPEG if WebP fails
            pass

    # Default to JPEG with optimization
    img.save(buffer, format="JPEG", quality=quality, optimize=True, progressive=True)
    buffer.seek(0)
    return buffer, "jpeg"


def generate_seo_filename(
    product_name: str, original_filename: str, index: int = 0
) -> str:
    """Generate SEO-friendly filename for product images."""
    # Clean product name for filename
    clean_name = re.sub(r"[^\w\s-]", "", product_name.lower())
    clean_name = re.sub(r"[-\s]+", "-", clean_name)

    # Limit length
    if len(clean_name) > 50:
        clean_name = clean_name[:50].rsplit("-", 1)[0]

    # Add index if multiple images
    if index > 0:
        clean_name = f"{clean_name}-{index + 1}"

    # Determine extension
    ext = original_filename.split(".")[-1].lower()
    if ext not in ["jpg", "jpeg", "png", "gif", "webp"]:
        ext = "jpg"

    return f"{clean_name}.{ext}"


def normalize_category_name(name: str) -> str:
    """Normalize category names for matching - handle & symbols properly."""
    if not name:
        return ""
    # Convert to lowercase, strip whitespace, and normalize & symbols
    normalized = name.strip().lower()
    # Replace various & formats with a standard 'and'
    normalized = normalized.replace(" & ", " and ")
    normalized = normalized.replace("&", "and")
    # Clean up multiple spaces
    normalized = " ".join(normalized.split())
    return normalized


def create_category_variations(name: str) -> list:
    """Create multiple variations of category name for better matching."""
    if not name:
        return []

    variations = []
    clean_name = name.strip()

    # Original name (case-insensitive)
    variations.append(clean_name.lower())

    # Normalized version
    variations.append(normalize_category_name(clean_name))

    # Replace & with 'and' (with spaces)
    if "&" in clean_name:
        variations.append(clean_name.lower().replace(" & ", " and "))
        variations.append(clean_name.lower().replace("&", " and "))
        variations.append(clean_name.lower().replace("&", "and"))

    # Replace 'and' with & (with spaces)
    if "and" in clean_name.lower():
        variations.append(clean_name.lower().replace(" and ", " & "))
        variations.append(clean_name.lower().replace("and", "&"))

    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for var in variations:
        if var not in seen:
            seen.add(var)
            unique_variations.append(var)

    return unique_variations


def debug_airtable_categories(fields: Dict[str, Any]):
    """Debug function to show what category data looks like in Airtable."""
    parent = fields.get(FIELD_MAPPINGS.get("parent_category"))
    sub = fields.get(FIELD_MAPPINGS.get("category"))

    logger.debug(f"Raw Airtable category data:")
    logger.debug(f"  Parent Category field: {parent} (type: {type(parent)})")
    logger.debug(f"  Category Name field: {sub} (type: {type(sub)})")

    return parent, sub


def get_all_existing_tags() -> Dict[str, int]:
    """Fetch all existing tags from WooCommerce and populate cache."""
    global _tag_cache

    if _tag_cache:  # Return cached data if already populated
        return _tag_cache

    logger.info("Fetching all existing tags from WooCommerce...")
    all_tags = []
    page = 1

    while page <= config.max_pages:
        try:
            response = wcapi.get(
                "products/tags", params={"per_page": config.per_page, "page": page}
            )

            if response.status_code != 200:
                logger.warning(
                    f"Failed to fetch tags page {page}: {response.status_code}"
                )
                break

            page_data = response.json()
            if not page_data:
                break

            all_tags.extend(page_data)
            logger.debug(f"Fetched {len(page_data)} tags from page {page}")
            page += 1

        except Exception as e:
            logger.error(f"Error fetching tags page {page}: {e}")
            break

    # Build cache with normalized names as keys
    for tag in all_tags:
        name = tag.get("name", "").strip()
        if name:
            # Store both original and normalized versions
            _tag_cache[name.lower()] = tag["id"]
            # Also store normalized version (for & -> and conversion)
            normalized = name.lower().replace("&", "and").replace("-", " ")
            _tag_cache[normalized] = tag["id"]

    logger.info(f"Cached {len(all_tags)} tags ({len(_tag_cache)} cache entries)")
    return _tag_cache


def get_all_existing_brands(endpoint: str = "products/brands") -> Dict[str, int]:
    """Fetch all existing brands from WooCommerce and populate cache."""
    global _brand_cache

    if _brand_cache:  # Return cached data if already populated
        return _brand_cache

    logger.info(f"Fetching all existing brands from WooCommerce endpoint: {endpoint}")
    all_brands = []
    page = 1

    while page <= config.max_pages:
        try:
            response = wcapi.get(
                endpoint, params={"per_page": config.per_page, "page": page}
            )

            if response.status_code == 404:
                logger.warning(
                    f"Brand endpoint {endpoint} not found (404) - brands plugin may not be installed"
                )
                break
            elif response.status_code != 200:
                logger.warning(
                    f"Failed to fetch brands page {page}: {response.status_code}"
                )
                break

            page_data = response.json()
            if not page_data:
                break

            all_brands.extend(page_data)
            logger.debug(f"Fetched {len(page_data)} brands from page {page}")
            page += 1

        except Exception as e:
            logger.error(f"Error fetching brands page {page}: {e}")
            break

    # Build cache with normalized names as keys
    for brand in all_brands:
        name = brand.get("name", "").strip()
        if name:
            # Store both original and normalized versions
            _brand_cache[name.lower()] = brand["id"]
            # Also store normalized version (for & -> and conversion)
            normalized = name.lower().replace("&", "and").replace("-", " ")
            _brand_cache[normalized] = brand["id"]

    logger.info(f"Cached {len(all_brands)} brands ({len(_brand_cache)} cache entries)")
    return _brand_cache


@retry_on_failure()
def fetch_all_categories() -> List[Dict[str, Any]]:
    """Fetch all categories from WooCommerce with pagination."""
    logger.info("Fetching WooCommerce categories...")
    categories = []
    page = 1

    while page <= config.max_pages:
        try:
            logger.info(f"Fetching categories page {page}...")
            response = wcapi.get(
                "products/categories",
                params={"per_page": config.per_page, "page": page},
            )

            if response.status_code != 200:
                logger.warning(
                    f"Received status code {response.status_code}, stopping category fetch"
                )
                break

            page_data = response.json()
            if not page_data:
                logger.info(f"Finished fetching categories (total: {len(categories)})")
                break

            categories.extend(page_data)
            logger.info(f"Found {len(page_data)} categories on page {page}")
            page += 1

        except Exception as e:
            logger.error(f"Error fetching categories on page {page}: {e}")
            break

    return categories


def test_brand_endpoints():
    """Test which brand endpoints are available in WooCommerce."""
    logger.info("Testing brand endpoints to determine WooCommerce setup...")

    # Test WooCommerce Brands plugin endpoint
    try:
        response = wcapi.get("products/brands", params={"per_page": 1})
        if response.status_code == 200:
            logger.info(
                "✅ WooCommerce Brands plugin is active (products/brands endpoint works)"
            )
            brands = response.json()
            if brands:
                logger.info(f"Found {len(brands)} existing brands via Brands plugin")
            return "brands_plugin"
        else:
            logger.warning(
                f"❌ WooCommerce Brands plugin endpoint failed: {response.status_code}"
            )
    except Exception as e:
        logger.warning(f"❌ Error testing Brands plugin endpoint: {e}")

    # Test product attributes endpoint for brand
    try:
        response = wcapi.get("products/attributes")
        if response.status_code == 200:
            attributes = response.json()
            brand_attr = None
            for attr in attributes:
                if attr.get("name", "").lower() in ["brand", "brands", "pa_brand"]:
                    brand_attr = attr
                    break

            if brand_attr:
                logger.info(
                    f"✅ Found brand attribute: {brand_attr['name']} (ID: {brand_attr['id']})"
                )
                # Test the terms endpoint
                terms_response = wcapi.get(
                    f"products/attributes/{brand_attr['id']}/terms",
                    params={"per_page": 1},
                )
                if terms_response.status_code == 200:
                    logger.info("✅ Brand attribute terms endpoint works")
                    return f"attribute_{brand_attr['id']}"
            else:
                logger.warning("❌ No brand attribute found in WooCommerce")
    except Exception as e:
        logger.warning(f"❌ Error testing attribute endpoints: {e}")

    logger.error("❌ No working brand endpoints found. Brands will not be processed.")
    return None


def get_or_create_term(
    name: str, taxonomy: str = "product_tag", max_attempts: int = 2
) -> Optional[int]:
    """Get tag or brand ID by name, create if needed. Uses caching for better performance."""
    if not name or len(name.strip()) < 2:
        logger.warning(f"Skipping invalid term: '{name}'")
        return None

    # Clean the name
    clean_name = name.strip()
    search_key = clean_name.lower()

    # Use cached data first for tags
    if taxonomy == "product_tag":
        cached_tags = get_all_existing_tags()

        # Try exact match first
        if search_key in cached_tags:
            logger.debug(
                f"Found existing tag in cache: {clean_name} (ID: {cached_tags[search_key]})"
            )
            return cached_tags[search_key]

        # Try normalized variations
        normalized = search_key.replace("&", "and").replace("-", " ")
        if normalized in cached_tags:
            logger.debug(
                f"Found existing tag via normalization: {clean_name} -> {normalized} (ID: {cached_tags[normalized]})"
            )
            return cached_tags[normalized]

        # If not found in cache, create new tag
        endpoint = "products/tags"
    elif taxonomy == "product_brand":
        endpoint = "products/brands"
        cached_brands = get_all_existing_brands(endpoint)

        if search_key in cached_brands:
            logger.debug(
                f"Found existing brand in cache: {clean_name} (ID: {cached_brands[search_key]})"
            )
            return cached_brands[search_key]

        normalized = search_key.replace("&", "and").replace("-", " ")
        if normalized in cached_brands:
            logger.debug(
                f"Found existing brand via normalization: {clean_name} -> {normalized} (ID: {cached_brands[normalized]})"
            )
            return cached_brands[normalized]
    elif taxonomy.startswith("attribute_"):
        # Extract attribute ID from taxonomy
        attr_id = taxonomy.split("_")[1]
        endpoint = f"products/attributes/{attr_id}/terms"
    elif taxonomy == "pa_brand":
        # Try both brands plugin and product attributes
        # First try WooCommerce Brands plugin
        brand_id = get_or_create_term(clean_name, "product_brand", max_attempts)
        if brand_id:
            return brand_id
        # Fallback to product attributes if brands plugin not available
        endpoint = "products/attributes/pa_brand/terms"
    else:
        endpoint = f"products/{taxonomy}s"

    # Create new term if not found in cache
    for attempt in range(max_attempts):
        try:
            # Create new term
            slug = clean_name.lower().replace(" ", "-").replace("&", "and")
            # Remove special characters from slug
            slug = re.sub(r"[^a-z0-9\-]", "", slug)

            data = {"name": clean_name, "slug": slug}
            logger.info(f"Creating new {taxonomy}: {clean_name}")
            create = wcapi.post(endpoint, data)

            if create.status_code == 201:
                term_id = create.json()["id"]
                logger.info(f"✅ Created {taxonomy}: {clean_name} (ID: {term_id})")

                # Update cache with new term
                if taxonomy == "product_tag":
                    _tag_cache[search_key] = term_id
                elif taxonomy == "product_brand":
                    _brand_cache[search_key] = term_id

                return term_id
            elif create.status_code == 404:
                logger.warning(
                    f"Cannot create {taxonomy} - endpoint not found: {endpoint}"
                )
                # If brands endpoint fails, try as regular taxonomy
                if taxonomy == "product_brand":
                    return get_or_create_term(clean_name, "pa_brand", max_attempts)
                return None
            else:
                logger.warning(
                    f"Failed to create {taxonomy} '{clean_name}': {create.status_code} - {create.text[:200]}"
                )

        except Exception as e:
            logger.warning(
                f"Attempt {attempt + 1} failed for {taxonomy} '{clean_name}': {e}"
            )
            if attempt < max_attempts - 1:
                time.sleep(1)

    logger.error(
        f"Failed to get/create {taxonomy}: {clean_name} after {max_attempts} attempts"
    )
    return None


@retry_on_failure()
def upload_image_to_woocommerce(
    image_url: str, product_name: str = "", index: int = 0
) -> Optional[int]:
    """Upload an optimized image to WooCommerce with proper SEO metadata."""
    try:
        logger.info(f"Downloading and optimizing image: {image_url[:50]}...")

        # Generate SEO-friendly filename
        original_filename = image_url.split("/")[-1].split("?")[0]
        seo_filename = generate_seo_filename(product_name, original_filename, index)

        # Download image
        image_data = requests.get(image_url, timeout=20)
        if image_data.status_code != 200:
            logger.error(f"Failed to download image: {image_url}")
            return None

        # Open and optimize image
        img = Image.open(BytesIO(image_data.content))

        # Optimize for ecommerce
        buffer, img_format = optimize_image_for_ecommerce(
            img, config.image_size, config.image_quality
        )

        # Update filename with correct extension
        if img_format == "webp":
            seo_filename = seo_filename.rsplit(".", 1)[0] + ".webp"
            content_type = "image/webp"
        else:
            seo_filename = seo_filename.rsplit(".", 1)[0] + ".jpg"
            content_type = "image/jpeg"

        # Generate SEO metadata
        if product_name:
            # More descriptive alt text for better SEO
            alt_text = (
                f"{product_name} - Product photo {index + 1}"
                if index > 0
                else f"{product_name} - Main product photo"
            )
            title = f"{product_name} - Image {index + 1}" if index > 0 else product_name
            caption = f"View of {product_name}"
            description = f"Product image showing {product_name} available at Pay4more"
        else:
            alt_text = f"Product image {index + 1}" if index > 0 else "Product image"
            title = "Product photo"
            caption = "Product view"
            description = "Product image from Pay4more inventory"

        # Upload to WordPress
        upload_url = f"{BASE_URL}/wp-json/wp/v2/media"
        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{WP_USER}:{WP_PASS}".encode()).decode("utf-8"),
            "Content-Disposition": f"attachment; filename={seo_filename}",
        }

        files = {"file": (seo_filename, buffer, content_type)}

        # Upload the optimized image
        response = requests.post(upload_url, headers=headers, files=files)
        if response.status_code == 201:
            media_data = response.json()
            media_id = media_data.get("id")

            # Small delay to ensure media is processed
            time.sleep(1)

            # Update media with SEO metadata
            metadata_url = f"{BASE_URL}/wp-json/wp/v2/media/{media_id}"
            metadata_payload = {
                "alt_text": alt_text,
                "caption": caption,
                "description": description,
                "title": title,
            }

            metadata_headers = {
                "Authorization": "Basic "
                + base64.b64encode(f"{WP_USER}:{WP_PASS}".encode()).decode("utf-8"),
                "Content-Type": "application/json",
            }

            # Update metadata
            metadata_response = requests.patch(
                metadata_url, headers=metadata_headers, json=metadata_payload
            )

            if metadata_response.status_code == 200:
                logger.info(
                    f"✅ Uploaded optimized image (ID: {media_id}, Format: {img_format.upper()}, Size: {len(buffer.getvalue()) / 1024:.1f}KB)"
                )
                logger.debug(f"SEO: Alt='{alt_text}', Title='{title}'")
            else:
                logger.warning(
                    f"Image uploaded but metadata update failed: {metadata_response.status_code}"
                )

            return media_id
        else:
            logger.error(
                f"Failed to upload image: {response.status_code} - {response.text[:300]}"
            )
            return None

    except Exception as e:
        logger.error(f"Exception during image upload: {e}")
        return None


def get_existing_product(sku: str) -> Optional[int]:
    """Check if product exists by SKU with enhanced search including trash."""
    try:
        # Try multiple search methods to find existing products
        logger.debug(f"Searching for existing product with SKU: {sku}")

        # Method 1: Direct SKU search (published products)
        response = wcapi.get("products", params={"sku": sku})
        if response.status_code == 200:
            products = response.json()
            if products and len(products) > 0:
                logger.debug(
                    f"Found existing published product via SKU search: ID {products[0]['id']}"
                )
                return products[0]["id"]

        # Method 2: Search by SKU in search field (published products)
        response = wcapi.get("products", params={"search": sku})
        if response.status_code == 200:
            products = response.json()
            for product in products:
                if product.get("sku") == sku:
                    logger.debug(
                        f"Found existing published product via search: ID {product['id']}"
                    )
                    return product["id"]

        # Method 3: Search in trash for products with same SKU
        logger.debug(f"Searching in trash for SKU: {sku}")
        trash_response = wcapi.get(
            "products", params={"status": "trash", "per_page": 100}
        )
        if trash_response.status_code == 200:
            trash_products = trash_response.json()
            for product in trash_products:
                if product.get("sku") == sku:
                    logger.warning(
                        f"Found product with SKU {sku} in trash (ID: {product['id']})"
                    )

                    # Ask what to do with trashed product
                    choice = handle_trashed_product(product["id"], sku)
                    if choice == "restore":
                        return product["id"]
                    elif choice == "delete":
                        return None  # Product will be permanently deleted
                    else:
                        return None  # Skip this product

        logger.debug(f"No product found with SKU: {sku}")

    except Exception as e:
        logger.warning(f"Error checking for existing product with SKU {sku}: {e}")
    return None


def handle_trashed_product(product_id: int, sku: str) -> str:
    """Handle products found in trash. Returns 'restore', 'delete', or 'skip'."""
    try:
        logger.info(f"Product with SKU {sku} found in trash (ID: {product_id})")

        # For automated processing, let's restore and update the trashed product
        logger.info(
            f"Automatically restoring trashed product {product_id} to update it..."
        )

        # Restore the product by updating its status
        restore_response = wcapi.put(f"products/{product_id}", {"status": "draft"})
        if restore_response.status_code in [200, 201]:
            logger.info(f"Successfully restored product {product_id} from trash")
            return "restore"
        else:
            logger.error(
                f"Failed to restore product {product_id} from trash: {restore_response.text[:200]}"
            )

            # If restore fails, try to permanently delete it
            logger.info(
                f"Attempting to permanently delete trashed product {product_id}..."
            )
            delete_response = wcapi.delete(
                f"products/{product_id}", params={"force": True}
            )
            if delete_response.status_code in [200, 201]:
                logger.info(f"Successfully deleted trashed product {product_id}")
                return "delete"
            else:
                logger.error(
                    f"Failed to delete trashed product {product_id}: {delete_response.text[:200]}"
                )
                return "skip"

    except Exception as e:
        logger.error(f"Error handling trashed product {product_id}: {e}")
        return "skip"


def map_airtable_to_woocommerce(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Map Airtable fields to WooCommerce data structure using field mappings."""
    data = DEFAULT_VALUES.copy()

    # Debug: Log all available fields from Airtable
    logger.debug("Available Airtable fields:")
    for key, value in fields.items():
        logger.debug(
            f"  - {key}: {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}"
        )

    # Valid WooCommerce product types
    VALID_TYPES = ["simple", "grouped", "external", "variable"]

    # Fields that need special processing (exclude from generic mapping)
    SPECIAL_FIELDS = ["tags", "brand", "parent_category", "category", "dimensions"]

    # Map basic fields
    for wc_field, airtable_field in FIELD_MAPPINGS.items():
        # Skip fields that have special processing
        if wc_field in SPECIAL_FIELDS:
            continue

        if airtable_field in fields and fields[airtable_field]:
            if wc_field in ["regular_price", "sale_price"]:
                data[wc_field] = str(fields[airtable_field])
            elif wc_field == "stock_quantity":
                data[wc_field] = int(fields[airtable_field])
            elif wc_field == "featured":
                data[wc_field] = bool(fields[airtable_field])
            elif wc_field == "type":
                # Validate product type
                product_type = str(fields[airtable_field]).lower()
                if product_type in VALID_TYPES:
                    data[wc_field] = product_type
                else:
                    logger.warning(
                        f"Invalid product type '{product_type}', using default 'simple'"
                    )
                    data[wc_field] = "simple"
            else:
                data[wc_field] = fields[airtable_field]

    # Ensure product name is set (critical field)
    if FIELD_MAPPINGS["name"] in fields and fields[FIELD_MAPPINGS["name"]]:
        data["name"] = fields[FIELD_MAPPINGS["name"]]
        logger.debug(f"Product name set: {data['name']}")
    else:
        logger.warning("No product name found in Airtable fields!")

    # Handle dimensions separately with proper parsing
    dimensions_field = FIELD_MAPPINGS.get("dimensions")
    if dimensions_field in fields and fields[dimensions_field]:
        dimensions = parse_dimensions(fields[dimensions_field])
        data["dimensions"] = dimensions
        logger.debug(f"Parsed dimensions: {dimensions}")

    # Ensure critical fields are present
    # Double-check name is set (WooCommerce requires this)
    if "name" not in data or not data["name"]:
        logger.error("CRITICAL: Product name is missing from data!")
        # Try to get it directly
        name_field = FIELD_MAPPINGS.get("name", "Item Name")
        if name_field in fields and fields[name_field]:
            data["name"] = fields[name_field]
            logger.info(f"Recovered product name: {data['name']}")
        else:
            data["name"] = "Unnamed Product"
            logger.warning("Using default product name 'Unnamed Product'")

    # Handle meta data (SEO fields)
    meta_data = []
    for meta_key, airtable_field in META_MAPPINGS.items():
        if airtable_field in fields and fields[airtable_field]:
            meta_data.append({"key": meta_key, "value": fields[airtable_field]})

    # Add Yoast SEO fields if Rank Math fields aren't present
    if "Meta Title" in fields and fields["Meta Title"]:
        meta_data.append({"key": "_yoast_wpseo_title", "value": fields["Meta Title"]})
    if "Meta Description" in fields and fields["Meta Description"]:
        meta_data.append(
            {"key": "_yoast_wpseo_metadesc", "value": fields["Meta Description"]}
        )
    if "Focus Keyword" in fields and fields["Focus Keyword"]:
        meta_data.append(
            {"key": "_yoast_wpseo_focuskw", "value": fields["Focus Keyword"]}
        )

    # Handle ACF fields with proper linked record handling
    for acf_key, airtable_field in ACF_MAPPINGS.items():
        # Default warehouse location to W#13 for all products
        if acf_key == "location":
            meta_data.append({"key": "location", "value": "W#13"})
            logger.debug(f"Added default ACF field: location = W#13")
            continue

        if airtable_field in fields and fields[airtable_field] is not None:
            acf_value = fields[airtable_field]

            # Handle linked records (arrays with record IDs)
            if isinstance(acf_value, list):
                if len(acf_value) > 0:
                    # For linked records, we need to get the actual display value
                    # This might be a record ID, so we'll use the first item as string
                    if isinstance(acf_value[0], dict) and "id" in acf_value[0]:
                        # This is a linked record with an ID, skip for now
                        logger.warning(
                            f"Skipping linked record field {acf_key} - needs proper handling"
                        )
                        continue
                    else:
                        # Take the first value if it's a simple array
                        acf_value = str(acf_value[0])
                else:
                    continue  # Skip empty arrays

            # Convert boolean-like values for Yes/No fields
            if isinstance(acf_value, str):
                if acf_value.lower() in ["yes", "true", "1"]:
                    acf_value = True
                elif acf_value.lower() in ["no", "false", "0"]:
                    acf_value = False

            meta_data.append({"key": acf_key, "value": acf_value})
            logger.debug(f"Added ACF field: {acf_key} = {acf_value}")

    if meta_data:
        data["meta_data"] = meta_data

    return data


def process_product_tags(fields: Dict[str, Any]) -> List[Dict[str, int]]:
    """Process tags from Airtable fields using cached data for better performance."""
    tag_ids = []
    tags = fields.get(FIELD_MAPPINGS["tags"])

    if isinstance(tags, list):
        logger.info(f"Processing {len(tags)} tags...")
        cached_count = 0
        created_count = 0
        skipped_count = 0

        for i, tag in enumerate(tags):
            if not tag or not isinstance(tag, str):
                logger.warning(f"Skipping invalid tag at index {i}: {tag}")
                skipped_count += 1
                continue

            # Skip very short or problematic tags
            clean_tag = tag.strip()
            if len(clean_tag) < 2:
                logger.warning(f"Skipping too short tag: '{clean_tag}'")
                skipped_count += 1
                continue

            # Skip common problematic words
            if clean_tag.lower() in ["your", "this", "that", "the", "and", "or", "but"]:
                logger.warning(f"Skipping problematic tag: '{clean_tag}'")
                skipped_count += 1
                continue

            # Check if tag exists in cache first
            search_key = clean_tag.lower()
            cached_tags = get_all_existing_tags()

            if search_key in cached_tags:
                tag_id = cached_tags[search_key]
                tag_ids.append({"id": tag_id})
                logger.debug(f"[CACHED] Using existing tag: {clean_tag} (ID: {tag_id})")
                cached_count += 1
            else:
                # Try to create new tag
                tag_id = get_or_create_term(clean_tag, "product_tag")
                if tag_id:
                    tag_ids.append({"id": tag_id})
                    logger.debug(f"[NEW] Created tag: {clean_tag} (ID: {tag_id})")
                    created_count += 1
                else:
                    logger.warning(f"[FAIL] Failed to process tag: {clean_tag}")
                    skipped_count += 1

            # Small delay to prevent API rate limiting only when creating new tags
            if search_key not in cached_tags:
                time.sleep(0.1)

        logger.info(
            f"Tag processing complete: {cached_count} from cache, {created_count} created, {skipped_count} skipped"
        )
        return tag_ids

    logger.info("No tags to process")
    return tag_ids


def process_product_brand(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process brand from Airtable fields and return brand data for product."""
    brand = fields.get(FIELD_MAPPINGS.get("brand"))
    if brand and isinstance(brand, str) and len(brand.strip()) > 1:
        logger.info(f"Processing brand: {brand}")

        # Try WooCommerce Brands plugin first, then fallback to product attributes
        brand_id = get_or_create_term(brand.strip(), "product_brand")

        if brand_id:
            logger.info(f"[OK] Brand processed: {brand} (ID: {brand_id})")
            return {"id": brand_id, "name": brand.strip(), "taxonomy": "product_brand"}
        else:
            # Try as product attribute
            brand_id = get_or_create_term(brand.strip(), "pa_brand")
            if brand_id:
                logger.info(
                    f"[OK] Brand processed as attribute: {brand} (ID: {brand_id})"
                )
                return {"id": brand_id, "name": brand.strip(), "taxonomy": "pa_brand"}
            else:
                logger.warning(f"[FAIL] Failed to process brand: {brand}")
    return None


def process_product_categories(
    fields: Dict[str, Any], all_categories: List[Dict[str, Any]]
) -> List[Dict[str, int]]:
    """Process categories from Airtable fields."""
    cats = []

    # Debug what's in Airtable first
    debug_airtable_categories(fields)

    parent = fields.get(FIELD_MAPPINGS.get("parent_category"))
    sub = fields.get(FIELD_MAPPINGS.get("category"))

    # Handle cases where fields might be lists (e.g., from lookups/rollups)
    if isinstance(parent, list) and parent:
        parent = parent[0] if parent[0] else None
    elif isinstance(parent, str):
        parent = parent.strip() if parent.strip() else None
    else:
        parent = None

    if isinstance(sub, list) and sub:
        sub = sub[0] if sub[0] else None
    elif isinstance(sub, str):
        sub = sub.strip() if sub.strip() else None
    else:
        sub = None

    logger.info(f"Looking for categories - Parent: '{parent}', Sub: '{sub}'")

    # Create a comprehensive mapping with all variations for better matching
    category_lookup = {}
    for wc_cat in all_categories:
        variations = create_category_variations(wc_cat["name"])
        for variation in variations:
            category_lookup[variation] = wc_cat
        logger.debug(f"WC Category '{wc_cat['name']}' variations: {variations}")

    # Function to match a category name against all variations
    def find_category_match(category_name):
        if not category_name:
            return None

        input_variations = create_category_variations(category_name)
        logger.info(
            f"Searching for '{category_name}' using variations: {input_variations}"
        )

        # Try to find a match using any variation
        for variation in input_variations:
            if variation in category_lookup:
                matched_cat = category_lookup[variation]
                logger.info(
                    f"[OK] Matched '{category_name}' -> '{matched_cat['name']}' (ID: {matched_cat['id']}) using variation '{variation}'"
                )
                return {"id": matched_cat["id"]}

        logger.warning(f"[FAIL] No exact match found for category: '{category_name}'")
        return None

    # Match parent category
    if parent:
        parent_match = find_category_match(parent)
        if parent_match:
            cats.append(parent_match)

    # Match sub category
    if sub:
        sub_match = find_category_match(sub)
        if sub_match:
            # Avoid adding duplicates if sub and parent are the same
            if not any(c["id"] == sub_match["id"] for c in cats):
                cats.append(sub_match)

    # If no categories found, assign to a default category
    if not cats:
        logger.warning(
            "No categories matched, looking for 'Miscellaneous' or 'Uncategorized'"
        )
        default_match = find_category_match("Miscellaneous") or find_category_match(
            "Uncategorized"
        )
        if default_match:
            cats.append(default_match)
            logger.info(f"[DEFAULT] Assigned to default category")

    logger.info(f"Final categories assigned: {len(cats)} categories")
    return cats


def process_product_images(
    fields: Dict[str, Any], product_name: str = ""
) -> List[Dict[str, int]]:
    """Process and optimize images from Airtable fields."""
    image_ids = []
    image_index = 0

    for field in IMAGE_FIELDS:
        images = fields.get(field)
        if isinstance(images, list):
            for img in images:
                url = img.get("url")
                if url:
                    img_id = upload_image_to_woocommerce(url, product_name, image_index)
                    if img_id:
                        image_ids.append({"id": img_id})
                        image_index += 1
                    time.sleep(config.upload_delay)

    logger.info(f"Processed {len(image_ids)} images for product")
    return image_ids


def process_single_product(
    record: Dict[str, Any],
    all_categories: List[Dict[str, Any]],
    brand_method: str = None,
) -> bool:
    """Process a single product record."""
    fields = record.get("fields", {})
    sku = fields.get(FIELD_MAPPINGS["sku"])
    name = fields.get(FIELD_MAPPINGS["name"], "Unknown")

    logger.info(f"Product name from Airtable: '{name}'")
    if not sku:
        logger.warning("Skipping - No SKU found")
        return False

    try:
        data = map_airtable_to_woocommerce(fields)
        logger.info(f"Product type: {data.get('type', 'not set')}")

        if "meta_data" in data:
            acf_fields = [
                item for item in data["meta_data"] if item["key"] in ACF_MAPPINGS.keys()
            ]
            if acf_fields:
                logger.info(f"Setting {len(acf_fields)} ACF fields:")
                for acf in acf_fields:
                    logger.info(f"  - {acf['key']}: {acf['value']}")

        logger.info("Processing tags...")
        data["tags"] = process_product_tags(fields)

        logger.info("Processing brand...")
        brand_data = process_product_brand(fields)
        if brand_data:
            if brand_data["taxonomy"] == "product_brand":
                data["brands"] = [{"id": brand_data["id"]}]
                logger.info(
                    f"Added brand to product.brands: {brand_data['name']} (ID: {brand_data['id']})"
                )
            else:
                if "attributes" not in data:
                    data["attributes"] = []
                data["attributes"].append(
                    {
                        "id": 0,
                        "name": "Brand",
                        "position": 0,
                        "visible": True,
                        "variation": False,
                        "options": [brand_data["name"]],
                    }
                )
                logger.info(f"Added brand as product attribute: {brand_data['name']}")
                if "meta_data" not in data:
                    data["meta_data"] = []
                data["meta_data"].append(
                    {"key": "pa_brand", "value": brand_data["name"]}
                )

        logger.info("Processing categories...")
        cats = process_product_categories(fields, all_categories)
        if cats:
            data["categories"] = cats
            cat_ids = [f"ID:{cat['id']}" for cat in cats]
            logger.info(f"Assigned {len(cats)} categories to product: {cat_ids}")

        logger.info("Processing and optimizing images...")
        image_ids = process_product_images(fields, name)
        if image_ids:
            data["images"] = image_ids
            logger.info(f"Assigned {len(image_ids)} optimized images to product")

        # Clean up temporary fields before sending to WooCommerce
        for field in ["brand", "parent_category", "category"]:
            if field in data:
                del data[field]

        logger.info("Checking if product exists...")
        existing_id = get_existing_product(sku)

        if existing_id:
            logger.info(f"Updating existing product (ID: {existing_id})...")
            response = wcapi.put(f"products/{existing_id}", data)
            if response.status_code in [200, 201]:
                logger.info("Successfully updated product!")
                return True
            else:
                logger.error(
                    f"Failed to update: {response.status_code} - {response.text[:500]}"
                )
                return False
        else:
            logger.info("Creating new product...")
            response = wcapi.post("products", data)
            if response.status_code in [200, 201]:
                logger.info("Successfully created product!")
                return True
            elif response.status_code == 400 and "already present" in response.text:
                logger.warning(f"SKU {sku} conflict. Attempting to resolve...")
                # Simplified conflict resolution: assume it's in trash and try updating
                conflicting_id = get_existing_product(sku)  # Re-check to be sure
                if conflicting_id:
                    logger.info(
                        f"Found conflicting product (ID: {conflicting_id}). Updating it."
                    )
                    update_response = wcapi.put(f"products/{conflicting_id}", data)
                    if update_response.status_code in [200, 201]:
                        logger.info("Successfully updated conflicting product!")
                        return True
                logger.error("Could not resolve SKU conflict.")
                return False
            else:
                logger.error(
                    f"Failed to create: {response.status_code} - {response.text[:500]}"
                )
                return False

    except Exception as e:
        logger.error(f"Error processing product SKU {sku}: {e}")
        return False


def sync_products():
    """Main sync logic."""
    logger.info("Starting product sync from Airtable to WooCommerce...")
    try:
        brand_method = test_brand_endpoints()
        logger.info(f"Brand processing method: {brand_method}")

        logger.info("Initializing caches...")
        get_all_existing_tags()
        if brand_method == "brands_plugin":
            get_all_existing_brands("products/brands")
        elif brand_method and brand_method.startswith("attribute_"):
            attr_id = brand_method.split("_")[1]
            get_all_existing_brands(f"products/attributes/{attr_id}/terms")

        logger.info("Fetching products from Airtable...")
        records = items_table.all(view=AIRTABLE_VIEW_NAME)
        logger.info(f"Found {len(records)} products to sync")

        all_categories = fetch_all_categories()
        success_count, error_count, skip_count = 0, 0, 0

        for i, rec in enumerate(records, 1):
            fields = rec.get("fields", {})
            sku = fields.get(FIELD_MAPPINGS["sku"])
            name = fields.get(FIELD_MAPPINGS["name"], "Unknown")
            logger.info(f"[{i}/{len(records)}] Processing: {name} (SKU: {sku})")

            if not sku:
                logger.warning("Skipping - No SKU found")
                skip_count += 1
                continue

            if process_single_product(rec, all_categories, brand_method):
                success_count += 1
            else:
                error_count += 1

        logger.info("=" * 60)
        logger.info("SYNC COMPLETE!")
        logger.info(
            f"Success: {success_count}, Errors: {error_count}, Skipped: {skip_count}"
        )
        logger.info(f"Total processed: {len(records)}")
        logger.info(
            f"Total cached tags: {len(_tag_cache)}, Total cached brands: {len(_brand_cache)}"
        )
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        raise


if __name__ == "__main__":
    sync_products()
