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
from typing import List, Dict, Optional, Any
from functools import wraps

# Handle Windows console encoding
if sys.platform.startswith('win'):
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        pass  # Fallback to default encoding

# Load environment variables
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Setup logging with Windows-compatible encoding
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sync.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
except:
    # Fallback logging setup for encoding issues
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
logger = logging.getLogger(__name__)

# Global cache for tags and brands to avoid repeated API calls
_tag_cache = {}
_brand_cache = {}

@dataclass
class SyncConfig:
    """Configuration for the sync process."""
    max_pages: int = 100
    image_size: tuple = (800, 800)
    upload_delay: float = 0.5
    timeout: int = 30
    per_page: int = 100
    max_retries: int = 3

# Configuration
config = SyncConfig()

# Field mappings from your CSV
FIELD_MAPPINGS = {
    "name": "Item Name",
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
    "category": "Category Rollup (from Category)"
}

# Meta data mappings
META_MAPPINGS = {
    "rank_math_title": "Meta Title",
    "rank_math_description": "Meta Description",
    "rank_math_focus_keyword": "Focus Keyword"
}

# ACF (Advanced Custom Fields) mappings
ACF_MAPPINGS = {
    # Inspection Details
    "inspected": "Inspected",
    "condition": "Condition",
    
    # Product Location
    "location": "Warehouse",
    "aisle_bin": "Shelf Location",
    
    # Product Shipping/Pickup Specs
    "heavy": "Heavy",
    "fragile": "Fragile",
    "big": "Big"
}

# Image field mappings
IMAGE_FIELDS = ["Item Featured Photo", "Item Photos", "Inspection Photos"]

# Default values from your CSV
DEFAULT_VALUES = {
    "status": "publish",
    "catalog_visibility": "visible",
    "tax_status": "taxable",
    "stock_status": "instock",
    "backorders": "no",
    "type": "simple",
    "manage_stock": True
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
        "AIRTABLE_BASE_ID"
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
    timeout=config.timeout
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
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def normalize_category_name(name: str) -> str:
    """Normalize category names for matching - handle & symbols properly."""
    if not name:
        return ""
    # Convert to lowercase, strip whitespace, and normalize & symbols
    normalized = name.strip().lower()
    # Replace various & formats with a standard 'and'
    normalized = normalized.replace(' & ', ' and ')
    normalized = normalized.replace('&', 'and')
    # Clean up multiple spaces
    normalized = ' '.join(normalized.split())
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
    if '&' in clean_name:
        variations.append(clean_name.lower().replace(' & ', ' and '))
        variations.append(clean_name.lower().replace('&', ' and '))
        variations.append(clean_name.lower().replace('&', 'and'))
    
    # Replace 'and' with & (with spaces)  
    if 'and' in clean_name.lower():
        variations.append(clean_name.lower().replace(' and ', ' & '))
        variations.append(clean_name.lower().replace('and', '&'))
    
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
    
    logger.debug(f"Raw Airtable rollup category data:")
    logger.debug(f"  Parent Category Rollup field: {parent} (type: {type(parent)})")
    logger.debug(f"  Category Rollup field: {sub} (type: {type(sub)})")
    
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
            response = wcapi.get("products/tags", params={"per_page": config.per_page, "page": page})
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch tags page {page}: {response.status_code}")
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
        name = tag.get('name', '').strip()
        if name:
            # Store both original and normalized versions
            _tag_cache[name.lower()] = tag['id']
            # Also store normalized version (for & -> and conversion)
            normalized = name.lower().replace('&', 'and').replace('-', ' ')
            _tag_cache[normalized] = tag['id']
    
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
            response = wcapi.get(endpoint, params={"per_page": config.per_page, "page": page})
            
            if response.status_code == 404:
                logger.warning(f"Brand endpoint {endpoint} not found (404) - brands plugin may not be installed")
                break
            elif response.status_code != 200:
                logger.warning(f"Failed to fetch brands page {page}: {response.status_code}")
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
        name = brand.get('name', '').strip()
        if name:
            # Store both original and normalized versions
            _brand_cache[name.lower()] = brand['id']
            # Also store normalized version (for & -> and conversion)
            normalized = name.lower().replace('&', 'and').replace('-', ' ')
            _brand_cache[normalized] = brand['id']
    
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
            response = wcapi.get("products/categories",
                               params={"per_page": config.per_page, "page": page})

            if response.status_code != 200:
                logger.warning(f"Received status code {response.status_code}, stopping category fetch")
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
            logger.info("✅ WooCommerce Brands plugin is active (products/brands endpoint works)")
            brands = response.json()
            if brands:
                logger.info(f"Found {len(brands)} existing brands via Brands plugin")
            return "brands_plugin"
        else:
            logger.warning(f"❌ WooCommerce Brands plugin endpoint failed: {response.status_code}")
    except Exception as e:
        logger.warning(f"❌ Error testing Brands plugin endpoint: {e}")
    
    # Test product attributes endpoint for brand
    try:
        response = wcapi.get("products/attributes")
        if response.status_code == 200:
            attributes = response.json()
            brand_attr = None
            for attr in attributes:
                if attr.get('name', '').lower() in ['brand', 'brands', 'pa_brand']:
                    brand_attr = attr
                    break
            
            if brand_attr:
                logger.info(f"✅ Found brand attribute: {brand_attr['name']} (ID: {brand_attr['id']})")
                # Test the terms endpoint
                terms_response = wcapi.get(f"products/attributes/{brand_attr['id']}/terms", params={"per_page": 1})
                if terms_response.status_code == 200:
                    logger.info("✅ Brand attribute terms endpoint works")
                    return f"attribute_{brand_attr['id']}"
            else:
                logger.warning("❌ No brand attribute found in WooCommerce")
    except Exception as e:
        logger.warning(f"❌ Error testing attribute endpoints: {e}")
    
    logger.error("❌ No working brand endpoints found. Brands will not be processed.")
    return None

def get_or_create_term(name: str, taxonomy: str = "product_tag", max_attempts: int = 2) -> Optional[int]:
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
            logger.debug(f"Found existing tag in cache: {clean_name} (ID: {cached_tags[search_key]})")
            return cached_tags[search_key]
        
        # Try normalized variations
        normalized = search_key.replace('&', 'and').replace('-', ' ')
        if normalized in cached_tags:
            logger.debug(f"Found existing tag via normalization: {clean_name} -> {normalized} (ID: {cached_tags[normalized]})")
            return cached_tags[normalized]
        
        # If not found in cache, create new tag
        endpoint = "products/tags"
    elif taxonomy == "product_brand":
        endpoint = "products/brands"
        cached_brands = get_all_existing_brands(endpoint)
        
        if search_key in cached_brands:
            logger.debug(f"Found existing brand in cache: {clean_name} (ID: {cached_brands[search_key]})")
            return cached_brands[search_key]
        
        normalized = search_key.replace('&', 'and').replace('-', ' ')
        if normalized in cached_brands:
            logger.debug(f"Found existing brand via normalization: {clean_name} -> {normalized} (ID: {cached_brands[normalized]})")
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
            slug = re.sub(r'[^a-z0-9\-]', '', slug)
            
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
                logger.warning(f"Cannot create {taxonomy} - endpoint not found: {endpoint}")
                # If brands endpoint fails, try as regular taxonomy
                if taxonomy == "product_brand":
                    return get_or_create_term(clean_name, "pa_brand", max_attempts)
                return None
            else:
                logger.warning(f"Failed to create {taxonomy} '{clean_name}': {create.status_code} - {create.text[:200]}")
                
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {taxonomy} '{clean_name}': {e}")
            if attempt < max_attempts - 1:
                time.sleep(1)
    
    logger.error(f"Failed to get/create {taxonomy}: {clean_name} after {max_attempts} attempts")
    return None

@retry_on_failure()
def upload_image_to_woocommerce(image_url: str, product_name: str = "") -> Optional[int]:
    """Upload an image to WooCommerce and return its media ID with proper SEO metadata."""
    try:
        logger.info(f"Uploading image: {image_url[:50]}...")

        filename = image_url.split("/")[-1].split("?")[0]
        if not filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
            filename += '.jpg'

        image_data = requests.get(image_url, timeout=20)
        if image_data.status_code != 200:
            logger.error(f"Failed to download image: {image_url}")
            return None

        # Improved image processing with aspect ratio preservation
        img = Image.open(BytesIO(image_data.content)).convert("RGB")
        img.thumbnail(config.image_size, Image.Resampling.LANCZOS)
        
        # Create new image with white background if needed
        if img.size != config.image_size:
            new_img = Image.new("RGB", config.image_size, "white")
            new_img.paste(img, ((config.image_size[0] - img.width) // 2, 
                               (config.image_size[1] - img.height) // 2))
            img = new_img

        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=85)
        buffer.seek(0)

        # Generate SEO-friendly metadata
        clean_filename = filename.rsplit('.', 1)[0].replace('-', ' ').replace('_', ' ')
        
        # Create user-friendly caption instead of using complex filename
        if product_name:
            caption = f"{product_name} image"
            title = f"{product_name} - Product Image"
            alt_text = f"{product_name} product image"
            description = f"Product image for {product_name}"
        else:
            caption = "Product image"
            title = clean_filename if len(clean_filename) < 50 else "Product image"
            alt_text = clean_filename if len(clean_filename) < 100 else "Product image"
            description = f"Product image - {clean_filename}" if len(clean_filename) < 50 else "Product image"

        upload_url = f"{BASE_URL}/wp-json/wp/v2/media"
        headers = {
            'Authorization': 'Basic ' + base64.b64encode(
                f"{WP_USER}:{WP_PASS}".encode()
            ).decode('utf-8'),
            'Content-Disposition': f'attachment; filename={filename}'
        }

        files = {
            'file': (filename, buffer, 'image/jpeg')
        }

        # Upload the image first
        response = requests.post(upload_url, headers=headers, files=files)
        if response.status_code == 201:
            media_data = response.json()
            media_id = media_data.get('id')
            
            # Small delay to ensure media is fully processed
            time.sleep(1)
            
            # Now update the media with SEO metadata using WordPress REST API
            metadata_url = f"{BASE_URL}/wp-json/wp/v2/media/{media_id}"
            metadata_payload = {
                'alt_text': alt_text,
                'caption': caption,
                'description': description,
                'title': title
            }
            
            metadata_headers = {
                'Authorization': 'Basic ' + base64.b64encode(
                    f"{WP_USER}:{WP_PASS}".encode()
                ).decode('utf-8'),
                'Content-Type': 'application/json'
            }
            
            # Update media metadata using PATCH
            metadata_response = requests.patch(
                metadata_url, 
                headers=metadata_headers, 
                json=metadata_payload
            )
            
            if metadata_response.status_code == 200:
                logger.info(f"Uploaded successfully with SEO data (ID: {media_id})")
                logger.debug(f"SEO metadata set - Title: {title}, Alt: {alt_text}")
            else:
                logger.warning(f"Image uploaded but SEO metadata failed: {metadata_response.text[:200]}")
                logger.debug(f"Metadata payload: {metadata_payload}")
                
            return media_id
        else:
            logger.error(f"Failed to upload image to WooCommerce: {response.text[:300]}")
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
                logger.debug(f"Found existing published product via SKU search: ID {products[0]['id']}")
                return products[0]["id"]
        
        # Method 2: Search by SKU in search field (published products)
        response = wcapi.get("products", params={"search": sku})
        if response.status_code == 200:
            products = response.json()
            for product in products:
                if product.get("sku") == sku:
                    logger.debug(f"Found existing published product via search: ID {product['id']}")
                    return product["id"]
        
        # Method 3: Search in trash for products with same SKU
        logger.debug(f"Searching in trash for SKU: {sku}")
        trash_response = wcapi.get("products", params={"status": "trash", "per_page": 100})
        if trash_response.status_code == 200:
            trash_products = trash_response.json()
            for product in trash_products:
                if product.get("sku") == sku:
                    logger.warning(f"Found product with SKU {sku} in trash (ID: {product['id']})")
                    
                    # Ask what to do with trashed product
                    choice = handle_trashed_product(product['id'], sku)
                    if choice == "restore":
                        return product['id']
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
        logger.info(f"Automatically restoring trashed product {product_id} to update it...")
        
        # Restore the product by updating its status
        restore_response = wcapi.put(f"products/{product_id}", {"status": "draft"})
        if restore_response.status_code in [200, 201]:
            logger.info(f"Successfully restored product {product_id} from trash")
            return "restore"
        else:
            logger.error(f"Failed to restore product {product_id} from trash: {restore_response.text[:200]}")
            
            # If restore fails, try to permanently delete it
            logger.info(f"Attempting to permanently delete trashed product {product_id}...")
            delete_response = wcapi.delete(f"products/{product_id}", params={"force": True})
            if delete_response.status_code in [200, 201]:
                logger.info(f"Successfully deleted trashed product {product_id}")
                return "delete"
            else:
                logger.error(f"Failed to delete trashed product {product_id}: {delete_response.text[:200]}")
                return "skip"
                
    except Exception as e:
        logger.error(f"Error handling trashed product {product_id}: {e}")
        return "skip"

def map_airtable_to_woocommerce(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Map Airtable fields to WooCommerce data structure using field mappings."""
    data = DEFAULT_VALUES.copy()
    
    # Valid WooCommerce product types
    VALID_TYPES = ["simple", "grouped", "external", "variable"]
    
    # Fields that need special processing (exclude from generic mapping)
    SPECIAL_FIELDS = ["tags", "brand", "parent_category", "category"]
    
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
                    logger.warning(f"Invalid product type '{product_type}', using default 'simple'")
                    data[wc_field] = "simple"
            else:
                data[wc_field] = fields[airtable_field]
    
    # Handle meta data (SEO fields)
    meta_data = []
    for meta_key, airtable_field in META_MAPPINGS.items():
        if airtable_field in fields and fields[airtable_field]:
            meta_data.append({"key": meta_key, "value": fields[airtable_field]})
    
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
                    if isinstance(acf_value[0], dict) and 'id' in acf_value[0]:
                        # This is a linked record with an ID, skip for now
                        logger.warning(f"Skipping linked record field {acf_key} - needs proper handling")
                        continue
                    else:
                        # Take the first value if it's a simple array
                        acf_value = str(acf_value[0])
                else:
                    continue  # Skip empty arrays
            
            # Convert boolean-like values for Yes/No fields
            if isinstance(acf_value, str):
                if acf_value.lower() in ['yes', 'true', '1']:
                    acf_value = True
                elif acf_value.lower() in ['no', 'false', '0']:
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
            if clean_tag.lower() in ['your', 'this', 'that', 'the', 'and', 'or', 'but']:
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
    
        logger.info(f"Tag processing complete: {cached_count} from cache, {created_count} created, {skipped_count} skipped")
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
            return {
                "id": brand_id,
                "name": brand.strip(),
                "taxonomy": "product_brand"
            }
        else:
            # Try as product attribute
            brand_id = get_or_create_term(brand.strip(), "pa_brand")
            if brand_id:
                logger.info(f"[OK] Brand processed as attribute: {brand} (ID: {brand_id})")
                return {
                    "id": brand_id,
                    "name": brand.strip(),
                    "taxonomy": "pa_brand"
                }
            else:
                logger.warning(f"[FAIL] Failed to process brand: {brand}")
    return None

def process_product_categories(fields: Dict[str, Any], all_categories: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    """Process categories from Airtable fields using rollup fields."""
    cats = []
    
    # Debug what's in Airtable first
    debug_airtable_categories(fields)
    
    parent = fields.get(FIELD_MAPPINGS.get("parent_category"))
    sub = fields.get(FIELD_MAPPINGS.get("category"))
    
    # Handle rollup fields - they can be arrays or strings
    if isinstance(parent, list) and parent:
        # Take the first value from rollup array
        parent = parent[0] if parent[0] else None
    elif isinstance(parent, str):
        # Already a string, keep as is
        parent = parent.strip() if parent.strip() else None
    else:
        parent = None
        
    if isinstance(sub, list) and sub:
        # Take the first value from rollup array
        sub = sub[0] if sub[0] else None
    elif isinstance(sub, str):
        # Already a string, keep as is
        sub = sub.strip() if sub.strip() else None
    else:
        sub = None
    
    logger.info(f"Looking for categories - Parent: '{parent}', Sub: '{sub}'")
    
    # Create a comprehensive mapping with all variations for better matching
    category_lookup = {}
    for wc_cat in all_categories:
        # Get all possible variations of this WooCommerce category name
        variations = create_category_variations(wc_cat['name'])
        for variation in variations:
            category_lookup[variation] = wc_cat
        
        logger.debug(f"WC Category '{wc_cat['name']}' variations: {variations}")
    
    logger.debug(f"Available WooCommerce categories: {[cat['name'] for cat in all_categories[:10]]}...")  # Show first 10
    
    # Function to match a category name against all variations
    def find_category_match(category_name):
        if not category_name:
            return None
            
        # Get all variations of the input category name
        input_variations = create_category_variations(category_name)
        logger.info(f"Searching for '{category_name}' using variations: {input_variations}")
        
        # Debug: Show some WooCommerce categories that might be similar
        similar_cats = []
        for wc_cat in all_categories:
            if any(word in wc_cat['name'].lower() for word in category_name.lower().split()):
                similar_cats.append(wc_cat['name'])
        if similar_cats:
            logger.debug(f"WooCommerce categories containing words from '{category_name}': {similar_cats[:5]}")
        
        # Try to find a match using any variation
        for variation in input_variations:
            if variation in category_lookup:
                matched_cat = category_lookup[variation]
                logger.info(f"[OK] Matched '{category_name}' -> '{matched_cat['name']}' (ID: {matched_cat['id']}) using variation '{variation}'")
                return {"id": matched_cat["id"]}
        
        # If no exact match found, try partial matching with better logic
        logger.debug(f"No exact match found for '{category_name}', trying partial matching...")
        
        # Split category name into words for better partial matching
        category_words = [word.strip() for word in category_name.lower().replace('&', 'and').split()]
        best_match = None
        best_score = 0
        
        for wc_cat in all_categories:
            wc_name = wc_cat['name'].lower().replace('&', 'and')
            wc_words = wc_name.split()
            
            # Count matching words
            matching_words = sum(1 for word in category_words if word in wc_words)
            if matching_words > 0:
                score = matching_words / max(len(category_words), len(wc_words))
                if score > best_score and score >= 0.5:  # At least 50% word match
                    best_score = score
                    best_match = wc_cat
        
        if best_match:
            logger.info(f"[PARTIAL] Best match '{category_name}' -> '{best_match['name']}' (ID: {best_match['id']}) with {best_score:.2f} score")
            return {"id": best_match["id"]}
        
        # Final fallback: try simple contains matching
        for variation in input_variations:
            for wc_cat in all_categories:
                wc_variations = create_category_variations(wc_cat['name'])
                for wc_variation in wc_variations:
                    if (variation in wc_variation or wc_variation in variation) and len(variation) > 3:
                        logger.info(f"[CONTAINS] Contains match '{category_name}' -> '{wc_cat['name']}' (ID: {wc_cat['id']}) using '{variation}' ~ '{wc_variation}'")
                        return {"id": wc_cat["id"]}
        
        logger.warning(f"[FAIL] No match found for category: '{category_name}'")
        logger.debug(f"Available category names containing 'sport': {[cat['name'] for cat in all_categories if 'sport' in cat['name'].lower()]}")
        logger.debug(f"Available category names containing 'outdoor': {[cat['name'] for cat in all_categories if 'outdoor' in cat['name'].lower()]}")
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
            cats.append(sub_match)
    
    # If no categories found, assign to "Miscellaneous" or create a default
    if not cats:
        logger.warning("No categories matched, looking for 'Miscellaneous' or 'Uncategorized'")
        default_match = find_category_match("Miscellaneous")
        if not default_match:
            default_match = find_category_match("Uncategorized")
        if not default_match:
            default_match = find_category_match("Default")
        
        if default_match:
            cats.append(default_match)
            logger.info(f"[DEFAULT] Assigned to default category")
    
    logger.info(f"Final categories assigned: {len(cats)} categories")
    return cats

def process_product_images(fields: Dict[str, Any], product_name: str = "") -> List[Dict[str, int]]:
    """Process images from Airtable fields."""
    image_ids = []
    for field in IMAGE_FIELDS:
        images = fields.get(field)
        if isinstance(images, list):
            for img in images:
                url = img.get("url")
                if url:
                    img_id = upload_image_to_woocommerce(url, product_name)
                    if img_id:
                        image_ids.append({"id": img_id})
                    time.sleep(config.upload_delay)
    return image_ids

def process_single_product(record: Dict[str, Any], all_categories: List[Dict[str, Any]], brand_method: str = None) -> bool:
    """Process a single product record."""
    fields = record.get("fields", {})
    sku = fields.get(FIELD_MAPPINGS["sku"])
    name = fields.get(FIELD_MAPPINGS["name"], "Unknown")

    if not sku:
        logger.warning("Skipping - No SKU found")
        return False

    try:
        # Map basic product data (includes ACF fields)
        logger.info("Mapping product data and ACF fields...")
        data = map_airtable_to_woocommerce(fields)
        
        # Debug: Log the product type being used
        logger.info(f"Product type: {data.get('type', 'not set')}")
        
        # Log ACF fields being set
        if "meta_data" in data:
            acf_fields = [item for item in data["meta_data"] if item["key"] in ACF_MAPPINGS.keys()]
            if acf_fields:
                logger.info(f"Setting {len(acf_fields)} ACF fields:")
                for acf in acf_fields:
                    logger.info(f"  - {acf['key']}: {acf['value']}")
        
        # Process tags with timeout protection
        logger.info("Processing tags...")
        start_time = time.time()
        try:
            data["tags"] = process_product_tags(fields)
            tag_time = time.time() - start_time
            logger.info(f"Tag processing completed in {tag_time:.2f}s")
        except Exception as e:
            logger.error(f"Tag processing failed: {e}")
            data["tags"] = []  # Continue without tags

        # Process brand
        logger.info("Processing brand...")
        brand_data = process_product_brand(fields)
        if brand_data:
            logger.info(f"Brand data received: {brand_data}")
            if brand_data["taxonomy"] == "product_brand":
                # Use WooCommerce Brands plugin format
                data["brands"] = [{"id": brand_data["id"]}]
                logger.info(f"Added brand to product.brands: {brand_data['name']} (ID: {brand_data['id']})")
            else:
                # Use product attribute format
                if "attributes" not in data:
                    data["attributes"] = []
                data["attributes"].append({
                    "id": 0,  # 0 for custom attributes
                    "name": "Brand",
                    "position": 0,
                    "visible": True,
                    "variation": False,
                    "options": [brand_data["name"]]
                })
                logger.info(f"Added brand as product attribute: {brand_data['name']}")
                
                # Also add to meta_data for compatibility
                if "meta_data" not in data:
                    data["meta_data"] = []
                data["meta_data"].append({"key": "pa_brand", "value": brand_data["name"]})
        else:
            logger.info("No brand found for this product")

        # Process categories
        logger.info("Processing categories...")
        cats = process_product_categories(fields, all_categories)
        if cats:
            data["categories"] = cats
            cat_ids = [f"ID:{cat['id']}" for cat in cats]
            logger.info(f"Assigned {len(cats)} categories to product: {cat_ids}")
        else:
            logger.warning("No categories assigned to product - will use WooCommerce default")

        # Process images
        logger.info("Processing images...")
        image_ids = process_product_images(fields, name)
        if image_ids:
            data["images"] = image_ids
            logger.info(f"Assigned {len(image_ids)} images to product")

        # Debug: Log final data structure before sending
        logger.debug("Final product data structure:")
        logger.debug(f"  - Name: {data.get('name', 'not set')}")
        logger.debug(f"  - SKU: {data.get('sku', 'not set')}")
        logger.debug(f"  - Categories: {data.get('categories', 'not set')}")
        logger.debug(f"  - Tags: {len(data.get('tags', []))} tags")
        logger.debug(f"  - Brands: {data.get('brands', 'not set')}")
        logger.debug(f"  - Images: {len(data.get('images', []))} images")
        
        # Remove any leftover category/brand fields that shouldn't be there
        unwanted_fields = ['brand', 'parent_category', 'category']
        for field in unwanted_fields:
            if field in data:
                logger.warning(f"Removing unwanted field from data: {field} = {data[field]}")
                del data[field]

        # Check if product exists and update/create
        logger.info("Checking if product exists...")
        existing_id = get_existing_product(sku)

        if existing_id:
            logger.info(f"Updating existing product (ID: {existing_id})...")
            response = wcapi.put(f"products/{existing_id}", data)
            if response.status_code in [200, 201]:
                logger.info("Successfully updated product!")
                return True
            else:
                logger.error(f"Failed to update: {response.text[:500]}")
                logger.error(f"Data sent: {data}")
                return False
        else:
            logger.info("Creating new product...")
            response = wcapi.post("products", data)
            if response.status_code in [200, 201]:
                logger.info("Successfully created product!")
                return True
            elif response.status_code == 400 and "already present in the lookup table" in response.text:
                logger.warning(f"Product with SKU {sku} already exists but wasn't found in initial search.")
                logger.info("This usually means the product is in trash. Performing comprehensive search...")
                
                # Try to find the product again with a comprehensive search including trash
                try:
                    # First, do a more thorough search of published products
                    search_response = wcapi.get("products", params={"per_page": 100, "search": sku})
                    if search_response.status_code == 200:
                        products = search_response.json()
                        for product in products:
                            if product.get("sku") == sku:
                                existing_id = product["id"]
                                logger.info(f"Found existing published product via comprehensive search (ID: {existing_id}). Updating...")
                                update_response = wcapi.put(f"products/{existing_id}", data)
                                if update_response.status_code in [200, 201]:
                                    logger.info("Successfully updated existing product!")
                                    return True
                                else:
                                    logger.error(f"Failed to update found product: {update_response.text[:500]}")
                                    return False
                    
                    # If not found in published, search trash
                    logger.info("Searching in trash for conflicting SKU...")
                    trash_response = wcapi.get("products", params={"status": "trash", "per_page": 100})
                    if trash_response.status_code == 200:
                        trash_products = trash_response.json()
                        for product in trash_products:
                            if product.get("sku") == sku:
                                logger.info(f"Found conflicting product in trash (ID: {product['id']}). Attempting to resolve...")
                                
                                # Try to permanently delete the trashed product first
                                delete_response = wcapi.delete(f"products/{product['id']}", params={"force": True})
                                if delete_response.status_code in [200, 201]:
                                    logger.info(f"Successfully deleted trashed product {product['id']}. Retrying creation...")
                                    # Now try to create the product again
                                    retry_response = wcapi.post("products", data)
                                    if retry_response.status_code in [200, 201]:
                                        logger.info("Successfully created product after removing trash conflict!")
                                        return True
                                    else:
                                        logger.error(f"Failed to create product even after removing trash: {retry_response.text[:500]}")
                                        return False
                                else:
                                    logger.warning(f"Could not delete trashed product {product['id']}. Trying to restore and update instead...")
                                    # Try to restore and update
                                    restore_response = wcapi.put(f"products/{product['id']}", {"status": "publish"})
                                    if restore_response.status_code in [200, 201]:
                                        logger.info(f"Restored trashed product {product['id']}. Now updating with new data...")
                                        update_response = wcapi.put(f"products/{product['id']}", data)
                                        if update_response.status_code in [200, 201]:
                                            logger.info("Successfully updated restored product!")
                                            return True
                                        else:
                                            logger.error(f"Failed to update restored product: {update_response.text[:500]}")
                                            return False
                                    else:
                                        logger.error(f"Failed to restore trashed product: {restore_response.text[:200]}")
                                        return False
                    
                    logger.error(f"Could not resolve SKU conflict for {sku}. Product may need manual cleanup in WooCommerce.")
                    return False
                    
                except Exception as e:
                    logger.error(f"Error during comprehensive product search and cleanup: {e}")
                    return False
            else:
                logger.error(f"Failed to create: {response.text[:500]}")
                logger.error(f"Data sent: {data}")
                return False

    except Exception as e:
        logger.error(f"Error processing product: {e}")
        return False

def sync_products():
    """Main sync logic."""
    logger.info("Starting product sync from Airtable to WooCommerce...")

    try:
        # Test brand endpoints first
        brand_method = test_brand_endpoints()
        logger.info(f"Brand processing method: {brand_method}")
        
        # Initialize caches to avoid duplicate API calls
        logger.info("Initializing tag and brand caches...")
        get_all_existing_tags()  # This will populate _tag_cache
        if brand_method == "brands_plugin":
            get_all_existing_brands("products/brands")
        elif brand_method and brand_method.startswith("attribute_"):
            attr_id = brand_method.split("_")[1]
            get_all_existing_brands(f"products/attributes/{attr_id}/terms")
        
        # Fetch all records from Airtable
        logger.info("Fetching products from Airtable...")
        records = items_table.all(view=AIRTABLE_VIEW_NAME)
        logger.info(f"Found {len(records)} products to sync")

        # Fetch all categories
        all_categories = fetch_all_categories()
        
        # Process each record
        success_count = 0
        error_count = 0
        skip_count = 0

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

        # Final summary
        logger.info("="*60)
        logger.info("SYNC COMPLETE!")
        logger.info(f"Success: {success_count}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Skipped: {skip_count}")
        logger.info(f"Total processed: {len(records)}")
        logger.info(f"Total cached tags: {len(_tag_cache)}")
        logger.info(f"Total cached brands: {len(_brand_cache)}")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        raise

if __name__ == "__main__":
    sync_products()