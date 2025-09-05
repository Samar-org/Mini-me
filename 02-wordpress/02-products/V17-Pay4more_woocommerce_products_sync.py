#!/usr/bin/env python3
"""
Enhanced Pay4more WooCommerce Product Sync v17
Includes shipping class logic: Sets "Pickup only" when Big or Heavy is checked
Maintains all v16 optimizations for SEO, image processing, and sync tracking
"""

import os, sys, logging, requests, re, time, html, argparse, json, hashlib
from io import BytesIO
from PIL import Image, ImageEnhance, ImageOps
from woocommerce import API as WCAPI
from pyairtable import Api
from typing import Dict, List, Optional, Any, Set, Tuple
from functools import wraps, lru_cache
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from collections import defaultdict

# --- Logging Setup ---
log_filename = f"product_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Enhanced Configuration ---
class SyncConfig:
    # Image settings optimized for e-commerce
    IMAGE_QUALITY = 85
    WEBP_QUALITY = 80
    JPEG_PROGRESSIVE_THRESHOLD = 10240  # 10KB
    IMAGE_SIZE = (1200, 1200)  # Fixed size for all product images
    THUMBNAIL_SIZE = (300, 300)
    UPLOAD_DELAY = 0.3  # Reduced delay

    # API settings
    WC_API_TIMEOUT = 90
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0

    # Performance settings
    BATCH_SIZE = 10
    MAX_WORKERS = 4  # Increased for better parallelism
    IMAGE_WORKERS = 3  # Dedicated workers for image processing
    CACHE_CATEGORIES = True
    PROGRESS_INTERVAL = 10

    # Image management settings
    DELETE_OLD_IMAGES = True  # Delete old images when updating
    CLEANUP_ORPHANS = True  # Clean up orphaned images
    IMAGE_HASH_CHECK = True  # Check if image already exists by hash
    MAX_IMAGES_PER_PRODUCT = 10  # Limit images per product

    # SEO settings
    AUTO_ALT_TEXT = True  # Generate alt text if not provided
    AUTO_IMAGE_TITLE = True  # Generate image titles
    COMPRESS_IMAGES = True  # Enable image compression
    
    # Sync tracking
    SYNC_SOURCE = "Python"  # Default sync source identifier
    
    # Shipping settings
    PICKUP_ONLY_SHIPPING_CLASS = "pickup-only"  # Slug for pickup only shipping class

config = SyncConfig()

# --- Environment Variables ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WC_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")
WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
UPDATE_IMAGES = os.getenv("UPDATE_IMAGES", "false").lower() == "true"
DELETE_OLD_IMAGES = os.getenv("DELETE_OLD_IMAGES", "true").lower() == "true"

# Validate environment
required_vars = ["AIRTABLE_API_KEY","AIRTABLE_BASE_ID","WC_URL","WC_CONSUMER_KEY","WC_CONSUMER_SECRET","WP_USER","WP_PASS"]
if any(not globals().get(v) for v in required_vars):
    logger.error("Missing required environment variables")
    sys.exit(1)

# Initialize APIs with connection pooling
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
session.mount('http://', adapter)
session.mount('https://', adapter)

airtable_api = Api(AIRTABLE_API_KEY)
items_table = airtable_api.table(AIRTABLE_BASE_ID, "Items-Pay4more")
catalogue_table = airtable_api.table(AIRTABLE_BASE_ID, "Product Catalogue")
wcapi = WCAPI(url=WC_URL, consumer_key=WC_CONSUMER_KEY, consumer_secret=WC_CONSUMER_SECRET, version="wc/v3", timeout=config.WC_API_TIMEOUT)

# --- Global Caches ---
category_cache = {}
brand_cache = {}
tag_cache = {}
existing_products_cache = {}
image_hash_cache = {}  # Cache for image hashes
orphan_images = set()  # Track orphan images
shipping_class_cache = {}  # Cache for shipping classes
missing_categories = []
processed_count = 0
start_time = time.time()
sync_stats = {"created": 0, "updated": 0, "failed": 0, "skipped": 0}

# --- Enhanced Helpers ---
def retry(max_retries=3, delay=2.0, backoff=1.5):
    """Enhanced retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(f"{func.__name__} failed ({attempt+1}/{max_retries}): {e}")
                        time.sleep(current_delay)
                        current_delay *= backoff
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"{func.__name__} failed ({attempt+1}/{max_retries}): {e}")
                    time.sleep(current_delay)
                    current_delay *= backoff
            raise last_exception
        return wrapper
    return decorator

def calculate_image_hash(image_data: bytes) -> str:
    """Calculate hash of image for deduplication"""
    return hashlib.md5(image_data).hexdigest()

def first(x):
    """Get first element if list, otherwise return value"""
    return x[0] if isinstance(x, list) and x else x

def clean_and_normalize(name: str) -> str:
    """Clean and normalize text for matching"""
    if not isinstance(name, str):
        return ""
    unescaped = html.unescape(name)
    cleaned = re.sub(r"[\u200B-\u200D\uFEFF\s]+", " ", unescaped).strip()
    return re.sub(r"\s+", " ", cleaned.lower().replace(" & ", " and ").replace("&", "and"))

def slugify_product_name(product_name: str) -> str:
    """Create a clean slug from product name for consistency"""
    # Remove special characters and convert to lowercase
    slug = re.sub(r'[^\w\s-]', '', product_name.lower())
    # Replace spaces with hyphens
    slug = re.sub(r'[-\s]+', '-', slug)
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    return slug

def get_current_timestamp() -> str:
    """Get current timestamp in ISO format for Airtable"""
    return datetime.now().isoformat()

def update_sync_metadata(record_id: str, woocommerce_id: int, status: str = "Listed"):
    """Update Airtable record with sync metadata"""
    try:
        update_data = {
            "Listing Status": status,
            "Last Sync": get_current_timestamp(),
            "WooCommerce ID": str(woocommerce_id),
            "Sync-Source": config.SYNC_SOURCE
        }
        items_table.update(record_id, update_data)
        logger.debug(f"Updated sync metadata for record {record_id}: WC ID {woocommerce_id}")
    except Exception as e:
        logger.warning(f"Failed to update sync metadata for record {record_id}: {e}")

# --- Shipping Class Management ---
def initialize_shipping_class_cache():
    """Initialize the shipping class cache"""
    global shipping_class_cache
    logger.info("Initializing shipping class cache...")
    try:
        page = 1
        while page <= 10:
            r = wcapi.get("products/shipping_classes", params={"per_page": 100, "page": page})
            if r.status_code != 200:
                logger.warning(f"Failed to fetch shipping classes page {page}: {r.status_code}")
                break
            classes = r.json()
            if not classes:
                break
            for sc in classes:
                slug = sc.get("slug", "")
                shipping_class_cache[slug] = sc["id"]
                # Also cache by name
                name = clean_and_normalize(sc.get("name", ""))
                if name:
                    shipping_class_cache[name] = sc["id"]
            page += 1
        logger.info(f"Cached {len(shipping_class_cache)} shipping classes")
        
        # Log if pickup-only class exists
        if config.PICKUP_ONLY_SHIPPING_CLASS in shipping_class_cache:
            logger.info(f"✅ Found '{config.PICKUP_ONLY_SHIPPING_CLASS}' shipping class (ID: {shipping_class_cache[config.PICKUP_ONLY_SHIPPING_CLASS]})")
        else:
            logger.warning(f"⚠️ '{config.PICKUP_ONLY_SHIPPING_CLASS}' shipping class not found - will try to create it")
    except Exception as e:
        logger.warning(f"Could not initialize shipping class cache: {e}")

def get_or_create_shipping_class(slug: str, name: str = None) -> Optional[int]:
    """Get or create a shipping class"""
    # Check cache first
    if slug in shipping_class_cache:
        return shipping_class_cache[slug]
    
    # Try to fetch it
    try:
        r = wcapi.get("products/shipping_classes", params={"slug": slug})
        if r.status_code == 200 and r.json():
            sc = r.json()[0]
            shipping_class_cache[slug] = sc["id"]
            return sc["id"]
    except Exception as e:
        logger.debug(f"Shipping class {slug} not found, will create: {e}")
    
    # Create new shipping class
    try:
        data = {
            "name": name or slug.replace("-", " ").title(),
            "slug": slug,
            "description": f"Shipping class for {name or slug}"
        }
        r = wcapi.post("products/shipping_classes", data)
        if r.status_code in (200, 201):
            sc = r.json()
            shipping_class_cache[slug] = sc["id"]
            logger.info(f"Created shipping class: {data['name']} (ID: {sc['id']})")
            return sc["id"]
        else:
            logger.error(f"Failed to create shipping class {slug}: {r.status_code} - {r.text[:200]}")
    except Exception as e:
        logger.error(f"Error creating shipping class {slug}: {e}")
    
    return None

def determine_shipping_class(fields: Dict[str, Any]) -> Optional[int]:
    """Determine the appropriate shipping class based on product attributes"""
    # Check if product is Heavy or Big
    is_heavy = first(fields.get("Heavy", False))
    is_big = first(fields.get("Big", False))
    
    # Convert to boolean if string
    if isinstance(is_heavy, str):
        is_heavy = is_heavy.lower() in ['yes', 'true', '1']
    if isinstance(is_big, str):
        is_big = is_big.lower() in ['yes', 'true', '1']
    
    # If either Heavy or Big is true, set to Pickup Only
    if is_heavy or is_big:
        shipping_class_id = get_or_create_shipping_class(
            config.PICKUP_ONLY_SHIPPING_CLASS, 
            "Pickup Only"
        )
        if shipping_class_id:
            logger.info(f"Setting shipping class to 'Pickup Only' (Heavy: {is_heavy}, Big: {is_big})")
            return shipping_class_id
        else:
            logger.warning(f"Could not set 'Pickup Only' shipping class (Heavy: {is_heavy}, Big: {is_big})")
    
    return None

# --- Media Management ---
@lru_cache(maxsize=1000)
def get_existing_media_by_hash(image_hash: str) -> Optional[int]:
    """Check if image already exists by hash"""
    if not config.IMAGE_HASH_CHECK:
        return None
    # Check cache first
    if image_hash in image_hash_cache:
        return image_hash_cache[image_hash]
    return None

def scan_orphan_images():
    """Scan for orphaned product images"""
    if not config.CLEANUP_ORPHANS:
        return
    logger.info("Scanning for orphaned images...")
    try:
        page = 1
        all_media = []
        while page <= 10:
            r = requests.get(
                f"{WC_URL}/wp-json/wp/v2/media",
                params={"per_page": 100, "page": page, "media_type": "image"},
                auth=(WP_USER, WP_PASS),
                timeout=30
            )
            if r.status_code != 200: break
            media = r.json()
            if not media: break
            all_media.extend(media)
            page += 1

        used_images = set()
        page = 1
        while page <= 50:
            r = wcapi.get("products", params={"per_page": 100, "page": page})
            if r.status_code != 200: break
            products = r.json()
            if not products: break
            for product in products:
                for img in product.get("images", []):
                    if img.get("id"):
                        used_images.add(img["id"])
            page += 1

        for media in all_media:
            if media["id"] not in used_images:
                if any(keyword in media.get("title", {}).get("rendered", "").lower() for keyword in ["product", "item", "sku"]):
                    orphan_images.add(media["id"])
        logger.info(f"Found {len(orphan_images)} orphaned product images")
    except Exception as e:
        logger.warning(f"Could not scan for orphan images: {e}")

def delete_orphan_images(limit: int = 50):
    """Delete orphaned images"""
    if not orphan_images or not config.CLEANUP_ORPHANS:
        return
    deleted = 0
    logger.info(f"Deleting up to {limit} orphaned images...")
    for media_id in list(orphan_images)[:limit]:
        try:
            r = requests.delete(
                f"{WC_URL}/wp-json/wp/v2/media/{media_id}",
                params={"force": True}, auth=(WP_USER, WP_PASS), timeout=30
            )
            if r.status_code in (200, 204):
                orphan_images.remove(media_id)
                deleted += 1
                logger.debug(f"Deleted orphan image ID: {media_id}")
        except Exception as e:
            logger.warning(f"Could not delete orphan image {media_id}: {e}")
    if deleted > 0:
        logger.info(f"Deleted {deleted} orphaned images")

def delete_old_product_images(product_id: int, keep_ids: List[int] = None):
    """Delete old images from a product"""
    if not config.DELETE_OLD_IMAGES:
        return
    keep_ids = keep_ids or []
    try:
        r = wcapi.get(f"products/{product_id}")
        if r.status_code != 200: return
        product = r.json()
        current_images = product.get("images", [])
        for img in current_images:
            img_id = img.get("id")
            if img_id and img_id not in keep_ids:
                try:
                    del_r = requests.delete(
                        f"{WC_URL}/wp-json/wp/v2/media/{img_id}",
                        params={"force": True}, auth=(WP_USER, WP_PASS), timeout=30
                    )
                    if del_r.status_code in (200, 204):
                        logger.debug(f"Deleted old image ID: {img_id}")
                except Exception as e:
                    logger.warning(f"Could not delete old image {img_id}: {e}")
    except Exception as e:
        logger.warning(f"Could not delete old images for product {product_id}: {e}")

# --- Enhanced Image Processing ---
@retry()
def optimize_and_upload_image(url, name, sku, alt, seo_title, seo_desc, focus, index):
    """Enhanced image optimization with exact 1200x1200 sizing and SEO filenames"""
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        image_data = r.content

        if config.IMAGE_HASH_CHECK:
            image_hash = calculate_image_hash(image_data)
            existing_id = get_existing_media_by_hash(image_hash)
            if existing_id:
                logger.debug(f"Image already exists (hash match): {existing_id}")
                return existing_id

        img = Image.open(BytesIO(image_data))
        if img.mode in ("RGBA", "P"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "RGBA": bg.paste(img, mask=img.split()[3])
            else: bg.paste(img)
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")

        img = ImageOps.exif_transpose(img)
        img.thumbnail((1180, 1180), Image.Resampling.LANCZOS)

        if config.COMPRESS_IMAGES:
            img = ImageEnhance.Sharpness(img).enhance(1.2)
            img = ImageEnhance.Contrast(img).enhance(1.05)

        canvas = Image.new("RGB", config.IMAGE_SIZE, (255, 255, 255))
        x = (config.IMAGE_SIZE[0] - img.width) // 2
        y = (config.IMAGE_SIZE[1] - img.height) // 2
        canvas.paste(img, (x, y))

        buf = BytesIO()
        format_used = "webp"
        try:
            canvas.save(buf, format="WEBP", quality=config.WEBP_QUALITY, method=6)
        except:
            buf.seek(0)
            buf.truncate()
            if len(image_data) > config.JPEG_PROGRESSIVE_THRESHOLD:
                canvas.save(buf, format="JPEG", quality=config.IMAGE_QUALITY, optimize=True, progressive=True)
            else:
                canvas.save(buf, format="JPEG", quality=config.IMAGE_QUALITY, optimize=True)
            format_used = "jpg"
        buf.seek(0)

        product_slug = slugify_product_name(name)
        filename = f"{product_slug}-{index+1}.{format_used}" if index > 0 else f"{product_slug}.{format_used}"
        
        if config.AUTO_ALT_TEXT and not alt:
            alt = f"{name} - Product Image {index + 1}" if index > 0 else f"{name} - Main Product Image"
        if config.AUTO_IMAGE_TITLE and not seo_title:
            seo_title = f"{name} - Product Photo {index + 1}" if index > 0 else f"{name} - Main Product Photo"

        upload_headers = {
            'Authorization': 'Basic ' + base64.b64encode(f"{WP_USER}:{WP_PASS}".encode()).decode('utf-8'),
            'Content-Disposition': f'attachment; filename={filename}',
            'Content-Type': f'image/{format_used}'
        }
        upload_data = buf.read()
        up = requests.post(f"{WC_URL}/wp-json/wp/v2/media", headers=upload_headers, data=upload_data, timeout=60)
        up.raise_for_status()
        media_data = up.json()
        media_id = media_data["id"]

        if config.IMAGE_HASH_CHECK:
            image_hash_cache[calculate_image_hash(upload_data)] = media_id

        time.sleep(0.5)
        seo_payload = {
            "alt_text": alt, "title": seo_title, "caption": alt, "description": seo_desc or f"Product image for {name}",
            "meta": {
                "rank_math_title": seo_title, "rank_math_description": seo_desc, "rank_math_focus_keyword": focus,
                "_yoast_wpseo_title": seo_title, "_yoast_wpseo_metadesc": seo_desc, "_yoast_wpseo_focuskw": focus
            }
        }
        seo_r = requests.post(f"{WC_URL}/wp-json/wp/v2/media/{media_id}", auth=(WP_USER, WP_PASS), json=seo_payload, timeout=30)
        if seo_r.status_code not in (200, 201):
            logger.warning(f"SEO metadata update failed: {seo_r.text[:200]}")
        
        logger.debug(f"Uploaded optimized image: {filename} (ID: {media_id}, Size: 1200x1200, Format: {format_used})")
        return media_id
    except Exception as e:
        logger.error(f"Failed to upload image {url[:50]}: {e}")
        raise

def process_product_images(cfields, name, sku, seo_title, seo_desc, focus, existing_product_id=None):
    """Enhanced image processing with specified order, cleanup, and exact sizing."""
    # Process images in the specified order: 1. Featured Photos, 2. Photo Files
    featured_urls = [a["url"] for a in cfields.get("Featured Photos", []) if "url" in a]
    other_urls = [a["url"] for a in cfields.get("Photo Files", []) if "url" in a]
    
    # Combine lists, ensuring featured images are first and there are no duplicates
    all_urls = []
    seen_urls = set()
    for url in featured_urls + other_urls:
        if url not in seen_urls:
            all_urls.append(url)
            seen_urls.add(url)

    alt = first(cfields.get("Image Alt Text", "")) or name
    if not all_urls: return []

    urls = all_urls[:config.MAX_IMAGES_PER_PRODUCT]
    uploaded, uploaded_ids = [], []

    with ThreadPoolExecutor(max_workers=config.IMAGE_WORKERS) as executor:
        futures = {executor.submit(optimize_and_upload_image, u, name, sku, alt, seo_title, seo_desc, focus, i): i for i, u in enumerate(urls)}
        for future in as_completed(futures):
            try:
                media_id = future.result()
                uploaded.append({"id": media_id})
                uploaded_ids.append(media_id)
            except Exception as e:
                logger.error(f"Image upload failed: {e}")
    
    if existing_product_id and config.DELETE_OLD_IMAGES:
        delete_old_product_images(existing_product_id, uploaded_ids)
    
    return uploaded

# --- WooCommerce Categories, Brands, Tags ---
def fetch_all_categories():
    cats, page = [], 1
    logger.info("Fetching WooCommerce categories...")
    while page <= 20:
        try:
            r = wcapi.get("products/categories", params={"per_page": 100, "page": page})
            if r.status_code != 200: logger.error(f"Category fetch failed page {page}: {r.text}"); break
            data = r.json()
            if not data: break
            cats.extend(data)
            page += 1
        except Exception as e:
            logger.error(f"Error fetching categories page {page}: {e}"); break
    logger.info(f"Fetched {len(cats)} categories")
    for c in cats:
        norm = clean_and_normalize(c.get("name", ""))
        category_cache[norm] = c["id"]
        slug_norm = clean_and_normalize(c.get("slug", "").replace("-", " "))
        if slug_norm: category_cache[slug_norm] = c["id"]
    return cats

all_wc_categories = []

def get_existing_category(name: str) -> Optional[int]:
    norm = clean_and_normalize(name)
    if norm in category_cache: return category_cache[norm]
    for c in all_wc_categories:
        if norm in (clean_and_normalize(c.get("name", "")), clean_and_normalize(c.get("slug", " ").replace("-", " "))):
            category_cache[norm] = c["id"]
            return c["id"]
    missing_categories.append({"original": name, "normalized": norm})
    logger.warning(f"Category '{name}' not found in WooCommerce")
    return None

def initialize_brand_cache():
    global brand_cache
    logger.info("Initializing brand cache...")
    try:
        page = 1
        while page <= 10:
            r = wcapi.get("products/brands", params={"per_page": 100, "page": page})
            if r.status_code != 200:
                if r.status_code == 404: logger.info("Brands plugin not installed")
                break
            brands = r.json()
            if not brands: break
            for b in brands:
                brand_cache[clean_and_normalize(b.get("name", ""))] = b["id"]
            page += 1
        logger.info(f"Cached {len(brand_cache)} brands")
    except Exception as e:
        logger.warning(f"Could not initialize brand cache: {e}")

def get_or_create_brand(name: str) -> Optional[int]:
    norm = clean_and_normalize(name)
    if norm in brand_cache: return brand_cache[norm]
    r = wcapi.get("products/brands", params={"search": name})
    if r.status_code == 200:
        for b in r.json():
            if clean_and_normalize(b.get("name", "")) == norm:
                brand_cache[norm] = b["id"]
                return b["id"]
    cr = wcapi.post("products/brands", {"name": name})
    if cr.status_code in (200, 201):
        bid = cr.json().get("id")
        brand_cache[norm] = bid
        logger.info(f"Created brand: {name} (ID: {bid})")
        return bid
    logger.warning(f"Failed to create brand '{name}': {cr.text}")
    return None

def initialize_tag_cache():
    global tag_cache
    logger.info("Initializing tag cache...")
    try:
        page = 1
        while page <= 10:
            r = wcapi.get("products/tags", params={"per_page": 100, "page": page})
            if r.status_code != 200: break
            tags = r.json()
            if not tags: break
            for t in tags:
                tag_cache[clean_and_normalize(t.get("name", ""))] = t["id"]
            page += 1
        logger.info(f"Cached {len(tag_cache)} tags")
    except Exception as e:
        logger.warning(f"Could not initialize tag cache: {e}")

def get_or_create_tag(name: str) -> Dict:
    norm = clean_and_normalize(name)
    if norm in tag_cache: return {"id": tag_cache[norm]}
    try:
        r = wcapi.post("products/tags", {"name": name})
        if r.status_code in (200, 201):
            tag_id = r.json().get("id")
            tag_cache[norm] = tag_id
            return {"id": tag_id}
        elif r.status_code == 400 and "term_exists" in r.text:
            search_r = wcapi.get("products/tags", params={"search": name})
            if search_r.status_code == 200:
                for t in search_r.json():
                    if clean_and_normalize(t.get("name", "")) == norm:
                        tag_cache[norm] = t["id"]
                        return {"id": t["id"]}
    except Exception as e:
        logger.warning(f"Failed to create tag '{name}': {e}")
    return {"name": name}

# --- Product Cache ---
def initialize_product_cache():
    global existing_products_cache
    logger.info("Initializing product cache...")
    try:
        page = 1
        while page <= 30:
            r = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"})
            if r.status_code != 200: break
            products = r.json()
            if not products: break
            for p in products:
                sku = p.get("sku")
                if sku: existing_products_cache[sku] = p["id"]
            page += 1
            if page % 5 == 0: logger.info(f"Cached {len(existing_products_cache)} products so far...")
        logger.info(f"Cached {len(existing_products_cache)} existing products")
    except Exception as e:
        logger.warning(f"Could not fully initialize product cache: {e}")

# --- Enhanced Product Sync with Metadata Tracking and Shipping Class ---
def process_single_product(record: Dict) -> bool:
    global processed_count, sync_stats
    try:
        fields, rid = record.get("fields", {}), record.get("id")
        sku, cat_code = first(fields.get("SKU")), first(fields.get("4more-Product-Code-linked"))
        
        if not sku or not cat_code: 
            items_table.update(rid, {
                "Listing Status": "Loading Error",
                "Last Sync": get_current_timestamp(),
                "Sync-Source": config.SYNC_SOURCE
            })
            sync_stats["skipped"] += 1
            return False
        
        cat_record = catalogue_table.first(formula=f"{{4more-Product-Code}}='{cat_code}'")
        if not cat_record: 
            items_table.update(rid, {
                "Listing Status": "Loading Error",
                "Last Sync": get_current_timestamp(),
                "Sync-Source": config.SYNC_SOURCE
            })
            sync_stats["skipped"] += 1
            return False
        
        cfields = cat_record.get("fields", {})
        name = first(cfields.get("Product Name"))
        if not name: 
            items_table.update(rid, {
                "Listing Status": "Loading Error",
                "Last Sync": get_current_timestamp(),
                "Sync-Source": config.SYNC_SOURCE
            })
            sync_stats["skipped"] += 1
            return False
        
        processed_count += 1
        if processed_count % config.PROGRESS_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {processed_count} products | Rate: {rate:.1f}/s")
        
        logger.info(f"Processing [{processed_count}]: {name} (SKU: {sku})")
        
        seo_title, seo_desc, focus_kw = first(cfields.get("Meta Title", name)), first(cfields.get("Meta Description", "")), first(cfields.get("Focus Keyword", ""))
        
        payload = {
            "type": "simple", "status": "publish", "manage_stock": True, "name": name, "sku": sku,
            "description": first(cfields.get("Description", "")), "short_description": first(cfields.get("Meta Description", "")),
            "meta_data": [
                {"key": "rank_math_title", "value": seo_title}, {"key": "rank_math_description", "value": seo_desc},
                {"key": "rank_math_focus_keyword", "value": focus_kw}, {"key": "rank_math_pillar_content", "value": "off"},
                {"key": "rank_math_robots", "value": ["index", "follow"]}, {"key": "_yoast_wpseo_title", "value": seo_title},
                {"key": "_yoast_wpseo_metadesc", "value": seo_desc}, {"key": "_yoast_wpseo_focuskw", "value": focus_kw},
                {"key": "_schema_markup", "value": "Product"},
                {"key": "_airtable_record_id", "value": rid},  # Store Airtable record ID in WooCommerce
                {"key": "_sync_source", "value": config.SYNC_SOURCE},
                {"key": "_last_sync", "value": get_current_timestamp()}
            ]
        }
        
        if (rp := first(cfields.get("Unit Retail Price"))): payload["regular_price"] = str(rp)
        if (sp := first(cfields.get("4more Unit Price"))): payload["sale_price"] = str(sp)
        if (sq := first(fields.get("Quantity", 0))) is not None:
            payload["stock_quantity"] = int(sq)
            payload["stock_status"] = "instock" if int(sq) > 0 else "outofstock"
        
        if (w := first(cfields.get("Weight"))): payload["weight"] = str(w)
        if (d := first(cfields.get("Dimensions"))):
            try:
                parts = d.replace(" ", "").split("x")
                if len(parts) == 3: payload["dimensions"] = {"length": parts[0], "width": parts[1], "height": parts[2]}
            except: pass
        
        # NEW: Determine and set shipping class based on Heavy/Big fields
        shipping_class_id = determine_shipping_class(fields)
        if shipping_class_id:
            payload["shipping_class_id"] = shipping_class_id
            logger.info(f"Shipping class set to ID: {shipping_class_id}")
        
        parent_field, child_field = cfields.get("Parent Category Rollup (from Category)"), cfields.get("Category Name")
        cat_names = []
        if isinstance(parent_field, list): cat_names.extend(parent_field)
        elif isinstance(parent_field, str): cat_names.append(parent_field)
        if isinstance(child_field, list): cat_names.extend(child_field)
        elif isinstance(child_field, str): cat_names.append(child_field)
        
        wc_categories = []
        if parent_field:
            parent_name = parent_field[0] if isinstance(parent_field, list) else parent_field
            if (parent_id := get_existing_category(parent_name)): wc_categories.append({"id": parent_id})
        for cat in set(filter(None, cat_names)):
            if not parent_field or cat != (parent_field[0] if isinstance(parent_field, list) else parent_field):
                if (cid := get_existing_category(cat)): wc_categories.append({"id": cid})
        if wc_categories: payload["categories"] = wc_categories
        
        tags = cfields.get("Product Tags", [])
        taglist = [t.strip() for t in (tags if isinstance(tags, list) else tags.split(",") if isinstance(tags, str) else []) if t.strip()]
        if taglist and (tag_objects := [get_or_create_tag(t) for t in taglist[:20] if get_or_create_tag(t)]):
            payload["tags"] = tag_objects
        
        if (brand := first(cfields.get("Brand"))):
            if (bid := get_or_create_brand(brand)): payload["brands"] = [bid]
            else:
                if "attributes" not in payload: payload["attributes"] = []
                payload["attributes"].append({"name": "Brand", "options": [brand], "visible": True, "variation": False})
        
        acf_fields = {"inspected": first(fields.get("Inspected")), "condition": first(fields.get("Condition")),
                      "location": first(fields.get("Warehouse", "W#13")), "aisle_bin": first(fields.get("Shelf Location")),
                      "heavy": first(fields.get("Heavy")), "fragile": first(fields.get("Fragile")), "big": first(fields.get("Big"))}
        for key, value in acf_fields.items():
            if value is not None: payload["meta_data"].append({"key": key, "value": value})
        
        # Check for existing WooCommerce ID in Airtable first
        existing_wc_id = fields.get("WooCommerce ID")
        existing, existing_id = None, None
        
        if existing_wc_id:
            # Try to use the stored WooCommerce ID
            try:
                existing_id = int(existing_wc_id)
                resp = wcapi.get(f"products/{existing_id}")
                if resp.status_code == 200:
                    existing = resp.json()
                    logger.debug(f"Found product using stored WooCommerce ID: {existing_id}")
                else:
                    logger.warning(f"Stored WooCommerce ID {existing_id} not found, searching by SKU")
                    existing_wc_id = None  # Clear invalid ID
            except (ValueError, TypeError):
                logger.warning(f"Invalid WooCommerce ID stored: {existing_wc_id}")
                existing_wc_id = None
        
        # Fallback to SKU search if no valid WooCommerce ID
        if not existing_wc_id:
            if sku in existing_products_cache:
                existing_id = existing_products_cache[sku]
                try:
                    if (resp := wcapi.get(f"products/{existing_id}")).status_code == 200: 
                        existing = resp.json()
                except: pass
            else:
                if (resp := wcapi.get("products", params={"sku": sku})).status_code == 200 and (ex_list := resp.json()):
                    existing, existing_id = ex_list[0], ex_list[0]["id"]
                    existing_products_cache[sku] = existing_id
        
        imgs = existing.get("images", []) if existing else []
        if UPDATE_IMAGES or not imgs:
            payload["images"] = process_product_images(cfields, name, sku, seo_title, seo_desc, focus_kw, existing_id)
        
        resp = wcapi.put(f"products/{existing_id}", payload) if existing else wcapi.post("products", payload)
        
        if resp.status_code in (200, 201):
            product_data = resp.json()
            product_id = product_data.get("id", existing_id if existing else "Unknown")
            logger.info(f"✅ {'Updated' if existing else 'Created'}: {name} (ID: {product_id})")
            
            # Update Airtable with sync metadata
            update_sync_metadata(rid, product_id, "Listed")
            
            if existing:
                sync_stats["updated"] += 1
            else:
                sync_stats["created"] += 1
            
            return True
        else:
            logger.error(f"Failed to sync {sku}: {resp.status_code} - {resp.text[:200]}")
            items_table.update(rid, {
                "Listing Status": "Loading Error",
                "Last Sync": get_current_timestamp(),
                "Sync-Source": config.SYNC_SOURCE
            })
            sync_stats["failed"] += 1
            return False
            
    except Exception as e:
        logger.error(f"Error syncing SKU {record.get('id')}: {e}", exc_info=True)
        if record.get("id"): 
            items_table.update(record.get("id"), {
                "Listing Status": "Loading Error",
                "Last Sync": get_current_timestamp(),
                "Sync-Source": config.SYNC_SOURCE
            })
        sync_stats["failed"] += 1
        return False

def process_batch(records: List[Dict]) -> int:
    success = 0
    for r in records:
        if process_single_product(r): success += 1
    return success

def generate_sync_report(total: int, success: int, elapsed: float):
    report = {
        "timestamp": datetime.now().isoformat(), 
        "duration_seconds": round(elapsed, 2),
        "total_products": total, 
        "successful": success, 
        "failed": total - success,
        "created": sync_stats["created"],
        "updated": sync_stats["updated"],
        "skipped": sync_stats["skipped"],
        "rate_per_second": round(total / elapsed, 2) if elapsed > 0 else 0,
        "rate_per_minute": round((total / elapsed) * 60, 2) if elapsed > 0 else 0,
        "caches": {
            "categories": len(category_cache), 
            "brands": len(brand_cache), 
            "tags": len(tag_cache),
            "products": len(existing_products_cache), 
            "image_hashes": len(image_hash_cache),
            "shipping_classes": len(shipping_class_cache)
        },
        "orphan_images_found": len(orphan_images), 
        "missing_categories": len(set(cat["normalized"] for cat in missing_categories)),
        "sync_source": config.SYNC_SOURCE,
        "config": {
            "batch_size": config.BATCH_SIZE, 
            "max_workers": config.MAX_WORKERS, 
            "image_workers": config.IMAGE_WORKERS,
            "delete_old_images": config.DELETE_OLD_IMAGES, 
            "cleanup_orphans": config.CLEANUP_ORPHANS,
            "update_images": UPDATE_IMAGES,
            "pickup_only_shipping_class": config.PICKUP_ONLY_SHIPPING_CLASS
        }
    }
    report_file = f"sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, "w") as f: json.dump(report, f, indent=2)
    logger.info(f"📊 Report saved to: {report_file}")
    return report

def main():
    global start_time, all_wc_categories
    logger.info("="*60 + "\n🚀 Starting Pay4more Product Sync v17 (With Shipping Class Logic)\n" + 
                f"Sync Source: {config.SYNC_SOURCE}\n" +
                f"Update Images: {UPDATE_IMAGES}\n" +
                f"Delete Old Images: {config.DELETE_OLD_IMAGES}\n" + 
                f"Cleanup Orphans: {config.CLEANUP_ORPHANS}\n" +
                f"Batch Size: {config.BATCH_SIZE}\n" + 
                f"Workers: {config.MAX_WORKERS} (Images: {config.IMAGE_WORKERS})\n" +
                f"Pickup Only Shipping Class: {config.PICKUP_ONLY_SHIPPING_CLASS}\n" + "="*60)
    
    logger.info("Initializing caches...")
    all_wc_categories = fetch_all_categories()
    initialize_brand_cache()
    initialize_tag_cache()
    initialize_shipping_class_cache()  # NEW: Initialize shipping class cache
    initialize_product_cache()
    if config.CLEANUP_ORPHANS: scan_orphan_images()
    
    logger.info("Fetching products from Airtable...")
    recs = items_table.all(view="Pay4more Sync View", formula="{Listing Status}='Ready for Listing'")
    total = len(recs)
    logger.info(f"Found {total} products to sync")
    
    if total == 0:
        logger.info("No products to sync")
        if config.CLEANUP_ORPHANS and orphan_images: delete_orphan_images(50)
        return
    
    success, batch_count = 0, 0
    total_batches = (total + config.BATCH_SIZE - 1) // config.BATCH_SIZE
    for i in range(0, total, config.BATCH_SIZE):
        batch = recs[i:i + config.BATCH_SIZE]
        batch_count += 1
        logger.info(f"\n📦 Processing batch {batch_count}/{total_batches} ({len(batch)} products)")
        batch_success = process_batch(batch)
        success += batch_success
        
        elapsed = time.time() - start_time
        rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
        eta = (total - (i + len(batch))) / rate if rate > 0 else 0
        logger.info(f"Batch complete: {batch_success}/{len(batch)} successful")
        logger.info(f"Overall progress: {i + len(batch)}/{total} | Rate: {rate:.1f}/s | ETA: {eta:.0f}s")
        logger.info(f"Stats - Created: {sync_stats['created']}, Updated: {sync_stats['updated']}, Failed: {sync_stats['failed']}, Skipped: {sync_stats['skipped']}")
        
        if batch_count % 5 == 0 and config.CLEANUP_ORPHANS: delete_orphan_images(10)
        if batch_count < total_batches: time.sleep(2)
    
    if config.CLEANUP_ORPHANS and orphan_images: delete_orphan_images(50)
    
    report = generate_sync_report(total, success, time.time() - start_time)
    
    logger.info("\n" + "="*60 + "\n✅ SYNC COMPLETE!\n" + 
                f"Success: {success}/{total} ({(success/total*100):.1f}%)\n" +
                f"Created: {sync_stats['created']} | Updated: {sync_stats['updated']} | Failed: {sync_stats['failed']} | Skipped: {sync_stats['skipped']}\n" +
                f"Time: {report['duration_seconds']:.1f}s | Rate: {report['rate_per_second']:.1f} products/s\n" +
                f"Images cached: {len(image_hash_cache)}\n" + 
                f"Orphans found: {report['orphan_images_found']}\n" +
                f"Shipping classes cached: {len(shipping_class_cache)}\n" +
                f"Sync Source: {config.SYNC_SOURCE}")
    
    if missing_categories:
        unique_missing = list({cat["normalized"] for cat in missing_categories})
        logger.warning(f"\n⚠️ {len(unique_missing)} categories not found in WooCommerce:")
        for cat in unique_missing[:10]: logger.warning(f"  - {cat}")
    
    logger.info("="*60 + "\n📌 Next Steps:\n1. Review the sync report for details.\n2. Check WooCommerce admin for new/updated products.\n" +
                "3. Verify Airtable sync metadata fields are populated.\n4. Verify shipping classes are correctly set for Heavy/Big items.\n" +
                "5. Clear any site caches (e.g., WP Rocket, CDN).\n" +
                "6. Test product pages for correct images and SEO data.\n" + "="*60)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Enhanced Pay4more WooCommerce Product Sync v17 with Shipping Class Logic")
    p.add_argument("--update-images", action="store_true", help="Update images for existing products")
    p.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    p.add_argument("--skip-orphans", action="store_true", help="Skip orphan image cleanup")
    p.add_argument("--keep-old-images", action="store_true", help="Don't delete old product images")
    p.add_argument("--max-workers", type=int, default=4, help="Maximum parallel workers")
    p.add_argument("--sync-source", type=str, default="Python", help="Sync source identifier")
    p.add_argument("--pickup-only-class", type=str, default="pickup-only", help="Slug for pickup only shipping class")
    args = p.parse_args()
    
    UPDATE_IMAGES = args.update_images or UPDATE_IMAGES
    config.BATCH_SIZE = args.batch_size
    config.CLEANUP_ORPHANS = not args.skip_orphans
    config.DELETE_OLD_IMAGES = not args.keep_old_images
    config.MAX_WORKERS = args.max_workers
    config.SYNC_SOURCE = args.sync_source
    config.PICKUP_ONLY_SHIPPING_CLASS = args.pickup_only_class
    
    main()