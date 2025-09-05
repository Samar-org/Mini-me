#!/usr/bin/env python3
"""
Optimized Pay4more WooCommerce Product Sync v14
Preserves all V13 functionality with performance improvements for large catalogs
"""

import os, sys, logging, requests, re, time, html, argparse, json
from io import BytesIO
from PIL import Image, ImageEnhance
from woocommerce import API as WCAPI
from pyairtable import Api
from typing import Dict, List, Optional, Any, Set
from functools import wraps
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# --- Configuration ---
class SyncConfig: 
    # Image settings (matching V13)
    IMAGE_QUALITY = 85
    WEBP_QUALITY = 80
    UPLOAD_DELAY = 0.5
    WC_API_TIMEOUT = 90
    MAX_RETRIES = 3
    
    # Performance settings (new)
    BATCH_SIZE = 10
    MAX_WORKERS = 3
    CACHE_CATEGORIES = True
    PROGRESS_INTERVAL = 10

config = SyncConfig()

# --- Environment Variables ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
WC_URL = os.getenv("BIN4MORE_WOOCOMMERCE_STORE_URL")
WC_CONSUMER_KEY = os.getenv("BIN4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("BIN4MORE_WOOCOMMERCE_CONSUMER_SECRET")
WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
UPDATE_IMAGES = os.getenv("UPDATE_IMAGES", "false").lower() == "true"

# Validate environment
if any(not globals().get(v) for v in ["AIRTABLE_API_KEY","AIRTABLE_BASE_ID","WC_URL","WC_CONSUMER_KEY","WC_CONSUMER_SECRET","WP_USER","WP_PASS"]):
    logger.error("Missing environment variables")
    sys.exit(1)

# Initialize APIs
airtable_api = Api(AIRTABLE_API_KEY)
items_table = airtable_api.table(AIRTABLE_BASE_ID, "Items-Bin4more")
catalogue_table = airtable_api.table(AIRTABLE_BASE_ID, "Product Catalogue")
wcapi = WCAPI(url=WC_URL, consumer_key=WC_CONSUMER_KEY, consumer_secret=WC_CONSUMER_SECRET, version="wc/v3", timeout=config.WC_API_TIMEOUT)

# --- Global Caches ---
category_cache = {}
brand_cache = {}
tag_cache = {}
existing_products_cache = {}
missing_categories = []
processed_count = 0
start_time = time.time()

# --- Helpers (preserving V13 functionality) ---
def retry(max_retries=3, delay=2.0):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try: 
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"{func.__name__} failed ({attempt+1}/{max_retries}): {e}")
                    time.sleep(current_delay)
                    current_delay *= 1.5  # Exponential backoff
            raise
        return wrapper
    return decorator

def first(x): 
    """Get first element if list, otherwise return value"""
    return x[0] if isinstance(x, list) and x else x

def clean_and_normalize(name: str) -> str:
    """Clean and normalize text for matching (V13 compatible)"""
    if not isinstance(name, str): 
        return ""
    unescaped = html.unescape(name)
    cleaned = re.sub(r"[\u200B-\u200D\uFEFF\s]+", " ", unescaped).strip()
    return re.sub(r"\s+", " ", cleaned.lower().replace(" & ", " and ").replace("&", "and"))

# --- WooCommerce Categories (Enhanced V13) ---
def fetch_all_categories():
    """Fetch all categories with pagination and caching"""
    cats, page = [], 1
    logger.info("Fetching WooCommerce categories...")
    
    while page <= 20:  # Limit to prevent infinite loops
        try:
            r = wcapi.get("products/categories", params={"per_page": 100, "page": page})
            if r.status_code != 200: 
                logger.error(f"Category fetch failed page {page}: {r.text}")
                break
            data = r.json()
            if not data: 
                break
            cats.extend(data)
            logger.debug(f"Fetched {len(data)} categories from page {page}")
            page += 1
        except Exception as e:
            logger.error(f"Error fetching categories page {page}: {e}")
            break
    
    logger.info(f"Fetched {len(cats)} categories")
    
    # Build comprehensive cache
    for c in cats:
        norm = clean_and_normalize(c.get("name", ""))
        category_cache[norm] = c["id"]
        # Also cache slug variations
        slug_norm = clean_and_normalize(c.get("slug", "").replace("-", " "))
        if slug_norm:
            category_cache[slug_norm] = c["id"]
    
    return cats

# Initialize categories on module load
all_wc_categories = []

def get_existing_category(name: str) -> Optional[int]:
    """Get existing category ID (V13 compatible with better error handling)"""
    norm = clean_and_normalize(name)
    
    if norm in category_cache: 
        return category_cache[norm]
    
    # Try variations
    for c in all_wc_categories:
        if norm in (clean_and_normalize(c.get("name", "")), 
                   clean_and_normalize(c.get("slug", " ").replace("-", " "))):
            category_cache[norm] = c["id"]
            return c["id"]
    
    # Track missing categories
    missing_categories.append({"original": name, "normalized": norm})
    logger.warning(f"Category '{name}' not found in WooCommerce")
    
    # Return None instead of raising to allow processing to continue
    return None

# --- Brand Cache (V13 compatible) ---
def initialize_brand_cache():
    """Pre-fetch all brands for better performance"""
    global brand_cache
    logger.info("Initializing brand cache...")
    
    try:
        page = 1
        while page <= 10:
            r = wcapi.get("products/brands", params={"per_page": 100, "page": page})
            if r.status_code != 200:
                if r.status_code == 404:
                    logger.info("Brands plugin not installed")
                break
            
            brands = r.json()
            if not brands:
                break
                
            for b in brands:
                norm = clean_and_normalize(b.get("name", ""))
                brand_cache[norm] = b["id"]
            
            page += 1
        
        logger.info(f"Cached {len(brand_cache)} brands")
    except Exception as e:
        logger.warning(f"Could not initialize brand cache: {e}")

def get_or_create_brand(name: str) -> Optional[int]:
    """Get or create brand (V13 compatible)"""
    norm = clean_and_normalize(name)
    
    if norm in brand_cache: 
        return brand_cache[norm]
    
    # Search for existing brand
    r = wcapi.get("products/brands", params={"search": name})
    if r.status_code == 200:
        for b in r.json():
            if clean_and_normalize(b.get("name", "")) == norm:
                brand_cache[norm] = b["id"]
                return b["id"]
    
    # Create new brand
    cr = wcapi.post("products/brands", {"name": name})
    if cr.status_code in (200, 201):
        bid = cr.json().get("id")
        brand_cache[norm] = bid
        logger.info(f"Created brand: {name} (ID: {bid})")
        return bid
    
    logger.warning(f"Failed to create brand '{name}': {cr.text}")
    return None

# --- Tag Management (New for performance) ---
def initialize_tag_cache():
    """Pre-fetch all tags for better performance"""
    global tag_cache
    logger.info("Initializing tag cache...")
    
    try:
        page = 1
        while page <= 10:
            r = wcapi.get("products/tags", params={"per_page": 100, "page": page})
            if r.status_code != 200:
                break
            
            tags = r.json()
            if not tags:
                break
                
            for t in tags:
                norm = clean_and_normalize(t.get("name", ""))
                tag_cache[norm] = t["id"]
            
            page += 1
        
        logger.info(f"Cached {len(tag_cache)} tags")
    except Exception as e:
        logger.warning(f"Could not initialize tag cache: {e}")

def get_or_create_tag(name: str) -> Dict:
    """Get or create tag with caching"""
    norm = clean_and_normalize(name)
    
    # Check cache first
    if norm in tag_cache:
        return {"id": tag_cache[norm]}
    
    # Create new tag
    try:
        r = wcapi.post("products/tags", {"name": name})
        if r.status_code in (200, 201):
            tag_id = r.json().get("id")
            tag_cache[norm] = tag_id
            return {"id": tag_id}
        elif r.status_code == 400 and "term_exists" in r.text:
            # Tag exists, find it
            search_r = wcapi.get("products/tags", params={"search": name})
            if search_r.status_code == 200:
                for t in search_r.json():
                    if clean_and_normalize(t.get("name", "")) == norm:
                        tag_cache[norm] = t["id"]
                        return {"id": t["id"]}
    except Exception as e:
        logger.warning(f"Failed to create tag '{name}': {e}")
    
    # Fallback to name-based tag
    return {"name": name}

# --- Existing Products Cache (New for performance) ---
def initialize_product_cache():
    """Pre-fetch existing products by SKU"""
    global existing_products_cache
    logger.info("Initializing product cache...")
    
    try:
        page = 1
        while page <= 30:  # Adjust based on catalog size
            r = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"})
            if r.status_code != 200:
                break
            
            products = r.json()
            if not products:
                break
                
            for p in products:
                sku = p.get("sku")
                if sku:
                    existing_products_cache[sku] = p["id"]
            
            page += 1
            
            # Progress update
            if page % 5 == 0:
                logger.info(f"Cached {len(existing_products_cache)} products so far...")
        
        logger.info(f"Cached {len(existing_products_cache)} existing products")
    except Exception as e:
        logger.warning(f"Could not fully initialize product cache: {e}")

# --- Rank Math SEO Updates (V13 preserved) ---
def update_media_seo(media_id, alt_text, seo_title, seo_desc, focus_keyword):
    """Update media with SEO metadata"""
    payload = {
        "alt_text": alt_text,
        "title": seo_title or alt_text,
        "caption": alt_text,
        "description": seo_desc,
        "meta": [
            {"key": "rank_math_title", "value": seo_title},
            {"key": "rank_math_description", "value": seo_desc},
            {"key": "rank_math_focus_keyword", "value": focus_keyword},
        ],
    }
    r = requests.post(f"{WC_URL}/wp-json/wp/v2/media/{media_id}", auth=(WP_USER, WP_PASS), json=payload, timeout=30)
    if r.status_code not in (200, 201):
        logger.warning(f"Failed SEO for media {media_id}: {r.text}")

# --- Image Upload (V13 preserved with enhancements) ---
@retry()
def optimize_and_upload_image(url, name, alt, seo_title, seo_desc, focus, index):
    """Optimize and upload image with SEO (V13 compatible)"""
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        
        img = Image.open(BytesIO(r.content))
        
        # Convert to RGB (V13 logic preserved)
        if img.mode in ("RGBA", "P"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3] if img.mode == "RGBA" else None)
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")
        
        # Resize while maintaining aspect ratio
        img.thumbnail((1200, 1200), Image.Resampling.LANCZOS)
        
        # Apply sharpening (V13)
        img = ImageEnhance.Sharpness(img).enhance(1.1)
        
        buf = BytesIO()
        
        # Try WebP first, fallback to JPEG (V13 logic)
        try: 
            img.save(buf, format="WEBP", quality=config.WEBP_QUALITY, method=6)
            ext = "webp"
        except: 
            buf.seek(0)
            img.save(buf, format="JPEG", quality=config.IMAGE_QUALITY, optimize=True, progressive=True)
            ext = "jpg"
        
        buf.seek(0)
        
        # Generate filename (V13 compatible)
        filename = f"{re.sub(r'[^A-Za-z0-9_.-]', '-', name)[:80]}-{index+1}.{ext}"
        
        # Upload to WordPress
        up = requests.post(
            f"{WC_URL}/wp-json/wp/v2/media", 
            headers={"Content-Disposition": f"attachment; filename={filename}", "Content-Type": f"image/{ext}"}, 
            data=buf, 
            auth=(WP_USER, WP_PASS),
            timeout=60
        )
        up.raise_for_status()
        
        media_id = up.json()["id"]
        
        # Update SEO metadata
        update_media_seo(media_id, alt, seo_title, seo_desc, focus)
        
        logger.debug(f"Uploaded image: {filename} (ID: {media_id})")
        return media_id
        
    except Exception as e:
        logger.error(f"Failed to upload image {url[:50]}: {e}")
        raise

def process_product_images(fields, name, seo_title, seo_desc, focus):
    """Process product images (V13 compatible with concurrency)"""
    urls = [a["url"] for f in ["Photo Files"] if f in fields for a in fields[f] if "url" in a]
    alt = first(fields.get("Image Alt Text", "")) or name
    
    if not urls:
        return []
    
    # Upload images with thread pool for better performance
    uploaded = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {}
        for i, u in enumerate(urls[:10]):  # Limit to 10 images
            future = executor.submit(optimize_and_upload_image, u, name, alt, seo_title, seo_desc, focus, i)
            futures[future] = i
            time.sleep(config.UPLOAD_DELAY)  # Rate limiting
        
        for future in as_completed(futures):
            try:
                media_id = future.result()
                uploaded.append({"id": media_id})
            except Exception as e:
                logger.error(f"Image upload failed: {e}")
    
    return uploaded

# --- Product Sync (V13 preserved with enhancements) ---
def process_single_product(record: Dict) -> bool:
    """Process single product (V13 logic preserved)"""
    global processed_count
    
    try:
        fields = record.get("fields", {})
        rid = record.get("id")
        sku = first(fields.get("SKU"))
        cat_code = first(fields.get("4more-Product-Code-linked"))
        
        # Validation (V13)
        if not sku or not cat_code: 
            items_table.update(rid, {"Listing Status": "Loading Error"})
            return False
        
        # Get catalog record (V13)
        cat_record = catalogue_table.first(formula=f"{{4more-Product-Code}}='{cat_code}'")
        if not cat_record: 
            items_table.update(rid, {"Listing Status": "Loading Error"})
            return False
        
        cfields = cat_record.get("fields", {})
        name = first(cfields.get("Product Name"))
        
        if not name: 
            items_table.update(rid, {"Listing Status": "Loading Error"})
            return False
        
        processed_count += 1
        
        # Progress logging
        if processed_count % config.PROGRESS_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {processed_count} products | Rate: {rate:.1f}/s")
        
        logger.info(f"Processing [{processed_count}]: {name} (SKU: {sku})")
        
        # SEO fields (V13)
        seo_title = first(cfields.get("Meta Title", name))
        seo_desc = first(cfields.get("Meta Description", ""))
        focus_kw = first(cfields.get("Focus Keyword", ""))
        
        # Build payload (V13 structure)
        payload = {
            "type": "simple",
            "status": "publish",
            "manage_stock": True,
            "name": name,
            "sku": sku,
            "description": first(cfields.get("Description", "")),
            "short_description": first(cfields.get("Meta Description", "")),
            "meta_data": [
                {"key": "rank_math_title", "value": seo_title},
                {"key": "rank_math_description", "value": seo_desc},
                {"key": "rank_math_focus_keyword", "value": focus_kw},
                # Also add Yoast SEO compatibility
                {"key": "_yoast_wpseo_title", "value": seo_title},
                {"key": "_yoast_wpseo_metadesc", "value": seo_desc},
                {"key": "_yoast_wpseo_focuskw", "value": focus_kw}
            ]
        }
        
        # Add prices and stock
        regular_price = first(fields.get("Unit Retail Price"))
        sale_price = first(fields.get("4more Price"))
        stock_qty = first(fields.get("Quantity", 0))
        
        if regular_price:
            payload["regular_price"] = str(regular_price)
        if sale_price:
            payload["sale_price"] = str(sale_price)
        if stock_qty is not None:
            payload["stock_quantity"] = int(stock_qty)
            payload["stock_status"] = "instock" if int(stock_qty) > 0 else "outofstock"
        
        # Add weight and dimensions if available
        weight = first(fields.get("Weight"))
        if weight:
            payload["weight"] = str(weight)
        
        dimensions = first(fields.get("Dimensions"))
        if dimensions:
            # Parse dimensions if in format "LxWxH"
            try:
                parts = dimensions.replace(" ", "").split("x")
                if len(parts) == 3:
                    payload["dimensions"] = {
                        "length": parts[0],
                        "width": parts[1],
                        "height": parts[2]
                    }
            except:
                pass
        
        # Categories (V13 logic)
        cat_names = []
        parent_field = cfields.get("Parent Category Rollup (from Category)")
        child_field = cfields.get("Category Name")
        
        if isinstance(parent_field, list): 
            cat_names.extend(parent_field)
        elif isinstance(parent_field, str): 
            cat_names.append(parent_field)
        
        if isinstance(child_field, list): 
            cat_names.extend(child_field)
        elif isinstance(child_field, str): 
            cat_names.append(child_field)
        
        wc_categories = []
        parent_id = None
        
        if parent_field:
            parent_name = parent_field[0] if isinstance(parent_field, list) else parent_field
            parent_id = get_existing_category(parent_name)
            if parent_id: 
                wc_categories.append({"id": parent_id})
        
        for cat in set(filter(None, cat_names)):
            if not parent_field or cat != (parent_field[0] if isinstance(parent_field, list) else parent_field):
                cid = get_existing_category(cat)
                if cid: 
                    wc_categories.append({"id": cid})
        
        if wc_categories:
            payload["categories"] = wc_categories
        else:
            logger.warning(f"No matching categories for {name}")
        
        # Tags with SEO (V13 enhanced)
        tags = cfields.get("Product Tags", [])
        taglist = [t.strip() for t in (tags if isinstance(tags, list) else tags.split(",") if isinstance(tags, str) else []) if t.strip()]
        
        if taglist:
            # Use cached tag creation
            tag_objects = []
            for t in taglist[:20]:  # Limit tags
                tag_obj = get_or_create_tag(t)
                if tag_obj:
                    tag_objects.append(tag_obj)
            
            if tag_objects:
                payload["tags"] = tag_objects
            
            # Add tag metadata for SEO
            for t in taglist:
                payload["meta_data"].append({"key": f"rank_math_tag_{t}", "value": t})
        
        # Brand (V13)
        brand = first(cfields.get("Brand"))
        if brand:
            bid = get_or_create_brand(brand)
            if bid: 
                payload["brands"] = [bid]
            else:
                # Add as custom attribute if brands plugin not available
                if "attributes" not in payload:
                    payload["attributes"] = []
                payload["attributes"].append({
                    "name": "Brand",
                    "options": [brand],
                    "visible": True,
                    "variation": False
                })
            payload["meta_data"].append({"key": "rank_math_brand", "value": brand})
        
        # ACF fields (from V2)
        acf_fields = {
            "inspected": first(fields.get("Inspected")),
            "condition": first(fields.get("Condition")),
            "location": first(fields.get("Warehouse", "W#13")),
            "aisle_bin": first(fields.get("Shelf Location")),
            "heavy": first(fields.get("Heavy")),
            "fragile": first(fields.get("Fragile")),
            "big": first(fields.get("Big"))
        }
        
        for key, value in acf_fields.items():
            if value is not None:
                payload["meta_data"].append({"key": key, "value": value})
        
        # Images (V13 logic)
        # Check existing product for images
        existing = None
        if sku in existing_products_cache:
            existing_id = existing_products_cache[sku]
            try:
                existing_resp = wcapi.get(f"products/{existing_id}")
                if existing_resp.status_code == 200:
                    existing = existing_resp.json()
            except:
                pass
        else:
            # Try to find by SKU
            existing_resp = wcapi.get("products", params={"sku": sku})
            if existing_resp.status_code == 200:
                existing_list = existing_resp.json()
                if existing_list:
                    existing = existing_list[0]
                    existing_products_cache[sku] = existing["id"]
        
        imgs = existing.get("images", []) if existing else []
        urls = [a["url"] for f in ["Photo Files"] if f in cfields for a in cfields[f] if "url" in a]
        
        if UPDATE_IMAGES or not imgs:
            if urls:
                payload["images"] = process_product_images(cfields, name, seo_title, seo_desc, focus_kw)
        
        # Create or Update (V13 logic)
        if existing: 
            resp = wcapi.put(f"products/{existing['id']}", payload)
        else: 
            resp = wcapi.post("products", payload)
        
        if resp.status_code in (200, 201):
            product_id = resp.json().get("id", existing["id"] if existing else "Unknown")
            logger.info(f"✅ {'Updated' if existing else 'Created'}: {name} (ID: {product_id})")
            items_table.update(rid, {"Listing Status": "Listed"})
            return True
        else:
            logger.error(f"Failed to sync {sku}: {resp.status_code} - {resp.text[:200]}")
            items_table.update(rid, {"Listing Status": "Loading Error"})
            return False
            
    except Exception as e:
        logger.error(f"Error syncing SKU {record.get('id')}: {e}", exc_info=True)
        if record.get("id"):
            items_table.update(record.get("id"), {"Listing Status": "Loading Error"})
        return False

def process_batch(records: List[Dict]) -> int:
    """Process a batch of records"""
    success = 0
    for r in records:
        if process_single_product(r):
            success += 1
    return success

def main():
    """Main function with batch processing and progress tracking"""
    global start_time, all_wc_categories
    
    logger.info("="*60)
    logger.info("🚀 Starting Pay4more Product Sync v14")
    logger.info(f"Update Images: {UPDATE_IMAGES}")
    logger.info(f"Batch Size: {config.BATCH_SIZE}")
    logger.info("="*60)
    
    # Initialize caches for better performance
    logger.info("Initializing caches...")
    all_wc_categories = fetch_all_categories()
    initialize_brand_cache()
    initialize_tag_cache()
    initialize_product_cache()
    
    # Fetch records
    logger.info("Fetching products from Airtable...")
    recs = items_table.all(view="Pay4more Sync View", formula="{Listing Status}='Ready for Listing'")
    total = len(recs)
    logger.info(f"Found {total} products to sync")
    
    if total == 0:
        logger.info("No products to sync")
        return
    
    # Process in batches
    success = 0
    batch_count = 0
    total_batches = (total + config.BATCH_SIZE - 1) // config.BATCH_SIZE
    
    for i in range(0, total, config.BATCH_SIZE):
        batch = recs[i:i + config.BATCH_SIZE]
        batch_count += 1
        
        logger.info(f"\n📦 Processing batch {batch_count}/{total_batches} ({len(batch)} products)")
        batch_success = process_batch(batch)
        success += batch_success
        
        # Progress update
        elapsed = time.time() - start_time
        rate = (i + len(batch)) / elapsed if elapsed > 0 else 0
        eta = (total - (i + len(batch))) / rate if rate > 0 else 0
        
        logger.info(f"Batch complete: {batch_success}/{len(batch)} successful")
        logger.info(f"Overall progress: {i + len(batch)}/{total} | Rate: {rate:.1f}/s | ETA: {eta:.0f}s")
        
        # Small delay between batches to avoid overwhelming the API
        if batch_count < total_batches:
            time.sleep(2)
    
    # Final summary
    elapsed = time.time() - start_time
    logger.info("\n" + "="*60)
    logger.info(f"✅ Sync complete. Success: {success}/{total}")
    logger.info(f"Time: {elapsed:.1f}s | Rate: {total/elapsed:.1f} products/s")
    
    # Report missing categories
    if missing_categories:
        unique_missing = list({cat["normalized"] for cat in missing_categories})
        logger.warning(f"\n⚠️ {len(unique_missing)} categories not found in WooCommerce:")
        for cat in unique_missing[:10]:
            logger.warning(f"  - {cat}")
    
    logger.info("="*60)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--update-images", action="store_true", help="Update images for existing products")
    p.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    p.add_argument("--skip-categories", action="store_true", help="Skip category assignment if not found")
    args = p.parse_args()
    
    UPDATE_IMAGES = args.update_images or UPDATE_IMAGES
    config.BATCH_SIZE = args.batch_size
    
    main()