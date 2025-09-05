import os
import io
import re
import unicodedata
import requests
import csv
import json
import time
import base64
from datetime import datetime
from PIL import Image
from dotenv import load_dotenv
from woocommerce import API
from pyairtable import Api
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# === Load environment ===
load_dotenv()

WC_STORE_URL = os.getenv("BIN4MORE_WOOCOMMERCE_STORE_URL").rstrip("/")
WC_CONSUMER_KEY = os.getenv("BIN4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("BIN4MORE_WOOCOMMERCE_CONSUMER_SECRET")

WP_USERNAME = os.getenv("WORDPRESS_USERNAME")
WP_APP_PASSWORD = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
WORDPRESS_XMLRPC_URL = os.getenv("WORDPRESS_XMLRPC_URL", f"{WC_STORE_URL}/xmlrpc.php")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Categories"

# === Performance Settings ===
MAX_WORKERS = 5  # Number of parallel threads for image processing
BATCH_SIZE = 50  # Batch size for WooCommerce API calls
CONNECTION_POOL_SIZE = 10  # Connection pool size for requests

# === Optimized Session with Connection Pooling ===
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=CONNECTION_POOL_SIZE, pool_maxsize=CONNECTION_POOL_SIZE)
session.mount("http://", adapter)
session.mount("https://", adapter)

# === Clients ===
wcapi = API(
    url=WC_STORE_URL,
    consumer_key=WC_CONSUMER_KEY,
    consumer_secret=WC_CONSUMER_SECRET,
    version="wc/v3",
    timeout=30,
)

airtable = Api(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

# === Image Settings ===
IMAGE_SIZES = {
    "main": (800, 800),
}

# === Cache for API calls ===
category_cache = {}
cache_lock = threading.Lock()


def slugify(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9\s-]", "", text.lower())
    text = re.sub(r"[\s_-]+", "-", text).strip("-")
    return text


def download_image(image_url):
    """Download image with optimized session"""
    try:
        response = session.get(image_url, timeout=10)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except Exception as e:
        print(f"   ❌ Error downloading image: {str(e)[:50]}")
        return None


def optimize_image_for_ecommerce(image_data, slug, category_name):
    """Optimized image processing"""
    try:
        img = Image.open(image_data)
        
        # Skip processing if already correct size and format
        if img.format == "JPEG" and img.size == IMAGE_SIZES["main"]:
            image_data.seek(0)
            return image_data
        
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGBA")

        img.thumbnail(IMAGE_SIZES["main"], Image.Resampling.LANCZOS)
        background = Image.new("RGB", IMAGE_SIZES["main"], "white")

        paste_position = (
            (IMAGE_SIZES["main"][0] - img.width) // 2,
            (IMAGE_SIZES["main"][1] - img.height) // 2,
        )

        if img.mode == "RGBA":
            background.paste(img, paste_position, mask=img.split()[3])
        else:
            background.paste(img, paste_position)

        buffer = io.BytesIO()
        background.save(buffer, format="JPEG", quality=85, optimize=True)
        buffer.seek(0)

        return buffer
    except Exception as e:
        print(f"   ❌ Error optimizing image: {str(e)[:50]}")
        return None


def batch_get_categories():
    """Get all WooCommerce categories in one efficient call"""
    categories = []
    page = 1
    while True:
        response = wcapi.get(
            "products/categories", params={"per_page": 100, "page": page}
        )
        if response.status_code != 200:
            break
        batch = response.json()
        if not batch:
            break
        categories.extend(batch)
        page += 1
    
    # Build cache for quick lookups
    for cat in categories:
        with cache_lock:
            category_cache[cat['slug']] = cat
    
    return categories


@lru_cache(maxsize=300)
def get_category_by_slug_cached(slug):
    """Cached category lookup"""
    with cache_lock:
        return category_cache.get(slug)


def process_image_async(image_url, slug, category_name):
    """Async image processing function for parallel execution"""
    if not image_url:
        return None
    
    image_data = download_image(image_url)
    if not image_data:
        return None
    
    optimized = optimize_image_for_ecommerce(image_data, slug, category_name)
    if not optimized:
        return None
    
    return {
        'slug': slug,
        'name': category_name,
        'data': optimized.getvalue(),
        'url': image_url
    }


def batch_upload_images(image_batch):
    """Upload multiple images in batch via WordPress REST API"""
    if not WP_USERNAME or not WP_APP_PASSWORD:
        return {}
    
    results = {}
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()
        ).decode("utf-8")
    }
    
    for item in image_batch:
        if not item:
            continue
            
        try:
            # Upload to WordPress Media Library
            files = {
                'file': (f"{item['slug']}.jpg", item['data'], 'image/jpeg')
            }
            
            response = session.post(
                f"{WC_STORE_URL}/wp-json/wp/v2/media",
                headers=headers,
                files=files
            )
            
            if response.status_code in [200, 201]:
                media_data = response.json()
                results[item['slug']] = {
                    'id': media_data['id'],
                    'src': media_data['source_url']
                }
                print(f"   ✅ Uploaded: {item['name']}")
            else:
                print(f"   ❌ Failed upload: {item['name']}")
                
        except Exception as e:
            print(f"   ❌ Error uploading {item['name']}: {str(e)[:50]}")
    
    return results


def batch_create_update_categories(categories_data, image_results):
    """Batch create/update categories"""
    results = {}
    
    # Group by parent/child for proper ordering
    parent_cats = [c for c in categories_data if not c.get('parent')]
    child_cats = [c for c in categories_data if c.get('parent')]
    
    # Process parents first
    for cat_data in parent_cats:
        existing = get_category_by_slug_cached(cat_data['slug'])
        
        data = {
            "name": cat_data['name'],
            "slug": cat_data['slug'],
            "description": cat_data.get('description', ''),
            "display": "default",
        }
        
        # Add image if available
        if cat_data['slug'] in image_results:
            data['image'] = image_results[cat_data['slug']]
        
        try:
            if existing:
                response = wcapi.put(f"products/categories/{existing['id']}", data)
                if response.status_code in [200, 201]:
                    results[cat_data['name']] = existing['id']
                    print(f"🔄 Updated: {cat_data['name']}")
            else:
                response = wcapi.post("products/categories", data)
                if response.status_code in [200, 201]:
                    result = response.json()
                    results[cat_data['name']] = result['id']
                    # Update cache
                    with cache_lock:
                        category_cache[cat_data['slug']] = result
                    print(f"✅ Created: {cat_data['name']}")
        except Exception as e:
            print(f"❌ Error with {cat_data['name']}: {str(e)[:50]}")
    
    # Process children
    for cat_data in child_cats:
        parent_id = results.get(cat_data['parent'])
        if not parent_id:
            # Try to find in existing categories
            parent_slug = slugify(cat_data['parent'])
            parent_cat = get_category_by_slug_cached(parent_slug)
            if parent_cat:
                parent_id = parent_cat['id']
        
        if not parent_id:
            print(f"⚠️  Skipping {cat_data['name']} - parent not found")
            continue
        
        existing = get_category_by_slug_cached(cat_data['slug'])
        
        data = {
            "name": cat_data['name'],
            "slug": cat_data['slug'],
            "parent": parent_id,
            "description": cat_data.get('description', ''),
            "display": "default",
        }
        
        # Add image if available
        if cat_data['slug'] in image_results:
            data['image'] = image_results[cat_data['slug']]
        
        try:
            if existing:
                response = wcapi.put(f"products/categories/{existing['id']}", data)
                if response.status_code in [200, 201]:
                    results[cat_data['name']] = existing['id']
                    print(f"🔄 Updated: {cat_data['name']}")
            else:
                response = wcapi.post("products/categories", data)
                if response.status_code in [200, 201]:
                    result = response.json()
                    results[cat_data['name']] = result['id']
                    print(f"✅ Created: {cat_data['name']}")
        except Exception as e:
            print(f"❌ Error with {cat_data['name']}: {str(e)[:50]}")
    
    return results


def export_seo_data(categories_data, name_to_id):
    """Export SEO data to CSV"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"woocommerce_seo_data_{timestamp}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "category_id", "category_name", "category_slug",
            "seo_title", "meta_description", "focus_keyword", "category_url"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        seo_count = 0
        for cat_data in categories_data:
            if cat_data['name'] in name_to_id:
                cat_id = name_to_id[cat_data['name']]
                category_url = f"{WC_STORE_URL}/product-category/{cat_data['slug']}/"

                if any([cat_data.get('seo_title'), cat_data.get('seo_meta_description'), 
                       cat_data.get('focus_keyword')]):
                    seo_count += 1
                    writer.writerow({
                        "category_id": cat_id,
                        "category_name": cat_data['name'],
                        "category_slug": cat_data['slug'],
                        "seo_title": cat_data.get('seo_title', ''),
                        "meta_description": cat_data.get('seo_meta_description', ''),
                        "focus_keyword": cat_data.get('focus_keyword', ''),
                        "category_url": category_url,
                    })

    return filename, seo_count


def main():
    print("\n🚀 FAST WooCommerce Category Sync from Airtable")
    print("    ⚡ Optimized for speed with parallel processing")
    print("=" * 60)
    
    start_time = time.time()
    
    # Check capabilities
    can_upload_images = bool(WP_USERNAME and WP_APP_PASSWORD)
    
    if can_upload_images:
        print("✅ WordPress credentials found - Images will be uploaded")
        print(f"   • Using {MAX_WORKERS} parallel workers for image processing")
    else:
        print("⚠️  No WordPress credentials - Will use Airtable URLs directly")
    
    # Step 1: Fetch all existing categories at once
    print("\n📊 Loading existing categories...")
    existing_categories = batch_get_categories()
    print(f"   ✅ Loaded {len(existing_categories)} categories in cache")
    
    # Step 2: Fetch Airtable data
    print("\n📊 Fetching Airtable data...")
    records = airtable.all()
    print(f"   ✅ Found {len(records)} records")
    
    # Step 3: Prepare category data
    categories_data = []
    image_urls_to_process = []
    
    for record in records:
        fields = record.get("fields", {})
        name = fields.get("Category")
        if not name:
            continue
        
        slug = slugify(name)
        parent = fields.get("Parent Category")
        photo = fields.get("Photo")
        
        image_url = None
        if photo and isinstance(photo, list) and len(photo) > 0:
            image_url = photo[0].get("url")
            if image_url and can_upload_images:
                image_urls_to_process.append({
                    'url': image_url,
                    'slug': slug,
                    'name': name
                })
        
        categories_data.append({
            "name": name,
            "slug": slug,
            "parent": parent[0] if isinstance(parent, list) and parent else parent,
            "image_url": image_url,
            "description": fields.get("Description"),
            "seo_meta_description": fields.get("SEO Meta description"),
            "seo_title": fields.get("SEO Title"),
            "focus_keyword": fields.get("Focus keyword"),
        })
    
    print(f"\n📋 Ready to process {len(categories_data)} categories")
    print(f"   • Parent categories: {len([c for c in categories_data if not c['parent']])}")
    print(f"   • Child categories: {len([c for c in categories_data if c['parent']])}")
    print(f"   • Images to process: {len(image_urls_to_process)}")
    
    proceed = input("\nProceed with fast import? (yes/no): ").strip().lower()
    if proceed != "yes":
        print("❌ Import cancelled.")
        return
    
    # Step 4: Process images in parallel
    image_results = {}
    if can_upload_images and image_urls_to_process:
        print(f"\n🖼️  Processing {len(image_urls_to_process)} images in parallel...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all image processing tasks
            futures = {
                executor.submit(
                    process_image_async, 
                    item['url'], 
                    item['slug'], 
                    item['name']
                ): item for item in image_urls_to_process
            }
            
            # Collect processed images
            processed_images = []
            for future in as_completed(futures):
                result = future.result()
                if result:
                    processed_images.append(result)
                    print(f"   ⚡ Processed: {result['name']}")
        
        # Batch upload all processed images
        if processed_images:
            print(f"\n📤 Uploading {len(processed_images)} images to WordPress...")
            image_results = batch_upload_images(processed_images)
            print(f"   ✅ Successfully uploaded {len(image_results)} images")
    
    # Step 5: Batch create/update categories
    print("\n📁 Creating/updating categories...")
    name_to_id = batch_create_update_categories(categories_data, image_results)
    
    # Step 6: Export SEO data
    print("\n📄 Exporting SEO data...")
    seo_filename, seo_count = export_seo_data(categories_data, name_to_id)
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Final summary
    print("\n✅ SYNC COMPLETED!")
    print("=" * 60)
    print(f"⏱️  Total execution time: {execution_time:.1f} seconds")
    print(f"✓ Categories processed: {len(name_to_id)}")
    print(f"✓ Failed: {len(categories_data) - len(name_to_id)}")
    if can_upload_images:
        print(f"✓ Images uploaded: {len(image_results)}")
    print(f"✓ SEO data exported: {seo_count} categories")
    print(f"✓ CSV file: {seo_filename}")
    
    # Performance metrics
    if len(categories_data) > 0:
        avg_time = execution_time / len(categories_data)
        print(f"\n📊 Performance: {avg_time:.2f} seconds per category")
        print(f"   • Categories/minute: {(60/avg_time):.0f}")
    
    print("\n📝 NEXT STEPS:")
    print(f"1. Verify categories at: {WC_STORE_URL}/wp-admin/")
    print(f"2. Import SEO data from: {seo_filename}")
    print("3. Clear all caches")
    
    # Create log
    log_filename = f"sync_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_filename, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "execution_time_seconds": round(execution_time, 2),
            "categories_synced": len(name_to_id),
            "categories_failed": len(categories_data) - len(name_to_id),
            "images_uploaded": len(image_results) if can_upload_images else 0,
            "seo_export": seo_filename,
            "performance_metrics": {
                "seconds_per_category": round(avg_time, 2) if len(categories_data) > 0 else 0,
                "categories_per_minute": round(60/avg_time) if len(categories_data) > 0 and avg_time > 0 else 0
            }
        }, f, indent=2)
    
    print(f"📋 Log saved to: {log_filename}")
    print("\n✨ Fast sync complete!")


if __name__ == "__main__":
    main()