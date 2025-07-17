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
from wordpress_xmlrpc import Client
from wordpress_xmlrpc.methods import media

# === Load environment ===
load_dotenv()

WC_STORE_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL").rstrip("/")
WC_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")

WP_USERNAME = os.getenv("WORDPRESS_USERNAME")
WP_APP_PASSWORD = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
WORDPRESS_XMLRPC_URL = os.getenv("WORDPRESS_XMLRPC_URL", f"{WC_STORE_URL}/xmlrpc.php")

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Categories"

# === Clients ===
wcapi = API(
    url=WC_STORE_URL,
    consumer_key=WC_CONSUMER_KEY,
    consumer_secret=WC_CONSUMER_SECRET,
    version="wc/v3",
    timeout=30
)

airtable = Api(AIRTABLE_API_KEY).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

# === Image Settings ===
IMAGE_SIZES = {
    'main': (800, 800),
}

def slugify(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^a-zA-Z0-9\s-]', '', text.lower())
    text = re.sub(r'[\s_-]+', '-', text).strip('-')
    return text

def download_image(image_url):
    try:
        response = requests.get(image_url, timeout=20)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except Exception as e:
        print(f"   ❌ Error downloading image from {image_url}: {e}")
        return None

def optimize_image_for_ecommerce(image_data, slug, category_name):
    try:
        print(f"   🖼️  Optimizing image for {category_name}...")
        img = Image.open(image_data)
        print(f"   🔍 Original: format={img.format}, mode={img.mode}, size={img.size}")

        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGBA")

        img.thumbnail(IMAGE_SIZES['main'], Image.Resampling.LANCZOS)
        background = Image.new("RGB", IMAGE_SIZES['main'], "white")

        paste_position = (
            (IMAGE_SIZES['main'][0] - img.width) // 2,
            (IMAGE_SIZES['main'][1] - img.height) // 2
        )

        if img.mode == "RGBA":
            background.paste(img, paste_position, mask=img.split()[3])
        else:
            background.paste(img, paste_position)

        buffer = io.BytesIO()
        background.save(buffer, format="JPEG", quality=85)
        buffer.seek(0)

        size_kb = buffer.getbuffer().nbytes / 1024
        if size_kb < 5:
            print(f"   ⚠️  Output image too small ({size_kb:.1f}KB), likely blank.")
            return None

        print(f"   ✅ Image optimized: {slug}.jpg ({size_kb:.1f}KB)")
        return buffer
    except Exception as e:
        print(f"   ❌ Error optimizing image for '{category_name}': {str(e)}")
        return None

def upload_image_via_xmlrpc(image_url, image_name_base, category_name):
    """Uploads an image to WordPress via XML-RPC and returns its media ID and URL."""
    if not WP_USERNAME or not WP_APP_PASSWORD:
        print("   ⚠️  WordPress credentials not found, skipping upload")
        return None

    try:
        client = Client(WORDPRESS_XMLRPC_URL, WP_USERNAME, WP_APP_PASSWORD)

        image_data = download_image(image_url)
        if image_data is None:
            return None

        optimized_image = optimize_image_for_ecommerce(image_data, image_name_base, category_name)
        if optimized_image is None:
            return None

        filename = f"{image_name_base}.jpg"
        mime_type = "image/jpeg"
        upload_image_dict = {
            'name': filename,
            'type': mime_type,
            'bits': optimized_image.getvalue(),  # 👈 FIXED: Use raw bytes
            'overwrite': False
        }

        print(f"   📤 Uploading '{filename}' to WordPress Media Library...")
        response = client.call(media.UploadFile(upload_image_dict))

        if response and 'id' in response:
            image_id = response['id']
            image_url = response.get('url', '')
            print(f"   ✅ Successfully uploaded image. Media ID: {image_id}")

            update_image_metadata(image_id, category_name)
            return {'id': image_id, 'url': image_url}
        else:
            print("   ❌ Failed to get media ID from response")
            return None

    except Exception as e:
        print(f"   ❌ Error uploading image via XML-RPC: {e}")
        return None


def update_image_metadata(media_id, category_name):
    """Update image metadata using REST API after XML-RPC upload"""
    try:
        # SEO metadata
        metadata_payload = {
            'alt_text': f"{category_name} - Product Category",
            'caption': f"{category_name} category",
            'description': f"Product category image for {category_name}",
            'title': f"{category_name} - Product Category"
        }
        
        metadata_url = f"{WC_STORE_URL}/wp-json/wp/v2/media/{media_id}"
        metadata_headers = {
            'Authorization': 'Basic ' + base64.b64encode(
                f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()
            ).decode('utf-8'),
            'Content-Type': 'application/json'
        }
        
        # Update media metadata
        metadata_response = requests.patch(
            metadata_url, 
            headers=metadata_headers, 
            json=metadata_payload
        )
        
        if metadata_response.status_code == 200:
            print(f"   ✅ Updated image metadata for SEO")
        else:
            print(f"   ⚠️  Failed to update image metadata")
            
    except Exception as e:
        print(f"   ⚠️  Error updating metadata: {e}")

def get_media_url_by_id(media_id):
    """Get the URL of a media item by its ID using REST API"""
    try:
        url = f"{WC_STORE_URL}/wp-json/wp/v2/media/{media_id}"
        headers = {
            'Authorization': 'Basic ' + base64.b64encode(
                f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()
            ).decode('utf-8')
        }
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            media_data = response.json()
            return media_data.get('source_url', '')
        else:
            print(f"   ⚠️  Failed to get media URL for ID {media_id}")
            return None
    except Exception as e:
        print(f"   ⚠️  Error getting media URL: {e}")
        return None

def process_category_image(image_url, slug, category_name):
    """Process category image using XML-RPC upload method"""
    if not image_url:
        return None
    
    # Check if we can upload to WordPress
    if WP_USERNAME and WP_APP_PASSWORD:
        # Upload via XML-RPC
        upload_result = upload_image_via_xmlrpc(image_url, slug, category_name)
        
        if upload_result and isinstance(upload_result, dict):
            media_id = upload_result.get('id')
            media_url = upload_result.get('url')
            
            # If we don't have the URL, fetch it
            if media_id and not media_url:
                media_url = get_media_url_by_id(media_id)
            
            if media_url:
                # Return URL format for category image
                return {
                    'src': media_url,
                    'id': media_id
                }
            else:
                # Just return the ID if we can't get the URL
                return media_id
        
        return upload_result
    
    # If no WordPress credentials, return URL with metadata
    return {
        'src': image_url,
        'name': slug,
        'alt': f"{category_name} - Product Category"
    }

def get_all_categories():
    """Get all WooCommerce categories"""
    categories = []
    page = 1
    while True:
        response = wcapi.get("products/categories", params={"per_page": 100, "page": page})
        if response.status_code != 200:
            break
        batch = response.json()
        if not batch:
            break
        categories.extend(batch)
        page += 1
    return categories

def get_category_by_slug(slug, existing_categories=None):
    """Get WooCommerce category by slug"""
    if existing_categories:
        for cat in existing_categories:
            if cat.get("slug") == slug:
                return cat
    try:
        response = wcapi.get("products/categories", params={"slug": slug})
        if response.status_code == 200:
            categories = response.json()
            if isinstance(categories, list) and categories:
                return categories[0]
    except Exception as e:
        print(f"❌ Error getting category {slug}: {str(e)}")
    return None

def create_or_update_category(name, slug, image_data=None, parent_id=None, description=None, existing_categories=None):
    """Create or update WooCommerce category"""
    existing = get_category_by_slug(slug, existing_categories)
    
    # Basic category data
    data = {
        "name": name,
        "slug": slug,
        "description": description or "",
        "display": "default"
    }
    
    # Add parent if specified
    if parent_id:
        data["parent"] = parent_id
    
    # Add image data
    if image_data:
        if isinstance(image_data, int):
            # It's a media ID from WordPress upload - get the URL
            media_url = get_media_url_by_id(image_data)
            if media_url:
                data["image"] = {
                    "src": media_url,
                    "id": image_data
                }
            else:
                data["image"] = {"id": image_data}
        elif isinstance(image_data, dict):
            # It's already formatted with src/id
            data["image"] = image_data
        else:
            # It's a URL string
            data["image"] = {
                "src": image_data,
                "name": slug,
                "alt": name
            }
    
    try:
        if existing:
            # Update existing category
            cat_id = existing["id"]
            response = wcapi.put(f"products/categories/{cat_id}", data)
            if response.status_code in [200, 201]:
                print(f"🔄 Updated: {name} (ID: {cat_id})")
                return cat_id
            else:
                print(f"❌ Failed to update {name}: {response.status_code} - {response.text}")
                return None
        else:
            # Create new category
            response = wcapi.post("products/categories", data)
            if response.status_code in [200, 201]:
                result = response.json()
                cat_id = result["id"]
                print(f"✅ Created: {name} (ID: {cat_id})")
                return cat_id
            else:
                print(f"❌ Failed to create {name}: {response.status_code} - {response.text}")
                return None
    except Exception as e:
        print(f"❌ Error with category {name}: {str(e)}")
        return None

def export_seo_data(categories_data, name_to_id):
    """Export SEO data to CSV for manual import"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"woocommerce_seo_data_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['category_id', 'category_name', 'category_slug', 'seo_title', 'meta_description', 'focus_keyword', 'category_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        seo_count = 0
        for cat_data in categories_data:
            if cat_data['name'] in name_to_id:
                cat_id = name_to_id[cat_data['name']]
                category_url = f"{WC_STORE_URL}/product-category/{cat_data['slug']}/"
                
                # Only write rows that have at least one SEO field
                if cat_data.get('seo_title') or cat_data.get('seo_meta_description') or cat_data.get('focus_keyword'):
                    seo_count += 1
                    writer.writerow({
                        'category_id': cat_id,
                        'category_name': cat_data['name'],
                        'category_slug': cat_data['slug'],
                        'seo_title': cat_data.get('seo_title', ''),
                        'meta_description': cat_data.get('seo_meta_description', ''),
                        'focus_keyword': cat_data.get('focus_keyword', ''),
                        'category_url': category_url
                    })
    
    return filename, seo_count

def generate_import_summary(categories_data):
    """Generate a summary of categories to import"""
    print("\n📋 IMPORT SUMMARY")
    print("=" * 60)
    
    parent_cats = [c for c in categories_data if not c['parent']]
    child_cats = [c for c in categories_data if c['parent']]
    
    print(f"\n📁 Parent Categories ({len(parent_cats)}):")
    for cat in parent_cats:
        extras = []
        if cat['image_url']: extras.append("has image")
        if cat.get('seo_title'): extras.append("has SEO")
        extras_str = f" [{', '.join(extras)}]" if extras else ""
        print(f"   • {cat['name']}{extras_str}")
    
    print(f"\n📂 Subcategories ({len(child_cats)}):")
    for cat in child_cats:
        extras = []
        if cat['image_url']: extras.append("has image")
        if cat.get('seo_title'): extras.append("has SEO")
        extras_str = f" [{', '.join(extras)}]" if extras else ""
        print(f"   └─ {cat['name']} (under {cat['parent']}){extras_str}")
    
    print("\n📊 Statistics:")
    print(f"   • Total categories: {len(categories_data)}")
    print(f"   • Categories with images: {len([c for c in categories_data if c['image_url']])}")
    print(f"   • Categories with SEO data: {len([c for c in categories_data if c.get('seo_title') or c.get('seo_meta_description') or c.get('focus_keyword')])}")
    print("=" * 60)

# === Main ===
def main():
    print("\n🚀 WooCommerce Category Sync from Airtable")
    print("    with E-commerce Optimized Images (XML-RPC)")
    print("=" * 60)
    
    # Check for WordPress credentials
    can_upload_images = bool(WP_USERNAME and WP_APP_PASSWORD)
    
    if can_upload_images:
        print("✅ WordPress credentials found - Will upload images via XML-RPC")
        print(f"   XML-RPC URL: {WORDPRESS_XMLRPC_URL}")
        print("   Images will be:")
        print("   • Named with category slugs for SEO")
        print("   • Optimized for fast loading (JPEG 85%)")
        print("   • Properly sized for e-commerce")
        print("   • Tagged with SEO-friendly metadata")
    else:
        print("⚠️  No WordPress credentials - Will use Airtable URLs directly")
        print("\n   To enable optimized image upload, add to .env:")
        print("   WORDPRESS_USERNAME=your_username")
        print("   WORDPRESS_APPLICATION_PASSWORD=your_app_password")
        print("   WORDPRESS_XMLRPC_URL=https://yoursite.com/xmlrpc.php (optional)")
    
    # Fetch existing WooCommerce categories
    print("\n📊 Fetching existing WooCommerce categories...")
    existing_categories = get_all_categories()
    print(f"   Found {len(existing_categories)} existing categories")
    
    # Fetch all records from Airtable
    print("\n📊 Fetching data from Airtable...")
    records = airtable.all()
    print(f"   Found {len(records)} records")
    
    # Prepare category data
    categories_data = []
    for record in records:
        fields = record.get("fields", {})
        name = fields.get("Category")
        if not name:
            continue
            
        parent = fields.get("Parent Category")
        photo = fields.get("Photo")
        description = fields.get("Description")
        seo_meta = fields.get("SEO Meta description")
        seo_title = fields.get("SEO Title")
        focus_keyword = fields.get("Focus keyword")
        
        image_url = None
        if photo and isinstance(photo, list) and len(photo) > 0:
            image_url = photo[0].get("url")
        
        categories_data.append({
            'name': name,
            'slug': slugify(name),
            'parent': parent[0] if isinstance(parent, list) and parent else parent,
            'image_url': image_url,
            'description': description,
            'seo_meta_description': seo_meta,
            'seo_title': seo_title,
            'focus_keyword': focus_keyword
        })
    
    # Show import summary
    generate_import_summary(categories_data)
    
    # Confirm before proceeding
    print("\n⚠️  WHAT WILL HAPPEN:")
    print("1. Categories will be created/updated in WooCommerce")
    if can_upload_images:
        print("2. Images will be downloaded, optimized, and uploaded via XML-RPC")
    else:
        print("2. Images will be linked from Airtable URLs")
    print("3. SEO data will be exported to a CSV file")
    print("4. You'll get step-by-step instructions for importing SEO data")
    
    proceed = input("\nProceed with import? (yes/no): ").strip().lower()
    if proceed != "yes":
        print("❌ Import cancelled.")
        return
    
    # Process categories
    name_to_id = {}
    
    # First pass: Parent categories
    print("\n📁 Processing parent categories...")
    for cat_data in categories_data:
        if cat_data['parent']:
            continue
        
        print(f"\n🔄 Processing: {cat_data['name']}")
        
        # Process image
        image_data = None
        if cat_data['image_url']:
            image_data = process_category_image(
                cat_data['image_url'],
                cat_data['slug'],
                cat_data['name']
            )
        
        cat_id = create_or_update_category(
            name=cat_data['name'],
            slug=cat_data['slug'],
            image_data=image_data,
            description=cat_data['description'],
            existing_categories=existing_categories
        )
        
        if cat_id:
            name_to_id[cat_data['name']] = cat_id
    
    # Second pass: Subcategories
    print("\n📂 Processing subcategories...")
    for cat_data in categories_data:
        if not cat_data['parent']:
            continue
        
        print(f"\n🔄 Processing: {cat_data['name']}")
        
        parent_id = name_to_id.get(cat_data['parent'])
        if not parent_id:
            # Try to find parent in existing categories
            parent_slug = slugify(cat_data['parent'])
            parent_cat = get_category_by_slug(parent_slug, existing_categories)
            if parent_cat:
                parent_id = parent_cat['id']
                name_to_id[cat_data['parent']] = parent_id
            else:
                print(f"⚠️  Skipping '{cat_data['name']}' - parent '{cat_data['parent']}' not found")
                continue
        
        # Process image
        image_data = None
        if cat_data['image_url']:
            image_data = process_category_image(
                cat_data['image_url'],
                cat_data['slug'],
                cat_data['name']
            )
        
        cat_id = create_or_update_category(
            name=cat_data['name'],
            slug=cat_data['slug'],
            image_data=image_data,
            parent_id=parent_id,
            description=cat_data['description'],
            existing_categories=existing_categories
        )
        
        if cat_id:
            name_to_id[cat_data['name']] = cat_id
    
    # Export SEO data
    print("\n📄 Exporting SEO data...")
    seo_filename, seo_count = export_seo_data(categories_data, name_to_id)
    
    # Final summary
    print("\n✅ SYNC COMPLETED!")
    print("=" * 60)
    print(f"✓ Categories processed: {len(name_to_id)}")
    print(f"✓ Failed: {len(categories_data) - len(name_to_id)}")
    print(f"✓ SEO data exported: {seo_count} categories with SEO data")
    print(f"✓ CSV file: {seo_filename}")
    
    print("\n📝 NEXT STEPS:")
    print("\n1. VERIFY CATEGORIES:")
    print(f"   • Go to: {WC_STORE_URL}/wp-admin/edit-tags.php?taxonomy=product_cat&post_type=product")
    print("   • Check that all categories were created correctly")
    if can_upload_images:
        print("   • Verify images have proper names (slug-based)")
    
    if seo_count > 0:
        print("\n2. IMPORT SEO DATA:")
        print("   • Edit each category in WordPress admin")
        print("   • Open the CSV file in Excel/Google Sheets")
        print("   • Copy and paste the SEO fields")
    
    print("\n3. OPTIMIZATION TIPS:")
    print("   • Enable lazy loading for category images")
    print("   • Use a CDN for faster image delivery")
    print("   • Install an image optimization plugin (Smush, ShortPixel, etc.)")
    print("   • Clear all caches after import")
    
    # Create detailed log
    log_filename = f"sync_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_filename, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'store_url': WC_STORE_URL,
            'categories_synced': name_to_id,
            'categories_failed': [c['name'] for c in categories_data if c['name'] not in name_to_id],
            'seo_export': seo_filename,
            'seo_categories_count': seo_count,
            'total_categories': len(categories_data),
            'images_optimized': can_upload_images
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n📋 Detailed log saved to: {log_filename}")
    print("\n✨ Import complete! Your categories now have SEO-optimized images!")

if __name__ == "__main__":
    main()