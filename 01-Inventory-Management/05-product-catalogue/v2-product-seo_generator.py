#!/usr/bin/env python3
"""
Complete SEO & E-commerce Product Optimization System
With Airtable Integration + Gemini AI Recommendations
For Toronto, Oakville, Mississauga Local Store

Features:
- SEO Title & Meta Title generation
- Compelling Meta Descriptions 
- Product Slug generation
- Smart Category Recommendation with proper record linking
- Image Alt Text for accessibility
- Focus Keywords optimization
- Product Tags as text
- E-commerce friendly names
- Brand identification
- Local promotions integration
- Airtable integration for reading and updating records
- Gemini AI for intelligent content generation
"""

import os
import json
import time
import logging
import sys
import re
import csv
import random
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from pathlib import Path

# Third-party imports with installation check
try:
    from pyairtable import Api
    from dotenv import load_dotenv
    import google.generativeai as genai
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Please install required packages:")
    print("pip install pyairtable python-dotenv google-generativeai")
    sys.exit(1)

# Set console encoding to UTF-8 for Windows compatibility
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Load variables from the .env file into the environment
load_dotenv()

# ========================= LOGGING SETUP =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("seo_generator.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
# =====================================================================

# ============================ CONFIGURATION ============================
# --- Read credentials and settings from the environment ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_PRODUCT_CATALOGUE_TABLE_NAME", "Product Catalogue")
AIRTABLE_CATEGORIES_TABLE_NAME = "Categories"  # Categories table name
AIRTABLE_CATEGORIES_VIEW_NAME = "Product-catalogue-recommendation"  # Categories view name
AIRTABLE_VIEW_NAME = "SEO-View"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Airtable Field Names ---
FIELD_PRODUCT_NAME = "Product Name"
FIELD_DESCRIPTION = "Description"
FIELD_SEO_READY_FLAG = "SEO Ready"
FIELD_SEO_TITLE = "Product SEO Title"
FIELD_META_TITLE = "Meta Title"
FIELD_META_DESC = "Meta Description"
FIELD_IMAGE_ALT = "Image Alt Text"
FIELD_PRODUCT_SLUG = "Product Slug"
FIELD_CATEGORY = "Category"  # Link to Categories table
FIELD_KEYWORDS = "Focus Keyword"
FIELD_TAGS = "Product Tags"
FIELD_ECOMM_NAME = "ecommerce-Friendly Name"
FIELD_BRAND = "Brand"
FIELD_PRICE = "Price"
FIELD_FEATURES = "Features"
FIELD_COLOR = "Color"
FIELD_SIZE = "Size"
FIELD_MATERIAL = "Material"
FIELD_SKU = "SKU"
FIELD_MODEL = "Model"
FIELD_TARGET_LOCATION = "Target Location"
FIELD_APPLIED_PROMOTION = "Applied Promotion"
FIELD_OPTIMIZATION_SCORE = "Optimization Score"
FIELD_SEO_UPDATED_DATE = "SEO Updated Date"
# =======================================================================


class EnhancedSEOOptimizer:
    """Enhanced SEO optimizer with Gemini AI integration"""
    
    def __init__(self, categories_table=None):
        self.locations = ['Toronto', 'Oakville', 'Mississauga']
        self.store_name = 'Promote Local'
        self.promotions = [
            'Save with 4more',
            'Sale up to 90%',
            'Best Deals in GTA',
            'Local Pickup Available',
            'Free Same-Day Delivery',
            'Price Match Guarantee'
        ]
        
        # Load available categories from Airtable
        self.available_categories = []
        if categories_table:
            self.load_categories_from_airtable(categories_table)
        
        # Initialize Gemini AI
        if GEMINI_API_KEY:
            genai.configure(api_key=GEMINI_API_KEY)
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash')
        else:
            self.gemini_model = None
            logging.warning("⚠️ Gemini API key not found - AI features will be disabled")
    
    def load_categories_from_airtable(self, categories_table):
        """Load available categories from Airtable Categories table view"""
        try:
            logging.info(f"📋 Loading categories from '{AIRTABLE_CATEGORIES_VIEW_NAME}' view...")
            records = categories_table.all(view=AIRTABLE_CATEGORIES_VIEW_NAME)
            
            for record in records:
                category_name = record['fields'].get('Category')
                if category_name:
                    self.available_categories.append(category_name)
            
            logging.info(f"✅ Loaded {len(self.available_categories)} categories from Airtable:")
            for i, cat in enumerate(self.available_categories[:10]):  # Show first 10
                logging.info(f"   {i+1}. {cat}")
            if len(self.available_categories) > 10:
                logging.info(f"   ... and {len(self.available_categories) - 10} more categories")
                
        except Exception as e:
            logging.error(f"❌ ERROR: Could not load categories from Airtable: {e}")
            logging.error("Will use fallback categories instead")
            # Fallback to basic categories if Airtable fails
            self.available_categories = [
                'Electronics', 'Home & Furniture', 'Beauty & Personal Care',
                'Sports & Outdoors', 'Toys & Games', 'Apparel & Accessories',
                'Tools & Home Improvement', 'Office & School Supplies'
            ]

    def find_category_record_id(self, categories_table, category_name):
        """Find the record ID for a category name in the Categories table from the specific view"""
        try:
            # Search in the specific view only
            records = categories_table.all(view=AIRTABLE_CATEGORIES_VIEW_NAME)
            for record in records:
                if record['fields'].get('Category') == category_name:
                    return record['id']
            
            # If exact match not found, try partial match within the view
            for record in records:
                stored_category = record['fields'].get('Category', '')
                if stored_category and category_name.lower() in stored_category.lower():
                    logging.info(f"📝 Found partial match: '{category_name}' → '{stored_category}'")
                    return record['id']
            
            logging.warning(f"⚠️ Category '{category_name}' not found in '{AIRTABLE_CATEGORIES_VIEW_NAME}' view")
            return None
            
        except Exception as e:
            logging.error(f"❌ ERROR: Could not search Categories table view '{AIRTABLE_CATEGORIES_VIEW_NAME}': {e}")
            return None

    def generate_seo_content_with_ai(self, product_name: str, product_description: str, 
                                    additional_data: Dict = None) -> Dict:
        """Generate SEO content using Gemini AI with enhanced prompts"""
        if not self.gemini_model:
            logging.warning("⚠️ Gemini AI not available, falling back to local generation")
            return self.generate_seo_content_local(product_name, product_description, additional_data)
        
        if additional_data is None:
            additional_data = {}
            
        # Build dynamic category list for AI prompt
        categories_text = "**Available Categories (choose the MOST SPECIFIC and RELEVANT category):**\n"
        categories_text += "\n".join(f"- {cat}" for cat in self.available_categories)
        
        # Enhanced prompt with actual categories from Airtable
        prompt = f"""
        Act as a senior e-commerce SEO specialist, copywriter, and product categorization expert for a LOCAL Toronto/Oakville/Mississauga store called "Promote Local".
        
        Based on the product data below, generate compelling SEO and marketing content optimized for LOCAL SEARCH in the Greater Toronto Area.

        **Product Data:**
        - Name: "{product_name}"
        - Description: "{product_description}"
        - Price: "{additional_data.get('price', '')}"
        - Brand: "{additional_data.get('brand', '')}"
        - Features: "{additional_data.get('features', '')}"
        - Color: "{additional_data.get('color', '')}"
        - Size: "{additional_data.get('size', '')}"

        **LOCAL PROMOTIONS to Incorporate:**
        - Save with 4more
        - Sale up to 90%
        - Best Deals in GTA
        - Local Pickup Available in Toronto, Oakville, Mississauga
        - Free Same-Day Delivery
        - Price Match Guarantee

        **TARGET LOCATIONS:** Toronto, Oakville, Mississauga (choose one as primary)

        {categories_text}

        **CRITICAL REQUIREMENTS:**
        - Choose ONLY from the categories listed above - do not create new categories
        - Use the EXACT category name as shown in the list
        - Focus on LOCAL SEO for Toronto/Oakville/Mississauga
        - Include local promotions naturally
        - Make meta descriptions COMPELLING and CLICK-WORTHY (not just rewrites)
        - Use emotional triggers and urgency
        - Include benefits over features
        - Optimize for local search intent

        Your entire response MUST be a single, valid JSON object with no additional text.
        
        {{
            "seo_title": "SEO-optimized title for search results (max 60 characters) with local focus",
            "meta_title": "HTML meta title tag content (max 60 characters)",
            "meta_description": "COMPELLING new meta description (max 160 chars) with emotional appeal & urgency",
            "product_slug": "url-friendly-slug-with-brand-location",
            "recommended_category": "Most specific category from the list above",
            "image_alt_text": "Descriptive alt text for accessibility (max 125 characters)",
            "focus_keywords": ["keyword1", "keyword2", "local keyword with city"],
            "product_tags": ["tag1", "tag2", "local tag", "promotion tag"],
            "ecommerce_name": "Customer-friendly display name",
            "brand": "Identified or suggested brand name",
            "target_location": "Toronto, Oakville, or Mississauga",
            "applied_promotion": "One of the available promotions",
            "local_seo_keywords": ["local keyword 1", "local keyword 2"],
            "optimization_score": 85
        }}
        """

        try:
            response = self.gemini_model.generate_content(prompt)
            if not response.text:
                logging.error(f"❌ ERROR: Empty response from Gemini for: '{product_name}'")
                return self.generate_seo_content_local(product_name, product_description, additional_data)
                
            # Clean up potential markdown formatting
            cleaned_response = response.text.strip()
            if cleaned_response.startswith("```json"):
                cleaned_response = cleaned_response.replace("```json", "").replace("```", "").strip()
            elif cleaned_response.startswith("```"):
                cleaned_response = cleaned_response.replace("```", "").strip()
            
            seo_data = json.loads(cleaned_response)
            
            # Validate required keys
            required_keys = ["seo_title", "meta_title", "meta_description", "product_slug", 
                           "recommended_category", "image_alt_text", "focus_keywords", 
                           "product_tags", "ecommerce_name", "brand"]
            
            if not all(key in seo_data for key in required_keys):
                logging.error(f"❌ ERROR: Missing required keys in AI response for: '{product_name}'")
                return self.generate_seo_content_local(product_name, product_description, additional_data)
            
            # Add generated timestamp
            seo_data['generated_at'] = datetime.now().isoformat()
            seo_data['generation_method'] = 'gemini_ai'
            
            logging.info(f"✅ Successfully generated AI SEO content for: '{product_name}'")
            return seo_data
            
        except json.JSONDecodeError as e:
            logging.error(f"❌ ERROR: Failed to parse JSON from Gemini for: '{product_name}' - {e}")
            return self.generate_seo_content_local(product_name, product_description, additional_data)
        except Exception as e:
            logging.error(f"❌ ERROR: Gemini generation failed for: '{product_name}' - {e}")
            return self.generate_seo_content_local(product_name, product_description, additional_data)

    def generate_seo_content_local(self, product_name: str, product_description: str, 
                                  additional_data: Dict = None) -> Dict:
        """Fallback local SEO content generation"""
        if additional_data is None:
            additional_data = {}
            
        # Local generation logic (simplified but functional)
        brand = self.identify_brand(product_name, product_description)
        location = random.choice(self.locations)
        promotion = random.choice(self.promotions)
        
        # Generate basic SEO content
        seo_title = f"{brand} {product_name} | {promotion} | {location}".strip()[:60]
        meta_description = f"Shop {product_name} in {location}. {promotion} with free local delivery!".strip()[:160]
        
        # Simple slug generation
        slug = f"{brand}-{product_name}".lower()
        slug = re.sub(r'[^a-z0-9\s-]', '', slug)
        slug = re.sub(r'\s+', '-', slug)[:50]
        
        # Basic category recommendation
        category = self.recommend_basic_category(product_name, product_description)
        
        return {
            'seo_title': seo_title,
            'meta_title': seo_title,
            'meta_description': meta_description,
            'product_slug': slug,
            'recommended_category': category,
            'image_alt_text': f"{brand} {product_name}".strip()[:125],
            'focus_keywords': [product_name.lower(), f"{product_name.lower()} {location.lower()}"],
            'product_tags': [product_name, brand, location, 'Local Store'],
            'ecommerce_name': f"{brand} {product_name}".strip(),
            'brand': brand,
            'target_location': location,
            'applied_promotion': promotion,
            'local_seo_keywords': [f"{product_name.lower()} {location.lower()}"],
            'optimization_score': 75,
            'generated_at': datetime.now().isoformat(),
            'generation_method': 'local_fallback'
        }

    def identify_brand(self, product_name: str, description: str = '') -> str:
        """Simple brand identification"""
        search_text = f"{product_name} {description}".lower()
        
        common_brands = [
            'apple', 'samsung', 'sony', 'lg', 'microsoft', 'google', 'amazon',
            'nike', 'adidas', 'levi', 'calvin klein', 'tommy hilfiger',
            'ikea', 'wayfair', 'home depot', 'canadian tire',
            'lego', 'fisher price', 'mattel'
        ]
        
        for brand in common_brands:
            if brand in search_text:
                return brand.title()
        
        return ''

    def recommend_basic_category(self, product_name: str, description: str) -> str:
        """Basic category recommendation using available Airtable categories"""
        text = f"{product_name} {description}".lower()
        
        # Try to match against available categories from Airtable
        for category in self.available_categories:
            category_lower = category.lower()
            # Check if any words from the category appear in the product text
            category_words = category_lower.replace('&', '').replace('-', ' ').split()
            if any(word in text for word in category_words if len(word) > 3):
                return category
        
        # Fallback to first available category if no match
        return self.available_categories[0] if self.available_categories else 'General'


def validate_environment():
    """Validate all required environment variables are present."""
    missing_vars = []
    
    if not AIRTABLE_API_KEY:
        missing_vars.append("AIRTABLE_API_KEY")
    if not AIRTABLE_BASE_ID:
        missing_vars.append("AIRTABLE_BASE_ID")
    if not GEMINI_API_KEY:
        missing_vars.append("GEMINI_API_KEY (optional but recommended)")
    
    if len(missing_vars) > 1 or (missing_vars and 'GEMINI_API_KEY' not in missing_vars[0]):
        logging.error(f"❌ ERROR: Missing environment variables: {', '.join(missing_vars)}")
        logging.error("Please check your .env file and ensure all required variables are set.")
        return False
    
    return True


def main():
    """
    Main function to fetch products from Airtable SEO-View,
    generate enhanced SEO content with AI, and update records.
    """
    logging.info("🚀 Starting Enhanced SEO Generation Process with AI...")
    
    # Validate environment variables
    if not validate_environment():
        return

    try:
        # Initialize API clients
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        categories_table = api.table(AIRTABLE_BASE_ID, AIRTABLE_CATEGORIES_TABLE_NAME)
        
        # Initialize enhanced SEO optimizer with categories table
        seo_optimizer = EnhancedSEOOptimizer(categories_table)
        
        logging.info("✅ Successfully initialized all API clients and SEO optimizer")
        
    except Exception as e:
        logging.exception(f"❌ ERROR: Failed to initialize API clients - {e}")
        logging.error("Please verify your API keys and base/table IDs are correct.")
        return

    # Fetch records from the SEO-View
    try:
        logging.info(f"🔍 Fetching records from view: '{AIRTABLE_VIEW_NAME}' in table: '{AIRTABLE_TABLE_NAME}'")
        records_to_process = table.all(view=AIRTABLE_VIEW_NAME)
        logging.info(f"✅ Successfully fetched {len(records_to_process)} records")
        
    except Exception as e:
        logging.exception(f"❌ ERROR: Failed to fetch records from Airtable - {e}")
        logging.error(f"Please verify that table '{AIRTABLE_TABLE_NAME}' and view '{AIRTABLE_VIEW_NAME}' exist.")
        return

    if not records_to_process:
        logging.info(f"✅ No records found in '{AIRTABLE_VIEW_NAME}' view. All products may already be processed.")
        return

    logging.info(f"🔍 Found {len(records_to_process)} products to process in '{AIRTABLE_VIEW_NAME}'.")

    successful_updates = 0
    failed_updates = 0

    for index, record in enumerate(records_to_process, 1):
        logging.info(f"🔄 Processing product {index}/{len(records_to_process)}")
        record_id = record['id']
        fields = record['fields']

        product_name = fields.get(FIELD_PRODUCT_NAME)
        product_desc = fields.get(FIELD_DESCRIPTION)

        if not product_name:
            logging.warning(f"⚠️ SKIPPING record {record_id} - missing Product Name")
            failed_updates += 1
            continue
            
        if not product_desc:
            logging.warning(f"⚠️ SKIPPING record {record_id} - missing Product Description for '{product_name}'")
            failed_updates += 1
            continue

        logging.info(f"📦 Processing Product: '{product_name}' (Record ID: {record_id})")
        
        # Collect additional data for enhanced AI generation
        additional_data = {
            'price': fields.get(FIELD_PRICE, ''),
            'brand': fields.get(FIELD_BRAND, ''),
            'features': fields.get(FIELD_FEATURES, ''),
            'color': fields.get(FIELD_COLOR, ''),
            'size': fields.get(FIELD_SIZE, ''),
            'material': fields.get(FIELD_MATERIAL, ''),
            'sku': fields.get(FIELD_SKU, ''),
            'model': fields.get(FIELD_MODEL, '')
        }
        
        # Generate enhanced SEO content with AI
        seo_data = seo_optimizer.generate_seo_content_with_ai(
            product_name, product_desc, additional_data
        )

        if seo_data:
            # Log generated content
            logging.info(f"📊 Generated Enhanced SEO Data for '{product_name}' using {seo_data.get('generation_method', 'unknown')}:")
            logging.info(f"   - SEO Title: {seo_data.get('seo_title', 'N/A')}")
            logging.info(f"   - Meta Title: {seo_data.get('meta_title', 'N/A')}")
            logging.info(f"   - Meta Description: {seo_data.get('meta_description', 'N/A')}")
            logging.info(f"   - Product Slug: {seo_data.get('product_slug', 'N/A')}")
            logging.info(f"   - Recommended Category: {seo_data.get('recommended_category', 'N/A')}")
            logging.info(f"   - Target Location: {seo_data.get('target_location', 'N/A')}")
            logging.info(f"   - Applied Promotion: {seo_data.get('applied_promotion', 'N/A')}")
            logging.info(f"   - Optimization Score: {seo_data.get('optimization_score', 'N/A')}")

            # Prepare update payload - only include existing Airtable fields
            update_payload = {
                FIELD_SEO_TITLE: seo_data.get("seo_title"),
                FIELD_META_TITLE: seo_data.get("meta_title"),
                FIELD_META_DESC: seo_data.get("meta_description"),
                FIELD_PRODUCT_SLUG: seo_data.get("product_slug"),
                FIELD_IMAGE_ALT: seo_data.get("image_alt_text"),
                FIELD_KEYWORDS: ", ".join(seo_data.get("focus_keywords", [])),
                FIELD_TAGS: ", ".join(seo_data.get("product_tags", [])),
                FIELD_ECOMM_NAME: seo_data.get("ecommerce_name"),
                FIELD_BRAND: seo_data.get("brand"),
                FIELD_SEO_READY_FLAG: True
            }
            
            # Optional: Log the additional data (but don't try to update non-existent fields)
            logging.info(f"   - Additional Generated Data:")
            logging.info(f"     * Target Location: {seo_data.get('target_location', 'N/A')}")
            logging.info(f"     * Applied Promotion: {seo_data.get('applied_promotion', 'N/A')}")
            logging.info(f"     * Optimization Score: {seo_data.get('optimization_score', 'N/A')}")
            logging.info(f"     * Generation Method: {seo_data.get('generation_method', 'N/A')}")
            
            # You can add these fields to your Airtable later if needed:
            # - Target Location (Single line text)
            # - Applied Promotion (Single line text) 
            # - Optimization Score (Number)
            # - SEO Updated Date (Date)

            try:
                logging.info(f"⬆️ Updating Airtable record for: '{product_name}' (ID: {record_id})")
                table.update(record_id, update_payload)
                logging.info(f"✅ Successfully updated main SEO fields for: '{product_name}'")
                successful_updates += 1
                
                # Try to update Category separately using record ID
                try:
                    category_name = seo_data.get("recommended_category")
                    if category_name:
                        logging.info(f"🔄 Looking up Category record ID for: '{category_name}'")
                        category_record_id = seo_optimizer.find_category_record_id(categories_table, category_name)
                        
                        if category_record_id:
                            # Link to the category record using record ID
                            table.update(record_id, {FIELD_CATEGORY: [category_record_id]})
                            logging.info(f"✅ Successfully linked Category for: '{product_name}' → '{category_name}'")
                        else:
                            logging.warning(f"⚠️ Could not find Category '{category_name}' in Categories table for '{product_name}'")
                    
                except Exception as cat_error:
                    logging.error(f"❌ ERROR: Could not update Category field for '{product_name}': {cat_error}")
                    logging.info(f"ℹ️ Category field requires linking to existing records in Categories table")
                
            except Exception as e:
                logging.exception(f"❌ ERROR: Could not update Airtable record {record_id} for product: '{product_name}' - {e}")
                failed_updates += 1
        else:
            logging.error(f"❌ Failed to generate SEO content for: '{product_name}'")
            failed_updates += 1

        # Rate limiting - be nice to the APIs
        time.sleep(2)

    # Final summary
    logging.info("🎉 Enhanced SEO generation process complete!")
    logging.info(f"📈 Summary: {successful_updates} successful, {failed_updates} failed out of {len(records_to_process)} total records")
    
    # Export summary to CSV for backup
    export_summary_to_csv(successful_updates, failed_updates, len(records_to_process))


def export_summary_to_csv(successful: int, failed: int, total: int):
    """Export processing summary to CSV"""
    try:
        filename = f"seo_processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Metric', 'Count', 'Percentage'])
            writer.writerow(['Total Records', total, '100%'])
            writer.writerow(['Successful Updates', successful, f'{(successful/total*100):.1f}%' if total > 0 else '0%'])
            writer.writerow(['Failed Updates', failed, f'{(failed/total*100):.1f}%' if total > 0 else '0%'])
            writer.writerow(['Processing Date', datetime.now().isoformat(), '-'])
            writer.writerow(['Store Name', 'Promote Local', '-'])
            writer.writerow(['Target Locations', 'Toronto, Oakville, Mississauga', '-'])
        
        logging.info(f"📄 Processing summary exported to: {filename}")
        
    except Exception as e:
        logging.error(f"❌ Failed to export summary: {e}")


if __name__ == "__main__":
    main()