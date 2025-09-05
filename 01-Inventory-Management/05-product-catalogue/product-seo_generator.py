import os
import json
import time
import logging
import sys
from pyairtable import Api
from dotenv import load_dotenv
import google.generativeai as genai

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
# =======================================================================

def find_category_record_id(categories_table, category_name):
    """
    Find the record ID for a category name in the Categories table.
    Returns the record ID if found, None otherwise.
    """
    try:
        # Search for the category by name
        records = categories_table.all()
        for record in records:
            if record['fields'].get('Category') == category_name:
                return record['id']
        
        # If exact match not found, try partial match
        for record in records:
            stored_category = record['fields'].get('Category', '')
            if stored_category and category_name.lower() in stored_category.lower():
                logging.info(f"📝 Found partial match: '{category_name}' → '{stored_category}'")
                return record['id']
        
        logging.warning(f"⚠️ Category '{category_name}' not found in Categories table")
        return None
        
    except Exception as e:
        logging.error(f"❌ ERROR: Could not search Categories table: {e}")
        return None

def generate_seo_content(product_name: str, product_description: str, promotions: list) -> dict | None:
    """
    Generates SEO content for a product using the Gemini API, including promotions and category recommendation.
    Returns a dictionary with the SEO data or None if an error occurs.
    """
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')  # Updated to current model name
    except Exception as e:
        logging.error(f"❌ ERROR: Failed to initialize Gemini model: {e}")
        return None

    # Enhanced Prompt with Category Recommendation + Complete SEO Package
    prompt = f"""
    Act as a senior e-commerce SEO specialist, copywriter, and product categorization expert.
    Based on the product data and promotions below, generate compelling SEO and marketing content.

    **Product Data:**
    - Name: "{product_name}"
    - Description: "{product_description}"

    **Current Promotions to Incorporate:**
    {chr(10).join(f"- {promo}" for promo in promotions)}

    **Available Categories (choose the MOST SPECIFIC and RELEVANT category):**
    
    **Electronics:** TV & Home Theater, Audio, Cables & Adapters, Computer Accessories, Batteries, Computers & Laptops, Storage Devices, Gaming, Smartphones & Accessories, Cameras & Photography, Smartwatch

    **Home & Furniture:** Outdoor Furniture, Mirrors, Kitchen & Dining, Living Room, Bedding, Bakeware, Dinnerware, Storage, Wall Art, Cookware, Decor, Office Furniture, Furniture, Home Appliances, Bedroom, Lighting Decor, Dining Room, Bathroom, Rugs, Candles, Drinkware Accessories, Drinkware

    **Beauty & Personal Care:** Makeup, Hair Care, Skincare, Personal Care, Fragrances, Oral Care, Toiletries

    **Sports & Outdoors:** Sports Equipment, Outdoor Recreation, Exercise & Fitness, Sports Apparel, Swim Accessories, Dance & Gymnastics, Team Sports, Sports Technology, Action Sports, Swimwear, Swim Gear, Insects and Bugs Repellent, Sun Protection

    **Toys & Games:** Building Toys, Action Figures, Card Games, Puzzles, Vehicles Toys, Educational Toys, Stuffed Animals & Plush, Board Games, Outdoor Play, Dolls & Accessories, Dress Up & Pretend Play, Collectible Toys, Electronics for Kids, Bikes, Scooters & Ride-Ons, Musical Instruments, Water & Pool Toys, Bubble & Pool Toys, Water & Dive Toys, Remote Control Toys, Beach Toy

    **Apparel & Accessories:** Women's Clothing, Men's Clothing, Accessories, Kids' Clothing, Footware

    **Tools & Home Improvement:** Hand Tools, Electrical Tools, Drills, Power Tools, Paint & Primers, Hardware, Plumbing, Electrical, Flooring, Kitchen & Bath, Lighting, Doors & Windows, Outdoor & Garden, Smart home, Building Materials

    **Office & School Supplies:** Office Machines, Notebooks & Pads, Pens, Pencils, Markers, Desk Accessories, Backpacks and Lunch Bags, Classroom Basics

    **And many more categories available...**

    **Instructions:**
    Your entire response MUST be a single, valid JSON object with no additional text.
    Do not include markdown formatting, explanations, or any text outside the JSON.
    
    **IMPORTANT for Category Selection:**
    - Choose the MOST SPECIFIC category that matches the product
    - If it's a rope light, choose "Outdoor Lighting" not just "Lighting"
    - If it's a plush toy, choose "Stuffed Animals & Plush" not just "Toys & Games"
    - If it's a kitchen item, be specific: "Cookware", "Bakeware", "Dinnerware" etc.
    
    **IMPORTANT for Meta Description:**
    - DO NOT just rewrite the existing description
    - CREATE an entirely new, attractive, marketing-focused meta description
    - Focus on benefits, emotional appeal, and urgency
    - Include power words like "perfect," "amazing," "exclusive," "limited time"
    - Highlight unique selling points and customer benefits
    - Make it irresistible and click-worthy for search results
    
    The JSON object must contain exactly these keys:
    - "seo_title": SEO-optimized title for search results (max 60 characters) incorporating promotions
    - "meta_title": HTML meta title tag content (max 60 characters, focus on brand + key benefit)
    - "meta_description": COMPLETELY NEW attractive meta description (max 160 characters) that sells the product with emotional appeal, benefits, and urgency - NOT a rewrite of the existing description
    - "product_slug": URL-friendly slug (lowercase, hyphens, no spaces, max 50 chars) like "product-name-brand-model"
    - "recommended_category": The MOST SPECIFIC and relevant category from the list above
    - "image_alt_text": Descriptive alt text for product images (max 125 characters) for accessibility
    - "focus_keywords": Array of 3-5 relevant SEO keywords as strings
    - "product_tags": Array of 5-8 product category tags as strings
    - "ecommerce_name": Customer-friendly product name for e-commerce display
    - "brand": Identified or suggested brand name

    **Product Slug Guidelines:**
    - Use lowercase letters only
    - Replace spaces with hyphens (-)
    - Remove special characters, punctuation
    - Include brand and key product identifiers
    - Keep under 50 characters
    - Make it descriptive but concise
    - Examples: "lepro-outdoor-rope-lights-100ft", "lego-target-plush-green-alien", "threshold-asheboro-glasses-set"

    **Focus on:**
    - Emotional triggers (love, excitement, exclusivity)
    - Benefits over features (what it does FOR the customer)
    - Urgency and scarcity (limited time, exclusive, today only)
    - Social proof hints (popular, trending, customer favorite)
    - Use emojis sparingly for visual appeal

    Example format:
    {{
        "seo_title": "Product Name - Save with 4more | Sale up to 90%",
        "meta_title": "Brand Product Name - Key Benefit | Store",
        "meta_description": "Discover the perfect [product]! Premium quality meets unbeatable savings - 90% OFF limited time. Your [target audience] will love this!",
        "product_slug": "brand-product-name-key-feature",
        "recommended_category": "Outdoor Lighting",
        "image_alt_text": "Brand product name showing key visual features in use",
        "focus_keywords": ["primary keyword", "secondary keyword", "long tail keyword"],
        "product_tags": ["category1", "category2", "use case", "target audience"],
        "ecommerce_name": "Clear Customer-Friendly Name",
        "brand": "Brand Name"
    }}
    """

    logging.info(f"🧠 Generating SEO content for: '{product_name}'")
    try:
        response = model.generate_content(prompt)
        if not response.text:
            logging.error(f"❌ ERROR: Empty response from Gemini for: '{product_name}'")
            return None
            
        # Clean up potential markdown formatting
        cleaned_response = response.text.strip()
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response.replace("```json", "").replace("```", "").strip()
        elif cleaned_response.startswith("```"):
            cleaned_response = cleaned_response.replace("```", "").strip()
        
        seo_data = json.loads(cleaned_response)
        
        # Validate required keys including category
        required_keys = ["seo_title", "meta_title", "meta_description", "product_slug", "recommended_category", "image_alt_text", "focus_keywords", "product_tags", "ecommerce_name", "brand"]
        if not all(key in seo_data for key in required_keys):
            logging.error(f"❌ ERROR: Missing required keys in response for: '{product_name}'")
            logging.error(f"Expected keys: {required_keys}")
            logging.error(f"Received keys: {list(seo_data.keys())}")
            return None
            
        logging.info(f"✅ Successfully generated SEO content for: '{product_name}'")
        return seo_data
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ ERROR: Failed to parse JSON response from Gemini for: '{product_name}' - {e}")
        logging.error(f"Raw response: {response.text if 'response' in locals() else 'No response'}")
        return None
    except Exception as e:
        logging.exception(f"❌ ERROR: Failed to generate content from Gemini for: '{product_name}' - {e}")
        return None

def validate_environment():
    """Validate all required environment variables are present."""
    missing_vars = []
    
    if not AIRTABLE_API_KEY:
        missing_vars.append("AIRTABLE_API_KEY")
    if not AIRTABLE_BASE_ID:
        missing_vars.append("AIRTABLE_BASE_ID")
    if not GEMINI_API_KEY:
        missing_vars.append("GEMINI_API_KEY")
    
    if missing_vars:
        logging.error(f"❌ ERROR: Missing environment variables: {', '.join(missing_vars)}")
        logging.error("Please check your .env file and ensure all required variables are set.")
        return False
    
    return True

def main():
    """
    Main function to fetch products from Airtable SEO-View,
    generate SEO content with promotions and category recommendations, and update records.
    """
    logging.info("🚀 Starting SEO Generation Process...")
    
    # Validate environment variables
    if not validate_environment():
        return

    try:
        # Initialize API clients
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        categories_table = api.table(AIRTABLE_BASE_ID, AIRTABLE_CATEGORIES_TABLE_NAME)  # Categories table
        genai.configure(api_key=GEMINI_API_KEY)
        logging.info("✅ Successfully initialized API clients")
        
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

    # Your specific promotions
    promotions = [
        "Save with 4more",
        "Sale up to 90%"
    ]

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
        
        seo_data = generate_seo_content(product_name, product_desc, promotions)

        if seo_data:
            # Log generated content including category
            logging.info(f"📊 Generated SEO Data for '{product_name}':")
            logging.info(f"   - SEO Title: {seo_data.get('seo_title', 'N/A')}")
            logging.info(f"   - Meta Title: {seo_data.get('meta_title', 'N/A')}")
            logging.info(f"   - Meta Description: {seo_data.get('meta_description', 'N/A')}")
            logging.info(f"   - Product Slug: {seo_data.get('product_slug', 'N/A')}")
            logging.info(f"   - Recommended Category: {seo_data.get('recommended_category', 'N/A')}")
            logging.info(f"   - Image Alt Text: {seo_data.get('image_alt_text', 'N/A')}")
            logging.info(f"   - Focus Keywords: {', '.join(seo_data.get('focus_keywords', []))}")
            logging.info(f"   - Product Tags: {', '.join(seo_data.get('product_tags', []))}")
            logging.info(f"   - E-commerce Name: {seo_data.get('ecommerce_name', 'N/A')}")
            logging.info(f"   - Brand: {seo_data.get('brand', 'N/A')}")

            # Prepare update payload - handle Category field carefully
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
                        category_record_id = find_category_record_id(categories_table, category_name)
                        
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
    logging.info("🎉 SEO generation process complete!")
    logging.info(f"📈 Summary: {successful_updates} successful, {failed_updates} failed out of {len(records_to_process)} total records")

if __name__ == "__main__":
    main()