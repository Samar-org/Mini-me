import os
import requests
import logging
from urllib.parse import quote
import json
import re
import time
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import google.generativeai as genai

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AirtableProductScraper:
    def __init__(self, airtable_api_key: str, base_id: str, gemini_api_key: str):
        self.airtable_api_key = airtable_api_key
        self.base_id = base_id
        self.headers = {
            'Authorization': f'Bearer {airtable_api_key}',
            'Content-Type': 'application/json'
        }
        self.base_url = f'https://api.airtable.com/v0/{base_id}'
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')

    def get_category_record_id(self, category_name: str) -> Optional[str]:
        """Look up category record ID by name from the Categories table"""
        if not category_name or category_name == "Not found":
            return None
            
        url = f"{self.base_url}/Categories"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            records = response.json().get("records", [])
            
            # Search for matching category name (case-insensitive)
            category_name_lower = category_name.lower().strip()
            for record in records:
                fields = record.get("fields", {})
                record_category = fields.get("Category", "").lower().strip()
                if record_category == category_name_lower:
                    logger.info(f"Found category match: '{category_name}' -> {record.get('id')}")
                    return record.get("id")
            
            # If no exact match, try partial matching
            for record in records:
                fields = record.get("fields", {})
                record_category = fields.get("Category", "").lower().strip()
                if category_name_lower in record_category or record_category in category_name_lower:
                    logger.info(f"Found partial category match: '{category_name}' -> '{fields.get('Category')}' ({record.get('id')})")
                    return record.get("id")
                    
            logger.warning(f"No category found for: '{category_name}'")
            return None
            
        except Exception as e:
            logger.error(f"Failed to lookup category '{category_name}': {e}")
            return None

    def inspect_table_schema(self, table_name: str):
        """Inspect the table schema to understand field types and requirements"""
        url = f"https://api.airtable.com/v0/meta/bases/{self.base_id}/tables"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            tables = response.json().get("tables", [])
            
            for table in tables:
                if table.get("name") == table_name:
                    logger.info(f"Schema for table '{table_name}':")
                    fields = table.get("fields", [])
                    for field in fields:
                        field_name = field.get("name")
                        field_type = field.get("type")
                        logger.info(f"  - {field_name}: {field_type}")
                    return fields
            
            logger.warning(f"Table '{table_name}' not found")
            return []
            
        except Exception as e:
            logger.error(f"Failed to inspect table schema: {e}")
            return []

    def generate_4more_code(self, all_codes: List[str]) -> str:
        prefix = "O1-"
        existing_numbers = [int(code.split("-")[1]) for code in all_codes if code.startswith(prefix)]
        next_number = max(existing_numbers + [0]) + 1
        return f"{prefix}{next_number:05d}"

    def fetch_existing_4more_codes(self) -> List[str]:
        url = f"{self.base_url}/Product%20Catalogue?view=full-table-view"
        codes = []
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            records = response.json().get("records", [])
            for rec in records:
                code = rec.get("fields", {}).get("4more-Product-Code")
                if code:
                    codes.append(code)
            logger.info(f"Fetched {len(codes)} existing product codes")
        except Exception as e:
            logger.warning(f"Could not fetch existing codes: {e}")
        return codes

    def update_source_item(self, table: str, record_id: str, product_code: str):
        url = f"{self.base_url}/{table}/{record_id}"
        
        # Try different possible field name variations
        possible_payloads = [
            # Most likely field names based on your schema
            {
                "fields": {
                    "4more-Product-Code": product_code,
                    "Status": "Linked to Catalogue"
                }
            },
            # Alternative field name variations
            {
                "fields": {
                    "4more-Product-Code": product_code,
                    "Scraping Status": "Linked to Catalogue"
                }
            },
            # Try with just the product code first
            {
                "fields": {
                    "4more-Product-Code": product_code
                }
            }
        ]
        
        for i, payload in enumerate(possible_payloads):
            try:
                response = requests.patch(url, headers=self.headers, json=payload)
                if response.status_code in [200, 201]:
                    logger.info(f"🔗 Linked item {record_id} with code {product_code} (attempt {i+1})")
                    return
                elif response.status_code == 422:
                    logger.debug(f"Attempt {i+1} failed with 422: {response.text}")
                    continue
                else:
                    response.raise_for_status()
            except Exception as e:
                logger.debug(f"Attempt {i+1} failed: {e}")
                continue
        
        # If all attempts failed, log the detailed error
        logger.warning(f"❌ Failed to update source item {record_id} after all attempts")
        
        # Try to get the actual field names for debugging
        try:
            get_response = requests.get(url, headers=self.headers)
            if get_response.status_code == 200:
                existing_fields = get_response.json().get("fields", {})
                logger.info(f"Available fields in record: {list(existing_fields.keys())}")
        except Exception as e:
            logger.debug(f"Could not fetch record for debugging: {e}")

    def push_to_product_catalogue(self, record: Dict[str, Any]) -> Optional[str]:
        url = f"{self.base_url}/Product%20Catalogue"
        
        # Clean and validate the record data with proper field mapping
        cleaned_record = {}
        
        # Valid fields based on your Airtable schema
        valid_fields = {
            "Product Name": "singleLineText",
            "Product URL": "url", 
            "ecommerce-Friendly Name": "singleLineText",
            "Brand": "singleLineText",
            "Category": "multipleRecordLinks",  # This is likely a linked record field
            "Description": "multilineText",
            "Photo Files": "multipleAttachments",
            "Featured Photos": "multipleAttachments",
            "Unit Retail Price": "currency",
            "Currency": "singleLineText",
            "4more Unit Price": "currency",
            "4more-Product-Code": "singleLineText",
            "GTIN": "singleLineText",
            "Product Tags": "singleLineText",
            "Product SEO Title": "singleLineText",
            "Focus Keyword": "singleLineText",
            "Product Slug": "singleLineText",
            "Image Alt Text": "singleLineText",
            "Big": "checkbox",
            "Fragile": "checkbox", 
            "Heavy": "checkbox",
            "Dimensions": "singleLineText",
            "Weight": "singleLineText",
            "Color": "singleLineText",
            "Scraping Website": "singleLineText",
            "Model": "singleLineText",
            "Category Name": "rollup",
            "Notes": "multilineText"
        }
        
        for key, value in record.items():
            if key not in valid_fields:
                logger.warning(f"Skipping invalid field: {key}")
                continue
                
            if value is None or value == '' or value == 'Not found':
                continue
                
            # Handle specific field types
            if key == "Photo Files":
                if isinstance(value, list) and len(value) > 0:
                    # Airtable attachments need specific format: [{"url": "..."}]
                    attachments = []
                    for img_url in value:
                        if img_url and isinstance(img_url, str) and img_url.startswith('http'):
                            attachments.append({"url": img_url})
                    if attachments:
                        cleaned_record[key] = attachments
            elif key == "Product URL":
                # URL field - validate it's a proper URL
                if isinstance(value, str) and (value.startswith('http://') or value.startswith('https://')):
                    cleaned_record[key] = value
            elif key == "Category":
                # Handle linked record field - look up category ID
                if isinstance(value, str) and value.strip():
                    category_id = self.get_category_record_id(value.strip())
                    if category_id:
                        # Linked record fields expect an array of record IDs
                        cleaned_record[key] = [category_id]
                    else:
                        logger.info(f"Skipping Category field - no matching record found for: {value}")
                else:
                    logger.info(f"Skipping Category field - invalid value: {value}")
            elif key == "Unit Retail Price":
                # Currency field - clean up formatting
                if isinstance(value, str):
                    # Remove common currency symbols and extract number
                    import re
                    price_match = re.search(r'[\d,]+\.?\d*', value.replace('

    def resolve_shortlink(self, url: str) -> str:
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            logger.info(f"Resolved URL: {response.url}")
            return response.url
        except Exception as e:
            logger.warning(f"Failed to resolve shortlink: {e}")
            return url

    def fetch_webpage_with_selenium(self, url: str) -> str:
        driver = None
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(5)
            content = driver.page_source
            logger.info(f"[Selenium] Fetched content from: {url}")
            return content
        except Exception as e:
            logger.error(f"[Selenium] Error: {e}")
            return ""
        finally:
            if driver:
                driver.quit()

    def extract_amazon_product_data(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        def get_text(selector, attr=None):
            try:
                element = soup.select_one(selector)
                if not element:
                    return 'Not found'
                if attr:
                    return element.get(attr, '').strip()
                return element.get_text(strip=True)
            except Exception:
                return 'Not found'

        data['Product Name'] = get_text('#productTitle')
        
        # Try multiple selectors for description
        description = get_text('#productDescription')
        if description == 'Not found':
            description = get_text('#feature-bullets')
        data['Description'] = description
        
        data['Retail Price (CAD)'] = get_text('.a-price .a-offscreen')
        data['Weight'] = get_text('#detailBullets_feature_div li:contains("Item weight") span:last-child')
        data['Dimension'] = get_text('#detailBullets_feature_div li:contains("Product dimensions") span:last-child')
        data['Brand'] = get_text('#bylineInfo')
        
        # Extract category from breadcrumbs or department info
        category = get_text('#wayfinding-breadcrumbs_feature_div a')
        if category == 'Not found':
            category = get_text('#nav-subnav a.nav-a')
        if category == 'Not found':
            # Try to get from department in product details
            category = get_text('#detailBullets_feature_div li:contains("Department") span:last-child')
        data['Category'] = category

        # Extract images
        images = []
        for img in soup.select('img[src]'):
            src = img.get('src', '')
            if 'media-amazon' in src and src not in images:
                images.append(src)
        data['Photo Files'] = images[:5] if images else []

        return data

    def extract_product_data_with_gemini(self, product_url: str, inspection_photo: str = None) -> Dict[str, Any]:
        product_url = self.resolve_shortlink(product_url)

        if "amazon." in product_url:
            logger.info("[Amazon Scraper] Using custom parser for Amazon domain...")
            html = self.fetch_webpage_with_selenium(product_url)
            return self.extract_amazon_product_data(html)

        html = self.fetch_webpage_with_selenium(product_url)
        if not html:
            logger.error("Failed to fetch webpage content")
            return {
                'Product Name': 'Extraction failed',
                'Description': 'Failed to fetch webpage',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found',
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Category': 'Not found',
                'Photo Files': []
            }

        prompt = f"""
Extract product information from the following HTML for {product_url}.
Return in JSON:
{{
  "Product Name": "...",
  "Description": "...",
  "Retail Price (CAD)": "...",
  "Weight": "...",
  "Dimension": "...",
  "Brand": "...",
  "Category": "...",
  "Photo Files": ["..."]
}}
Content:
{html[:8000]}
"""
        try:
            response = self.model.generate_content([prompt])
            response_text = response.text
            json_text = re.search(r'```(?:json)?(.*?)```', response_text, re.DOTALL)
            if json_text:
                return json.loads(json_text.group(1).strip())
            else:
                return json.loads(response_text.strip())
        except Exception as e:
            logger.error(f"Gemini extraction error: {e}")
            return {
                'Product Name': 'Extraction failed',
                'Description': 'Gemini failed',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found',
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Category': 'Not found',
                'Photo Files': []
            }

    def process_and_sync_product(self, table: str, record_id: str, product_url: str):
        """Process a single product record and sync to catalogue"""
        try:
            logger.info(f"🔄 Processing record {record_id} from {table}")
            data = self.extract_product_data_with_gemini(product_url)
            
            if not data or data.get("Product Name") in [None, "Extraction failed", "", "Not found"]:
                logger.warning(f"Skipping record {record_id} — no valid product name extracted.")
                return

            # Step 1: Fetch all codes and generate new one
            all_codes = self.fetch_existing_4more_codes()
            code = self.generate_4more_code(all_codes)

            # Step 2: Map extracted data to correct Airtable field names
            fields = {
                "Product URL": product_url,
                "4more-Product-Code": code
            }
            
            # Map the extracted data to the correct field names in your Airtable
            field_mapping = {
                "Product Name": "Product Name",  # Exact match
                "Description": "Description",    # Exact match  
                "Retail Price (CAD)": "Unit Retail Price",  # Your field is "Unit Retail Price"
                "Weight": "Weight",  # Exact match - this field exists
                "Dimension": "Dimensions",  # Your field is "Dimensions" (plural)
                "Brand": "Brand",  # Exact match
                "Photo Files": "Photo Files",  # Exact match
                "Category": "Category"  # Exact match - Category field
            }
            
            # Add fields only if they have valid data
            for extracted_field, airtable_field in field_mapping.items():
                value = data.get(extracted_field)
                if value and value not in ["Not found", "", None]:
                    fields[airtable_field] = value
            
            logger.info(f"Prepared fields for {fields.get('Product Name', 'Unknown Product')}")
            logger.info(f"Fields to be sent: {list(fields.keys())}")
            
            # Step 3: Push to Product Catalogue
            new_id = self.push_to_product_catalogue(fields)

            # Step 4: Link back to Items table
            if new_id:
                self.update_source_item(table, record_id, code)
            else:
                logger.warning(f"Failed to create catalogue entry for record {record_id}")
                
        except Exception as e:
            logger.error(f"Failed to process record {record_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

def main():
    scraper = AirtableProductScraper(
        airtable_api_key=os.getenv('AIRTABLE_API_KEY'),
        base_id=os.getenv('AIRTABLE_BASE_ID'),
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )

    # Process Items-Bid4more table
    table = "Items-Bid4more"
    formula = "AND({Status}='Entered', {4more-Product-Code}='')"
    encoded_formula = quote(formula)
    url = f"{scraper.base_url}/{table}?view=AI-Product-Fetch&filterByFormula={encoded_formula}"
    
    try:
        response = requests.get(url, headers=scraper.headers)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])
        
        if not isinstance(records, list):
            raise ValueError(f"Unexpected Airtable response: {data}")
            
        logger.info(f"Found {len(records)} records in {table} to process.")
        
        for rec in records:
            fields = rec.get("fields", {})
            product_url = fields.get("Product URL")
            record_id = rec.get("id")
            
            if product_url and record_id:
                scraper.process_and_sync_product(table, record_id, product_url)
            else:
                logger.warning(f"Skipping record {record_id} - missing Product URL")
                
    except Exception as e:
        logger.error(f"Error fetching records: {e}")
        raise

if __name__ == "__main__":
    main(), '').replace(',', ''))
                    if price_match:
                        try:
                            # Convert to float for currency field
                            price_float = float(price_match.group().replace(',', ''))
                            cleaned_record[key] = price_float
                        except ValueError:
                            logger.warning(f"Could not convert price to number: {value}")
                    else:
                        logger.warning(f"Could not extract price from: {value}")
            else:
                # Text fields - just clean whitespace
                if isinstance(value, str) and value.strip():
                    # Limit length to avoid Airtable limits
                    cleaned_value = value.strip()
                    if len(cleaned_value) > 50000:  # Conservative limit
                        cleaned_value = cleaned_value[:50000] + "..."
                    cleaned_record[key] = cleaned_value
                elif not isinstance(value, str):
                    cleaned_record[key] = str(value)
        
        # Ensure required fields are present
        required_fields = ["Product Name", "4more-Product-Code"]
        for req_field in required_fields:
            if req_field not in cleaned_record:
                logger.error(f"❌ Missing required field: {req_field}")
                return None
        
        # Log what we're sending for debugging
        logger.info(f"Attempting to add product: {cleaned_record.get('Product Name', 'Unknown')}")
        logger.debug(f"Cleaned payload fields: {list(cleaned_record.keys())}")
        
        try:
            payload = {"fields": cleaned_record}
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.status_code == 422:
                logger.error(f"❌ Airtable validation error (422). Response: {response.text}")
                logger.error(f"❌ Payload that failed:")
                for field_name, field_value in cleaned_record.items():
                    logger.error(f"   {field_name}: {type(field_value).__name__} = {field_value}")
                
                # Try with minimal fields only
                logger.info("🔄 Retrying with minimal required fields only...")
                minimal_record = {
                    "Product Name": cleaned_record.get("Product Name"),
                    "4more-Product-Code": cleaned_record.get("4more-Product-Code")
                }
                
                if "Product URL" in cleaned_record:
                    minimal_record["Product URL"] = cleaned_record["Product URL"]
                
                retry_response = requests.post(url, headers=self.headers, json={"fields": minimal_record})
                if retry_response.status_code in [200, 201]:
                    logger.info("✅ Success with minimal fields")
                    return retry_response.json().get("id")
                else:
                    logger.error(f"❌ Minimal retry also failed: {retry_response.text}")
                return None
                
            response.raise_for_status()
            product_name = cleaned_record.get('Product Name', 'Unknown Product')
            logger.info(f"✅ Added new product to Product Catalogue: {product_name}")
            return response.json().get("id")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to add product to catalogue: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"❌ Response details: {e.response.text}")
            return None

    def resolve_shortlink(self, url: str) -> str:
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            logger.info(f"Resolved URL: {response.url}")
            return response.url
        except Exception as e:
            logger.warning(f"Failed to resolve shortlink: {e}")
            return url

    def fetch_webpage_with_selenium(self, url: str) -> str:
        driver = None
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(5)
            content = driver.page_source
            logger.info(f"[Selenium] Fetched content from: {url}")
            return content
        except Exception as e:
            logger.error(f"[Selenium] Error: {e}")
            return ""
        finally:
            if driver:
                driver.quit()

    def extract_amazon_product_data(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        def get_text(selector, attr=None):
            try:
                element = soup.select_one(selector)
                if not element:
                    return 'Not found'
                if attr:
                    return element.get(attr, '').strip()
                return element.get_text(strip=True)
            except Exception:
                return 'Not found'

        data['Product Name'] = get_text('#productTitle')
        
        # Try multiple selectors for description
        description = get_text('#productDescription')
        if description == 'Not found':
            description = get_text('#feature-bullets')
        data['Description'] = description
        
        data['Retail Price (CAD)'] = get_text('.a-price .a-offscreen')
        data['Weight'] = get_text('#detailBullets_feature_div li:contains("Item weight") span:last-child')
        data['Dimension'] = get_text('#detailBullets_feature_div li:contains("Product dimensions") span:last-child')
        data['Brand'] = get_text('#bylineInfo')
        
        # Extract category from breadcrumbs or department info
        category = get_text('#wayfinding-breadcrumbs_feature_div a')
        if category == 'Not found':
            category = get_text('#nav-subnav a.nav-a')
        if category == 'Not found':
            # Try to get from department in product details
            category = get_text('#detailBullets_feature_div li:contains("Department") span:last-child')
        data['Category'] = category

        # Extract images
        images = []
        for img in soup.select('img[src]'):
            src = img.get('src', '')
            if 'media-amazon' in src and src not in images:
                images.append(src)
        data['Photo Files'] = images[:5] if images else []

        return data

    def extract_product_data_with_gemini(self, product_url: str, inspection_photo: str = None) -> Dict[str, Any]:
        product_url = self.resolve_shortlink(product_url)

        if "amazon." in product_url:
            logger.info("[Amazon Scraper] Using custom parser for Amazon domain...")
            html = self.fetch_webpage_with_selenium(product_url)
            return self.extract_amazon_product_data(html)

        html = self.fetch_webpage_with_selenium(product_url)
        if not html:
            logger.error("Failed to fetch webpage content")
            return {
                'Product Name': 'Extraction failed',
                'Description': 'Failed to fetch webpage',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found',
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Category': 'Not found',
                'Photo Files': []
            }

        prompt = f"""
Extract product information from the following HTML for {product_url}.
Return in JSON:
{{
  "Product Name": "...",
  "Description": "...",
  "Retail Price (CAD)": "...",
  "Weight": "...",
  "Dimension": "...",
  "Brand": "...",
  "Category": "...",
  "Photo Files": ["..."]
}}
Content:
{html[:8000]}
"""
        try:
            response = self.model.generate_content([prompt])
            response_text = response.text
            json_text = re.search(r'```(?:json)?(.*?)```', response_text, re.DOTALL)
            if json_text:
                return json.loads(json_text.group(1).strip())
            else:
                return json.loads(response_text.strip())
        except Exception as e:
            logger.error(f"Gemini extraction error: {e}")
            return {
                'Product Name': 'Extraction failed',
                'Description': 'Gemini failed',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found',
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Category': 'Not found',
                'Photo Files': []
            }

    def process_and_sync_product(self, table: str, record_id: str, product_url: str):
        """Process a single product record and sync to catalogue"""
        try:
            logger.info(f"🔄 Processing record {record_id} from {table}")
            data = self.extract_product_data_with_gemini(product_url)
            
            if not data or data.get("Product Name") in [None, "Extraction failed", "", "Not found"]:
                logger.warning(f"Skipping record {record_id} — no valid product name extracted.")
                return

            # Step 1: Fetch all codes and generate new one
            all_codes = self.fetch_existing_4more_codes()
            code = self.generate_4more_code(all_codes)

            # Step 2: Map extracted data to correct Airtable field names
            fields = {
                "Product URL": product_url,
                "4more-Product-Code": code
            }
            
            # Map the extracted data to the correct field names in your Airtable
            field_mapping = {
                "Product Name": "Product Name",  # Exact match
                "Description": "Description",    # Exact match  
                "Retail Price (CAD)": "Unit Retail Price",  # Your field is "Unit Retail Price"
                "Weight": "Weight",  # Exact match - this field exists
                "Dimension": "Dimensions",  # Your field is "Dimensions" (plural)
                "Brand": "Brand",  # Exact match
                "Photo Files": "Photo Files",  # Exact match
                "Category": "Category"  # Exact match - Category field
            }
            
            # Add fields only if they have valid data
            for extracted_field, airtable_field in field_mapping.items():
                value = data.get(extracted_field)
                if value and value not in ["Not found", "", None]:
                    fields[airtable_field] = value
            
            logger.info(f"Prepared fields for {fields.get('Product Name', 'Unknown Product')}")
            logger.info(f"Fields to be sent: {list(fields.keys())}")
            
            # Step 3: Push to Product Catalogue
            new_id = self.push_to_product_catalogue(fields)

            # Step 4: Link back to Items table
            if new_id:
                self.update_source_item(table, record_id, code)
            else:
                logger.warning(f"Failed to create catalogue entry for record {record_id}")
                
        except Exception as e:
            logger.error(f"Failed to process record {record_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

def main():
    scraper = AirtableProductScraper(
        airtable_api_key=os.getenv('AIRTABLE_API_KEY'),
        base_id=os.getenv('AIRTABLE_BASE_ID'),
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )

    # Process Items-Bid4more table
    table = "Items-Bid4more"
    formula = "AND({Status}='Entered', {4more-Product-Code}='')"
    encoded_formula = quote(formula)
    url = f"{scraper.base_url}/{table}?view=AI-Product-Fetch&filterByFormula={encoded_formula}"
    
    try:
        response = requests.get(url, headers=scraper.headers)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])
        
        if not isinstance(records, list):
            raise ValueError(f"Unexpected Airtable response: {data}")
            
        logger.info(f"Found {len(records)} records in {table} to process.")
        
        for rec in records:
            fields = rec.get("fields", {})
            product_url = fields.get("Product URL")
            record_id = rec.get("id")
            
            if product_url and record_id:
                scraper.process_and_sync_product(table, record_id, product_url)
            else:
                logger.warning(f"Skipping record {record_id} - missing Product URL")
                
    except Exception as e:
        logger.error(f"Error fetching records: {e}")
        raise

if __name__ == "__main__":
    main()