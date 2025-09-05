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

    def validate_product_completeness(self, fields: Dict[str, Any]) -> str:
        """
        Validate if all required fields are present and return appropriate Product Status.
        Returns "Code Generated" if complete, "Needs Attention" if missing required data.
        """
        required_fields = {
            "Product Name": fields.get("Product Name"),
            "Description": fields.get("Description"),
            "Unit Retail Price": fields.get("Unit Retail Price"),
            "Photo Files": fields.get("Photo Files")
        }

        missing_fields = []

        if not required_fields["Product Name"] or required_fields["Product Name"] in ["Not found", "", "Extraction failed"]:
            missing_fields.append("Product Name")

        if not required_fields["Description"] or required_fields["Description"] in ["Not found", "", "Gemini failed", "Failed to fetch webpage"]:
            missing_fields.append("Description")

        price = required_fields["Unit Retail Price"]
        if not price or (isinstance(price, str) and price in ["Not found", ""]) or (isinstance(price, (int, float)) and price <= 0):
            missing_fields.append("Unit Retail Price")

        photos = required_fields["Photo Files"]
        if not photos or not isinstance(photos, list) or len(photos) == 0:
            missing_fields.append("Photo Files")

        if missing_fields:
            logger.warning(f"⚠️ Product missing required fields: {', '.join(missing_fields)} - Setting status to 'Needs Attention'")
            return "Needs Attention"
        else:
            logger.info("✅ Product has all required fields - Setting status to 'Code Generated'")
            return "Code Generated"

    def get_category_record_id(self, category_name: str) -> Optional[str]:
        """Look up category record ID by name from the Categories table."""
        if not category_name or category_name == "Not found":
            return None

        url = f"{self.base_url}/Categories"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            records = response.json().get("records", [])

            category_name_lower = category_name.lower().strip()
            for record in records:
                fields = record.get("fields", {})
                record_category = fields.get("Category", "").lower().strip()
                if record_category == category_name_lower:
                    logger.info(f"Found category match: '{category_name}' -> {record.get('id')}")
                    return record.get("id")

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

    def generate_4more_code(self, all_codes: List[str]) -> str:
        """Generates the next sequential product code."""
        prefix = "O1-"
        existing_numbers = [int(code.split("-")[1]) for code in all_codes if code.startswith(prefix) and code.split("-")[1].isdigit()]
        next_number = max(existing_numbers + [0]) + 1
        return f"{prefix}{next_number:05d}"

    def fetch_existing_4more_codes(self) -> List[str]:
        """Fetches all existing product codes from the Product Catalogue."""
        url = f"{self.base_url}/Product%20Catalogue?fields%5B%5D=4more-Product-Code"
        codes = []
        offset = None
        while True:
            try:
                paginated_url = f"{url}&offset={offset}" if offset else url
                response = requests.get(paginated_url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                records = data.get("records", [])
                for rec in records:
                    code = rec.get("fields", {}).get("4more-Product-Code")
                    if code:
                        codes.append(code)
                offset = data.get('offset')
                if not offset:
                    break
            except Exception as e:
                logger.warning(f"Could not fetch existing codes: {e}")
                break
        logger.info(f"Fetched {len(codes)} existing product codes.")
        return codes

    def update_source_item(self, table: str, record_id: str, catalogue_record_id: str):
        """Updates the source record in Items-Bid4more to link to the new catalogue entry."""
        url = f"{self.base_url}/{table}/{record_id}"
        payload = {
            "fields": {
                "4more-Product-Code": [catalogue_record_id],
                "Status": "Linked to Catalogue"
            }
        }
        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"✅ Successfully updated source item {record_id} to link to catalogue record {catalogue_record_id}")
        except Exception as e:
            logger.error(f"❌ Failed to update source item {record_id}: {e}")
            if hasattr(e, 'response'):
                logger.error(f"Error details: {e.response.text}")


    def push_to_product_catalogue(self, record: Dict[str, Any]) -> Optional[str]:
        """Pushes a new product record to the Product Catalogue table."""
        url = f"{self.base_url}/Product%20Catalogue"
        cleaned_record = {}
        valid_fields = {
            "Product Name": "singleLineText", "Product URL": "url", "Brand": "singleLineText",
            "Category": "multipleRecordLinks", "Description": "multilineText", "Photo Files": "multipleAttachments",
            "Unit Retail Price": "currency", "4more-Product-Code": "singleLineText", "Dimensions": "singleLineText",
            "Weight": "singleLineText", "Color": "singleLineText", "Model": "singleLineText",
            "Status": "singleSelect"
        }

        for key, value in record.items():
            if key not in valid_fields:
                logger.warning(f"Skipping invalid field: {key}")
                continue

            if value is None or value in ['', 'Not found']:
                continue

            field_type = valid_fields.get(key)
            if field_type == "multipleAttachments":
                if isinstance(value, list) and value:
                    attachments = [{"url": img_url} for img_url in value if isinstance(img_url, str) and img_url.startswith('http')]
                    if attachments:
                        cleaned_record[key] = attachments
            elif field_type == "multipleRecordLinks":
                category_id = self.get_category_record_id(value)
                if category_id:
                    cleaned_record[key] = [category_id]
            elif field_type == "currency":
                if isinstance(value, str):
                    price_match = re.search(r'[\d,]+\.?\d*', value.replace('$', '').replace(',', ''))
                    if price_match:
                        try:
                            cleaned_record[key] = float(price_match.group().replace(',', ''))
                        except ValueError:
                            logger.warning(f"Could not convert price '{value}' to a number.")
                elif isinstance(value, (int, float)):
                    cleaned_record[key] = value
            else: # singleLineText, url, singleSelect
                cleaned_record[key] = str(value).strip()

        if "Product Name" not in cleaned_record or "4more-Product-Code" not in cleaned_record:
            logger.error(f"❌ Aborting push. Missing required fields: Product Name or 4more-Product-Code.")
            return None

        logger.info(f"📦 Attempting to add product: {cleaned_record.get('Product Name')}")
        try:
            payload = {"fields": cleaned_record}
            response = requests.post(url, headers=self.headers, json=payload)

            if response.status_code == 422:
                logger.error(f"❌ Airtable validation error (422). Response: {response.text}")
                logger.error(f"❌ Payload that failed: {json.dumps(payload, indent=2)}")
                return None

            response.raise_for_status()
            new_record_id = response.json().get("id")
            logger.info(f"🎉 Successfully added product to Product Catalogue with ID: {new_record_id}")
            return new_record_id

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to add product to catalogue: {e}")
            if hasattr(e, 'response'):
                logger.error(f"❌ Response details: {e.response.text}")
            return None

    def fetch_webpage_with_selenium(self, url: str) -> str:
        """Fetches webpage content using Selenium to handle dynamic JavaScript."""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = None
        try:
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(5)  # Wait for page to load
            content = driver.page_source
            logger.info(f"Successfully fetched content from: {url}")
            return content
        except Exception as e:
            logger.error(f"Selenium failed to fetch {url}. Error: {e}")
            return ""
        finally:
            if driver:
                driver.quit()

    def extract_product_data_with_gemini(self, product_url: str) -> Dict[str, Any]:
        """Extracts product data from a URL using Gemini or a custom parser for Amazon."""
        html = self.fetch_webpage_with_selenium(product_url)
        if not html:
            return {}

        if "amazon." in product_url:
            logger.info("Using custom parser for Amazon domain...")
            return self.extract_amazon_product_data(html)

        prompt = f"""
        Extract product information from the following HTML. Return only a single JSON object.
        The desired fields are: "Product Name", "Description", "Retail Price (CAD)", "Weight", "Dimension", "Brand", "Category", "Photo Files" (as a list of strings).
        HTML Content:
        {html[:12000]}
        """
        try:
            response = self.model.generate_content(prompt)
            json_text = re.search(r'```(?:json)?(.*?```)', response.text, re.DOTALL).group(1).replace('```', '')
            return json.loads(json_text)
        except Exception as e:
            logger.error(f"Gemini extraction failed: {e}")
            return {}

    def extract_amazon_product_data(self, html: str) -> Dict[str, Any]:
        """Custom parser for Amazon product pages."""
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        get_text = lambda selector: soup.select_one(selector).get_text(strip=True) if soup.select_one(selector) else "Not found"

        data['Product Name'] = get_text('#productTitle')
        data['Description'] = get_text('#feature-bullets') or get_text('#productDescription')
        data['Retail Price (CAD)'] = get_text('.a-price .a-offscreen')
        data['Brand'] = get_text('#bylineInfo')
        data['Category'] = get_text('#wayfinding-breadcrumbs_feature_div a')

        images = [img.get('src') for img in soup.select('#altImages .a-button-inner img') if img.get('src') and not img.get('src').endswith('.gif')]
        data['Photo Files'] = list(dict.fromkeys(images))[:5] # Get unique images

        return data

    def process_and_sync_product(self, table: str, record: Dict[str, Any]):
        """Main processing function for a single record."""
        record_id = record.get("id")
        source_fields = record.get("fields", {})
        product_url = source_fields.get("Product URL")

        try:
            logger.info(f"🔄 Processing record {record_id} from {table}")
            data = self.extract_product_data_with_gemini(product_url)
            if not data or not data.get("Product Name"):
                logger.warning(f"Skipping {record_id} — no valid product name extracted.")
                return

            all_codes = self.fetch_existing_4more_codes()
            new_code = self.generate_4more_code(all_codes)

            fields_to_push = {
                "Product URL": product_url,
                "4more-Product-Code": new_code,
                "Product Name": data.get("Product Name"),
                "Description": data.get("Description"),
                "Unit Retail Price": data.get("Retail Price (CAD)"),
                "Weight": data.get("Weight"),
                "Dimensions": data.get("Dimension"), # Note: mapping 'Dimension' from scrape to 'Dimensions'
                "Brand": data.get("Brand"),
                "Category": data.get("Category"),
                "Photo Files": data.get("Photo Files"),
                "Color": source_fields.get("Color"), # From source table
                "Model": source_fields.get("Model"), # From source table
            }

            # Set status based on completeness
            status = self.validate_product_completeness(fields_to_push)
            fields_to_push["Status"] = status

            new_catalogue_id = self.push_to_product_catalogue(fields_to_push)

            if new_catalogue_id:
                self.update_source_item(table, record_id, new_catalogue_id)
            else:
                logger.warning(f"Failed to create catalogue entry for source record {record_id}")

        except Exception as e:
            logger.error(f"Failed to process record {record_id}: {e}", exc_info=True)

def main():
    scraper = AirtableProductScraper(
        airtable_api_key=os.getenv('AIRTABLE_API_KEY'),
        base_id=os.getenv('AIRTABLE_BASE_ID'),
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )

    table = "Items-Bid4more"
    formula = "AND({Status}='Entered', {4more-Product-Code}='')"
    url = f"{scraper.base_url}/{table}?view=AI-Product-Fetch&filterByFormula={quote(formula)}"

    try:
        response = requests.get(url, headers=scraper.headers)
        response.raise_for_status()
        records = response.json().get("records", [])
        logger.info(f"Found {len(records)} records in '{table}' to process.")

        for record in records:
            if record.get("fields", {}).get("Product URL"):
                scraper.process_and_sync_product(table, record)
            else:
                logger.warning(f"Skipping record {record.get('id')} - missing Product URL.")

    except Exception as e:
        logger.error(f"Error fetching records from Airtable: {e}", exc_info=True)


if __name__ == "__main__":
    main()