
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
        except Exception as e:
            logger.warning(f"Could not fetch existing codes: {e}")
        return codes

    def update_source_item(self, table: str, record_id: str, product_code: str):
        url = f"{self.base_url}/{table}/{record_id}"
        payload = {{
            "fields": {{
                "4more-Product-Code": product_code,
                "Status": "Linked to Catalogue"
            }}
        }}
        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            logger.info(f"🔗 Linked item {record_id} with code {product_code}")
        except Exception as e:
            logger.warning(f"❌ Failed to update source item {record_id}: {e}")


    def push_to_product_catalogue(self, record: Dict[str, Any]) -> Optional[str]:
        url = f"{self.base_url}/Product%20Catalogue"
        try:
            response = requests.post(url, headers=self.headers, json={"fields": record})
            response.raise_for_status()
            logger.info(f"✅ Added new product to Product Catalogue: {record.get('Product Name')}")
            return response.json().get("id")
        except Exception as e:
            logger.error(f"❌ Failed to add product to catalogue: {e}")
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
        try:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            driver = webdriver.Chrome(options=options)
            driver.get(url)
            time.sleep(5)
            content = driver.page_source
            driver.quit()
            logger.info(f"[Selenium] Fetched content from: {url}")
            return content
        except Exception as e:
            logger.error(f"[Selenium] Error: {e}")
            return ""

    def extract_amazon_product_data(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        def get_text(selector, attr=None):
            try:
                if attr:
                    return soup.select_one(selector)[attr].strip()
                return soup.select_one(selector).get_text(strip=True)
            except:
                return 'Not found'

        data['Product Name'] = get_text('#productTitle')
        data['Description'] = get_text('#productDescription') or get_text('#feature-bullets')
        data['Retail Price (CAD)'] = get_text('.a-price .a-offscreen')
        data['Weight'] = get_text('#detailBullets_feature_div li:contains("Item weight") span:last-child')
        data['Dimension'] = get_text('#detailBullets_feature_div li:contains("Product dimensions") span:last-child')
        data['Brand'] = get_text('#bylineInfo')

        images = []
        for img in soup.select('img[src]'):
            src = img['src']
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
        prompt = f"""
Extract product information from the following HTML for {product_url}.
Return in JSON:
{{
  \"Product Name\": \"...\",
  \"Description\": \"...\",
  \"Retail Price (CAD)\": \"...\",
  \"Weight\": \"...\",
  \"Dimension\": \"...\",
  \"Brand\": \"...\",
  \"Photo Files\": [\"...\"]
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
                'Photo Files': []
            }

def main():
    scraper = AirtableProductScraper(
        airtable_api_key=os.getenv('AIRTABLE_API_KEY'),
        base_id=os.getenv('AIRTABLE_BASE_ID'),
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )
    result = scraper.extract_product_data_with_gemini("https://a.co/d/8ky3Hcb")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()


def process_and_sync_product(self, table: str, record_id: str, product_url: str):
    try:
        data = self.extract_product_data_with_gemini(product_url)
        if not data or data.get("Product Name") in [None, "Extraction failed"]:
            logger.warning(f"Skipping record {record_id} — no product name extracted.")
            return

        # Step 1: Fetch all codes and generate new one
        all_codes = self.fetch_existing_4more_codes()
        code = self.generate_4more_code(all_codes)

        # Step 2: Push to Product Catalogue
        fields = {{
            "Product Name": data.get("Product Name"),
            "Description": data.get("Description"),
            "Retail Price (CAD)": data.get("Retail Price (CAD)"),
            "Weight": data.get("Weight"),
            "Dimension": data.get("Dimension"),
            "Brand": data.get("Brand"),
            "Photo Files": data.get("Photo Files"),
            "Product URL": product_url,
            "4more-Product-Code": code
        }}
        new_id = self.push_to_product_catalogue(dict(fields))

        # Step 3: Link back to Items table
        if new_id:
            self.update_source_item(table, record_id, code)
    except Exception as e:
        logger.error(f"Failed to process record {record_id}: {e}")

def main():
    scraper = AirtableProductScraper(
        airtable_api_key=os.getenv('AIRTABLE_API_KEY'),
        base_id=os.getenv('AIRTABLE_BASE_ID'),
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )

    table = "Items-Bid4more"
    formula = "AND({Status}='Entered', {4more-Product-Code}='')"
    encoded_formula = quote(formula)
    url = f"{scraper.base_url}/{table}?view=AI-Product-Fetch&filterByFormula={encoded_formula}"
    try:
        records = requests.get(url, headers=scraper.headers).json().get("records", [])
        logger.info(f"Found {len(records)} records in {table} to process.")
        for rec in records:
            product_url = rec.get("fields", {{}}).get("Product URL")
            record_id = rec.get("id")
            if product_url:
                logger.info(f"🔄 Processing record {record_id}...")
                scraper.process_and_sync_product(table, record_id, product_url)
    except Exception as e:
        logger.error(f"Error fetching records: {e}")

if __name__ == "__main__":
    main()
