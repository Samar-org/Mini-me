import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random
import os
import logging
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# --- Basic Setup ---
# region Logging and Environment Configuration
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

class WindowsCompatibleFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        replacements = {'✓': '[OK]', '✅': '[SUCCESS]', '❌': '[ERROR]', '⚠️': '[WARNING]', '🚀': '[START]'}
        for unicode_char, ascii_replacement in replacements.items():
            msg = msg.replace(unicode_char, ascii_replacement)
        return msg

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_formatter = WindowsCompatibleFormatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler('scraper.log', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("✓ .env file loaded successfully")
except ImportError:
    logger.warning("⚠️ python-dotenv not installed.")

try:
    from pyairtable import Api
    logger.info("✓ pyairtable library loaded successfully")
except ImportError:
    logger.error("❌ pyairtable library not found. Please install it: pip install pyairtable")
    Api = None
# endregion

# --- Airtable Configuration ---
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY', "YOUR_AIRTABLE_API_KEY_HERE")
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID', "YOUR_AIRTABLE_BASE_ID_HERE")
AIRTABLE_TABLE_NAME = "Items-Bid4more"
AIRTABLE_VIEW_NAME = "Fetch-Program-View"
FIELD_MAPPINGS = {
    'url': "Product URL", 'product_code': "4more-Product-Code", 'name': "Product Name",
    'description': "Description", 'sale_price': "Sale Price", 'original_price': "Original Price",
    'currency': "Currency", 'photos': "Photos", 'photo_files': "Photo Files",
    'dimensions': "Dimensions", 'weight': "Weight", 'source': "Scraping Website",
    'status': "Status", 'scraping_status': "Scraping Status", 'asin': "ASIN"
}

# --- Data Structures ---
# region Data Classes and Validation
@dataclass
class ProductData:
    url: str; source: str = "Unknown"; asin: Optional[str] = None; name: str = "Not Found"
    description: str = "Not Found"; images: List[str] = field(default_factory=list)
    sale_price: str = "Not Found"; currency: str = "USD"; status: str = "Failed"
    missing_fields: List[str] = field(default_factory=list); error_details: Optional[str] = None

class DataValidator:
    @staticmethod
    def validate_name(name: str) -> bool: return name not in ["Not Found", ""]
    @staticmethod
    def validate_images(images: List[str]) -> bool: return images and all(img.startswith('http') for img in images)
    @staticmethod
    def validate_price(price: str) -> bool:
        if price in ['Not Found', 'N/A', '']: return False
        try: return 0.01 <= float(price)
        except (ValueError, TypeError): return False

    @classmethod
    def validate_product(cls, product: ProductData) -> ProductData:
        # This method is called after a scrape attempt. It should always re-evaluate the status.
        if product.status == "Failed": # If a hard error occurred (e.g., API failure), don't change status.
            return product

        missing = []
        if not cls.validate_name(product.name): missing.append('name')
        if not cls.validate_images(product.images): missing.append('images')
        
        product.missing_fields = missing
        product.status = 'Needs Attention' if missing else 'Scraped'
        return product
# endregion

# --- Scrapers ---
# region BaseScraper and Factory
class BaseScraper:
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(total=3, status_forcelist=[429, 500, 503, 504], backoff_factor=2)
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'})

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'lxml')
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    def scrape(self, url: str) -> ProductData:
        raise NotImplementedError

class TargetScraper(BaseScraper):
    API_KEY = "eb2551e4accc14f38cc42d32fbc2b2ea"
    STORE_ID = "3991"

    def _find_image_urls(self, data, found_urls):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and value.startswith("https://target.scene7.com/is/image/"):
                    clean_url = value.split('?')[0]
                    found_urls.add(f"{clean_url}?wid=1000&hei=1000&qlt=80")
                elif isinstance(value, (dict, list)):
                    self._find_image_urls(value, found_urls)
        elif isinstance(data, list):
            for item in data:
                self._find_image_urls(item, found_urls)

    def scrape(self, url: str) -> ProductData:
        logger.info(f"🚀 Starting Target API scrape for: {url}")
        match = re.search(r'A-(\d+)', url)
        if not match: return ProductData(url=url, source="Target", error_details="Could not extract TCIN from URL")
        tcin = match.group(1)

        api_url = "https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1"
        params = {"key": self.API_KEY, "tcin": tcin, "store_id": self.STORE_ID, "pricing_store_id": self.STORE_ID}
        
        try:
            response = self.session.get(api_url, params=params, timeout=20)
            response.raise_for_status()
            api_data = response.json().get('data', {}).get('product')
            if not api_data: return ProductData(url=url, source="Target", error_details="Product data not in API response")

            product = ProductData(url=url, source="Target", asin=tcin, status="Processing") # Initial success status
            item = api_data.get('item', {})
            desc = item.get('product_description', {})
            
            product.name = desc.get('title', 'Not Found')
            
            bullets = desc.get('soft_bullets', {}).get('bullets', [])
            if bullets: product.description = ". ".join(bullets).replace('•', '').strip()
            
            price_info = api_data.get('price', {})
            if price_info.get('current_retail'):
                product.sale_price = str(price_info['current_retail'])
            product.currency = price_info.get('currency_code', 'USD')
            
            image_urls = set()
            self._find_image_urls(item, image_urls)
            product.images = list(image_urls)
            
            return DataValidator.validate_product(product)
        except requests.RequestException as e:
            return ProductData(url=url, source="Target", error_details=f"API request failed: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Target scrape: {e}", exc_info=True)
            return ProductData(url=url, source="Target", error_details=f"Failed to parse API response: {e}")

class AmazonScraper(BaseScraper):
    def scrape(self, url: str) -> ProductData:
        logger.info(f"🚀 Starting Amazon scrape for: {url}")
        soup = self.get_page_content(url)
        if not soup: return ProductData(url=url, source="Amazon", error_details="Failed to fetch page")
        
        product = ProductData(url=url, source="Amazon", status="Processing") # Initial success
        product.name = soup.select_one('span#productTitle').get_text(strip=True) if soup.select_one('span#productTitle') else "Not Found"
        # Add other Amazon-specific selectors for images, etc. here
        return DataValidator.validate_product(product)

def scraper_factory(url: str) -> Optional[BaseScraper]:
    if "target.com" in url: return TargetScraper()
    if "amazon." in url or "amzn.to" in url: return AmazonScraper()
    return None
# endregion

# --- Main Execution ---
def process_airtable_records():
    logger.info("=== 🚀 Starting Scraper for Airtable ===")
    if not Api or not AIRTABLE_API_KEY.startswith('pat'):
        logger.error("❌ Airtable API is not configured. Check your .env file.")
        return

    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    try:
        records = table.all(formula=f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')")
        if not records:
            logger.info("✅ No new records to process.")
            return
        logger.info(f"Found {len(records)} records to process.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch records from Airtable: {e}")
        return

    for i, record in enumerate(records, 1):
        record_id, url = record['id'], record['fields'].get(FIELD_MAPPINGS['url'])
        logger.info(f"\n--- Processing Record {i}/{len(records)} (ID: {record_id}) ---")
        
        scraper = scraper_factory(url) if url else None
        if not scraper:
            status, s_status = "Needs Attention", "Skipped (No URL or Unsupported)"
            table.update(record_id, {FIELD_MAPPINGS['status']: status, FIELD_MAPPINGS['scraping_status']: s_status})
            continue
        
        product_data = scraper.scrape(url.strip())
        
        s_status_detail = ""
        if product_data.error_details:
            s_status_detail = f": {product_data.error_details}"
        elif product_data.missing_fields:
            s_status_detail = f": Missing {', '.join(product_data.missing_fields)}"
        
        fields = {
            'Status': 'Scraped' if product_data.status == 'Scraped' else 'Needs Attention',
            'Scraping Status': f"{product_data.status}{s_status_detail}",
            'Product Name': product_data.name, 'Description': product_data.description,
            'ASIN': product_data.asin, 'Currency': product_data.currency,
            'Scraping Website': product_data.source, 'Photos': ", ".join(product_data.images),
            'Sale Price': float(product_data.sale_price) if DataValidator.validate_price(product_data.sale_price) else None,
            'Photo Files': [{'url': img} for img in product_data.images] if product_data.images else None
        }

        try:
            payload = {k: v for k, v in fields.items() if v is not None and v != ""}
            table.update(record_id, payload)
            logger.info(f"✅ Record {record_id} updated successfully.")
        except Exception as e:
            logger.error(f"❌ Airtable update failed for record {record_id}: {e}")
            table.update(record_id, {'Status': "Needs Attention", 'Scraping Status': "Airtable Update Error"})
        
        if i < len(records): time.sleep(random.uniform(2, 5))

if __name__ == "__main__":
    process_airtable_records()