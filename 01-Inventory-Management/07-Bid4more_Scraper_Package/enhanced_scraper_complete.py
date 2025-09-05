import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import re
import json
import time
import random
from PIL import Image
from io import BytesIO
import os
import logging
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
import hashlib

# --- Basic Setup ---
# region Logging and Environment Configuration
# Fix Windows console encoding issues for logging
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Enhanced logging setup
class WindowsCompatibleFormatter(logging.Formatter):
    """Custom formatter that handles Unicode characters on Windows."""
    def format(self, record):
        msg = super().format(record)
        replacements = {
            '✓': '[OK]', '✅': '[SUCCESS]', '❌': '[ERROR]', '⚠️': '[WARNING]',
            '🚀': '[START]', '✨': '[FINISH]', '📊': '[STATS]', '🔍': '[DEBUG]', '⏳': '[WAIT]'
        }
        for unicode_char, ascii_replacement in replacements.items():
            msg = msg.replace(unicode_char, ascii_replacement)
        return msg

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = WindowsCompatibleFormatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler('logs/enhanced_scraper.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

# Add dotenv support
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("✓ .env file loaded successfully")
except ImportError:
    logger.warning("⚠️ python-dotenv not installed. Using environment variables only.")

# Airtable library check
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

# --- Data Structures and Validators ---
# region Data Classes and Validation
@dataclass
class ProductData:
    """Data class for structured product information."""
    url: str
    source: str = "Unknown"
    asin: Optional[str] = None
    name: str = "Product Name Not Found"
    description: str = "Product Description Not Found"
    images: List[str] = field(default_factory=list)
    sale_price: str = "Not Found"
    original_price: str = "N/A"
    currency: str = "CAD"
    dimensions: str = "Product Dimensions Not Found"
    weight: str = "Product Weight Not Found"
    status: str = "Failed"
    missing_fields: List[str] = field(default_factory=list)
    error_details: Optional[str] = None

class DataValidator:
    """A pipeline for validating scraped product data."""
    @staticmethod
    def validate_name(name: str) -> bool: 
        return name and name != "Product Name Not Found" and len(name.strip()) >= 5
    
    @staticmethod
    def validate_images(images: List[str]) -> bool: 
        return images and all(isinstance(img, str) and img.startswith('http') for img in images)
    
    @staticmethod
    def validate_price(price: str) -> bool:
        if price in ['Not Found', 'N/A', '']: return False
        try: return 0.01 <= float(price.replace(',', '')) <= 50000
        except (ValueError, TypeError): return False

    @classmethod
    def validate_product(cls, product: ProductData) -> ProductData:
        missing = []
        if not cls.validate_name(product.name): missing.append('name')
        if not cls.validate_images(product.images): missing.append('images')
        
        product.missing_fields = missing
        if missing:
            product.status = 'Needs Attention'
            logger.warning(f"Product {product.asin or product.url}: Missing fields - {', '.join(missing)}")
        else:
            product.status = 'Scraped'
            logger.info(f"Product {product.asin or product.url}: All critical fields validated")
        return product
# endregion

# --- Base Scraper and Factory ---
# region Base Scraper Class
class BaseScraper:
    """A base class for website scrapers with common functionality."""
    def __init__(self):
        self.stats = {'total_requests': 0, 'successful_scrapes': 0, 'failed_scrapes': 0, 'blocked_requests': 0, 'retry_attempts': 0, 'skipped_records': 0}
        self.session = self._create_enhanced_session()
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
    
    def _create_enhanced_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        self.stats['total_requests'] += 1
        try:
            time.sleep(random.uniform(2, 5))
            response = self.session.get(url, headers=self.get_headers(), timeout=30, allow_redirects=True)
            response.raise_for_status()
            logger.info(f"Successfully fetched {response.status_code}: {response.url}")
            return BeautifulSoup(response.content, 'lxml')
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            self.stats['failed_scrapes'] += 1
        return None

    def scrape(self, url: str) -> ProductData:
        """Main method to be implemented by subclasses."""
        raise NotImplementedError("Each scraper must implement the 'scrape' method.")

    def get_stats(self) -> dict:
        return self.stats
# endregion

# --- Website-Specific Scrapers ---
# region AmazonScraper
class AmazonScraper(BaseScraper):
    """Scrapes product data from Amazon pages."""
    
    def scrape(self, url: str) -> ProductData:
        logger.info(f"🚀 Starting Amazon scrape for: {url}")
        
        # Resolve short URLs like amzn.to and a.co
        if any(domain in url for domain in ['amzn.to', 'a.co']):
            try:
                response = self.session.head(url, allow_redirects=True, timeout=15, headers=self.get_headers())
                url = response.url
                logger.info(f"Resolved Amazon short URL to: {url}")
            except requests.RequestException as e:
                return ProductData(url=url, source="Amazon", status="Failed", error_details=f"URL resolution failed: {e}")
        
        soup = self.get_page_content(url)
        if not soup:
            return ProductData(url=url, source="Amazon", status="Failed", error_details="Failed to fetch page content")

        if "captcha" in soup.get_text().lower() or "robot" in soup.get_text().lower():
            return ProductData(url=url, source="Amazon", status="Blocked", error_details="Blocked by Amazon (CAPTCHA/Robot Check)")

        try:
            product = ProductData(url=url, source="Amazon")
            
            # Extract ASIN first
            product.asin = self._extract_asin(url, soup)
            
            # Extract product name with multiple fallback strategies
            product.name = self._extract_name(soup)
            
            # Extract description
            product.description = self._extract_description(soup)
            
            # Extract images with multiple strategies
            product.images = self._extract_images(soup)
            
            # Extract price and currency
            product.sale_price, product.currency = self._extract_price(soup)
            
            # Extract product details
            details = self._extract_details(soup)
            product.dimensions = details.get('dimensions', 'Product Dimensions Not Found')
            product.weight = details.get('weight', 'Product Weight Not Found')
            
            # Validate and set status
            product = DataValidator.validate_product(product)
            self.stats['successful_scrapes'] += 1
            
            logger.info(f"✅ Amazon scrape completed for ASIN: {product.asin}")
            return product
            
        except Exception as e:
            logger.error(f"❌ Amazon extraction error for {url}: {e}", exc_info=True)
            return ProductData(url=url, source="Amazon", status="Failed", error_details=f"Extraction error: {e}")

    def _extract_asin(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Extract ASIN from URL or page content."""
        # Try URL first
        match = re.search(r'/dp/([A-Z0-9]{10})', url)
        if match:
            return match.group(1)
        
        # Try hidden input
        asin_input = soup.find('input', {'id': 'ASIN'})
        if asin_input and asin_input.get('value'):
            return asin_input['value']
        
        # Try data attributes
        asin_element = soup.find(attrs={'data-asin': True})
        if asin_element:
            return asin_element['data-asin']
        
        return None

    def _extract_name(self, soup: BeautifulSoup) -> str:
        """Extract product name with multiple fallback strategies."""
        # Primary selector
        name_element = soup.select_one('span#productTitle')
        if name_element:
            name = name_element.get_text(strip=True)
            if name and len(name) > 5:
                return name
        
        # Fallback selectors
        fallback_selectors = [
            'h1.a-size-large.a-spacing-none',
            'h1#title',
            'h1.product-title',
            '.product-title',
            'h1',
            '[data-automation-id="product-title"]',
            '.x-item-title-label'
        ]
        
        for selector in fallback_selectors:
            element = soup.select_one(selector)
            if element:
                name = element.get_text(strip=True)
                if name and len(name) > 5:
                    logger.info(f"Found product name using fallback selector: {selector}")
                    return name
        
        # Try JSON-LD data
        json_ld = self._get_json_ld(soup)
        if json_ld and json_ld.get('name'):
            return json_ld['name']
        
        # Try meta tags
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            return meta_title['content']
        
        return "Product Name Not Found"

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract product description."""
        # Try JSON-LD first
        json_ld = self._get_json_ld(soup)
        if json_ld and json_ld.get('description'):
            return json_ld['description']
        
        # Try feature bullets
        bullets = soup.select('#feature-bullets ul li span.a-list-item')
        if bullets:
            description_parts = []
            for bullet in bullets:
                text = bullet.get_text(strip=True)
                if text and not text.startswith('Make sure') and len(text) > 10:
                    description_parts.append(text)
            if description_parts:
                return ". ".join(description_parts[:5])  # Limit to first 5 bullets
        
        # Try other description areas
        desc_selectors = [
            '#productDescription p',
            '#aplus_feature_div',
            '.a-expander-content',
            '[data-feature-name="productDescription"]'
        ]
        
        for selector in desc_selectors:
            elements = soup.select(selector)
            if elements:
                descriptions = []
                for elem in elements[:3]:  # Limit to first 3 elements
                    text = elem.get_text(strip=True)
                    if text and len(text) > 20:
                        descriptions.append(text)
                if descriptions:
                    return " ".join(descriptions)
        
        return "Product Description Not Found"

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract product images with multiple strategies."""
        image_urls = set()
        
        # Strategy 1: JSON-LD data
        json_ld = self._get_json_ld(soup)
        if json_ld and 'image' in json_ld:
            images = json_ld['image']
            if isinstance(images, str):
                image_urls.add(images)
            elif isinstance(images, list):
                image_urls.update(images)
        
        # Strategy 2: Dynamic image data
        img_container = soup.select_one('#imgTagWrapperId')
        if img_container and img_container.get('data-a-dynamic-image'):
            try:
                dynamic_images = json.loads(img_container['data-a-dynamic-image'])
                image_urls.update(dynamic_images.keys())
            except json.JSONDecodeError:
                pass
        
        # Strategy 3: Main product image
        main_img = soup.select_one('#landingImage')
        if main_img and main_img.get('src'):
            image_urls.add(main_img['src'])
        
        # Strategy 4: Alternative image containers
        img_selectors = [
            '.a-dynamic-image',
            '#imgBlkFront',
            '.ImageBlockImage',
            'img[data-old-hires]',
            'img[data-a-image-source]'
        ]
        
        for selector in img_selectors:
            images = soup.select(selector)
            for img in images:
                # Try different attributes
                for attr in ['data-old-hires', 'data-a-image-source', 'src', 'data-src']:
                    img_url = img.get(attr)
                    if img_url and img_url.startswith('http'):
                        # Clean up the URL (remove size parameters for higher quality)
                        clean_url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', img_url)
                        image_urls.add(clean_url)
        
        # Strategy 5: Thumbnail images (as fallback)
        if not image_urls:
            thumb_images = soup.select('.a-button-thumbnail img')
            for img in thumb_images:
                img_url = img.get('src')
                if img_url:
                    # Convert thumbnail to full size
                    full_url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', img_url)
                    image_urls.add(full_url)
        
        # Filter and clean URLs
        cleaned_urls = []
        for url in image_urls:
            if url and url.startswith('http') and not url.endswith('.gif'):
                # Remove common unwanted parameters
                cleaned_url = re.sub(r'(\?|&)(psc|ref|tag)=[^&]*', '', url)
                cleaned_urls.append(cleaned_url)
        
        return list(set(cleaned_urls))[:10]  # Limit to 10 images

    def _extract_price(self, soup: BeautifulSoup) -> tuple:
        """Extract price and currency."""
        # Try JSON-LD first
        json_ld = self._get_json_ld(soup)
        if json_ld and 'offers' in json_ld:
            offers = json_ld['offers']
            if isinstance(offers, list):
                offers = offers[0]
            price = offers.get('price')
            currency = offers.get('priceCurrency')
            if price and currency:
                return str(price), currency
        
        # Price selectors in order of preference
        price_selectors = [
            '.a-price[data-a-color="price"] .a-offscreen',
            '.a-price.a-text-price.a-size-medium.apexPriceToPay .a-offscreen',
            '.a-price-current .a-offscreen',
            '.a-price .a-offscreen',
            '.a-price-deal .a-offscreen',
            'span.a-price-symbol + span.a-price-whole',
            '.price .a-price-whole'
        ]
        
        for selector in price_selectors:
            price_el = soup.select_one(selector)
            if price_el:
                price_text = price_el.get_text(strip=True)
                if price_text:
                    # Extract numeric value
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        price_val = price_match.group(0)
                        
                        # Determine currency
                        currency = 'USD'  # Default
                        canonical_link = soup.find('link', rel='canonical', href=True)
                        if canonical_link and '.ca/' in canonical_link.get('href', ''):
                            currency = 'CAD'
                        elif price_text.startswith('C$'):
                            currency = 'CAD'
                        elif price_text.startswith('£'):
                            currency = 'GBP'
                        elif price_text.startswith('€'):
                            currency = 'EUR'
                        
                        return price_val, currency
        
        return "Not Found", "USD"

    def _extract_details(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract product dimensions and weight."""
        details = {}
        
        # Strategy 1: Detail bullets
        detail_bullets = soup.select('#detailBullets_feature_div ul li')
        for li in detail_bullets:
            text = li.get_text(strip=True).lower()
            if 'dimensions' in text:
                detail_text = li.select_one('.a-list-item > span:last-child')
                if detail_text:
                    details['dimensions'] = detail_text.get_text(strip=True)
            elif 'weight' in text:
                detail_text = li.select_one('.a-list-item > span:last-child')
                if detail_text:
                    details['weight'] = detail_text.get_text(strip=True)
        
        # Strategy 2: Product details table
        if not details.get('dimensions') or not details.get('weight'):
            detail_rows = soup.select('#productDetails_detailBullets_sections1 tr')
            for row in detail_rows:
                header = row.select_one('td.a-span3')
                value = row.select_one('td.a-span9')
                if header and value:
                    header_text = header.get_text(strip=True).lower()
                    value_text = value.get_text(strip=True)
                    
                    if 'dimensions' in header_text and not details.get('dimensions'):
                        details['dimensions'] = value_text
                    elif 'weight' in header_text and not details.get('weight'):
                        details['weight'] = value_text
        
        # Strategy 3: Technical details section
        if not details.get('dimensions') or not details.get('weight'):
            tech_details = soup.select('#tech-spec-container tr')
            for row in tech_details:
                cells = row.select('td')
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    
                    if 'dimensions' in key and not details.get('dimensions'):
                        details['dimensions'] = value
                    elif 'weight' in key and not details.get('weight'):
                        details['weight'] = value
        
        return details

    def _get_json_ld(self, soup: BeautifulSoup) -> dict:
        """Extract JSON-LD structured data."""
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            if script and script.string:
                try:
                    data = json.loads(script.string)
                    # Handle cases where JSON-LD is a list
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get('@type') == 'Product':
                                return item
                    elif isinstance(data, dict) and data.get('@type') == 'Product':
                        return data
                except json.JSONDecodeError:
                    continue
        return {}
# endregion

# region WalmartScraper
class WalmartScraper(BaseScraper):
    """Scrapes product data from Walmart Canada pages."""
    
    def get_headers(self) -> Dict[str, str]:
        """Override headers for Walmart."""
        headers = super().get_headers()
        headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })
        return headers
    
    def scrape(self, url: str) -> ProductData:
        logger.info(f"🚀 Starting Walmart scrape for: {url}")
        
        soup = self.get_page_content(url)
        if not soup:
            return ProductData(url=url, source="Walmart", status="Failed", error_details="Failed to fetch page content")

        try:
            product = ProductData(url=url, source="Walmart")
            
            # Try to get data from Next.js data first
            json_data = self._get_initial_state(soup)
            
            if json_data:
                # Extract from JSON data
                product_info = json_data.get('product', {})
                product.asin = product_info.get('activeSkuId') or product_info.get('id')
                product.name = product_info.get('name', 'Product Name Not Found')
                product.description = product_info.get('shortDescription', 'Product Description Not Found')
                
                # Price
                price_info = json_data.get('price', {}).get('item', {}).get('price')
                if price_info:
                    product.sale_price = str(price_info)
                    product.currency = "CAD"
                
                # Images
                image_info = product_info.get('imageInfo', {})
                if image_info and 'allImages' in image_info:
                    product.images = [img.get('url') for img in image_info['allImages'] if img.get('url')]
                
                # Specifications
                specs = product_info.get('specifications', [])
                for spec in specs:
                    spec_name = spec.get('name', '').lower()
                    if 'dimensions' in spec_name:
                        product.dimensions = spec.get('value', 'Not Found')
                    elif 'weight' in spec_name:
                        product.weight = spec.get('value', 'Not Found')
            
            else:
                # Fallback to HTML parsing
                logger.info("JSON data not found, falling back to HTML parsing")
                product = self._extract_from_html(soup, product)
            
            product = DataValidator.validate_product(product)
            self.stats['successful_scrapes'] += 1
            return product
            
        except Exception as e:
            logger.error(f"❌ Walmart extraction error for {url}: {e}", exc_info=True)
            return ProductData(url=url, source="Walmart", status="Failed", error_details=f"Extraction error: {e}")

    def _get_initial_state(self, soup: BeautifulSoup) -> dict:
        """Extract the Next.js initial data from the page."""
        # Try __NEXT_DATA__ first
        script_tag = soup.find('script', id='__NEXT_DATA__')
        if script_tag and script_tag.string:
            try:
                data = json.loads(script_tag.string)
                return data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('data', {})
            except (json.JSONDecodeError, KeyError) as e:
                logger.debug(f"Could not parse __NEXT_DATA__: {e}")
        
        # Try other script tags with product data
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'window.__WML_REDUX_INITIAL_STATE__' in script.string:
                try:
                    # Extract the JSON part
                    json_match = re.search(r'window\.__WML_REDUX_INITIAL_STATE__\s*=\s*({.+?});', script.string, re.DOTALL)
                    if json_match:
                        data = json.loads(json_match.group(1))
                        return data.get('preso', {}).get('items', {})
                except (json.JSONDecodeError, AttributeError):
                    continue
        
        return {}

    def _extract_from_html(self, soup: BeautifulSoup, product: ProductData) -> ProductData:
        """Fallback HTML extraction for Walmart."""
        # Product name
        name_selectors = [
            'h1[data-automation-id="product-title"]',
            'h1.prod-ProductTitle',
            'h1.f2',
            'h1'
        ]
        
        for selector in name_selectors:
            name_elem = soup.select_one(selector)
            if name_elem:
                name = name_elem.get_text(strip=True)
                if name and len(name) > 5:
                    product.name = name
                    break
        
        # Description
        desc_selectors = [
            '[data-automation-id="product-highlights"]',
            '.about-desc',
            '.prod-ProductHighlights'
        ]
        
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                desc = desc_elem.get_text(strip=True)
                if desc and len(desc) > 10:
                    product.description = desc
                    break
        
        # Price
        price_selectors = [
            '[data-automation-id="product-price"] span',
            '.price-current',
            '.price .visuallyhidden'
        ]
        
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    product.sale_price = price_match.group(0)
                    product.currency = "CAD"
                    break
        
        # Images
        img_selectors = [
            '.prod-hero-image-image img',
            '.hero-image img',
            '.slider-slide img'
        ]
        
        images = []
        for selector in img_selectors:
            img_elements = soup.select(selector)
            for img in img_elements:
                img_url = img.get('src') or img.get('data-src')
                if img_url and img_url.startswith('http'):
                    images.append(img_url)
        
        product.images = list(set(images))
        
        return product
# endregion

def scraper_factory(url: str) -> Optional[BaseScraper]:
    """Returns the appropriate scraper instance based on the URL."""
    if any(domain in url for domain in ["amazon.", "amzn.to", "a.co"]):
        return AmazonScraper()
    elif "walmart.ca" in url:
        return WalmartScraper()
    elif "walmart.com" in url:
        # Handle US Walmart
        return WalmartScraper()
    return None

# --- Airtable Integration and Main Execution ---
def process_airtable_records():
    """Fetches records from Airtable and orchestrates the scraping process."""
    logger.info("=== 🚀 Starting Enhanced Multi-Site Scraper for Airtable ===")
    
    if not Api or AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        logger.error("❌ Airtable API is not configured. Please set AIRTABLE_API_KEY and BASE_ID.")
        return

    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        logger.info(f"✅ Connected to Airtable base '{AIRTABLE_BASE_ID}', table '{AIRTABLE_TABLE_NAME}'")
    except Exception as e:
        logger.error(f"❌ Airtable connection failed: {e}")
        return

    formula = f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')"
    try:
        records_to_process = table.all(formula=formula, view=AIRTABLE_VIEW_NAME)
        if not records_to_process:
            logger.info("✅ No new records to process.")
            return
        logger.info(f"Found {len(records_to_process)} records to process.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch records from Airtable: {e}")
        return

    for i, record in enumerate(records_to_process, 1):
        record_id = record['id']
        product_url = record['fields'].get(FIELD_MAPPINGS['url'])
        product_code = record['fields'].get("4more-Product-Code")
        
        logger.info(f"\n--- Processing Record {i}/{len(records_to_process)} (ID: {record_id}) ---")
        logger.debug(f"Product URL: {product_url}")
        logger.debug(f"4more-Product-Code: {product_code}")
        
        # Check if product code is already populated - skip scraping if so
        if product_code and str(product_code).strip():
            logger.info(f"✓ Product code '{product_code}' found in '4more-Product-Code' field for record {record_id} - skipping scraping")
            try:
                table.update(record_id, {
                    FIELD_MAPPINGS['status']: "Linked to Catalogue",
                    FIELD_MAPPINGS['scraping_status']: f"Skipped (Already linked to product code: {product_code})"
                })
                logger.info(f"✅ Record {record_id} status updated to 'Linked to Catalogue'")
            except Exception as e:
                logger.error(f"❌ Failed to update status for record {record_id}: {e}")
            continue
        
        if not product_url or not product_url.strip():
            table.update(record_id, {FIELD_MAPPINGS['status']: "Needs Attention", FIELD_MAPPINGS['scraping_status']: "Skipped (No URL)"})
            continue

        # Clean the URL
        product_url = product_url.strip()
        
        scraper = scraper_factory(product_url)
        if not scraper:
            logger.warning(f"⚠️ No scraper available for URL: {product_url}")
            table.update(record_id, {FIELD_MAPPINGS['status']: "Needs Attention", FIELD_MAPPINGS['scraping_status']: "Skipped (Unsupported Website)"})
            continue
        
        product_data = scraper.scrape(product_url)
        
        # Prepare data for Airtable update
        fields_to_update = {
            FIELD_MAPPINGS['name']: product_data.name,
            FIELD_MAPPINGS['description']: product_data.description,
            FIELD_MAPPINGS['dimensions']: product_data.dimensions,
            FIELD_MAPPINGS['weight']: product_data.weight,
            FIELD_MAPPINGS['source']: product_data.source,
            FIELD_MAPPINGS['currency']: product_data.currency,
            FIELD_MAPPINGS['asin']: product_data.asin,
            FIELD_MAPPINGS['status']: 'Scraped' if product_data.status == 'Scraped' else 'Needs Attention'
        }
        
        # Format scraping status message
        scraping_status = product_data.status
        if product_data.missing_fields:
            scraping_status += f" (Missing: {', '.join(product_data.missing_fields)})"
        elif product_data.error_details:
            scraping_status += f" ({product_data.error_details})"
        fields_to_update[FIELD_MAPPINGS['scraping_status']] = scraping_status

        # Handle numeric and attachment fields
        if DataValidator.validate_price(product_data.sale_price):
            try:
                fields_to_update[FIELD_MAPPINGS['sale_price']] = float(product_data.sale_price.replace(',', ''))
            except (ValueError, AttributeError):
                pass
        
        if product_data.images:
            fields_to_update[FIELD_MAPPINGS['photos']] = ", ".join(product_data.images)
            fields_to_update[FIELD_MAPPINGS['photo_files']] = [{'url': img} for img in product_data.images[:5]]  # Limit to 5 images

        try:
            table.update(record_id, fields_to_update)
            logger.info(f"✅ Record {record_id} updated successfully.")
        except Exception as e:
            logger.error(f"❌ Airtable update failed for record {record_id}: {e}")
            try:
                table.update(record_id, {FIELD_MAPPINGS['status']: "Needs Attention", FIELD_MAPPINGS['scraping_status']: f"Update Error: {str(e)[:100]}"})
            except:
                logger.error(f"❌ Failed to update error status for record {record_id}")
        
        # Random delay between requests
        if i < len(records_to_process):
            time.sleep(random.uniform(3, 8))

    logger.info("\n=== ✅ Processing complete. ===")

if __name__ == "__main__":
    process_airtable_records()