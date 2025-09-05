import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, quote
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
import base64

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
            '🚀': '[START]', '✨': '[FINISH]', '📊': '[STATS]', '🔍': '[DEBUG]', 
            '⏳': '[WAIT]', '📸': '[IMAGE]', '🔤': '[TEXT]', '💰': '[PRICE]', '🔎': '[SEARCH]'
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
    file_handler = logging.FileHandler('logs/google_lens_scraper.log', encoding='utf-8')
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

# Required libraries check
try:
    from pyairtable import Api
    logger.info("✓ pyairtable library loaded successfully")
except ImportError:
    logger.error("❌ pyairtable library not found. Please install it: pip install pyairtable")
    Api = None
# endregion

# --- API Configuration ---
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY', "YOUR_AIRTABLE_API_KEY_HERE")
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID', "YOUR_AIRTABLE_BASE_ID_HERE")
AIRTABLE_TABLE_NAME = "Items-Bid4more"
AIRTABLE_VIEW_NAME = "Fetch-Program-View"

# Google Custom Search API configuration
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', "YOUR_GOOGLE_API_KEY_HERE")
GOOGLE_CX = os.environ.get('GOOGLE_CX', "YOUR_CUSTOM_SEARCH_ENGINE_ID_HERE")

# SerpAPI configuration (alternative to Google Custom Search)
SERPAPI_KEY = os.environ.get('SERPAPI_KEY', "YOUR_SERPAPI_KEY_HERE")

FIELD_MAPPINGS = {
    'url': "Product URL", 'product_code': "4more-Product-Code", 'name': "Product Name",
    'description': "Description", 'sale_price': "Sale Price", 'original_price': "Original Price",
    'currency': "Currency", 'photos': "Photos", 'photo_files': "Photo Files",
    'dimensions': "Dimensions", 'weight': "Weight", 'source': "Scraping Website",
    'status': "Status", 'scraping_status': "Scraping Status", 'asin': "ASIN"
}

# --- Data Structures ---
@dataclass
class ProductData:
    """Data class for structured product information."""
    url: str = ""
    source: str = "Image Search"
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
    search_confidence: float = 0.0

@dataclass
class SearchResult:
    """Data class for search results."""
    title: str
    url: str
    snippet: str
    thumbnail: str = ""
    confidence: float = 0.0

class DataValidator:
    """A pipeline for validating scraped product data."""
    @staticmethod
    def validate_name(name: str) -> bool: 
        return name and name != "Product Name Not Found" and len(name.strip()) >= 3
    
    @staticmethod
    def validate_images(images: List[str]) -> bool: 
        return images and all(isinstance(img, str) and img.startswith('http') for img in images)
    
    @staticmethod
    def validate_price(price: str) -> bool:
        if price in ['Not Found', 'N/A', '']: return False
        try: 
            clean_price = price.replace('$', '').replace(',', '').replace('CAD', '').replace('USD', '').strip()
            return 0.01 <= float(clean_price) <= 50000
        except (ValueError, TypeError): return False

    @classmethod
    def validate_product(cls, product: ProductData) -> ProductData:
        missing = []
        if not cls.validate_name(product.name): missing.append('name')
        
        product.missing_fields = missing
        if missing:
            product.status = 'Needs Attention'
            logger.warning(f"Product search: Missing fields - {', '.join(missing)}")
        else:
            product.status = 'Scraped'
            logger.info(f"Product search: All critical fields validated")
        return product

# --- Image Search Classes ---
class GoogleLensSearcher:
    """Performs Google Lens-like image searches."""
    
    def __init__(self):
        self.session = self._create_session()
        self.stats = {
            'total_searches': 0, 'successful_searches': 0, 'failed_searches': 0,
            'products_found': 0, 'api_calls': 0
        }
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    
    def search_by_image(self, image_url: str) -> List[SearchResult]:
        """Search for products using image URL."""
        logger.info(f"🔎 Starting image search for: {image_url[:100]}...")
        self.stats['total_searches'] += 1
        
        # Try multiple search methods
        results = []
        
        # Method 1: Google Custom Search API (if configured)
        if GOOGLE_API_KEY != "YOUR_GOOGLE_API_KEY_HERE" and GOOGLE_CX != "YOUR_CUSTOM_SEARCH_ENGINE_ID_HERE":
            results = self._search_google_custom(image_url)
        
        # Method 2: SerpAPI (if configured and Google failed)
        if not results and SERPAPI_KEY != "YOUR_SERPAPI_KEY_HERE":
            results = self._search_serpapi(image_url)
        
        # Method 3: Direct Google Images scraping (fallback)
        if not results:
            results = self._search_google_images_direct(image_url)
        
        if results:
            self.stats['successful_searches'] += 1
            self.stats['products_found'] += len(results)
        else:
            self.stats['failed_searches'] += 1
        
        return results
    
    def _search_google_custom(self, image_url: str) -> List[SearchResult]:
        """Search using Google Custom Search API."""
        try:
            self.stats['api_calls'] += 1
            
            # Google Custom Search API endpoint
            url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CX,
                'searchType': 'image',
                'imgSize': 'large',
                'num': 10,
                'q': f'product site:amazon.com OR site:walmart.com OR site:ebay.com'
            }
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for item in data.get('items', []):
                result = SearchResult(
                    title=item.get('title', ''),
                    url=item.get('link', ''),
                    snippet=item.get('snippet', ''),
                    thumbnail=item.get('image', {}).get('thumbnailLink', ''),
                    confidence=0.8  # High confidence for API results
                )
                results.append(result)
            
            logger.info(f"✅ Google Custom Search found {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"❌ Google Custom Search failed: {e}")
            return []
    
    def _search_serpapi(self, image_url: str) -> List[SearchResult]:
        """Search using SerpAPI."""
        try:
            self.stats['api_calls'] += 1
            
            url = "https://serpapi.com/search"
            params = {
                'engine': 'google_lens',
                'api_key': SERPAPI_KEY,
                'url': image_url
            }
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            # Parse visual matches
            visual_matches = data.get('visual_matches', [])
            for match in visual_matches[:10]:  # Limit to 10 results
                result = SearchResult(
                    title=match.get('title', ''),
                    url=match.get('link', ''),
                    snippet=match.get('snippet', ''),
                    thumbnail=match.get('thumbnail', ''),
                    confidence=0.9  # High confidence for SerpAPI
                )
                results.append(result)
            
            logger.info(f"✅ SerpAPI found {len(results)} visual matches")
            return results
            
        except Exception as e:
            logger.error(f"❌ SerpAPI search failed: {e}")
            return []
    
    def _search_google_images_direct(self, image_url: str) -> List[SearchResult]:
        """Direct Google Images search (fallback method)."""
        try:
            # This method simulates what Google Lens does by using reverse image search
            search_url = f"https://www.google.com/searchbyimage?image_url={quote(image_url)}&hl=en"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = self.session.get(search_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            results = []
            
            # Look for search results
            search_results = soup.select('div.g, div[data-ved]')
            
            for result_div in search_results[:10]:  # Limit to 10 results
                title_elem = result_div.select_one('h3')
                link_elem = result_div.select_one('a[href]')
                snippet_elem = result_div.select_one('.VwiC3b, .s3v9rd, .st')
                
                if title_elem and link_elem:
                    url = link_elem.get('href', '')
                    if url.startswith('/url?q='):
                        # Extract actual URL from Google redirect
                        url = parse_qs(urlparse(url).query).get('q', [''])[0]
                    
                    # Filter for shopping/product sites
                    if any(site in url.lower() for site in ['amazon', 'walmart', 'ebay', 'target', 'bestbuy', 'shop']):
                        result = SearchResult(
                            title=title_elem.get_text(strip=True),
                            url=url,
                            snippet=snippet_elem.get_text(strip=True) if snippet_elem else '',
                            confidence=0.6  # Lower confidence for scraped results
                        )
                        results.append(result)
            
            logger.info(f"✅ Direct Google search found {len(results)} product results")
            return results
            
        except Exception as e:
            logger.error(f"❌ Direct Google search failed: {e}")
            return []
    
    def get_stats(self) -> dict:
        return self.stats

class ProductExtractor:
    """Extracts product information from search results."""
    
    def __init__(self):
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=2, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    
    def extract_from_results(self, search_results: List[SearchResult], original_images: List[str]) -> ProductData:
        """Extract product information from search results."""
        logger.info(f"📊 Extracting product info from {len(search_results)} search results")
        
        best_product = None
        highest_confidence = 0.0
        
        for i, result in enumerate(search_results[:5], 1):  # Check top 5 results
            logger.info(f"🔍 Analyzing result {i}: {result.title[:50]}...")
            
            try:
                product = self._extract_from_url(result.url, result, original_images)
                if product and product.search_confidence > highest_confidence:
                    best_product = product
                    highest_confidence = product.search_confidence
                    
            except Exception as e:
                logger.warning(f"⚠️ Failed to extract from {result.url}: {e}")
                continue
            
            # Small delay between requests
            time.sleep(random.uniform(1, 3))
        
        if best_product:
            return DataValidator.validate_product(best_product)
        else:
            return ProductData(
                status="Failed",
                error_details="No product information could be extracted from search results",
                images=original_images
            )
    
    def _extract_from_url(self, url: str, search_result: SearchResult, original_images: List[str]) -> Optional[ProductData]:
        """Extract product information from a specific URL."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            response = self.session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            product = ProductData(url=url, images=original_images)
            product.search_confidence = search_result.confidence
            
            # Determine source and extract accordingly
            if 'amazon' in url.lower():
                product = self._extract_amazon_data(soup, product, search_result)
            elif 'walmart' in url.lower():
                product = self._extract_walmart_data(soup, product, search_result)
            elif 'ebay' in url.lower():
                product = self._extract_ebay_data(soup, product, search_result)
            else:
                product = self._extract_generic_data(soup, product, search_result)
            
            return product
            
        except Exception as e:
            logger.error(f"Failed to extract from {url}: {e}")
            return None
    
    def _extract_amazon_data(self, soup: BeautifulSoup, product: ProductData, search_result: SearchResult) -> ProductData:
        """Extract data from Amazon pages."""
        product.source = "Amazon"
        
        # Product name
        title_elem = soup.select_one('span#productTitle, h1.a-size-large')
        if title_elem:
            product.name = title_elem.get_text(strip=True)
        else:
            product.name = search_result.title
        
        # Price
        price_selectors = [
            '.a-price[data-a-color="price"] .a-offscreen',
            '.a-price-current .a-offscreen',
            '.a-price .a-offscreen'
        ]
        
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    product.sale_price = price_match.group(0)
                    product.currency = 'CAD' if '.ca/' in product.url else 'USD'
                    break
        
        # ASIN
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', product.url)
        if asin_match:
            product.asin = asin_match.group(1)
        
        # Description from bullets
        bullets = soup.select('#feature-bullets ul li span.a-list-item')
        if bullets:
            desc_parts = [b.get_text(strip=True) for b in bullets[:3] if b.get_text(strip=True)]
            product.description = '. '.join(desc_parts) if desc_parts else search_result.snippet
        else:
            product.description = search_result.snippet
        
        return product
    
    def _extract_walmart_data(self, soup: BeautifulSoup, product: ProductData, search_result: SearchResult) -> ProductData:
        """Extract data from Walmart pages."""
        product.source = "Walmart"
        product.currency = "CAD" if ".ca" in product.url else "USD"
        
        # Product name
        title_selectors = ['h1[data-automation-id="product-title"]', 'h1', '.prod-ProductTitle']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                product.name = title_elem.get_text(strip=True)
                break
        else:
            product.name = search_result.title
        
        # Price
        price_selectors = ['[data-automation-id="product-price"]', '.price-current', '.price']
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    product.sale_price = price_match.group(0)
                    break
        
        product.description = search_result.snippet
        return product
    
    def _extract_ebay_data(self, soup: BeautifulSoup, product: ProductData, search_result: SearchResult) -> ProductData:
        """Extract data from eBay pages."""
        product.source = "eBay"
        
        # Product name
        title_elem = soup.select_one('h1#x-title-label-lbl, .x-item-title-label')
        if title_elem:
            product.name = title_elem.get_text(strip=True)
        else:
            product.name = search_result.title
        
        # Price
        price_elem = soup.select_one('.notranslate, .u-flL.condText')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
            if price_match:
                product.sale_price = price_match.group(0)
                # Determine currency from text
                if 'CAD' in price_text or 'C$' in price_text:
                    product.currency = 'CAD'
                else:
                    product.currency = 'USD'
        
        product.description = search_result.snippet
        return product
    
    def _extract_generic_data(self, soup: BeautifulSoup, product: ProductData, search_result: SearchResult) -> ProductData:
        """Extract data from generic e-commerce pages."""
        product.source = "Generic"
        
        # Try to find product name
        title_selectors = ['h1', '.product-title', '.product-name', '[class*="title"]']
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem and len(title_elem.get_text(strip=True)) > 5:
                product.name = title_elem.get_text(strip=True)
                break
        else:
            product.name = search_result.title
        
        # Try to find price
        price_selectors = ['.price', '[class*="price"]', '[class*="cost"]']
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_match = re