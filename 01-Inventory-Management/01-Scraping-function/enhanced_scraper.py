"""
Enhanced Multi-Site Product Scraper using Scrapy
=================================================
Supports: Amazon (.com/.ca), Walmart (.com/.ca), Staples, Home Depot CA

INSTALLATION REQUIREMENTS:
-------------------------
pip install scrapy pyairtable python-dotenv pillow

RECOMMENDED FOR BETTER SUCCESS:
-------------------------------
pip install cloudscraper

OPTIONAL FOR JAVASCRIPT SITES:
------------------------------
pip install scrapy-splash
# OR
pip install selenium

USAGE:
------
1. Set your Airtable credentials in .env file or environment variables
2. Run: python scraper_scrapy.py
"""

import scrapy
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.downloadermiddlewares.retry import RetryMiddleware
import json
import re
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import random
import time
from urllib.parse import urljoin, urlparse

# Fix Windows console encoding
if sys.platform.startswith("win"):
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("✓ .env file loaded successfully")
except ImportError:
    logger.warning("⚠️ python-dotenv not installed")

# Airtable imports
try:
    from pyairtable import Api
    AIRTABLE_AVAILABLE = True
    logger.info("✓ pyairtable library loaded")
except ImportError:
    logger.error("❌ pyairtable not found. Install with: pip install pyairtable")
    AIRTABLE_AVAILABLE = False
    Api = None

# Try to import cloudscraper for fallback
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
    logger.info("✓ cloudscraper available for fallback")
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False
    logger.info("ℹ cloudscraper not installed (optional)")

# --- Configuration ---
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "YOUR_AIRTABLE_API_KEY_HERE")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "YOUR_AIRTABLE_BASE_ID_HERE")
AIRTABLE_TABLE_NAME = "Items-Bid4more"
AIRTABLE_VIEW_NAME = "Fetch-Program-View"

FIELD_MAPPINGS = {
    "url": "Product URL",
    "product_code": "4more-Product-Code",
    "name": "Product Name",
    "description": "Description",
    "sale_price": "Sale Price",
    "original_price": "Original Price",
    "currency": "Currency",
    "photos": "Photos",
    "photo_files": "Photo Files",
    "dimensions": "Dimensions",
    "weight": "Weight",
    "source": "Scraping Website",
    "status": "Status",
    "scraping_status": "Scraping Status",
    "asin": "ASIN",
}

# --- Data Classes ---
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
    """Validates scraped product data."""
    
    @staticmethod
    def validate_name(name: str) -> bool:
        return name and name != "Product Name Not Found" and len(name.strip()) >= 5

    @staticmethod
    def validate_images(images: List[str]) -> bool:
        return images and all(isinstance(img, str) and img.startswith("http") for img in images)

    @staticmethod
    def validate_price(price: str) -> bool:
        if price in ["Not Found", "N/A", ""]:
            return False
        try:
            return 0.01 <= float(price) <= 50000
        except (ValueError, TypeError):
            return False

    @classmethod
    def validate_product(cls, product: ProductData) -> ProductData:
        missing = []
        if not cls.validate_name(product.name):
            missing.append("name")
        if not cls.validate_images(product.images):
            missing.append("images")

        product.missing_fields = missing
        if missing:
            product.status = "Needs Attention"
            logger.warning(f"Product {product.asin or product.url}: Missing - {', '.join(missing)}")
        else:
            product.status = "Scraped"
            logger.info(f"Product validated successfully")
        return product


# --- Scrapy Settings ---
def get_scrapy_settings():
    """Configure Scrapy settings for better anti-bot handling."""
    settings = {
        # Basic settings
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 1,  # One request at a time to avoid rate limiting
        'DOWNLOAD_DELAY': 3,  # Delay between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Randomize delays (0.5 * to 1.5 * DOWNLOAD_DELAY)
        
        # Retry settings
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 403],
        
        # Timeout settings
        'DOWNLOAD_TIMEOUT': 30,
        
        # Cookie settings
        'COOKIES_ENABLED': True,
        
        # Cache settings (optional - speeds up re-scraping)
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,
        
        # Middleware settings
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            f'{__name__}.CloudScraperMiddleware': 560,  # Our custom middleware
        },
        
        # Headers
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.google.com/',
        },
        
        # Logging
        'LOG_LEVEL': 'INFO',
        
        # AutoThrottle for automatic speed adjustment
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 2,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        'AUTOTHROTTLE_DEBUG': False,
    }
    
    return settings


# --- Custom Middleware for CloudScraper Fallback ---
class CloudScraperMiddleware:
    """Custom middleware to use cloudscraper for difficult requests."""
    
    def __init__(self):
        self.scraper = None
        if CLOUDSCRAPER_AVAILABLE:
            self.scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True,
                    'mobile': False
                }
            )
            logger.info("CloudScraper middleware initialized")
    
    def process_response(self, request, response, spider):
        """Process response and retry with cloudscraper if needed."""
        # Check if response indicates bot detection
        if response.status in [403, 503] or b'captcha' in response.body.lower():
            if self.scraper and request.meta.get('cloudscraper_retry', 0) < 2:
                logger.info(f"Retrying with CloudScraper: {request.url}")
                
                # Mark that we're using cloudscraper
                request.meta['cloudscraper_retry'] = request.meta.get('cloudscraper_retry', 0) + 1
                
                try:
                    # Use cloudscraper to fetch the page
                    cf_response = self.scraper.get(request.url)
                    
                    # Create a new Scrapy response
                    from scrapy.http import HtmlResponse
                    return HtmlResponse(
                        url=request.url,
                        body=cf_response.text.encode('utf-8'),
                        encoding='utf-8',
                        request=request
                    )
                except Exception as e:
                    logger.error(f"CloudScraper failed: {e}")
        
        return response


# --- Main Spider ---
class ProductSpider(Spider):
    name = 'product_spider'
    urls_data = []  # Class variable to store URLs
    results = []    # Class variable to store results
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Keywords to detect recommendation sections
        self.skip_section_keywords = [
            "frequently", "bought", "together", "sponsored", "carousel",
            "similar", "related", "also-bought", "comparison", "bundle",
            "customers-who-bought", "recommendations", "suggested", "inspired",
            "pairs-well", "complete-the-set"
        ]

    def start_requests(self):
        """Generate initial requests for each URL."""
        for url_data in self.__class__.urls_data:  # Access class variable
            url = url_data['url']
            
            # Fix missing URL scheme
            if not url.startswith(('http://', 'https://')):
                # Try to determine the correct scheme
                if 'amazon' in url:
                    url = 'https://www.' + url
                elif 'walmart' in url:
                    url = 'https://www.' + url
                else:
                    url = 'https://' + url
                logger.info(f"Fixed URL scheme: {url}")
            
            # Determine which parser to use based on URL
            url_lower = url.lower()
            
            # Supported sites
            if any(domain in url_lower for domain in ["amazon.", "amzn.to", "a.co"]):
                callback = self.parse_amazon
            elif "walmart" in url_lower:
                callback = self.parse_walmart
            elif "staples" in url_lower:
                callback = self.parse_staples
            elif "homedepot" in url_lower:
                callback = self.parse_homedepot
            elif "target.com" in url_lower:
                callback = self.parse_target
            elif "ebay.com" in url_lower:
                callback = self.parse_ebay
            elif "whatnot.com" in url_lower:
                callback = self.parse_whatnot  # New parser for Whatnot
            elif "poshmark.com" in url_lower or "poshmark.ca" in url_lower:
                callback = self.parse_poshmark  # New parser for Poshmark
            else:
                logger.warning(f"No parser for URL: {url}")
                # Add to results as unsupported
                self.__class__.results.append({
                    'record_id': url_data['record_id'],
                    'product': ProductData(
                        url=url,
                        status="Failed",
                        error_details="Unsupported website"
                    )
                })
                continue
            
            # Add random user agents
            headers = {
                'User-Agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
                ])
            }
            
            yield Request(
                url=url,
                callback=callback,
                headers=headers,
                meta={
                    'record_id': url_data['record_id'],
                    'original_url': url,
                    'handle_httpstatus_list': [403, 503],  # Handle these status codes
                },
                dont_filter=True,
                errback=self.handle_error
            )

    def handle_error(self, failure):
        """Handle failed requests."""
        url = failure.request.meta.get('original_url', '')
        record_id = failure.request.meta.get('record_id', '')
        
        logger.error(f"Failed to fetch {url}: {failure.value}")
        
        product = ProductData(
            url=url,
            status="Failed",
            error_details=f"Request failed: {str(failure.value)}"
        )
        
        self.__class__.results.append({  # Use class variable
            'record_id': record_id,
            'product': product
        })

    def parse_amazon(self, response):
        """Parse Amazon product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        # Check for CAPTCHA
        if b"enter the characters you see below" in response.body.lower():
            logger.warning(f"Amazon CAPTCHA detected for {original_url}")
            product = ProductData(
                url=original_url,
                source="Amazon",
                status="Blocked",
                error_details="Blocked by Amazon CAPTCHA"
            )
            self.__class__.results.append({'record_id': record_id, 'product': product})
            return
        
        try:
            product = ProductData(url=original_url, source="Amazon")
            
            # Extract ASIN
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', response.url)
            if asin_match:
                product.asin = asin_match.group(1)
            else:
                # Try to find ASIN in page
                asin_input = response.css('input#ASIN::attr(value)').get()
                if asin_input:
                    product.asin = asin_input
            
            # Extract product name
            product.name = self._extract_amazon_name(response)
            
            # Extract description
            product.description = self._extract_amazon_description(response)
            
            # Extract images
            product.images = self._extract_amazon_images(response)
            
            # Extract price
            product.sale_price, product.currency = self._extract_amazon_price(response)
            
            # Extract details
            details = self._extract_amazon_details(response)
            product.dimensions = details.get('dimensions', 'Product Dimensions Not Found')
            product.weight = details.get('weight', 'Product Weight Not Found')
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Amazon scrape completed for {product.asin or original_url}")
            
        except Exception as e:
            logger.error(f"Amazon parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Amazon",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})

    def _extract_amazon_name(self, response):
        """Extract product name from Amazon page."""
        # Try multiple selectors
        selectors = [
            'span#productTitle::text',
            'h1#title span::text',
            'h1.a-size-large span::text',
            'meta[property="og:title"]::attr(content)',
        ]
        
        for selector in selectors:
            name = response.css(selector).get()
            if name and len(name.strip()) > 5:
                return name.strip()
        
        return "Product Name Not Found"

    def _extract_amazon_description(self, response):
        """Extract product description from Amazon page."""
        # Try feature bullets
        bullets = response.css('#feature-bullets ul li span.a-list-item::text').getall()
        bullets = [b.strip() for b in bullets if b.strip() and not b.startswith('Make sure')]
        
        if bullets:
            return '. '.join(bullets[:5])
        
        # Try product description
        desc = response.css('#productDescription p::text').get()
        if desc:
            return desc.strip()
        
        return "Product Description Not Found"

    def _extract_amazon_images(self, response):
        """Extract unique product images from Amazon page."""
        images = []
        seen_images = set()  # Track unique images by their core URL
        
        # Helper function to normalize image URL for comparison
        def normalize_image_url(url):
            """Extract the core image identifier from Amazon URL."""
            # Remove size parameters and quality settings
            core_url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', url)
            # Remove query parameters
            core_url = core_url.split('?')[0]
            # Extract image ID if possible
            match = re.search(r'/([A-Z0-9]+)\.(jpg|jpeg|png|webp)', core_url)
            if match:
                return match.group(1)
            return core_url
        
        # Strategy 1: Check for dynamic image data (highest quality)
        img_data = response.css('div#imgTagWrapperId img::attr(data-a-dynamic-image)').get()
        if img_data:
            try:
                img_dict = json.loads(img_data)
                # Sort by resolution (highest first)
                sorted_imgs = sorted(img_dict.items(), 
                                   key=lambda x: x[1][0] * x[1][1] if isinstance(x[1], list) and len(x[1]) >= 2 else 0, 
                                   reverse=True)
                for img_url, _ in sorted_imgs:
                    core_id = normalize_image_url(img_url)
                    if core_id not in seen_images:
                        seen_images.add(core_id)
                        images.append(img_url)
                        break  # Only take the highest resolution version
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Strategy 2: Main landing image
        landing_img = response.css('#landingImage::attr(src)').get()
        if landing_img:
            core_id = normalize_image_url(landing_img)
            if core_id not in seen_images:
                seen_images.add(core_id)
                images.append(landing_img)
        
        # Strategy 3: Image gallery thumbnails (different product angles)
        thumbnails = response.css('#altImages ul li.item img, #altImages ul li.a-button-thumbnail img')
        for thumb in thumbnails:
            # Skip if in recommendation section
            if self._is_in_recommendation_section(thumb):
                continue
            
            thumb_url = thumb.css('::attr(src)').get() or thumb.css('::attr(data-src)').get()
            if thumb_url:
                # Convert thumbnail to full size
                full_url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', thumb_url)
                core_id = normalize_image_url(full_url)
                
                # Skip if we already have this image
                if core_id not in seen_images:
                    seen_images.add(core_id)
                    images.append(full_url)
        
        # Strategy 4: Additional image containers (but avoid duplicates)
        additional_selectors = [
            'img[data-old-hires]::attr(data-old-hires)',
            '.imageThumb img::attr(src)',
            '.a-dynamic-image::attr(src)',
        ]
        
        for selector in additional_selectors:
            additional_imgs = response.css(selector).getall()
            for img_url in additional_imgs:
                if img_url and img_url.startswith('http'):
                    # Skip recommendation sections
                    if any(keyword in img_url.lower() for keyword in self.skip_section_keywords):
                        continue
                    
                    clean_url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', img_url)
                    core_id = normalize_image_url(clean_url)
                    
                    if core_id not in seen_images:
                        seen_images.add(core_id)
                        images.append(clean_url)
        
        # Remove any remaining duplicates and limit to reasonable number
        unique_images = []
        final_seen = set()
        for img in images:
            core_id = normalize_image_url(img)
            if core_id not in final_seen:
                final_seen.add(core_id)
                unique_images.append(img)
        
        # Log for debugging
        logger.info(f"Found {len(unique_images)} unique product images")
        
        return unique_images[:5]  # Limit to 5 unique images
    
    def _is_in_recommendation_section(self, element):
        """Check if an element is within a recommendation section."""
        # Get parent elements to check
        for parent in element.css('::ancestor-or-self::*'):
            # Check class and id attributes
            classes = ' '.join(parent.css('::attr(class)').getall()).lower()
            elem_id = parent.css('::attr(id)').get('').lower()
            
            # Check for recommendation keywords
            for keyword in self.skip_section_keywords:
                if keyword in classes or keyword in elem_id:
                    return True
        
        return False

    def _extract_amazon_price(self, response):
        """Extract price and currency from Amazon page."""
        # Detect currency from domain
        currency = "CAD" if ".ca" in response.url else "USD"
        
        # Try various price selectors
        price_selectors = [
            '.a-price[data-a-color="price"] .a-offscreen::text',
            '.a-price .a-offscreen::text',
            'span.a-price-symbol + span.a-price-whole::text',
        ]
        
        for selector in price_selectors:
            price_text = response.css(selector).get()
            if price_text:
                # Extract numeric value
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    return price_match.group(0), currency
        
        return "Not Found", currency

    def _extract_amazon_details(self, response):
        """Extract product dimensions and weight."""
        details = {}
        
        # Try detail bullets
        detail_items = response.css('#detailBullets_feature_div ul li')
        for item in detail_items:
            text = ' '.join(item.css('::text').getall()).lower()
            if 'dimensions' in text:
                value = item.css('span:last-child::text').get()
                if value:
                    details['dimensions'] = value.strip()
            elif 'weight' in text:
                value = item.css('span:last-child::text').get()
                if value:
                    details['weight'] = value.strip()
        
        return details

    def parse_walmart(self, response):
        """Parse Walmart product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(
                url=original_url,
                source="Walmart CA" if ".ca" in response.url else "Walmart US",
                currency="CAD" if ".ca" in response.url else "USD"
            )
            
            # Try to extract from Next.js data
            next_data = response.css('script#__NEXT_DATA__::text').get()
            if next_data:
                try:
                    data = json.loads(next_data)
                    props = data.get('props', {}).get('pageProps', {})
                    initial_data = props.get('initialData', {}).get('data', {})
                    
                    if initial_data:
                        product_info = initial_data.get('product', {})
                        product.name = product_info.get('name', 'Product Name Not Found')
                        product.description = product_info.get('shortDescription', 'Product Description Not Found')
                        product.asin = product_info.get('id')
                        
                        # Price
                        price_info = initial_data.get('price', {}).get('item', {}).get('price')
                        if price_info:
                            product.sale_price = str(price_info)
                        
                        # Images
                        image_info = product_info.get('imageInfo', {})
                        if image_info and 'allImages' in image_info:
                            product.images = [img.get('url') for img in image_info.get('allImages', []) if img.get('url')]
                    
                except json.JSONDecodeError:
                    pass
            
            # Fallback to HTML parsing if needed
            if product.name == "Product Name Not Found":
                product.name = response.css('h1[data-automation="product-title"]::text').get() or \
                              response.css('h1.prod-ProductTitle::text').get() or \
                              "Product Name Not Found"
            
            if not product.images:
                product.images = response.css('.prod-hero-image img::attr(src)').getall()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Walmart scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Walmart parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Walmart",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})

    def parse_staples(self, response):
        """Parse Staples product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Staples", currency="USD")
            
            product.name = response.css('h1[data-testid="product-title"]::text').get() or \
                          response.css('h1.product-title::text').get() or \
                          "Product Name Not Found"
            
            product.description = response.css('.product-description::text').get() or \
                                response.css('[data-testid="product-description"]::text').get() or \
                                "Product Description Not Found"
            
            # Price
            price_text = response.css('[data-testid="product-price"]::text').get() or \
                        response.css('.price-now::text').get()
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    product.sale_price = price_match.group(0)
            
            # Images
            product.images = response.css('.product-image img::attr(src)').getall()
            
            # SKU
            product.asin = response.css('[data-testid="product-sku"]::text').get() or \
                          response.css('.product-sku::text').get()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Staples scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Staples parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Staples",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})

    def parse_target(self, response):
        """Parse Target product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Target", currency="USD")
            
            # Target often has JSON-LD data
            json_ld_scripts = response.css('script[type="application/ld+json"]::text').getall()
            for script_text in json_ld_scripts:
                try:
                    data = json.loads(script_text)
                    if isinstance(data, dict) and data.get('@type') == 'Product':
                        product.name = data.get('name', 'Product Name Not Found')
                        product.description = data.get('description', 'Product Description Not Found')
                        
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, list):
                                offers = offers[0]
                            product.sale_price = str(offers.get('price', 'Not Found'))
                        
                        if 'image' in data:
                            images = data['image']
                            product.images = [images] if isinstance(images, str) else images
                        
                        break
                except json.JSONDecodeError:
                    continue
            
            # Fallback to HTML parsing
            if product.name == "Product Name Not Found":
                product.name = response.css('h1[data-test="product-title"]::text').get() or \
                              response.css('h1::text').get() or \
                              "Product Name Not Found"
            
            if product.sale_price == "Not Found":
                price_text = response.css('[data-test="product-price"]::text').get() or \
                            response.css('.h-text-bold::text').get()
                if price_text:
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        product.sale_price = price_match.group(0)
            
            if not product.images:
                product.images = response.css('[data-test="product-image"] img::attr(src)').getall() or \
                                response.css('.styles__CarouselImage-sc-1tqcx5c-1 img::attr(src)').getall()
            
            # SKU/TCIN
            product.asin = response.css('[data-test="product-details-tcin"]::text').get()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Target scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Target parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Target",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.__class__.results.append({'record_id': record_id, 'product': product})

    def parse_ebay(self, response):
        """Parse eBay product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="eBay", currency="USD")
            
            # eBay item ID
            item_id_match = re.search(r'/itm/(\d+)', response.url)
            if item_id_match:
                product.asin = item_id_match.group(1)
            
            # Product name
            product.name = response.css('h1.it-ttl::text').get() or \
                          response.css('h1[itemprop="name"]::text').get() or \
                          response.css('.x-item-title__mainTitle::text').get() or \
                          "Product Name Not Found"
            
            # Price
            price_text = response.css('.x-price-primary span::text').get() or \
                        response.css('span[itemprop="price"]::text').get() or \
                        response.css('.notranslate::text').get()
            
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    product.sale_price = price_match.group(0)
                
                # Check currency
                if 'C 
        """Parse Home Depot product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Home Depot CA", currency="CAD")
            
            # Try JSON-LD first
            json_ld = response.css('script[type="application/ld+json"]::text').get()
            if json_ld:
                try:
                    data = json.loads(json_ld)
                    if data.get('@type') == 'Product':
                        product.name = data.get('name', 'Product Name Not Found')
                        product.description = data.get('description', 'Product Description Not Found')
                        
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, list):
                                offers = offers[0]
                            product.sale_price = str(offers.get('price', 'Not Found'))
                        
                        if 'image' in data:
                            images = data['image']
                            product.images = [images] if isinstance(images, str) else images
                
                except json.JSONDecodeError:
                    pass
            
            # Fallback to HTML
            if product.name == "Product Name Not Found":
                product.name = response.css('h1.product-details__title::text').get() or \
                              response.css('h1[data-testid="product-title"]::text').get() or \
                              "Product Name Not Found"
            
            if product.sale_price == "Not Found":
                price_text = response.css('.price__dollars::text').get()
                if price_text:
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        product.sale_price = price_match.group(0)
            
            if not product.images:
                product.images = response.css('.mediagallery__mainimage img::attr(src)').getall()
            
            # SKU
            product.asin = response.css('.product-info-bar__detail--sku::text').get()
            if product.asin:
                product.asin = product.asin.replace('SKU', '').strip()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Home Depot scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Home Depot parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Home Depot CA",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})


# --- Airtable Integration ---
def process_airtable_records():
    """Main function to process Airtable records using Scrapy."""
    logger.info("=== 🚀 Starting Scrapy-based Multi-Site Scraper ===")
    
    if not AIRTABLE_AVAILABLE or AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        logger.error("❌ Airtable API is not configured properly.")
        return
    
    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        logger.info(f"✅ Connected to Airtable")
    except Exception as e:
        logger.error(f"❌ Airtable connection failed: {e}")
        return
    
    # Fetch records to process
    formula = f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')"
    try:
        records_to_process = table.all(formula=formula, view=AIRTABLE_VIEW_NAME)
        if not records_to_process:
            logger.info("✅ No new records to process.")
            return
        logger.info(f"Found {len(records_to_process)} records to process.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch records: {e}")
        return
    
    # Prepare URLs for Scrapy
    urls_data = []
    for record in records_to_process:
        record_id = record["id"]
        product_url = record["fields"].get(FIELD_MAPPINGS["url"])
        product_code = record["fields"].get("4more-Product-Code")
        
        # Skip if already has product code
        if product_code and str(product_code).strip():
            logger.info(f"Skipping record {record_id} - already has product code")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Linked to Catalogue",
                        FIELD_MAPPINGS["scraping_status"]: f"Skipped (Already linked: {product_code})",
                    },
                )
            except Exception as e:
                logger.error(f"Failed to update record {record_id}: {e}")
            continue
        
        if product_url and product_url.strip():
            urls_data.append({
                'url': product_url.strip(),
                'record_id': record_id
            })
    
    if not urls_data:
        logger.info("No URLs to process.")
        return
    
    # Store urls_data and table reference for the spider
    ProductSpider.urls_data = urls_data
    ProductSpider.results = []
    
    # Configure and run Scrapy
    process = CrawlerProcess(get_scrapy_settings())
    
    # Pass the spider class, not an instance
    process.crawl(ProductSpider)
    process.start()
    
    # Process results after spider finishes
    logger.info(f"\n=== Processing {len(ProductSpider.results)} results ===")
    
    for result in ProductSpider.results:
        record_id = result['record_id']
        product_data = result['product']
        
        # Prepare Airtable update
        fields_to_update = {
            FIELD_MAPPINGS["name"]: product_data.name,
            FIELD_MAPPINGS["description"]: product_data.description[:1000] if product_data.description else "",
            FIELD_MAPPINGS["dimensions"]: product_data.dimensions,
            FIELD_MAPPINGS["weight"]: product_data.weight,
            FIELD_MAPPINGS["source"]: product_data.source,
            FIELD_MAPPINGS["currency"]: product_data.currency,
            FIELD_MAPPINGS["asin"]: product_data.asin,
            FIELD_MAPPINGS["status"]: (
                "Scraped" if product_data.status == "Scraped" else "Needs Attention"
            ),
        }
        
        # Format scraping status
        scraping_status = product_data.status
        if product_data.missing_fields:
            scraping_status += f" (Missing: {', '.join(product_data.missing_fields)})"
        elif product_data.error_details:
            scraping_status += f" ({product_data.error_details[:100]})"
        fields_to_update[FIELD_MAPPINGS["scraping_status"]] = scraping_status
        
        # Handle numeric fields
        if DataValidator.validate_price(product_data.sale_price):
            try:
                fields_to_update[FIELD_MAPPINGS["sale_price"]] = float(
                    product_data.sale_price.replace(",", "")
                )
            except (ValueError, AttributeError):
                pass
        
        # Handle images - ensure uniqueness before sending to Airtable
        if product_data.images:
            # Deduplicate images one more time before sending
            unique_imgs = []
            seen_cores = set()
            
            for img_url in product_data.images:
                # Extract core identifier
                core_id = re.sub(r'\._[A-Z0-9_,]+_\.', '.', img_url).split('?')[0]
                img_id_match = re.search(r'/([A-Z0-9]{6,})\.(jpg|jpeg|png|webp)', core_id)
                
                if img_id_match:
                    img_id = img_id_match.group(1)
                    if img_id not in seen_cores:
                        seen_cores.add(img_id)
                        unique_imgs.append(img_url)
                elif core_id not in seen_cores:
                    seen_cores.add(core_id)
                    unique_imgs.append(img_url)
            
            # Only use first 5 unique images
            unique_imgs = unique_imgs[:5]
            
            if unique_imgs:
                fields_to_update[FIELD_MAPPINGS["photos"]] = ", ".join(unique_imgs)
                fields_to_update[FIELD_MAPPINGS["photo_files"]] = [
                    {"url": img} for img in unique_imgs
                ]
        
        # Update Airtable
        try:
            table.update(record_id, fields_to_update)
            logger.info(f"✅ Record {record_id} updated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to update record {record_id}: {e}")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Needs Attention",
                        FIELD_MAPPINGS["scraping_status"]: f"Update Error: {str(e)[:100]}",
                    },
                )
            except:
                pass
    
    logger.info("\n=== ✨ Processing complete! ===")


if __name__ == "__main__":
    process_airtable_records()
 in price_text or 'CAD' in price_text:
                    product.currency = "CAD"
            
            # Description
            desc_elem = response.css('[data-testid="item-description"]::text').get() or \
                       response.css('.section-desc::text').get()
            if desc_elem:
                product.description = desc_elem.strip()
            
            # Images
            product.images = response.css('.ux-image-carousel-item img::attr(src)').getall() or \
                           response.css('.ux-image-grid-item img::attr(src)').getall() or \
                           response.css('#icImg::attr(src)').getall()
            
            # Clean up image URLs
            cleaned_images = []
            for img in product.images:
                if img and img.startswith('http'):
                    # Get higher resolution version
                    high_res = img.replace('s-l64.', 's-l1600.').replace('s-l140.', 's-l1600.')
                    cleaned_images.append(high_res)
            product.images = cleaned_images[:10]
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ eBay scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"eBay parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="eBay",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.__class__.results.append({'record_id': record_id, 'product': product})

    def parse_whatnot(self, response):
        """Parse Whatnot product/listing pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Whatnot", currency="USD")
            
            # Whatnot is a React app with data in JSON scripts
            # Try to find Next.js data or React props
            scripts = response.css('script::text').getall()
            
            for script in scripts:
                # Look for window.__INITIAL_STATE__ or similar
                if '__INITIAL_STATE__' in script or 'window.W_DATA' in script:
                    try:
                        # Extract JSON from script
                        json_match = re.search(r'({.+})', script)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            
                            # Navigate through possible data structures
                            # Whatnot structure varies, so we try multiple paths
                            if 'listing' in data:
                                listing = data['listing']
                                product.name = listing.get('title', 'Product Name Not Found')
                                product.description = listing.get('description', 'Product Description Not Found')
                                product.sale_price = str(listing.get('price', {}).get('amount', 'Not Found'))
                                
                                # Images
                                if 'images' in listing:
                                    product.images = [img.get('url') for img in listing.get('images', []) if img.get('url')]
                                elif 'media' in listing:
                                    product.images = [media.get('url') for media in listing.get('media', []) if media.get('url')]
                            
                            break
                    except (json.JSONDecodeError, KeyError):
                        continue
            
            # Fallback to HTML parsing
            if product.name == "Product Name Not Found":
                # Try meta tags
                product.name = response.css('meta[property="og:title"]::attr(content)').get() or \
                              response.css('h1::text').get() or \
                              response.css('[data-testid="listing-title"]::text').get() or \
                              "Product Name Not Found"
            
            if product.description == "Product Description Not Found":
                product.description = response.css('meta[property="og:description"]::attr(content)').get() or \
                                    response.css('[data-testid="listing-description"]::text').get() or \
                                    "Product Description Not Found"
            
            if not product.images:
                # Try to get image from meta tags
                og_image = response.css('meta[property="og:image"]::attr(content)').get()
                if og_image:
                    product.images = [og_image]
                else:
                    # Try various image selectors
                    product.images = response.css('img[data-testid="listing-image"]::attr(src)').getall() or \
                                   response.css('.listing-image img::attr(src)').getall() or \
                                   response.css('img.product-image::attr(src)').getall()
            
            # Extract listing ID if possible
            listing_id_match = re.search(r'/listing/([A-Za-z0-9]+)', response.url)
            if listing_id_match:
                product.asin = listing_id_match.group(1)
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Whatnot scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Whatnot parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Whatnot",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.__class__.results.append({'record_id': record_id, 'product': product})

    def parse_poshmark(self, response):
        """Parse Poshmark listing pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(
                url=original_url, 
                source="Poshmark",
                currency="USD" if "poshmark.com" in response.url else "CAD"
            )
            
            # Poshmark often has structured data in JSON-LD
            json_ld_scripts = response.css('script[type="application/ld+json"]::text').getall()
            for script_text in json_ld_scripts:
                try:
                    data = json.loads(script_text)
                    if isinstance(data, dict) and data.get('@type') == 'Product':
                        product.name = data.get('name', 'Product Name Not Found')
                        product.description = data.get('description', 'Product Description Not Found')
                        
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, dict):
                                product.sale_price = str(offers.get('price', 'Not Found'))
                                product.currency = offers.get('priceCurrency', product.currency)
                        
                        if 'image' in data:
                            images = data['image']
                            product.images = [images] if isinstance(images, str) else images
                        
                        break
                except json.JSONDecodeError:
                    continue
            
            # Fallback to HTML parsing
            if product.name == "Product Name Not Found":
                product.name = response.css('h1[data-test="listing-title"]::text').get() or \
                              response.css('.listing__title::text').get() or \
                              response.css('h1.title::text').get() or \
                              response.css('meta[property="og:title"]::attr(content)').get() or \
                              "Product Name Not Found"
            
            if product.description == "Product Description Not Found":
                product.description = response.css('[data-test="listing-description"]::text').get() or \
                                    response.css('.listing__description::text').get() or \
                                    response.css('.description-section::text').get() or \
                                    response.css('meta[property="og:description"]::attr(content)').get() or \
                                    "Product Description Not Found"
            
            if product.sale_price == "Not Found":
                # Try various price selectors
                price_text = response.css('[data-test="listing-price"]::text').get() or \
                            response.css('.listing__price::text').get() or \
                            response.css('.price::text').get() or \
                            response.css('[data-test="listing-price-amount"]::text').get()
                
                if price_text:
                    # Extract numeric value
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        product.sale_price = price_match.group(0)
                    
                    # Check for currency
                    if 'C
        """Parse Home Depot product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Home Depot CA", currency="CAD")
            
            # Try JSON-LD first
            json_ld = response.css('script[type="application/ld+json"]::text').get()
            if json_ld:
                try:
                    data = json.loads(json_ld)
                    if data.get('@type') == 'Product':
                        product.name = data.get('name', 'Product Name Not Found')
                        product.description = data.get('description', 'Product Description Not Found')
                        
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, list):
                                offers = offers[0]
                            product.sale_price = str(offers.get('price', 'Not Found'))
                        
                        if 'image' in data:
                            images = data['image']
                            product.images = [images] if isinstance(images, str) else images
                
                except json.JSONDecodeError:
                    pass
            
            # Fallback to HTML
            if product.name == "Product Name Not Found":
                product.name = response.css('h1.product-details__title::text').get() or \
                              response.css('h1[data-testid="product-title"]::text').get() or \
                              "Product Name Not Found"
            
            if product.sale_price == "Not Found":
                price_text = response.css('.price__dollars::text').get()
                if price_text:
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        product.sale_price = price_match.group(0)
            
            if not product.images:
                product.images = response.css('.mediagallery__mainimage img::attr(src)').getall()
            
            # SKU
            product.asin = response.css('.product-info-bar__detail--sku::text').get()
            if product.asin:
                product.asin = product.asin.replace('SKU', '').strip()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Home Depot scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Home Depot parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Home Depot CA",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})


# --- Airtable Integration ---
def process_airtable_records():
    """Main function to process Airtable records using Scrapy."""
    logger.info("=== 🚀 Starting Scrapy-based Multi-Site Scraper ===")
    
    if not AIRTABLE_AVAILABLE or AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        logger.error("❌ Airtable API is not configured properly.")
        return
    
    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        logger.info(f"✅ Connected to Airtable")
    except Exception as e:
        logger.error(f"❌ Airtable connection failed: {e}")
        return
    
    # Fetch records to process
    formula = f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')"
    try:
        records_to_process = table.all(formula=formula, view=AIRTABLE_VIEW_NAME)
        if not records_to_process:
            logger.info("✅ No new records to process.")
            return
        logger.info(f"Found {len(records_to_process)} records to process.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch records: {e}")
        return
    
    # Prepare URLs for Scrapy
    urls_data = []
    for record in records_to_process:
        record_id = record["id"]
        product_url = record["fields"].get(FIELD_MAPPINGS["url"])
        product_code = record["fields"].get("4more-Product-Code")
        
        # Skip if already has product code
        if product_code and str(product_code).strip():
            logger.info(f"Skipping record {record_id} - already has product code")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Linked to Catalogue",
                        FIELD_MAPPINGS["scraping_status"]: f"Skipped (Already linked: {product_code})",
                    },
                )
            except Exception as e:
                logger.error(f"Failed to update record {record_id}: {e}")
            continue
        
        if product_url and product_url.strip():
            urls_data.append({
                'url': product_url.strip(),
                'record_id': record_id
            })
    
    if not urls_data:
        logger.info("No URLs to process.")
        return
    
    # Store urls_data and table reference for the spider
    ProductSpider.urls_data = urls_data
    ProductSpider.results = []
    
    # Configure and run Scrapy
    process = CrawlerProcess(get_scrapy_settings())
    
    # Pass the spider class, not an instance
    process.crawl(ProductSpider)
    process.start()
    
    # Process results after spider finishes
    logger.info(f"\n=== Processing {len(ProductSpider.results)} results ===")
    
    for result in ProductSpider.results:
        record_id = result['record_id']
        product_data = result['product']
        
        # Prepare Airtable update
        fields_to_update = {
            FIELD_MAPPINGS["name"]: product_data.name,
            FIELD_MAPPINGS["description"]: product_data.description[:1000] if product_data.description else "",
            FIELD_MAPPINGS["dimensions"]: product_data.dimensions,
            FIELD_MAPPINGS["weight"]: product_data.weight,
            FIELD_MAPPINGS["source"]: product_data.source,
            FIELD_MAPPINGS["currency"]: product_data.currency,
            FIELD_MAPPINGS["asin"]: product_data.asin,
            FIELD_MAPPINGS["status"]: (
                "Scraped" if product_data.status == "Scraped" else "Needs Attention"
            ),
        }
        
        # Format scraping status
        scraping_status = product_data.status
        if product_data.missing_fields:
            scraping_status += f" (Missing: {', '.join(product_data.missing_fields)})"
        elif product_data.error_details:
            scraping_status += f" ({product_data.error_details[:100]})"
        fields_to_update[FIELD_MAPPINGS["scraping_status"]] = scraping_status
        
        # Handle numeric fields
        if DataValidator.validate_price(product_data.sale_price):
            try:
                fields_to_update[FIELD_MAPPINGS["sale_price"]] = float(
                    product_data.sale_price.replace(",", "")
                )
            except (ValueError, AttributeError):
                pass
        
        # Handle images
        if product_data.images:
            fields_to_update[FIELD_MAPPINGS["photos"]] = ", ".join(product_data.images[:5])
            fields_to_update[FIELD_MAPPINGS["photo_files"]] = [
                {"url": img} for img in product_data.images[:5]
            ]
        
        # Update Airtable
        try:
            table.update(record_id, fields_to_update)
            logger.info(f"✅ Record {record_id} updated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to update record {record_id}: {e}")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Needs Attention",
                        FIELD_MAPPINGS["scraping_status"]: f"Update Error: {str(e)[:100]}",
                    },
                )
            except:
                pass
    
    logger.info("\n=== ✨ Processing complete! ===")


if __name__ == "__main__":
    process_airtable_records()
 in price_text or 'CAD' in price_text:
                        product.currency = "CAD"
            
            if not product.images:
                # Try to get images
                product.images = response.css('.listing__covershot img::attr(src)').getall() or \
                               response.css('[data-test="listing-covershot"] img::attr(src)').getall() or \
                               response.css('.carousel-image img::attr(src)').getall() or \
                               response.css('img.covershot__image::attr(src)').getall()
                
                # If still no images, try meta tag
                if not product.images:
                    og_image = response.css('meta[property="og:image"]::attr(content)').get()
                    if og_image:
                        product.images = [og_image]
            
            # Extract listing ID
            listing_id_match = re.search(r'/listing/([a-f0-9]+)', response.url)
            if listing_id_match:
                product.asin = listing_id_match.group(1)
            
            # Extract brand if it's a brand page
            if '/brand/' in response.url:
                brand_name = response.css('h1.brand__name::text').get() or \
                            response.css('[data-test="brand-name"]::text').get()
                if brand_name:
                    product.name = f"{brand_name} - Brand Page"
                    product.description = f"Poshmark brand page for {brand_name}"
            
            # Clean up images - ensure they're full URLs
            cleaned_images = []
            for img in product.images:
                if img:
                    if img.startswith('//'):
                        img = 'https:' + img
                    elif img.startswith('/'):
                        img = 'https://poshmark.com' + img
                    cleaned_images.append(img)
            product.images = cleaned_images[:5]
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Poshmark scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Poshmark parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Poshmark",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.__class__.results.append({'record_id': record_id, 'product': product})

    def parse_homedepot(self, response):
        """Parse Home Depot product pages."""
        record_id = response.meta['record_id']
        original_url = response.meta['original_url']
        
        try:
            product = ProductData(url=original_url, source="Home Depot CA", currency="CAD")
            
            # Try JSON-LD first
            json_ld = response.css('script[type="application/ld+json"]::text').get()
            if json_ld:
                try:
                    data = json.loads(json_ld)
                    if data.get('@type') == 'Product':
                        product.name = data.get('name', 'Product Name Not Found')
                        product.description = data.get('description', 'Product Description Not Found')
                        
                        if 'offers' in data:
                            offers = data['offers']
                            if isinstance(offers, list):
                                offers = offers[0]
                            product.sale_price = str(offers.get('price', 'Not Found'))
                        
                        if 'image' in data:
                            images = data['image']
                            product.images = [images] if isinstance(images, str) else images
                
                except json.JSONDecodeError:
                    pass
            
            # Fallback to HTML
            if product.name == "Product Name Not Found":
                product.name = response.css('h1.product-details__title::text').get() or \
                              response.css('h1[data-testid="product-title"]::text').get() or \
                              "Product Name Not Found"
            
            if product.sale_price == "Not Found":
                price_text = response.css('.price__dollars::text').get()
                if price_text:
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        product.sale_price = price_match.group(0)
            
            if not product.images:
                product.images = response.css('.mediagallery__mainimage img::attr(src)').getall()
            
            # SKU
            product.asin = response.css('.product-info-bar__detail--sku::text').get()
            if product.asin:
                product.asin = product.asin.replace('SKU', '').strip()
            
            # Validate
            product = DataValidator.validate_product(product)
            
            logger.info(f"✅ Home Depot scrape completed for {original_url}")
            
        except Exception as e:
            logger.error(f"Home Depot parsing error: {e}")
            product = ProductData(
                url=original_url,
                source="Home Depot CA",
                status="Failed",
                error_details=f"Parsing error: {e}"
            )
        
        self.results.append({'record_id': record_id, 'product': product})


# --- Airtable Integration ---
def process_airtable_records():
    """Main function to process Airtable records using Scrapy."""
    logger.info("=== 🚀 Starting Scrapy-based Multi-Site Scraper ===")
    
    if not AIRTABLE_AVAILABLE or AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        logger.error("❌ Airtable API is not configured properly.")
        return
    
    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        logger.info(f"✅ Connected to Airtable")
    except Exception as e:
        logger.error(f"❌ Airtable connection failed: {e}")
        return
    
    # Fetch records to process
    formula = f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')"
    try:
        records_to_process = table.all(formula=formula, view=AIRTABLE_VIEW_NAME)
        if not records_to_process:
            logger.info("✅ No new records to process.")
            return
        logger.info(f"Found {len(records_to_process)} records to process.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch records: {e}")
        return
    
    # Prepare URLs for Scrapy
    urls_data = []
    for record in records_to_process:
        record_id = record["id"]
        product_url = record["fields"].get(FIELD_MAPPINGS["url"])
        product_code = record["fields"].get("4more-Product-Code")
        
        # Skip if already has product code
        if product_code and str(product_code).strip():
            logger.info(f"Skipping record {record_id} - already has product code")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Linked to Catalogue",
                        FIELD_MAPPINGS["scraping_status"]: f"Skipped (Already linked: {product_code})",
                    },
                )
            except Exception as e:
                logger.error(f"Failed to update record {record_id}: {e}")
            continue
        
        if product_url and product_url.strip():
            urls_data.append({
                'url': product_url.strip(),
                'record_id': record_id
            })
    
    if not urls_data:
        logger.info("No URLs to process.")
        return
    
    # Store urls_data and table reference for the spider
    ProductSpider.urls_data = urls_data
    ProductSpider.results = []
    
    # Configure and run Scrapy
    process = CrawlerProcess(get_scrapy_settings())
    
    # Pass the spider class, not an instance
    process.crawl(ProductSpider)
    process.start()
    
    # Process results after spider finishes
    logger.info(f"\n=== Processing {len(ProductSpider.results)} results ===")
    
    for result in ProductSpider.results:
        record_id = result['record_id']
        product_data = result['product']
        
        # Prepare Airtable update
        fields_to_update = {
            FIELD_MAPPINGS["name"]: product_data.name,
            FIELD_MAPPINGS["description"]: product_data.description[:1000] if product_data.description else "",
            FIELD_MAPPINGS["dimensions"]: product_data.dimensions,
            FIELD_MAPPINGS["weight"]: product_data.weight,
            FIELD_MAPPINGS["source"]: product_data.source,
            FIELD_MAPPINGS["currency"]: product_data.currency,
            FIELD_MAPPINGS["asin"]: product_data.asin,
            FIELD_MAPPINGS["status"]: (
                "Scraped" if product_data.status == "Scraped" else "Needs Attention"
            ),
        }
        
        # Format scraping status
        scraping_status = product_data.status
        if product_data.missing_fields:
            scraping_status += f" (Missing: {', '.join(product_data.missing_fields)})"
        elif product_data.error_details:
            scraping_status += f" ({product_data.error_details[:100]})"
        fields_to_update[FIELD_MAPPINGS["scraping_status"]] = scraping_status
        
        # Handle numeric fields
        if DataValidator.validate_price(product_data.sale_price):
            try:
                fields_to_update[FIELD_MAPPINGS["sale_price"]] = float(
                    product_data.sale_price.replace(",", "")
                )
            except (ValueError, AttributeError):
                pass
        
        # Handle images
        if product_data.images:
            fields_to_update[FIELD_MAPPINGS["photos"]] = ", ".join(product_data.images[:5])
            fields_to_update[FIELD_MAPPINGS["photo_files"]] = [
                {"url": img} for img in product_data.images[:5]
            ]
        
        # Update Airtable
        try:
            table.update(record_id, fields_to_update)
            logger.info(f"✅ Record {record_id} updated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to update record {record_id}: {e}")
            try:
                table.update(
                    record_id,
                    {
                        FIELD_MAPPINGS["status"]: "Needs Attention",
                        FIELD_MAPPINGS["scraping_status"]: f"Update Error: {str(e)[:100]}",
                    },
                )
            except:
                pass
    
    logger.info("\n=== ✨ Processing complete! ===")


if __name__ == "__main__":
    process_airtable_records()
