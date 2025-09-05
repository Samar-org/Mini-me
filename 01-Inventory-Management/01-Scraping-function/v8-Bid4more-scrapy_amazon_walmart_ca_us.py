#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Complete Standalone Scrapy Scraper for Amazon/Walmart with Airtable Integration
No external middleware dependencies required - all components included
"""

import scrapy
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
import json
import re
import os
import logging
import random
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import hashlib
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ========== CONFIGURATION ==========
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

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.2277.83',
]

# ========== DATA CLASSES ==========
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
    record_id: Optional[str] = None


# ========== CUSTOM MIDDLEWARE ==========
class RandomUserAgentMiddleware:
    """Middleware to rotate User-Agent headers."""
    
    def process_request(self, request, spider):
        ua = random.choice(USER_AGENTS)
        request.headers['User-Agent'] = ua
        spider.logger.debug(f'Using User-Agent: {ua[:50]}...')
        return None


class CustomRetryMiddleware:
    """Custom retry middleware with better handling and progressive delays."""
    
    def __init__(self, settings):
        self.max_retry_times = settings.getint('RETRY_TIMES', 2)
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.bot_detection_pause = 30  # Pause 30 seconds on bot detection
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    
    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        
        # Check for bot detection in successful responses
        if response.status == 200:
            text_lower = response.text.lower()
            bot_patterns = [
                'robot or human', 
                'captcha', 
                'access denied',
                'blocked',
                'security check',
                'verify you are human'
            ]
            if any(pattern in text_lower for pattern in bot_patterns):
                spider.logger.warning(f"Bot detection triggered for {request.url}")
                reason = 'Bot detection triggered'
                
                # On bot detection, pause longer before retry
                retries = request.meta.get('retry_times', 0)
                if retries == 0:
                    spider.logger.info(f"Pausing {self.bot_detection_pause} seconds due to bot detection...")
                    time.sleep(self.bot_detection_pause)
                
                return self._retry(request, reason, spider) or response
        
        # Handle Walmart's specific blocking (307 to /blocked)
        if response.status == 307 or '/blocked' in response.url:
            spider.logger.warning(f"Walmart blocking detected: {response.url}")
            # Don't retry Walmart blocks - they won't work
            return response
        
        # Handle error status codes
        if response.status in self.retry_http_codes:
            reason = f'HTTP {response.status}'
            return self._retry(request, reason, spider) or response
        
        return response
    
    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1
        
        if retries <= self.max_retry_times:
            # Progressive delay: 10s, 30s, 60s
            delay = min(10 * (2 ** (retries - 1)), 60)
            spider.logger.info(f'Retrying {request.url} (attempt {retries}/{self.max_retry_times}): {reason}. Waiting {delay}s')
            
            # Add delay
            time.sleep(delay)
            
            retry_req = request.copy()
            retry_req.meta['retry_times'] = retries
            retry_req.dont_filter = True
            
            # Change user agent on retry
            retry_req.headers['User-Agent'] = random.choice(USER_AGENTS)
            
            # Clear cookies on retry
            retry_req.meta['dont_merge_cookies'] = True
            
            return retry_req
        else:
            spider.logger.warning(f'Gave up retrying {request.url} after {retries} attempts: {reason}')
            return None


# ========== MAIN SPIDER ==========
class AmazonWalmartSpider(Spider):
    name = 'amazon_walmart_spider'
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }
    
    # Image filtering patterns
    EXCLUDE_IMAGE_PATTERNS = [
        'prime-logo', 'prime_logo', 'prime-badge', 'prime_badge',
        'amazon-prime', 'amazon_prime', 's-prime', 'icon', 'badge',
        'logo', 'sprite', 'transparent-pixel', 'blank.gif',
        'play-button', 'video-thumb', '1x1', 'x-locale',
        'customer-review', 'star', 'rating'
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.airtable = None
        self.setup_airtable()
    
    def setup_airtable(self):
        """Initialize Airtable connection."""
        try:
            from pyairtable import Api
            if AIRTABLE_API_KEY != "YOUR_AIRTABLE_API_KEY_HERE":
                api = Api(AIRTABLE_API_KEY)
                self.airtable = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
                self.logger.info("✅ Connected to Airtable successfully")
            else:
                self.logger.error("❌ Airtable API key not configured")
        except ImportError:
            self.logger.error("❌ pyairtable not installed. Run: pip install pyairtable")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Airtable: {e}")
    
    def start_requests(self):
        """Generate requests from Airtable records."""
        if not self.airtable:
            self.logger.error("Cannot proceed without Airtable connection")
            return
        
        try:
            formula = f"AND({{{FIELD_MAPPINGS['status']}}}='Entered')"
            records = self.airtable.all(formula=formula, view=AIRTABLE_VIEW_NAME)
            
            if not records:
                self.logger.info("No records to process")
                return
            
            self.logger.info(f"📊 Found {len(records)} records to process")
            
            for i, record in enumerate(records, 1):
                record_id = record["id"]
                product_url = record["fields"].get(FIELD_MAPPINGS["url"])
                product_code = record["fields"].get("4more-Product-Code")
                
                self.logger.info(f"\n--- Processing Record {i}/{len(records)} (ID: {record_id}) ---")
                
                # Skip if product code already exists
                if product_code and str(product_code).strip():
                    self.logger.info(f"✓ Skipping - already has product code: {product_code}")
                    self.update_airtable_status(record_id, "Linked to Catalogue", 
                                               f"Skipped (Already linked: {product_code})")
                    continue
                
                if not product_url or not product_url.strip():
                    self.logger.warning(f"⚠️ Skipping - no URL for record {record_id}")
                    self.update_airtable_status(record_id, "Needs Attention", "No URL provided")
                    continue
                
                product_url = product_url.strip()
                
                # Fix missing URL scheme
                if not product_url.startswith(('http://', 'https://')):
                    if 'amazon' in product_url or 'a.co' in product_url:
                        product_url = 'https://' + product_url
                    elif 'walmart' in product_url:
                        product_url = 'https://' + product_url
                    else:
                        product_url = 'https://' + product_url
                    self.logger.info(f"Fixed URL scheme: {product_url}")
                
                # Determine callback based on URL
                if any(domain in product_url for domain in ["amazon.", "amzn.to", "a.co"]):
                    callback = self.parse_amazon
                elif "walmart" in product_url:
                    callback = self.parse_walmart
                else:
                    self.logger.warning(f"⚠️ Unsupported URL: {product_url}")
                    self.update_airtable_status(record_id, "Needs Attention", "Unsupported website")
                    continue
                
                # Create request
                headers = self._get_custom_headers(product_url)
                headers['User-Agent'] = random.choice(USER_AGENTS)
                
                yield Request(
                    url=product_url,
                    callback=callback,
                    meta={
                        'record_id': record_id,
                        'original_url': product_url,
                        'handle_httpstatus_list': [403, 503],
                    },
                    headers=headers,
                    cookies=self._get_cookies(product_url),
                    dont_filter=True
                )
                
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch Airtable records: {e}")
    
    def _get_custom_headers(self, url: str) -> Dict:
        """Get custom headers based on URL."""
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        if "amazon" in url:
            headers['Referer'] = 'https://www.google.com/'
        
        return headers
    
    def _get_cookies(self, url: str) -> Dict:
        """Get cookies based on URL."""
        cookies = {}
        
        if "amazon" in url:
            cookies['i18n-prefs'] = 'CAD' if '.ca' in url else 'USD'
            cookies['session-id'] = ''.join(random.choices('0123456789', k=15))
        elif "walmart" in url:
            cookies['vtc'] = 'VGVzdENvb2tpZQ=='
            
        return cookies
    
    def parse_amazon(self, response):
        """Parse Amazon product page."""
        self.logger.info(f"🛒 Parsing Amazon page: {response.url}")
        
        # Check for bot detection
        if self._is_blocked(response):
            self.logger.warning(f"🤖 Bot detection on Amazon: {response.url}")
            self.handle_failed_scrape(response, "Blocked by Amazon (Bot Detection)")
            return
        
        product = ProductData(
            url=response.url,
            source="Amazon",
            record_id=response.meta.get('record_id')
        )
        
        # Extract all fields
        product.asin = self._extract_amazon_asin(response)
        product.name = self._extract_amazon_name(response)
        product.description = self._extract_amazon_description(response)
        product.images = self._extract_amazon_images(response)
        product.sale_price, product.currency = self._extract_amazon_price(response)
        
        # Extract details
        details = self._extract_amazon_details(response)
        product.dimensions = details.get('dimensions', 'Product Dimensions Not Found')
        product.weight = details.get('weight', 'Product Weight Not Found')
        
        # Validate and update Airtable
        self.process_product(product)
    
    def parse_walmart(self, response):
        """Parse Walmart product page."""
        self.logger.info(f"🛍️ Parsing Walmart page: {response.url}")
        
        # Check for bot detection
        if self._is_blocked(response):
            self.logger.warning(f"🤖 Bot detection on Walmart: {response.url}")
            self.handle_failed_scrape(response, "Blocked by Walmart (Bot Detection)")
            return
        
        product = ProductData(
            url=response.url,
            source="Walmart",
            record_id=response.meta.get('record_id')
        )
        
        # Try JSON extraction first
        json_data = self._extract_walmart_json(response)
        
        if json_data:
            product_info = json_data.get("product", {})
            product.asin = product_info.get("id")
            product.name = product_info.get("name", "Product Name Not Found")
            product.description = product_info.get("shortDescription", "")[:1000]
            
            price_info = json_data.get("price", {}).get("item", {}).get("price")
            if price_info:
                product.sale_price = str(price_info)
                product.currency = "CAD" if ".ca" in response.url else "USD"
            
            image_info = product_info.get("imageInfo", {})
            if image_info and "allImages" in image_info:
                product.images = [img.get("url") for img in image_info["allImages"] if img.get("url")][:10]
        else:
            # Fallback to HTML extraction
            product.name = self._extract_walmart_name(response)
            product.description = self._extract_walmart_description(response)
            product.images = self._extract_walmart_images(response)
            product.sale_price, product.currency = self._extract_walmart_price(response)
        
        # Validate and update Airtable
        self.process_product(product)
    
    def _is_blocked(self, response) -> bool:
        """Check if response indicates bot detection."""
        text_lower = response.text.lower()
        blocked_indicators = [
            "robot or human", "are you a robot", "enter the characters",
            "captcha", "access denied", "blocked", "unusual traffic"
        ]
        return any(indicator in text_lower for indicator in blocked_indicators)
    
    # ========== AMAZON EXTRACTION METHODS ==========
    def _extract_amazon_asin(self, response) -> Optional[str]:
        """Extract ASIN from Amazon page."""
        # From URL
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', response.url)
        if asin_match:
            return asin_match.group(1)
        
        # From page
        asin = response.css('input#ASIN::attr(value)').get()
        if asin:
            return asin
        
        asin = response.css('[data-asin]::attr(data-asin)').get()
        return asin
    
    def _extract_amazon_name(self, response) -> str:
        """Extract product name from Amazon page."""
        selectors = [
            'span#productTitle::text',
            'h1.a-size-large span::text',
            'h1#title span::text',
            'h1.a-size-base-plus::text',
            'meta[property="og:title"]::attr(content)',
            'title::text'
        ]
        
        for selector in selectors:
            name = response.css(selector).get()
            if name:
                name = name.strip()
                # Clean up title if from meta/title tags
                if 'Amazon' in name:
                    name = name.split(':')[0].split('-')[0].strip()
                if len(name) > 3 and not name.lower().startswith('robot'):
                    return name
        
        return "Product Name Not Found"
    
    def _extract_amazon_description(self, response) -> str:
        """Extract product description from Amazon page."""
        # Feature bullets
        bullets = response.css('#feature-bullets li span.a-list-item::text').getall()
        if bullets:
            bullets = [b.strip() for b in bullets if b.strip() and len(b.strip()) > 10]
            bullets = [b for b in bullets if not b.startswith('Make sure')]
            if bullets:
                return '. '.join(bullets[:5])
        
        # Product description
        desc = response.css('#productDescription p::text').get()
        if desc:
            return desc.strip()[:1000]
        
        return "Product Description Not Found"
    
    def _extract_amazon_images(self, response) -> List[str]:
        """Extract and filter product images from Amazon page."""
        images = set()
        
        def is_valid_image(url: str) -> bool:
            if not url or not url.startswith('http'):
                return False
            url_lower = url.lower()
            return not any(pattern in url_lower for pattern in self.EXCLUDE_IMAGE_PATTERNS)
        
        def clean_image_url(url: str) -> str:
            url = re.sub(r'\._[A-Z0-9_,]+_\.', '.', url)
            url = re.sub(r'(\?|&)(psc|ref|tag)=[^&]*', '', url)
            return url
        
        # Try multiple extraction methods
        
        # Method 1: Dynamic image data
        dynamic_images = response.css('[data-a-dynamic-image]::attr(data-a-dynamic-image)').get()
        if dynamic_images:
            try:
                img_dict = json.loads(dynamic_images)
                for url in list(img_dict.keys())[:10]:
                    if is_valid_image(url):
                        images.add(clean_image_url(url))
            except:
                pass
        
        # Method 2: Direct image selectors
        img_selectors = [
            'img#landingImage::attr(data-old-hires)',
            'img#landingImage::attr(data-a-dynamic-image)',
            'img#landingImage::attr(src)',
            'img.a-dynamic-image::attr(src)',
            '.imgTagWrapper img::attr(src)',
            '.imageThumb img::attr(src)'
        ]
        
        for selector in img_selectors:
            urls = response.css(selector).getall()
            for url in urls[:10]:
                if is_valid_image(url):
                    images.add(clean_image_url(url))
        
        # Method 3: JavaScript image data
        scripts = response.css('script::text').getall()
        for script in scripts:
            if 'colorImages' in script:
                try:
                    # Extract image URLs from JavaScript
                    matches = re.findall(r'"hiRes":"([^"]+)"', script)
                    for url in matches[:10]:
                        if is_valid_image(url):
                            images.add(clean_image_url(url))
                except:
                    pass
        
        return list(images)[:10]
    
    def _extract_amazon_price(self, response) -> tuple:
        """Extract price and currency from Amazon page."""
        price_selectors = [
            '.a-price.a-text-price.a-size-medium span::text',
            '.a-price span.a-offscreen::text',
            '.a-price-whole::text',
            'span.a-price.a-text-price span::text',
            '.a-color-price::text'
        ]
        
        for selector in price_selectors:
            price_text = response.css(selector).get()
            if price_text:
                # Extract numeric value
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    price = price_match.group(0)
                    # Determine currency
                    currency = 'CAD' if '.ca' in response.url else 'USD'
                    if 'C$' in price_text:
                        currency = 'CAD'
                    elif '$' in price_text and '.com' in response.url:
                        currency = 'USD'
                    return price, currency
        
        return "Not Found", "USD"
    
    def _extract_amazon_details(self, response) -> Dict:
        """Extract product dimensions and weight."""
        details = {}
        
        # Check detail bullets
        detail_items = response.css('#detailBullets_feature_div li')
        for item in detail_items:
            text = item.css('::text').getall()
            text = ' '.join(text).lower()
            
            if 'dimension' in text:
                value = item.css('span:last-child::text').get()
                if value:
                    details['dimensions'] = value.strip()
            elif 'weight' in text:
                value = item.css('span:last-child::text').get()
                if value:
                    details['weight'] = value.strip()
        
        return details
    
    # ========== WALMART EXTRACTION METHODS ==========
    def _extract_walmart_json(self, response) -> Optional[Dict]:
        """Extract JSON data from Walmart page."""
        next_data = response.css('script#__NEXT_DATA__::text').get()
        if next_data:
            try:
                data = json.loads(next_data)
                return data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('data', {})
            except:
                pass
        return None
    
    def _extract_walmart_name(self, response) -> str:
        """Extract product name from Walmart page."""
        selectors = [
            'h1[itemprop="name"]::text',
            'h1[data-automation-id="product-title"]::text',
            'h1.prod-ProductTitle::text',
            'h1::text'
        ]
        
        for selector in selectors:
            name = response.css(selector).get()
            if name and len(name.strip()) > 3:
                return name.strip()
        
        return "Product Name Not Found"
    
    def _extract_walmart_description(self, response) -> str:
        """Extract product description from Walmart page."""
        selectors = [
            '[data-automation-id="product-highlights"]::text',
            '.about-desc::text',
            '.prod-ProductHighlights li::text'
        ]
        
        for selector in selectors:
            texts = response.css(selector).getall()
            if texts:
                desc = ' '.join(texts).strip()
                if len(desc) > 10:
                    return desc[:1000]
        
        return "Product Description Not Found"
    
    def _extract_walmart_images(self, response) -> List[str]:
        """Extract product images from Walmart page."""
        images = []
        
        selectors = [
            'img[data-testid="hero-image"]::attr(src)',
            '.prod-hero-image img::attr(src)',
            'img[itemprop="image"]::attr(src)',
            '.prod-alt-image img::attr(src)'
        ]
        
        for selector in selectors:
            urls = response.css(selector).getall()
            for url in urls:
                if url and not url.startswith('data:'):
                    if not url.startswith('http'):
                        url = 'https:' + url if url.startswith('//') else f'https://i5.walmartimages.com{url}'
                    images.append(url)
        
        return list(set(images))[:10]
    
    def _extract_walmart_price(self, response) -> tuple:
        """Extract price and currency from Walmart page."""
        selectors = [
            '[itemprop="price"]::attr(content)',
            '[data-automation-id="product-price"] span::text',
            'span[itemprop="price"]::text',
            '.price-now::text'
        ]
        
        for selector in selectors:
            price_text = response.css(selector).get()
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    price = price_match.group(0)
                    currency = 'CAD' if '.ca' in response.url else 'USD'
                    return price, currency
        
        return "Not Found", "USD"
    
    # ========== AIRTABLE METHODS ==========
    def process_product(self, product: ProductData):
        """Validate product and update Airtable."""
        # Validate
        missing = []
        if product.name == "Product Name Not Found" or len(product.name) < 3:
            missing.append("name")
        if not product.images:
            missing.append("images")
        
        product.missing_fields = missing
        product.status = "Needs Attention" if missing else "Scraped"
        
        # Update Airtable
        self.update_airtable_product(product)
    
    def update_airtable_product(self, product: ProductData):
        """Update Airtable with product data."""
        if not self.airtable or not product.record_id:
            return
        
        try:
            fields = {
                FIELD_MAPPINGS["name"]: product.name,
                FIELD_MAPPINGS["description"]: product.description,
                FIELD_MAPPINGS["dimensions"]: product.dimensions,
                FIELD_MAPPINGS["weight"]: product.weight,
                FIELD_MAPPINGS["source"]: product.source,
                FIELD_MAPPINGS["currency"]: product.currency,
                FIELD_MAPPINGS["status"]: product.status,
            }
            
            if product.asin:
                fields[FIELD_MAPPINGS["asin"]] = product.asin
            
            # Scraping status
            status_msg = product.status
            if product.missing_fields:
                status_msg += f" (Missing: {', '.join(product.missing_fields)})"
            fields[FIELD_MAPPINGS["scraping_status"]] = status_msg
            
            # Price
            if product.sale_price != "Not Found":
                try:
                    fields[FIELD_MAPPINGS["sale_price"]] = float(product.sale_price.replace(',', ''))
                except:
                    pass
            
            # Images
            if product.images:
                fields[FIELD_MAPPINGS["photos"]] = ", ".join(product.images)
                fields[FIELD_MAPPINGS["photo_files"]] = [{"url": img} for img in product.images[:5]]
            
            self.airtable.update(product.record_id, fields)
            self.logger.info(f"✅ Updated Airtable record: {product.record_id}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update Airtable: {e}")
    
    def update_airtable_status(self, record_id: str, status: str, message: str):
        """Update just the status fields in Airtable."""
        if not self.airtable:
            return
        
        # Map "Failed" to "Needs Attention" since Airtable doesn't have "Failed" option
        if status == "Failed":
            status = "Needs Attention"
        
        try:
            fields = {
                FIELD_MAPPINGS["status"]: status,
                FIELD_MAPPINGS["scraping_status"]: message
            }
            self.airtable.update(record_id, fields)
        except Exception as e:
            self.logger.error(f"Failed to update status: {e}")
    
    def handle_failed_scrape(self, response, error_msg: str):
        """Handle failed scrape attempt."""
        record_id = response.meta.get('record_id')
        if record_id:
            # Use "Needs Attention" instead of "Failed"
            self.update_airtable_status(record_id, "Needs Attention", error_msg)


# ========== MAIN EXECUTION ==========
def run_scraper():
    """Run the Scrapy spider with all settings."""
    
    # Configure settings
    settings = {
        # Basic settings
        'BOT_NAME': 'amazon_walmart_scraper',
        'ROBOTSTXT_OBEY': False,
        
        # User agent (will be overridden by middleware)
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        
        # Download delays and concurrency - INCREASED DELAYS
        'DOWNLOAD_DELAY': 8,  # Increased from 3 to 8 seconds
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        
        # AutoThrottle - More conservative settings
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 8,
        'AUTOTHROTTLE_MAX_DELAY': 120,  # Increased from 60
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.5,  # Reduced from 1.0
        'AUTOTHROTTLE_DEBUG': True,
        
        # Retry settings
        'RETRY_TIMES': 2,  # Reduced from 3 to fail faster on bot detection
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],  # Removed 403
        
        # Timeout
        'DOWNLOAD_TIMEOUT': 45,  # Increased from 30
        
        # Cache
        'HTTPCACHE_ENABLED': False,
        
        # Cookies
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': True,  # Enable cookie debugging
        
        # Telnet Console (disable for production)
        'TELNETCONSOLE_ENABLED': False,
        
        # Logging
        'LOG_LEVEL': 'INFO',
        'LOG_FORMAT': '%(levelname)s: %(message)s',
        
        # Custom middleware
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            f'{__name__}.RandomUserAgentMiddleware': 400,
            f'{__name__}.CustomRetryMiddleware': 500,
        }
    }
    
    # Create crawler process
    process = CrawlerProcess(settings)
    
    # Add spider
    process.crawl(AmazonWalmartSpider)
    
    # Start crawling
    print("\n" + "="*60)
    print("🚀 Starting Amazon/Walmart Scraper with Airtable Integration")
    print("="*60 + "\n")
    
    process.start()
    
    print("\n" + "="*60)
    print("✅ Scraping completed!")
    print("="*60 + "\n")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Check requirements
    print("Checking requirements...")
    
    try:
        import scrapy
        print("✓ Scrapy installed")
    except ImportError:
        print("✗ Scrapy not installed. Run: pip install scrapy")
        exit(1)
    
    try:
        from pyairtable import Api
        print("✓ PyAirtable installed")
    except ImportError:
        print("✗ PyAirtable not installed. Run: pip install pyairtable")
        exit(1)
    
    try:
        from dotenv import load_dotenv
        print("✓ python-dotenv installed")
    except ImportError:
        print("⚠ python-dotenv not installed. Using environment variables only.")
    
    # Check configuration
    if AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        print("\n⚠️  Warning: Airtable API key not configured!")
        print("Please set AIRTABLE_API_KEY in your .env file or environment variables")
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            exit(0)
    
    # Run the scraper
    run_scraper()