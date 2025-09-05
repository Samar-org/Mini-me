#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Optimized Standalone Scrapy Scraper for Amazon/Walmart with Airtable Integration
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
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from urllib.parse import urlparse
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

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
    """Optimized data class for structured product information."""
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
    """Optimized middleware to rotate User-Agent headers with caching."""
    
    def __init__(self):
        self.ua_cache = {}
        
    def process_request(self, request, spider):
        domain = urlparse(request.url).netloc
        if domain not in self.ua_cache:
            self.ua_cache[domain] = random.choice(USER_AGENTS)
        request.headers['User-Agent'] = self.ua_cache[domain]
        return None


class SmartRetryMiddleware:
    """Optimized retry middleware with adaptive delays and domain-specific handling."""
    
    def __init__(self, settings):
        self.max_retry_times = settings.getint('RETRY_TIMES', 2)
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.domain_delays = {}  # Track delays per domain
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    
    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        
        domain = urlparse(request.url).netloc
        
        # Handle bot detection
        if self._is_bot_detected(response):
            spider.logger.warning(f"Bot detection triggered for {request.url}")
            return self._handle_retry(request, 'Bot detection', spider, domain)
        
        # Handle Walmart blocking
        if response.status == 307 or '/blocked' in response.url:
            spider.logger.warning(f"Walmart blocking detected: {response.url}")
            return response
        
        # Handle error status codes
        if response.status in self.retry_http_codes:
            return self._handle_retry(request, f'HTTP {response.status}', spider, domain)
        
        return response
    
    def _is_bot_detected(self, response) -> bool:
        """Optimized bot detection check."""
        if response.status != 200:
            return False
            
        text_lower = response.text.lower()
        bot_patterns = {
            'robot or human', 'captcha', 'access denied',
            'blocked', 'security check', 'verify you are human'
        }
        return any(pattern in text_lower for pattern in bot_patterns)
    
    def _handle_retry(self, request, reason, spider, domain):
        """Centralized retry handling with adaptive delays."""
        retries = request.meta.get('retry_times', 0) + 1
        
        if retries > self.max_retry_times:
            spider.logger.warning(f'Gave up retrying {request.url} after {retries} attempts: {reason}')
            return None
        
        # Adaptive delay based on domain and retry count
        if domain not in self.domain_delays:
            self.domain_delays[domain] = 5  # Start with 5s delay for new domains
        
        delay = min(self.domain_delays[domain] * (retries ** 1.5), 120)  # Fixed parenthesis
        spider.logger.info(f'Retrying {request.url} (attempt {retries}/{self.max_retry_times}): {reason}. Waiting {delay}s')
        
        # Update domain delay
        self.domain_delays[domain] = delay * 1.2  # Increase base delay for future requests
        
        time.sleep(delay)
        
        retry_req = request.copy()
        retry_req.meta['retry_times'] = retries
        retry_req.dont_filter = True
        retry_req.headers['User-Agent'] = random.choice(USER_AGENTS)
        retry_req.meta['dont_merge_cookies'] = True
        
        return retry_req

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
    EXCLUDE_IMAGE_PATTERNS = frozenset([
        'prime-logo', 'prime_logo', 'prime-badge', 'prime_badge',
        'amazon-prime', 'amazon_prime', 's-prime', 'icon', 'badge',
        'logo', 'sprite', 'transparent-pixel', 'blank.gif',
        'play-button', 'video-thumb', '1x1', 'x-locale',
        'customer-review', 'star', 'rating'
    ])
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.airtable = None
        self.setup_airtable()
        self.domain_stats = {}  # Track stats per domain
        self.executor = ThreadPoolExecutor(max_workers=4)  # For parallel image processing
    
    def setup_airtable(self):
        """Optimized Airtable connection setup with lazy loading."""
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
            self.logger.error(f"❌ Failed to connect to Airtable: {str(e)[:200]}")  # Truncate long errors
    
    def start_requests(self):
        """Optimized request generation with batch processing."""
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
            
            for record in records:
                yield from self._process_record(record)
                
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch Airtable records: {str(e)[:200]}")
    
    def _process_record(self, record) -> Optional[Request]:
        """Process a single Airtable record and yield Request if needed."""
        record_id = record["id"]
        fields = record.get("fields", {})
        product_url = fields.get(FIELD_MAPPINGS["url"], "").strip()
        product_code = fields.get("4more-Product-Code", "").strip()
        
        # Skip if product code already exists
        if product_code:
            self.logger.info(f"✓ Skipping - already has product code: {product_code}")
            self.update_airtable_status(record_id, "Linked to Catalogue", 
                                      f"Skipped (Already linked: {product_code})")
            return None
        
        if not product_url:
            self.logger.warning(f"⚠️ Skipping - no URL for record {record_id}")
            self.update_airtable_status(record_id, "Needs Attention", "No URL provided")
            return None
        
        # Fix URL scheme
        product_url = self._fix_url_scheme(product_url)
        
        # Determine callback based on URL
        if any(domain in product_url for domain in ["amazon.", "amzn.to", "a.co"]):
            callback = self.parse_amazon
        elif "walmart" in product_url:
            callback = self.parse_walmart
        else:
            self.logger.warning(f"⚠️ Unsupported URL: {product_url}")
            self.update_airtable_status(record_id, "Needs Attention", "Unsupported website")
            return None
        
        # Create request with optimized headers
        headers = self._get_custom_headers(product_url)
        cookies = self._get_cookies(product_url)
        
        return Request(
            url=product_url,
            callback=callback,
            meta={
                'record_id': record_id,
                'original_url': product_url,
                'handle_httpstatus_list': [403, 503],
                'dont_retry': False,
            },
            headers=headers,
            cookies=cookies,
            dont_filter=True
        )
    
    def _fix_url_scheme(self, url: str) -> str:
        """Ensure URL has proper scheme."""
        if not url.startswith(('http://', 'https://')):
            if any(x in url for x in ["amazon", "a.co", "walmart"]):
                return 'https://' + url
            return 'https://' + url
        return url
    
    def _get_custom_headers(self, url: str) -> Dict:
        """Optimized header generation with domain-specific settings."""
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
        """Optimized cookie generation."""
        if "amazon" in url:
            return {
                'i18n-prefs': 'CAD' if '.ca' in url else 'USD',
                'session-id': ''.join(random.choices('0123456789', k=15))
            }
        elif "walmart" in url:
            return {'vtc': 'VGVzdENvb2tpZQ=='}
        return {}
    
    def parse_amazon(self, response):
        """Optimized Amazon parser with early termination."""
        if self._is_blocked(response):
            self.logger.warning(f"🤖 Bot detection on Amazon: {response.url}")
            self.handle_failed_scrape(response, "Blocked by Amazon (Bot Detection)")
            return
        
        product = ProductData(
            url=response.url,
            source="Amazon",
            record_id=response.meta.get('record_id')
        )
        
        # Parallel extraction of fields
        with ThreadPoolExecutor(max_workers=3) as executor:
            product.asin = executor.submit(self._extract_amazon_asin, response).result()
            product.name = executor.submit(self._extract_amazon_name, response).result()
            product.description = executor.submit(self._extract_amazon_description, response).result()
            product.images = executor.submit(self._extract_amazon_images, response).result()
            product.sale_price, product.currency = executor.submit(self._extract_amazon_price, response).result()
            
            details = executor.submit(self._extract_amazon_details, response).result()
            product.dimensions = details.get('dimensions', 'Product Dimensions Not Found')
            product.weight = details.get('weight', 'Product Weight Not Found')
        
        self.process_product(product)
    
    def parse_walmart(self, response):
        """Optimized Walmart parser with JSON-first approach."""
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
            product.description = (product_info.get("shortDescription") or "")[:1000]
            
            price_info = json_data.get("price", {}).get("item", {}).get("price")
            if price_info:
                product.sale_price = str(price_info)
                product.currency = "CAD" if ".ca" in response.url else "USD"
            
            image_info = product_info.get("imageInfo", {})
            if image_info and "allImages" in image_info:
                product.images = [img.get("url") for img in image_info["allImages"] if img.get("url")][:10]
        else:
            # Fallback to HTML extraction
            with ThreadPoolExecutor(max_workers=3) as executor:
                product.name = executor.submit(self._extract_walmart_name, response).result()
                product.description = executor.submit(self._extract_walmart_description, response).result()
                product.images = executor.submit(self._extract_walmart_images, response).result()
                product.sale_price, product.currency = executor.submit(self._extract_walmart_price, response).result()
        
        self.process_product(product)
    
    def _is_blocked(self, response) -> bool:
        """Optimized bot detection check."""
        if response.status != 200:
            return False
            
        text_lower = response.text.lower()
        blocked_indicators = {
            "robot or human", "are you a robot", "enter the characters",
            "captcha", "access denied", "blocked", "unusual traffic"
        }
        return any(indicator in text_lower for indicator in blocked_indicators)
    
    # ========== OPTIMIZED EXTRACTION METHODS ==========
    def _extract_amazon_asin(self, response) -> Optional[str]:
        """Optimized ASIN extraction."""
        # From URL first (fastest)
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', response.url)
        if asin_match:
            return asin_match.group(1)
        
        # Then from common elements
        return (response.css('input#ASIN::attr(value)').get() or 
                response.css('[data-asin]::attr(data-asin)').get())
    
    def _extract_amazon_name(self, response) -> str:
        """Optimized name extraction with early termination."""
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
                if len(name) > 3 and not name.lower().startswith('robot'):
                    if 'Amazon' in name:  # Clean up meta/title tags
                        return name.split(':')[0].split('-')[0].strip()
                    return name
        return "Product Name Not Found"
    
    def _extract_amazon_description(self, response) -> str:
        """Optimized description extraction."""
        # Feature bullets first (most common)
        bullets = response.css('#feature-bullets li span.a-list-item::text').getall()
        if bullets:
            bullets = [b.strip() for b in bullets if b.strip() and len(b.strip()) > 10]
            bullets = [b for b in bullets if not b.startswith('Make sure')]
            if bullets:
                return '. '.join(bullets[:5])
        
        # Then product description
        desc = response.css('#productDescription p::text').get()
        if desc:
            return desc.strip()[:1000]
        
        return "Product Description Not Found"
    
    def _extract_amazon_images(self, response) -> List[str]:
        """Optimized image extraction with parallel processing."""
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
        
        # Method 1: Dynamic image data (fastest)
        dynamic_images = response.css('[data-a-dynamic-image]::attr(data-a-dynamic-image)').get()
        if dynamic_images:
            try:
                img_dict = json.loads(dynamic_images)
                images.update(clean_image_url(url) for url in list(img_dict.keys())[:10] if is_valid_image(url))
                if images:  # Early return if we found good images
                    return list(images)[:10]
            except json.JSONDecodeError:
                pass
        
        # Method 2: Direct image selectors
        img_selectors = [
            'img#landingImage::attr(data-old-hires)',
            'img#landingImage::attr(src)',
            'img.a-dynamic-image::attr(src)',
            '.imgTagWrapper img::attr(src)',
        ]
        
        for selector in img_selectors:
            for url in response.css(selector).getall()[:10]:
                if is_valid_image(url):
                    images.add(clean_image_url(url))
        
        # Method 3: JavaScript image data (last resort)
        for script in response.css('script::text').getall():
            if 'colorImages' in script:
                try:
                    matches = re.findall(r'"hiRes":"([^"]+)"', script)
                    images.update(clean_image_url(url) for url in matches[:10] if is_valid_image(url))
                except Exception:
                    pass
        
        return list(images)[:10]
    
    def _extract_amazon_price(self, response) -> Tuple[str, str]:
        """Optimized price extraction."""
        price_selectors = [
            '.a-price span.a-offscreen::text',  # Most common
            '.a-price-whole::text',
            'span.a-price.a-text-price span::text',
        ]
        
        for selector in price_selectors:
            price_text = response.css(selector).get()
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    currency = 'CAD' if '.ca' in response.url else 'USD'
                    if 'C$' in price_text:
                        currency = 'CAD'
                    elif '$' in price_text and '.com' in response.url:
                        currency = 'USD'
                    return price_match.group(0), currency
        
        return "Not Found", "USD"
    
    def _extract_amazon_details(self, response) -> Dict:
        """Optimized details extraction."""
        details = {}
        detail_items = response.css('#detailBullets_feature_div li')
        
        for item in detail_items:
            text = ' '.join(item.css('::text').getall()).lower()
            value = item.css('span:last-child::text').get()
            
            if value:
                if 'dimension' in text:
                    details['dimensions'] = value.strip()
                elif 'weight' in text:
                    details['weight'] = value.strip()
        
        return details
    
    def _extract_walmart_json(self, response) -> Optional[Dict]:
        """Optimized JSON extraction."""
        next_data = response.css('script#__NEXT_DATA__::text').get()
        if next_data:
            try:
                data = json.loads(next_data)
                return data.get('props', {}).get('pageProps', {}).get('initialData', {}).get('data', {})
            except json.JSONDecodeError:
                pass
        return None
    
    def _extract_walmart_name(self, response) -> str:
        """Optimized name extraction."""
        selectors = [
            'h1[itemprop="name"]::text',
            'h1[data-automation-id="product-title"]::text',
            'h1.prod-ProductTitle::text',
        ]
        
        for selector in selectors:
            name = response.css(selector).get()
            if name and len(name.strip()) > 3:
                return name.strip()
        
        return "Product Name Not Found"
    
    def _extract_walmart_description(self, response) -> str:
        """Optimized description extraction."""
        selectors = [
            '[data-automation-id="product-highlights"]::text',
            '.about-desc::text',
            '.prod-ProductHighlights li::text'
        ]
        
        for selector in selectors:
            texts = [t.strip() for t in response.css(selector).getall() if t.strip()]
            if texts:
                desc = ' '.join(texts)
                if len(desc) > 10:
                    return desc[:1000]
        
        return "Product Description Not Found"
    
    def _extract_walmart_images(self, response) -> List[str]:
        """Optimized image extraction."""
        images = set()
        selectors = [
            'img[data-testid="hero-image"]::attr(src)',
            '.prod-hero-image img::attr(src)',
            'img[itemprop="image"]::attr(src)',
        ]
        
        for selector in selectors:
            for url in response.css(selector).getall():
                if url and not url.startswith('data:'):
                    if not url.startswith('http'):
                        url = 'https:' + url if url.startswith('//') else f'https://i5.walmartimages.com{url}'
                    images.add(url)
        
        return list(images)[:10]
    
    def _extract_walmart_price(self, response) -> Tuple[str, str]:
        """Optimized price extraction."""
        selectors = [
            '[itemprop="price"]::attr(content)',
            '[data-automation-id="product-price"] span::text',
        ]
        
        for selector in selectors:
            price_text = response.css(selector).get()
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    currency = 'CAD' if '.ca' in response.url else 'USD'
                    return price_match.group(0), currency
        
        return "Not Found", "USD"
    
    # ========== OPTIMIZED AIRTABLE METHODS ==========
    def process_product(self, product: ProductData):
        """Optimized product processing with batch updates."""
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
        """Optimized Airtable update with batched fields."""
        if not self.airtable or not product.record_id:
            return
        
        try:
            fields = {
                FIELD_MAPPINGS["name"]: product.name,
                FIELD_MAPPINGS["description"]: product.description[:1000],  # Ensure length limit
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
                except (ValueError, AttributeError):
                    pass
            
            # Images (only if we have them)
            if product.images:
                fields[FIELD_MAPPINGS["photos"]] = ", ".join(product.images[:5])  # Limit to 5
                fields[FIELD_MAPPINGS["photo_files"]] = [{"url": img} for img in product.images[:3]]  # Limit to 3
            
            # Single update call
            self.airtable.update(product.record_id, fields)
            self.logger.info(f"✅ Updated Airtable record: {product.record_id}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to update Airtable: {str(e)[:200]}")
    
    def update_airtable_status(self, record_id: str, status: str, message: str):
        """Optimized status-only update."""
        if not self.airtable:
            return
        
        try:
            fields = {
                FIELD_MAPPINGS["status"]: status,
                FIELD_MAPPINGS["scraping_status"]: message[:200]  # Fixed syntax here
            }
            self.airtable.update(record_id, fields)
        except Exception as e:
            self.logger.error(f"Failed to update status: {str(e)[:200]}")
    
    def handle_failed_scrape(self, response, error_msg: str):
        """Optimized failure handling."""
        record_id = response.meta.get('record_id')
        if record_id:
            self.update_airtable_status(record_id, "Needs Attention", error_msg[:200])

# ========== OPTIMIZED MAIN EXECUTION ==========
def run_scraper():
    """Optimized scraper execution with better settings."""
    settings = {
        # Basic settings
        'BOT_NAME': 'amazon_walmart_scraper',
        'ROBOTSTXT_OBEY': False,
        
        # Performance optimized settings
        'DOWNLOAD_DELAY': 5,  # Balanced delay
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 2,  # Slightly increased
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'REACTOR_THREADPOOL_MAXSIZE': 20,
        
        # AutoThrottle optimized
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 5,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.75,
        
        # Retry settings
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
        
        # Timeout and caching
        'DOWNLOAD_TIMEOUT': 30,
        'HTTPCACHE_ENABLED': False,
        
        # Cookies and headers
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        
        # Logging and debugging
        'LOG_LEVEL': 'INFO',
        'LOG_FORMAT': '%(levelname)s: %(message)s',
        'TELNETCONSOLE_ENABLED': False,
        
        # Middleware
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            f'{__name__}.RandomUserAgentMiddleware': 400,
            f'{__name__}.SmartRetryMiddleware': 500,
        }
    }
    
    process = CrawlerProcess(settings)
    process.crawl(AmazonWalmartSpider)
    
    print("\n" + "="*60)
    print("🚀 Starting Optimized Amazon/Walmart Scraper")
    print("="*60 + "\n")
    
    process.start()
    
    print("\n" + "="*60)
    print("✅ Scraping completed!")
    print("="*60 + "\n")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Check requirements
    missing = False
    try:
        import scrapy
        print("✓ Scrapy installed")
    except ImportError:
        print("✗ Scrapy not installed. Run: pip install scrapy")
        missing = True
    
    try:
        from pyairtable import Api
        print("✓ PyAirtable installed")
    except ImportError:
        print("✗ PyAirtable not installed. Run: pip install pyairtable")
        missing = True
    
    if missing:
        exit(1)
    
    # Check configuration
    if AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE":
        print("\n⚠️  Warning: Airtable API key not configured!")
        print("Please set AIRTABLE_API_KEY in your .env file or environment variables")
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            exit(0)
    
    # Run the scraper
    run_scraper()