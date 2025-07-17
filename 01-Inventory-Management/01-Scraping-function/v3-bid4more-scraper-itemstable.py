import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import re
import json
from io import BytesIO
from PIL import Image
import tempfile
import base64
import time

# Add dotenv support for reading .env files
try:
    from dotenv import load_dotenv

    load_dotenv()  # This loads the .env file
    print("✓ .env file loaded successfully")
except ImportError:
    print("⚠️  python-dotenv not installed. Install with: pip install python-dotenv")
    print("   Falling back to system environment variables only")

try:
    from pyairtable import Api
except ImportError:
    print("pyairtable library not found. Please install it: pip install pyairtable")
    Api = None

# Try to import Selenium
SELENIUM_AVAILABLE = False
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, WebDriverException

    SELENIUM_AVAILABLE = True
    print("✓ Selenium is available for JavaScript-heavy sites")
except ImportError:
    print("⚠️  Selenium not installed. Install with: pip install selenium")
    print("   Also ensure you have ChromeDriver installed")
    print("   JavaScript-heavy sites may not scrape properly")

# --- Airtable Configuration ---
# IMPORTANT: These will now read from your .env file or environment variables
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY", "YOUR_AIRTABLE_API_KEY_HERE")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "YOUR_AIRTABLE_BASE_ID_HERE")
AIRTABLE_TABLE_NAME = "Items-Bid4more"
AIRTABLE_VIEW_NAME = "Samar - View"

# --- Airtable Field Names ---
ASSIGNED_TO_FIELD = "Assigned to"
URL_FIELD_NAME = "Product URL"
PRODUCT_NAME_FIELD = "Item Name"
DESCRIPTION_FIELD = "Description"
DIMENSIONS_FIELD = "Dimensions"
WEIGHT_FIELD = "Weight"
PHOTOS_FIELD = "Photos"
PHOTO_FILES_FIELD = "Photo Files"
SALE_PRICE_FIELD = "Sale Price"
ORIGINAL_PRICE_FIELD = "Original Price"
CURRENCY_FIELD = "Currency"
SOURCE_FIELD = "Scraping Website"
STATUS_FIELD = "Scraping Status"


# --- Helper Functions for Scraping ---


def get_page_with_selenium(url, wait_for_selector=None, timeout=20):
    """
    Enhanced Selenium fetcher with better Target.com and BrandClub.com support.
    Fixed to not remove main product images.
    """
    if not SELENIUM_AVAILABLE:
        print("⚠️  Selenium not available, falling back to regular requests")
        return None

    driver = None
    try:
        # Configure Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in background
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Additional options for better performance
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        # Initialize the driver
        print(f"  Loading page with Selenium...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)

        # Special handling for BrandClub
        if "brandclub.com" in url:
            print("  Special BrandClub.com handling...")

            # Wait for initial page load
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Wait for product container
            try:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            'div[class*="product"], h1, div[class*="Product"]',
                        )
                    )
                )
            except TimeoutException:
                print("  ⚠️  Timeout waiting for product container")

            # Scroll to trigger lazy loading
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(1)

            # Additional wait for dynamic content
            time.sleep(3)

            print("  ✓ Page fully loaded")

        # Special handling for Target
        elif "target.com" in url:
            print("  Special Target.com handling...")

            # Wait for initial page load
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Wait for main product container
            try:
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            'div[data-test*="product"], div[data-test*="hero"], h1',
                        )
                    )
                )
            except TimeoutException:
                print("  ⚠️  Timeout waiting for product container")

            # Scroll to trigger lazy loading of images
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(1)

            # Wait for images to appear in the main product section
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: len(
                        d.find_elements(
                            By.CSS_SELECTOR,
                            'div[data-test*="hero"] img, div[data-test*="gallery"] img, picture img',
                        )
                    )
                    > 0
                )
                print("  ✓ Product images detected")
            except TimeoutException:
                print("  ⚠️  Timeout waiting for product images")

            # Additional wait for images to fully load
            time.sleep(3)

            # Scroll back to top to ensure we capture everything
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            print("  ✓ Page fully loaded, keeping all content for processing")
        elif "walmart." in url:
            print("  Special Walmart handling...")

            # Wait for initial page load
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Additional wait and scroll
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

            print("  ✓ Walmart page fully loaded")

            # Walmart block fixed below
            # Additional wait for Walmart
            time.sleep(5)
            # Scroll down to trigger lazy loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # NEW: extra wait after full scroll
            # Additional wait for Walmart
            time.sleep(5)
            # Scroll down to trigger lazy loading
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        else:
            # Generic wait for other sites
            if wait_for_selector:
                try:
                    WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, wait_for_selector)
                        )
                    )
                    print(f"  ✓ Wait condition met: {wait_for_selector}")
                except TimeoutException:
                    print(f"  ⚠️  Timeout waiting for: {wait_for_selector}")
            else:
                time.sleep(5)

        page_source = driver.page_source
        print("  ✓ Page loaded successfully with Selenium")
        return page_source

    except WebDriverException as e:
        print(f"  ❌ Selenium error: {str(e)}")
        return None
    except Exception as e:
        print(f"  ❌ Unexpected error with Selenium: {str(e)}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"Error while quitting driver: {e}")


def get_product_name(soup, product_url):
    name_text = "Product Name Not Found"
    try:
        if "homedepot.ca" in product_url.lower():
            name_tag = soup.find("h1", class_="product-details__title")
            if name_tag:
                return name_tag.get_text(strip=True)
        elif "brandclub.com" in product_url.lower():
            # BrandClub specific selectors - based on actual HTML structure
            # No H1 tags found, so check other elements
            selectors_to_try = [
                # Based on debug output - product_name container
                "div.product_name",
                "div.product_name span",
                "div.product_name p",
                ".product_name",
                # Try other possible containers
                "div.product-title",
                "div.item-title",
                "span.product-name",
                "p.product-name",
                # Material UI patterns
                'div[class*="MuiTypography"][class*="h1"]',
                'div[class*="MuiTypography"][class*="h2"]',
                'div[class*="MuiTypography"][class*="h3"]',
                'div[class*="MuiTypography"][class*="h4"]',
                'div[class*="MuiTypography"][class*="h5"]',
                'div[class*="MuiTypography"][class*="h6"]',
                # Generic patterns
                "h2",
                "h3",
                "h4",  # Maybe they use h2/h3 instead of h1
                'div[class*="title"]',
                'div[class*="name"]',
            ]

            for selector in selectors_to_try:
                elements = soup.select(selector)
                for element in elements:
                    name_text = element.get_text(strip=True)
                    # More strict filtering - avoid navigation/UI elements
                    if (
                        name_text
                        and len(name_text) > 3
                        and not any(
                            skip in name_text.lower()
                            for skip in [
                                "cart",
                                "menu",
                                "search",
                                "login",
                                "sign",
                                "account",
                                "wishlist",
                                "help",
                                "support",
                                "browse",
                                "category",
                                "reward",
                                "cash back",
                                "earn",
                                "get",
                                "save",
                            ]
                        )
                    ):
                        print(f"Debug BrandClub Name: Found '{name_text}'")
                        return name_text
        elif "target.com" in product_url.lower():
            # First try JSON-LD structured data (primary method for Target)
            script_tags = soup.find_all("script", type="application/ld+json")
            for script_tag in script_tags:
                if script_tag and script_tag.string:
                    try:
                        data = json.loads(script_tag.string)
                        # Handle both single objects and arrays
                        data_list = data if isinstance(data, list) else [data]

                        for item in data_list:
                            if item.get("@type") == "Product" and item.get("name"):
                                name_text = item["name"]
                                print(
                                    f"Debug Target Name (JSON-LD): Found '{name_text}'"
                                )
                                return name_text
                    except (json.JSONDecodeError, AttributeError, KeyError):
                        continue

            # Second try: Look for product data in __NEXT_DATA__ or similar script tags
            script_tags_all = soup.find_all("script")
            for script in script_tags_all:
                if script.string and "productTitle" in script.string:
                    try:
                        # Try to extract product title from various JSON structures
                        script_content = script.string
                        if '"productTitle":' in script_content:
                            # Extract the title using regex
                            match = re.search(
                                r'"productTitle":\s*"([^"]+)"', script_content
                            )
                            if match:
                                name_text = match.group(1)
                                print(
                                    f"Debug Target Name (Script): Found '{name_text}'"
                                )
                                return name_text
                    except:
                        continue

            # Fallback: Traditional HTML selectors (updated)
            selectors_to_try = [
                'h1[data-test="product-title"]',
                'h1[id^="pdp-product-title"]',
                "h1.styles__ProductTitleText",
                'h1[class*="ProductTitle"]',
                'h1[class*="product-title"]',
            ]

            for selector in selectors_to_try:
                name_tag = soup.select_one(selector)
                if name_tag:
                    name_text = name_tag.get_text(strip=True)
                    if name_text and len(name_text) > 3:
                        print(f"Debug Target Name (HTML): Found '{name_text}'")
                        return name_text
        elif "ebay.com" in product_url.lower() or "ebay.ca" in product_url.lower():
            name_tag = soup.select_one(
                "h1.x-item-title__mainTitle span.ux-textspans--BOLD"
            )
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            if name_text == "Product Name Not Found" or not name_text.strip():
                name_tag = soup.select_one("h1.x-item-title__mainTitle")
                if name_tag:
                    name_text = name_tag.get_text(strip=True)
            if name_text == "Product Name Not Found" or not name_text.strip():
                name_tag = soup.select_one(
                    'div[data-testid="x-item-title"] span.ux-textspans'
                )
                if name_tag:
                    name_text = name_tag.get_text(strip=True)
            if name_text == "Product Name Not Found" or not name_text.strip():
                name_tag = soup.find("h1", id="itemTitle")
                if name_tag:
                    for child in name_tag.find_all(
                        text=re.compile(r"Details about\s*", re.I)
                    ):
                        child.extract()
                    name_text = name_tag.get_text(strip=True)

            print(f"Debug eBay Name: Found '{name_text}'")
        elif "amazon." in product_url.lower():
            name_tag_amazon = soup.find("span", id="productTitle")
            if name_tag_amazon:
                name_text = name_tag_amazon.get_text(strip=True)

            if name_text == "Product Name Not Found" or not name_text.strip():
                name_tag_amazon_alt = soup.select_one("h1#title span#productTitle")
                if name_tag_amazon_alt:
                    name_text = name_tag_amazon_alt.get_text(strip=True)

            if name_text == "Product Name Not Found" or not name_text.strip():
                name_tag_h1_title = soup.find("h1", id="title")
                if name_tag_h1_title:
                    name_text = name_tag_h1_title.get_text(strip=True)

            print(f"Debug Amazon Name: Found '{name_text}'")
        elif (
            "walmart.ca" in product_url.lower() or "walmart.com" in product_url.lower()
        ):
            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if next_data_script and next_data_script.string:
                try:
                    next_data = json.loads(next_data_script.string)
                    product_data = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )
                    if product_data and product_data.get("name"):
                        name_text = product_data["name"]
                        print(f"Debug Walmart Name (NEXT_DATA): Found '{name_text}'")
                        return name_text
                except (json.JSONDecodeError, AttributeError, KeyError) as e:
                    print(f"Debug Walmart Name: Error parsing __NEXT_DATA__: {e}")
            print(f"Debug Walmart Name: Found '{name_text}'")
        elif "kohls.com" in product_url.lower():
            name_tag = soup.find(
                "h1", class_=re.compile(r"pdp-product-title|product-title", re.I)
            )
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            print(f"Debug Kohls Name: Found '{name_text}'")
        elif "whatnot.com" in product_url.lower():
            name_tag = soup.select_one(
                'h1, h2, div[class*="title"], span[class*="title"]'
            )
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            print(f"Debug Whatnot Name: Found '{name_text}'")
        elif "rona.ca" in product_url.lower():
            name_tag = soup.find(
                "h1", class_=re.compile(r"product-name|product__name|heading", re.I)
            )
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            print(f"Debug Rona Name: Found '{name_text}'")
        elif "iherb.com" in product_url.lower():
            name_tag = soup.find("h1", id="name")
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            print(f"Debug iHerb Name: Found '{name_text}'")
        elif "canadiantire.ca" in product_url.lower():
            name_tag = soup.find("h1", class_="pdp-product-title__name")
            if name_tag:
                name_text = name_tag.get_text(strip=True)
            print(f"Debug Canadian Tire Name: Found '{name_text}'")
        else:
            # Generic fallback
            selectors = ["h1", "h2.product-title", "span.product-name"]
            for selector_str in selectors:
                parts = selector_str.split(".")
                tag_name = parts[0]
                class_name = parts[1] if len(parts) > 1 else None
                find_args = {}
                if class_name:
                    find_args["class_"] = class_name
                if tag_name:
                    name_tag_generic = soup.find(tag_name, **find_args)
                    if name_tag_generic:
                        return name_tag_generic.get_text(strip=True)
                elif class_name:
                    name_tag_generic_class_only = soup.find(class_=class_name)
                    if name_tag_generic_class_only:
                        return name_tag_generic_class_only.get_text(strip=True)
    except AttributeError:
        pass

    if name_text == "Product Name Not Found":
        with open("debug_walmart.html", "w", encoding="utf-8") as f:
            f.write(str(soup.prettify()))
        print("🔍 HTML dumped to debug_walmart.html for inspection")

    return name_text


def get_product_description(soup, product_url):
    description_text = "Product Description Not Found"
    try:

        if "homedepot.ca" in product_url.lower():
            desc_tag = soup.find("div", class_="product-description__content")
            if desc_tag:
                return desc_tag.get_text(separator=" ", strip=True)
        if "brandclub.com" in product_url.lower():
            # Try various selectors for BrandClub
            desc_selectors = [
                'div[class*="Description"]',
                'div[class*="description"]',
                'div[class*="product-desc"]',
                'div[class*="product_desc"]',
                'div[class*="ProductDescription"]',
                'section[class*="description"]',
                'div[class*="details"] p',
                'div[class*="Details"] p',
                'div[class*="product-info"]',
                'div[class*="product_info"]',
                'div[itemprop="description"]',
                'div[data-testid*="description"]',
                "main p",  # Sometimes descriptions are just in paragraphs
            ]

            desc_content = []
            for selector in desc_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(separator=" ", strip=True)
                    if (
                        text
                        and len(text) > 20
                        and not any(
                            skip in text.lower()
                            for skip in ["cart", "menu", "cookie", "privacy"]
                        )
                    ):
                        desc_content.append(text)

            if desc_content:
                full_desc = " ".join(desc_content).strip()
                if len(full_desc) > 30:
                    print(f"Debug BrandClub Desc (HTML): Found description")
                    return full_desc

            # Try JSON-LD structured data
            script_tags = soup.find_all("script", type="application/ld+json")
            for script_tag in script_tags:
                if script_tag and script_tag.string:
                    try:
                        data = json.loads(script_tag.string)
                        data_list = data if isinstance(data, list) else [data]

                        for item in data_list:
                            if item.get("@type") == "Product" and item.get(
                                "description"
                            ):
                                desc_text = item["description"]
                                if len(desc_text) > 30:
                                    print(
                                        f"Debug BrandClub Desc (JSON-LD): Found description"
                                    )
                                    return desc_text
                    except (json.JSONDecodeError, AttributeError, KeyError):
                        continue

            # Try to find any list items that might be features/descriptions
            feature_lists = soup.select("ul li, ol li")
            features = []
            for li in feature_lists:
                text = li.get_text(strip=True)
                if text and len(text) > 10 and len(text) < 200:
                    features.append(text)

            if features:
                description_text = ". ".join(features[:10])  # Limit to 10 features
                if len(description_text) > 30:
                    print(f"Debug BrandClub Desc (Features): Found description")
                    return description_text

        elif "target.com" in product_url.lower():
            # First try JSON-LD structured data
            script_tags = soup.find_all("script", type="application/ld+json")
            for script_tag in script_tags:
                if script_tag and script_tag.string:
                    try:
                        data = json.loads(script_tag.string)
                        data_list = data if isinstance(data, list) else [data]

                        for item in data_list:
                            if item.get("@type") == "Product" and item.get(
                                "description"
                            ):
                                desc_text = item["description"]
                                if len(desc_text) > 30:
                                    print(
                                        f"Debug Target Desc (JSON-LD): Found description"
                                    )
                                    return desc_text
                    except (json.JSONDecodeError, AttributeError, KeyError):
                        continue

            # Second try: Look for embedded product data
            script_tags_all = soup.find_all("script")
            for script in script_tags_all:
                if script.string and (
                    "description" in script.string
                    or "bullet_descriptions" in script.string
                ):
                    try:
                        script_content = script.string
                        # Try to find description in various formats

                        # Look for bullet_descriptions array
                        bullet_match = re.search(
                            r'"bullet_descriptions":\s*\[(.*?)\]',
                            script_content,
                            re.DOTALL,
                        )
                        if bullet_match:
                            bullets_text = bullet_match.group(1)
                            # Extract individual bullet points
                            bullet_items = re.findall(r'"([^"]+)"', bullets_text)
                            if bullet_items:
                                description_text = ". ".join(bullet_items)
                                if len(description_text) > 30:
                                    print(
                                        f"Debug Target Desc (Bullets): Found description"
                                    )
                                    return description_text

                        # Look for general description field
                        desc_match = re.search(
                            r'"description":\s*"([^"]+)"', script_content
                        )
                        if desc_match:
                            description_text = desc_match.group(1)
                            if len(description_text) > 30:
                                print(f"Debug Target Desc (Script): Found description")
                                return description_text
                    except:
                        continue

            # Fallback: Traditional HTML selectors (updated)
            desc_selectors = [
                'div[data-test="item-details-description"] div',
                'div[data-test="item-highlights"] ul',
                'div[class*="ProductDescription"]',
                'div[class*="product-description"]',
                'section[class*="about"] div',
            ]

            desc_content = []
            for selector in desc_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(separator=" ", strip=True)
                    if text and len(text) > 20:
                        desc_content.append(text)

            if desc_content:
                full_desc = " ".join(desc_content).strip()
                if len(full_desc) > 30:
                    print(f"Debug Target Desc (HTML): Found description")
                    return full_desc

        elif "ebay.com" in product_url.lower() or "ebay.ca" in product_url.lower():
            desc_section = soup.select_one("div#desc_div div.itmDesc, div#ds_div")
            if desc_section:
                for unwanted_tag in desc_section.select(
                    "script, style, noscript, iframe"
                ):
                    unwanted_tag.decompose()
                desc_text_ebay = desc_section.get_text(separator=" ", strip=True)
                if len(desc_text_ebay) > 50:
                    return desc_text_ebay

            about_item_section = soup.select_one(
                'div[data-testid="ux-layout-section--description"] div.ux-layout-section__row'
            )
            if about_item_section:
                all_text_parts = about_item_section.select(
                    "span.ux-textspans, div.ux-textspans"
                )
                if all_text_parts:
                    full_desc_text = " ".join(
                        [
                            span.get_text(separator=" ", strip=True)
                            for span in all_text_parts
                        ]
                    )
                    if len(full_desc_text) > 50:
                        return full_desc_text.strip()

        elif "amazon." in product_url.lower():
            description_tag_amazon = soup.find("div", id="productDescription")
            if description_tag_amazon:
                text_content = ""
                for elem in description_tag_amazon.contents:
                    if (
                        elem.name == "h3"
                        and "product description" in elem.get_text(strip=True).lower()
                    ):
                        continue
                    text_content += elem.get_text(separator=" ", strip=True) + " "
                if text_content.strip():
                    return text_content.strip()

            feature_bullets_div = soup.find("div", id="feature-bullets")
            if feature_bullets_div:
                bullets = feature_bullets_div.select(
                    "ul.a-unordered-list li span.a-list-item"
                )
                if bullets:
                    bullet_texts = [
                        b.get_text(separator=" ", strip=True) for b in bullets
                    ]
                    combined_bullets = ". ".join(bullet_texts)
                    if len(combined_bullets) > 50:
                        return combined_bullets

        elif (
            "walmart.ca" in product_url.lower() or "walmart.com" in product_url.lower()
        ):
            # First try __NEXT_DATA__ script tag (primary source for Walmart)
            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if next_data_script and next_data_script.string:
                try:
                    next_data = json.loads(next_data_script.string)
                    product_data = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )

                    # Try to get description from product data
                    short_desc = product_data.get("shortDescription", "")
                    long_desc = product_data.get("longDescription", "")

                    if long_desc and len(long_desc) > 30:
                        # Clean HTML tags from description
                        desc_soup = BeautifulSoup(long_desc, "html.parser")
                        description_text = desc_soup.get_text(separator=" ", strip=True)
                        if len(description_text) > 30:
                            print(
                                f"Debug Walmart Desc (NEXT_DATA): Found long description"
                            )
                            return description_text

                    if short_desc and len(short_desc) > 30:
                        desc_soup = BeautifulSoup(short_desc, "html.parser")
                        description_text = desc_soup.get_text(separator=" ", strip=True)
                        if len(description_text) > 30:
                            print(
                                f"Debug Walmart Desc (NEXT_DATA): Found short description"
                            )
                            return description_text

                except (json.JSONDecodeError, AttributeError, KeyError) as e:
                    print(f"Debug Walmart Desc: Error parsing __NEXT_DATA__: {e}")

            # Fallback to JSON-LD
            script_tag = soup.find("script", type="application/ld+json")
            if script_tag:
                try:
                    data = json.loads(script_tag.string)
                    if isinstance(data, list):
                        for item_data in data:
                            if item_data.get("@type") == "Product" and item_data.get(
                                "description"
                            ):
                                description_text = item_data["description"]
                                if len(description_text) > 30:
                                    return description_text
                    elif data.get("@type") == "Product" and data.get("description"):
                        description_text = data["description"]
                        if len(description_text) > 30:
                            return description_text
                except json.JSONDecodeError:
                    print("Debug Walmart Desc: Error decoding JSON-LD for description.")

            if description_text == "Product Description Not Found":
                about_section = soup.find(
                    "div",
                    class_=re.compile(
                        r"about-item-description|product-short-description|product-overview",
                        re.I,
                    ),
                )
                if about_section:
                    desc_text_walmart = about_section.get_text(
                        separator=" ", strip=True
                    )
                    if len(desc_text_walmart) > 30:
                        return desc_text_walmart
            if description_text == "Product Description Not Found":
                desc_prop = soup.find(attrs={"itemprop": "description"})
                if desc_prop:
                    desc_text_walmart_prop = desc_prop.get_text(
                        separator=" ", strip=True
                    )
                    if len(desc_text_walmart_prop) > 30:
                        return desc_text_walmart_prop

        elif "kohls.com" in product_url.lower():
            details_section = soup.find(
                "div", class_=re.compile(r"product-details|pdp-details-content", re.I)
            )
            if details_section:
                desc_parts = []
                feature_divs = details_section.select(
                    "div.product-features ul li, div.product_features ul li"
                )
                if feature_divs:
                    desc_parts.append(
                        "Features: "
                        + " ".join([li.get_text(strip=True) for li in feature_divs])
                    )

                detail_text_div = details_section.find(
                    "div",
                    class_=re.compile(r"description-container|pdp-description", re.I),
                )
                if detail_text_div:
                    desc_parts.append(
                        detail_text_div.get_text(separator=" ", strip=True)
                    )
                elif not desc_parts:
                    desc_parts.append(
                        details_section.get_text(separator=" ", strip=True)
                    )

                if desc_parts:
                    full_desc = " ".join(filter(None, desc_parts)).strip()
                    if len(full_desc) > 30:
                        return full_desc

            desc_prop = soup.find(attrs={"itemprop": "description"})
            if desc_prop:
                desc_kohls_prop = desc_prop.get_text(separator=" ", strip=True)
                if len(desc_kohls_prop) > 30:
                    return desc_kohls_prop

        elif "whatnot.com" in product_url.lower():
            desc_tag = soup.select_one(
                'div[class*="description"], p[class*="description"]'
            )
            if desc_tag:
                return desc_tag.get_text(separator=" ", strip=True)

        elif "rona.ca" in product_url.lower():
            desc_section = soup.find(
                "div",
                class_=re.compile(
                    r"product-description|product-details__description", re.I
                ),
            )
            if desc_section:
                return desc_section.get_text(separator=" ", strip=True)
            desc_prop = soup.find(attrs={"itemprop": "description"})
            if desc_prop:
                return desc_prop.get_text(separator=" ", strip=True)

        elif "iherb.com" in product_url.lower():
            desc_section = soup.find("div", itemprop="description")
            if desc_section:
                return desc_section.get_text(separator=" ", strip=True)
            desc_div = soup.select_one("div.product-description-text, div#description")
            if desc_div:
                return desc_div.get_text(separator=" ", strip=True)

        elif "canadiantire.ca" in product_url.lower():
            desc_parts = []
            features_heading = soup.find("h2", string=re.compile(r"Features", re.I))
            if features_heading:
                features_list_container = features_heading.find_next_sibling(
                    "ul", class_="pdp-feature-bullets__list"
                )
                if not features_list_container:
                    features_list_container = features_heading.find_next_sibling(
                        "div", class_="pdp-feature-bullets__list"
                    )

                if features_list_container:
                    features_list = features_list_container.find_all("li")
                    if features_list:
                        desc_parts.append(
                            "Features: "
                            + ". ".join(
                                [li.get_text(strip=True) for li in features_list]
                            )
                        )

            description_heading = soup.find(
                "h2", string=re.compile(r"Description", re.I)
            )
            if description_heading:
                description_div = description_heading.find_next_sibling(
                    "div", class_="pdp-description__text"
                )
                if description_div:
                    desc_parts.append(
                        description_div.get_text(separator=" ", strip=True)
                    )

            if not desc_parts:
                desc_prop = soup.find(attrs={"itemprop": "description"})
                if desc_prop:
                    desc_parts.append(desc_prop.get_text(separator=" ", strip=True))

            if desc_parts:
                full_desc = " ".join(filter(None, desc_parts)).strip()
                if len(full_desc) > 30:
                    return full_desc

        if description_text == "Product Description Not Found":
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.has_attr("content"):
                desc_from_meta = meta_desc["content"].strip()
                if len(desc_from_meta) > 50:
                    return desc_from_meta

            desc_containers = soup.find_all(
                "div", class_=re.compile(r"(description|desc|details)", re.I)
            )
            for container in desc_containers:
                text = container.get_text(separator=" ", strip=True)
                if len(text) > 50:
                    return text
    except AttributeError:
        pass
    return description_text


def _parse_srcset(srcset_str):
    """Helper function to parse srcset and get the highest resolution URL"""
    if not srcset_str:
        return None
    candidates = []
    for item in srcset_str.split(","):
        item = item.strip()
        parts = item.split(maxsplit=1)
        url = parts[0]
        width_descriptor = None
        if len(parts) > 1 and parts[1].endswith("w"):
            try:
                width_descriptor = int(parts[1][:-1])
            except ValueError:
                width_descriptor = None
        candidates.append({"url": url, "width": width_descriptor})

    if not candidates:
        return None

    # Sort by width (highest first)
    candidates.sort(key=lambda x: (x["width"] is not None, x["width"]), reverse=True)
    return candidates[0]["url"]


def get_product_images(soup, base_url):
    site_specific_found = False
    """
    Enhanced image scraper that finds ONLY main product images and deduplicates them.
    Fixed to be less aggressive with section removal.
    """
    images = {}  # Use dict to store images with their base identifier as key

    def extract_image_base_id(url):
        """Extract base image ID from URL to identify duplicates"""
        match = re.search(r"/([A-Z0-9]+)\._", url)
        if match:
            return match.group(1)
        filename = url.split("/")[-1]
        base = re.sub(r"[._-](SX|SY|SL|AC|SS|CR|SR|SP|UL|UX|UR|UY)\d+.*", "", filename)
        return base

    def get_image_resolution(url):
        """Extract resolution from URL to compare image sizes"""
        match = re.search(r"[._](SX|SY|SL|SS|SP|UL|UX)(\d+)", url)
        if match:
            return int(match.group(2))
        return 0

    # Site-specific logic for main product images only
    try:

        if "homedepot.ca" in base_url.lower():
            img_tags = soup.select(
                "div.product-image-carousel img, div.product-media img"
            )
            for img in img_tags:
                src = img.get("src") or img.get("data-src")
                if src and src.startswith("http"):
                    base_id = extract_image_base_id(src)
                    images[base_id] = src
            site_specific_found = False

        if "brandclub.com" in base_url.lower():
            print("Debug BrandClub: Starting image extraction...")

            # Method 1: Look for main product image containers based on debug output
            main_image_selectors = [
                # Specific selectors found in debug
                "div.product_images_desktop img",
                "div.product_images_mobile img",
                ".product_images_desktop img",
                ".product_images_mobile img",
                # Try without the img tag first
                "div.product_images_desktop",
                "div.product_images_mobile",
                # Try other possible containers
                'div[class*="product-images"] img',
                'div[class*="product_images"] img',
                'div[class*="ProductImages"] img',
                'div[class*="gallery"] img',
                'div[class*="Gallery"] img',
                # Main product body (but be careful with this)
                'div.main_product_body img[alt="preview"]',
                # Generic patterns
                'main img[alt*="preview"]',
                'main img[alt*="product"]',
            ]

            # First, try specific product image containers
            for selector in main_image_selectors:
                if selector.endswith(" img"):
                    # Direct img selection
                    imgs = soup.select(selector)
                    for img in imgs:
                        # Skip if this is clearly not a product image
                        alt_text = (img.get("alt") or "").lower()
                        src = img.get("src", "")

                        # Skip non-product images
                        if any(
                            skip in alt_text
                            for skip in ["logo", "icon", "badge", "brand", "reward"]
                        ):
                            continue

                        # Skip non-product URLs
                        if any(
                            skip in src.lower()
                            for skip in ["logo", "icon", "badge", "brand"]
                        ):
                            continue

                        # For BrandClub, we're looking for target.scene7.com images
                        if "target.scene7.com" not in src and src:
                            # If it's a relative URL, it might still be valid
                            pass

                        src_options = [
                            img.get("src"),
                            img.get("data-src"),
                            img.get("data-lazy-src"),
                            img.get("srcset"),
                        ]

                        for src in src_options:
                            if (
                                src
                                and not src.startswith("data:image")
                                and "placeholder" not in src.lower()
                            ):
                                # Handle srcset
                                if " " in str(src) and "w" in str(src):
                                    src = _parse_srcset(src)

                                if src:
                                    # Ensure full URL
                                    if not src.startswith("http"):
                                        src = urljoin(base_url, src)

                                    base_id = extract_image_base_id(src)
                                    if base_id not in images:
                                        images[base_id] = src
                                        site_specific_found = True

                                    # Don't limit too early, get all unique images
                                    if len(images) >= 10:
                                        break

                        if len(images) >= 10:
                            break
                else:
                    # Container selection
                    containers = soup.select(selector)
                    for container in containers:
                        imgs = container.find_all("img")
                        for img in imgs:
                            src = (
                                img.get("src")
                                or img.get("data-src")
                                or img.get("data-lazy-src")
                            )
                            if src and not src.startswith("data:image"):
                                # Ensure full URL
                                if not src.startswith("http"):
                                    src = urljoin(base_url, src)

                                base_id = extract_image_base_id(src)
                                if base_id not in images:
                                    images[base_id] = src
                                    site_specific_found = True

                if len(images) >= 5:  # We know there should be 5 images
                    break

            print(f"Debug BrandClub: Found {len(images)} product images from HTML")

            # Method 2: Try JSON-LD if no images found
            if not images:
                print("Debug BrandClub: No images in HTML, trying JSON-LD...")
                script_tags = soup.find_all("script", type="application/ld+json")
                for script_tag in script_tags:
                    if script_tag and script_tag.string:
                        try:
                            data = json.loads(script_tag.string)
                            data_list = data if isinstance(data, list) else [data]

                            for item in data_list:
                                if item.get("@type") == "Product":
                                    image_data = item.get("image")
                                    if image_data:
                                        image_urls = (
                                            image_data
                                            if isinstance(image_data, list)
                                            else [image_data]
                                        )
                                        # Limit to first 5
                                        for img_url in image_urls[:5]:
                                            if isinstance(
                                                img_url, str
                                            ) and not img_url.startswith("data:image"):
                                                # Ensure full URL
                                                if not img_url.startswith("http"):
                                                    img_url = urljoin(base_url, img_url)
                                                base_id = extract_image_base_id(img_url)
                                                images[base_id] = img_url
                                                site_specific_found = True
                                        print(
                                            f"Debug BrandClub: Found {len(images)} images from JSON-LD"
                                        )
                        except:
                            continue

            # Method 3: Look in scripts for product images
            if not images:
                print("Debug BrandClub: Looking for images in script tags...")
                script_tags = soup.find_all("script")
                for script in script_tags:
                    if script.string and (
                        "productImages" in script.string or "images" in script.string
                    ):
                        # Look for image URLs in JavaScript
                        url_pattern = (
                            r'"(https?://[^"]+\.(?:jpg|jpeg|png|webp|avif)[^"]*)"'
                        )
                        matches = re.findall(url_pattern, script.string)

                        for url in matches[:5]:  # Limit to first 5
                            if any(
                                skip in url.lower()
                                for skip in [
                                    "thumbnail",
                                    "thumb",
                                    "icon",
                                    "related",
                                    "logo",
                                ]
                            ):
                                continue

                            base_id = extract_image_base_id(url)
                            images[base_id] = url
                            site_specific_found = True

                        if images:
                            print(
                                f"Debug BrandClub: Found {len(images)} images from scripts"
                            )
                            break

        elif "target.com" in base_url.lower():
            print("Debug Target: Starting enhanced image extraction...")

            # Method 1: Try to find images WITHOUT removing sections first
            # Look for all images and filter them based on context
            all_img_tags = soup.find_all("img")
            print(f"Debug Target: Found {len(all_img_tags)} total img tags")

            for img in all_img_tags:
                # Skip if image is in a clearly related/recommended section
                parent_element = img
                skip_image = False

                # Check up to 5 parent levels for unwanted sections
                for _ in range(5):
                    parent_element = parent_element.parent
                    if not parent_element:
                        break

                    # Check data-test attributes
                    data_test = parent_element.get("data-test", "")
                    if any(
                        skip_word in data_test
                        for skip_word in [
                            "related",
                            "similar",
                            "recommendation",
                            "sponsored",
                            "frequently-bought",
                        ]
                    ):
                        skip_image = True
                        break

                    # Check class names
                    class_names = " ".join(parent_element.get("class", []))
                    if any(
                        skip_word in class_names.lower()
                        for skip_word in [
                            "related",
                            "recommendation",
                            "similar",
                            "sponsored",
                        ]
                    ):
                        skip_image = True
                        break

                if skip_image:
                    continue

                # Get image URL
                src_options = [img.get("src"), img.get("data-src"), img.get("srcset")]

                for src in src_options:
                    if (
                        src
                        and not src.startswith("data:image")
                        and "placeholder" not in src.lower()
                    ):
                        # Skip badge/icon images based on alt text
                        alt_text = img.get("alt", "").lower()
                        if any(
                            skip_word in alt_text
                            for skip_word in [
                                "icon",
                                "badge",
                                "logo",
                                "rating",
                                "stars",
                                "circle",
                            ]
                        ):
                            continue

                        # Handle srcset
                        if " " in str(src) and "w" in str(src):
                            src = _parse_srcset(src)

                        if src and "scene7.com" in src:
                            # Ensure high resolution
                            src = (
                                re.sub(r"\?.*$", "", src)
                                + "?fmt=webp&wid=1200&hei=1200"
                            )
                            base_id = extract_image_base_id(src)

                            # Only add if it looks like a product image
                            if "GUEST" in src:
                                images[base_id] = src
                                site_specific_found = True
                                break

            print(f"Debug Target: Found {len(images)} product images from HTML")

            # Method 2: If no images found, try JSON-LD
            if not images:
                print("Debug Target: No images in HTML, trying JSON-LD...")
                script_tags = soup.find_all("script", type="application/ld+json")
                for script_tag in script_tags:
                    if script_tag and script_tag.string:
                        try:
                            data = json.loads(script_tag.string)
                            data_list = data if isinstance(data, list) else [data]

                            for item in data_list:
                                if item.get("@type") == "Product":
                                    image_data = item.get("image")
                                    if image_data:
                                        image_urls = (
                                            image_data
                                            if isinstance(image_data, list)
                                            else [image_data]
                                        )
                                        # Limit to first 10
                                        for img_url in image_urls[:10]:
                                            if isinstance(
                                                img_url, str
                                            ) and not img_url.startswith("data:image"):
                                                if "scene7.com" in img_url:
                                                    img_url = (
                                                        re.sub(r"\?.*$", "", img_url)
                                                        + "?fmt=webp&wid=1200&hei=1200"
                                                    )
                                                base_id = extract_image_base_id(img_url)
                                                images[base_id] = img_url
                                                site_specific_found = True
                                        print(
                                            f"Debug Target: Found {len(images)} images from JSON-LD"
                                        )
                        except:
                            continue

            # Method 3: If still no images, look in scripts
            if not images:
                print("Debug Target: No images yet, searching in scripts...")
                all_scripts = soup.find_all("script")
                for script in all_scripts:
                    if script.string and (
                        "productImages" in script.string or "imageUrl" in script.string
                    ):
                        try:
                            # Look for Target scene7 URLs
                            matches = re.findall(
                                r'"(https://[^"]*target\.scene7\.com/is/image/Target/GUEST[^"]*)"',
                                script.string,
                            )

                            for url in matches[:10]:  # Limit to first 10
                                # Skip thumbnails and alt images
                                if any(
                                    skip in url.lower()
                                    for skip in ["_alt", "_swatch", "_thumbnail"]
                                ):
                                    continue

                                url = (
                                    re.sub(r"\?.*$", "", url)
                                    + "?fmt=webp&wid=1200&hei=1200"
                                )
                                base_id = extract_image_base_id(url)
                                images[base_id] = url
                                site_specific_found = True

                            if images:
                                print(
                                    f"Debug Target: Found {len(images)} images from scripts"
                                )
                                break
                        except:
                            continue

            # Final validation
            if len(images) > 15:
                print(
                    f"Debug Target: Too many images ({len(images)}), keeping first 10"
                )
                images = dict(list(images.items())[:10])

        elif "amazon." in base_url.lower():
            # Amazon image extraction logic (keep as is)
            landing_image = soup.find("img", id="landingImage")
            if landing_image:
                data_a_dynamic_image = landing_image.get("data-a-dynamic-image")
                if data_a_dynamic_image:
                    try:
                        dynamic_images = json.loads(data_a_dynamic_image)
                        if dynamic_images:
                            for img_url in dynamic_images.keys():
                                if img_url and not img_url.startswith("data:image"):
                                    base_id = extract_image_base_id(img_url)
                                    resolution = get_image_resolution(img_url)
                                    if (
                                        base_id not in images
                                        or resolution
                                        > get_image_resolution(images[base_id])
                                    ):
                                        images[base_id] = img_url
                                        site_specific_found = True
                    except:
                        pass

            alt_images_container = soup.find("div", id="altImages")
            if alt_images_container:
                thumb_images = alt_images_container.select(
                    "ul.a-unordered-list li.item img, ul.a-unordered-list li.a-spacing-small img"
                )

                for thumb in thumb_images:
                    parent_section = thumb.find_parent(
                        ["div", "section"],
                        class_=re.compile(r"review|customer|cr-", re.I),
                    )
                    if parent_section:
                        continue

                    hiRes = thumb.get("data-old-hires")
                    if hiRes and not hiRes.startswith("data:image"):
                        base_id = extract_image_base_id(hiRes)
                        if base_id not in images:
                            images[base_id] = hiRes
                            site_specific_found = True
                        continue

                    dynamic = thumb.get("data-a-dynamic-image")
                    if dynamic:
                        try:
                            dynamic_imgs = json.loads(dynamic)
                            if dynamic_imgs:
                                highest_res_url = max(
                                    dynamic_imgs.keys(),
                                    key=lambda x: get_image_resolution(x),
                                )
                                base_id = extract_image_base_id(highest_res_url)
                                if base_id not in images:
                                    images[base_id] = highest_res_url
                                    site_specific_found = True
                        except:
                            pass
                        continue

                    src = thumb.get("src")
                    if src and not src.startswith("data:image"):
                        src = re.sub(r"\._(.*?)_\.", "._AC_SL1500_.", src)
                        base_id = extract_image_base_id(src)
                        resolution = get_image_resolution(src)
                        if base_id not in images or resolution > get_image_resolution(
                            images[base_id]
                        ):
                            images[base_id] = src
                            site_specific_found = True

        elif "ebay.com" in base_url.lower() or "ebay.ca" in base_url.lower():
            main_image_container = soup.find("div", class_="ux-image-carousel")
            if main_image_container:
                main_image_tags = main_image_container.select(
                    "img[data-zoom-src], img.ux-image-carousel-item"
                )
                for tag in main_image_tags:
                    src = tag.get("data-zoom-src") or tag.get("src")
                    if src and not src.startswith("data:image"):
                        src = re.sub(
                            r"/s-l\d{2,4}(?=\.jpg|\.png|\.jpeg|\.gif|\.webp)",
                            "/s-l1600",
                            src,
                        )
                        base_id = extract_image_base_id(src)
                        images[base_id] = src
                        site_specific_found = True

            filmstrip_container = soup.find("div", class_="ux-image-filmstrip")
            if filmstrip_container:
                thumb_tags = filmstrip_container.select(
                    "button img, div.ux-image-filmstrip-carousel-item img"
                )
                for tag in thumb_tags:
                    src = tag.get("src") or tag.get("data-src")
                    if src and not src.startswith("data:image"):
                        src = re.sub(
                            r"/s-l\d{2,4}(?=\.jpg|\.png|\.jpeg|\.gif|\.webp)",
                            "/s-l1600",
                            src,
                        )
                        base_id = extract_image_base_id(src)
                        if base_id not in images:
                            images[base_id] = src
                            site_specific_found = True

        elif "walmart.ca" in base_url.lower() or "walmart.com" in base_url.lower():
            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if next_data_script and next_data_script.string:
                try:
                    next_data = json.loads(next_data_script.string)
                    product_data = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )

                    image_info = product_data.get("imageInfo", {})
                    all_images = image_info.get("allImages", [])

                    for img_data in all_images:
                        if isinstance(img_data, dict):
                            img_url = img_data.get("url")
                            if img_url and not img_url.startswith("data:image"):
                                if "?odnHeight=" in img_url or "?odnWidth=" in img_url:
                                    base_img_url = img_url.split("?")[0]
                                    img_url = f"{base_img_url}?odnHeight=2000&odnWidth=2000&odnBg=FFFFFF"

                                base_id = extract_image_base_id(img_url)
                                images[base_id] = img_url
                                site_specific_found = True

                    print(
                        f"Debug Walmart NEXT_DATA: Found {len(images)} images from __NEXT_DATA__"
                    )

                except (json.JSONDecodeError, AttributeError, KeyError) as e:
                    print(f"Debug Walmart: Error parsing __NEXT_DATA__ for images: {e}")

            if not images:
                product_images_container = soup.find(
                    "div", class_=re.compile(r"product.*image|image.*gallery", re.I)
                )
                if product_images_container:
                    img_tags = product_images_container.find_all("img")
                    for img_tag in img_tags:
                        src_options = [
                            img_tag.get("data-src"),
                            img_tag.get("src"),
                            img_tag.get("data-lazy-src"),
                        ]
                        for src in src_options:
                            if (
                                src
                                and not src.startswith("data:image")
                                and "placeholder" not in src.lower()
                            ):
                                if "?odnHeight=" in src or "?odnWidth=" in src:
                                    base_src = src.split("?")[0]
                                    src = f"{base_src}?odnHeight=2000&odnWidth=2000&odnBg=FFFFFF"

                                base_id = extract_image_base_id(src)
                                images[base_id] = src
                                site_specific_found = True
                                break

        elif "canadiantire.ca" in base_url.lower():
            hero_container = soup.find("div", class_="pdp-hero-image")
            if hero_container:
                main_img_tag = hero_container.find("img")
                if main_img_tag:
                    src = main_img_tag.get("src") or main_img_tag.get("data-src")
                    if src and not src.startswith("data:image"):
                        base_id = extract_image_base_id(src)
                        images[base_id] = src
                        site_specific_found = True

            thumb_container = soup.find("ul", class_="pdp-thumbnail-list")
            if thumb_container:
                thumb_images = thumb_container.select("li.pdp-thumbnail-list__item img")
                for thumb_img in thumb_images:
                    src = thumb_img.get("src") or thumb_img.get("data-src")
                    if src and not src.startswith("data:image"):
                        src = re.sub(r"_\d+x\d+$|_thumb$", "", src)
                        base_id = extract_image_base_id(src)
                        if base_id not in images:
                            images[base_id] = src
                            site_specific_found = True

        if site_specific_found:
            print(f"Debug SiteSpecific: Found {len(images)} unique product images.")

    except Exception as e:
        print(f"Error in site-specific image search: {e}")

    if not images:
        print(f"Warning: No product images found for {base_url}")

    # Convert dict values to list and apply final filtering
    unique_images = list(images.values())

    # Final filtering to ensure we don't have review or related product images
    filtered_images = [
        img
        for img in unique_images
        if not re.search(
            r"pixel|logo|icon|avatar|spinner|loading|blank|badge|stars|rating|banner|ad|promo|review|customer|sponsored|related",
            img,
            re.I,
        )
        and not img.endswith((".svg", ".gif"))
    ]

    return filtered_images


def _clean_price_text(price_text):
    """Helper to extract a clean numerical string from price text."""
    if not price_text:
        return None

    # Remove currency symbols and clean the text
    price_text = (
        price_text.replace("$", "").replace("€", "").replace("£", "").replace("¥", "")
    )
    price_text = price_text.replace("\xa0", "").replace(
        "\u00a0", ""
    )  # Remove non-breaking spaces

    # Handle French number format (space as thousand separator, comma as decimal)
    if "," in price_text and "." not in price_text:
        price_text = price_text.replace(" ", "").replace(",", ".")
    else:
        price_text = price_text.replace(" ", "").replace(",", "")

    # Extract numbers
    match = re.search(r"(\d+\.?\d*)", price_text)
    if match:
        return match.group(1)
    return None


def get_product_price(soup, product_url):
    """
    Enhanced price scraper to find sale price, original price, and currency.
    Returns a dictionary: {'sale_price': '...', 'original_price': '...', 'currency': '...'}
    """
    price_info = {
        "sale_price": "Not Found",
        "original_price": "N/A",
        "currency": "Unknown",
    }

    # --- 1. Try JSON-LD first ---
    script_tag_ld_json = soup.find("script", type="application/ld+json")
    if script_tag_ld_json:
        try:

            if "homedepot.ca" in product_url.lower():
                price_tag = soup.find("span", class_="price__value")
                if price_tag:
                    price_info["sale_price"] = _clean_price_text(price_tag.get_text())
                    price_info["currency"] = "CAD"
                    data = json.loads(script_tag_ld_json.string)
                    data_list = data if isinstance(data, list) else [data]
                    for item_data in data_list:
                        if item_data.get("@type") == "Product":
                            offers = item_data.get("offers")
                            offer = None
                            if isinstance(offers, list) and offers:
                                offer = offers[0]
                            elif isinstance(offers, dict):
                                offer = offers

                            if offer and offer.get("price"):
                                price_info["sale_price"] = str(offer["price"])
                                price_info["currency"] = offer.get(
                                    "priceCurrency", price_info["currency"]
                                )
                                print(
                                    f"Debug JSON-LD Price: Found sale price '{price_info['sale_price']}'"
                                )

        except (json.JSONDecodeError, TypeError):
            print("Debug Price: Could not parse JSON-LD.")

    # --- 2. Site-Specific HTML Parsing ---
    if "brandclub.com" in product_url.lower():
        # BrandClub specific price selectors - based on debug output
        price_selectors = [
            # Primary price selector from debug output - most specific first
            "div.detail_retailer_price > div.price",
            "div.detail_retailer_price div.price",
            # Generic price div (but not reward amounts)
            "div.price:not(.reward-amount)",
            "div.price",
            # Try other possible patterns
            "div.price-value",
            "span.price-value",
            'div[class*="price"]:not([class*="reward"])',
            'span[class*="price"]:not([class*="reward"])',
            # Material UI patterns
            'div[class*="MuiTypography"][class*="price"]',
            'span[class*="MuiTypography"][class*="price"]',
            # Generic but selective
            "div.product-price",
            "span.product-price",
            'meta[itemprop="price"][content]',
        ]

        for selector in price_selectors:
            if selector.startswith("meta"):
                # Handle meta tag differently
                meta_element = soup.select_one(selector)
                if meta_element and meta_element.get("content"):
                    cleaned_price = _clean_price_text(meta_element.get("content"))
                    if cleaned_price and cleaned_price != "Not Found":
                        try:
                            price_float = float(cleaned_price)
                            if 1.00 <= price_float <= 10000:  # More reasonable minimum
                                price_info["sale_price"] = cleaned_price
                                price_info["currency"] = "USD"
                                print(
                                    f"Debug BrandClub Price (Meta): Found '{cleaned_price}'"
                                )
                                break
                        except ValueError:
                            continue
            else:
                price_elements = soup.select(selector)

                for price_element in price_elements:
                    if not price_element:
                        continue

                    # Check parent class to ensure it's the right price
                    parent = price_element.parent
                    parent_classes = " ".join(parent.get("class", [])) if parent else ""

                    # Prioritize prices with detail_retailer_price parent
                    if (
                        "detail_retailer_price" in parent_classes
                        or selector == "div.price"
                    ):
                        price_text = price_element.get_text(strip=True)

                        # Skip reward amounts and other non-price text
                        if (
                            any(
                                skip in price_text.lower()
                                for skip in ["reward", "cash back", "earn", "get"]
                            )
                            or len(price_text) > 20
                        ):
                            continue

                        cleaned_price = _clean_price_text(price_text)
                        if cleaned_price and cleaned_price != "Not Found":
                            try:
                                price_float = float(cleaned_price)
                                # Skip suspiciously low prices (rewards)
                                if 1.00 <= price_float <= 10000:
                                    price_info["sale_price"] = cleaned_price
                                    # Default for BrandClub
                                    price_info["currency"] = "USD"
                                    print(
                                        f"Debug BrandClub Price (HTML {selector}): Found '{cleaned_price}'"
                                    )
                                    return price_info  # Return immediately when we find the right price
                            except ValueError:
                                continue

                if price_info["sale_price"] != "Not Found":
                    break

        # Try to find price in scripts if HTML search failed
        if price_info["sale_price"] == "Not Found":
            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string and '"price"' in script.string:
                    # Look for price patterns in JSON
                    price_patterns = [
                        r'"price"\s*:\s*"?(\d+\.?\d*)"?',
                        r'"currentPrice"\s*:\s*"?(\d+\.?\d*)"?',
                        r'"salePrice"\s*:\s*"?(\d+\.?\d*)"?',
                        r'"amount"\s*:\s*"?(\d+\.?\d*)"?',
                    ]
                    for pattern in price_patterns:
                        match = re.search(pattern, script.string)
                        if match:
                            price_value = match.group(1)
                            try:
                                price_float = float(price_value)
                                if 1.00 <= price_float <= 10000:
                                    price_info["sale_price"] = price_value
                                    price_info["currency"] = "USD"
                                    print(
                                        f"Debug BrandClub Price (Script): Found '{price_value}'"
                                    )
                                    break
                            except ValueError:
                                continue

                if price_info["sale_price"] != "Not Found":
                    break

        # Look for original/was price
        original_price_selectors = [
            'span[class*="was-price"]',
            'span[class*="original-price"]',
            'div[class*="was-price"]',
            'div[class*="original-price"]',
            "s span",  # strikethrough price
            "del span",  # deleted price
            'span[style*="text-decoration: line-through"]',
        ]

        for selector in original_price_selectors:
            orig_price_element = soup.select_one(selector)
            if orig_price_element:
                orig_price_text = orig_price_element.get_text(strip=True)
                cleaned_orig_price = _clean_price_text(orig_price_text)
                if cleaned_orig_price and cleaned_orig_price != "Not Found":
                    try:
                        orig_float = float(cleaned_orig_price)
                        sale_float = float(price_info.get("sale_price", "0"))
                        # Original price should be higher than sale price
                        if orig_float > sale_float and orig_float <= 10000:
                            price_info["original_price"] = cleaned_orig_price
                            print(
                                f"Debug BrandClub Price (HTML): Found original '{cleaned_orig_price}'"
                            )
                            break
                    except:
                        pass

    elif "target.com" in product_url.lower():
        # First try JSON-LD structured data
        script_tags = soup.find_all("script", type="application/ld+json")
        for script_tag in script_tags:
            if script_tag and script_tag.string:
                try:
                    data = json.loads(script_tag.string)
                    data_list = data if isinstance(data, list) else [data]

                    for item in data_list:
                        if item.get("@type") == "Product":
                            offers = item.get("offers")
                            if offers:
                                offer = (
                                    offers[0] if isinstance(offers, list) else offers
                                )
                                if offer and offer.get("price"):
                                    price_info["sale_price"] = str(offer["price"])
                                    price_info["currency"] = offer.get(
                                        "priceCurrency", "USD"
                                    )
                                    print(
                                        f"Debug Target Price (JSON-LD): Found '{price_info['sale_price']}'"
                                    )
                                    return price_info
                except (json.JSONDecodeError, AttributeError, KeyError):
                    continue

        # Second try: Look for price data in embedded scripts
        script_tags_all = soup.find_all("script")
        for script in script_tags_all:
            if script.string and (
                "price" in script.string or "current_retail" in script.string
            ):
                try:
                    script_content = script.string

                    # Look for current retail price
                    current_price_match = re.search(
                        r'"current_retail":\s*(\d+\.?\d*)', script_content
                    )
                    if current_price_match:
                        price_value = current_price_match.group(1)
                        price_info["sale_price"] = price_value
                        print(
                            f"Debug Target Price (Script current_retail): Found '{price_value}'"
                        )

                    # Look for reg retail price (original price)
                    reg_price_match = re.search(
                        r'"reg_retail":\s*(\d+\.?\d*)', script_content
                    )
                    if reg_price_match:
                        reg_price_value = reg_price_match.group(1)
                        if float(reg_price_value) > float(
                            price_info.get("sale_price", "0")
                        ):
                            price_info["original_price"] = reg_price_value
                            print(
                                f"Debug Target Price (Script reg_retail): Found original '{reg_price_value}'"
                            )

                    # Try to find currency
                    currency_match = re.search(
                        r'"currency":\s*"([^"]+)"', script_content
                    )
                    if currency_match:
                        price_info["currency"] = currency_match.group(1).upper()

                    if price_info["sale_price"] != "Not Found":
                        return price_info

                except:
                    continue

        # Third try: Enhanced HTML selectors for dynamic content
        price_selectors = [
            # Data-test attributes (most reliable)
            '[data-test="product-price"] span',
            '[data-test="product-price"]',
            '[data-test="current-price"] span',
            '[data-test="current-price"]',
            # Specific price containers
            'div[data-test="product-price-container"] span',
            'div[data-test="price-container"] span',
            # Class-based selectors
            'span[class*="styles__CurrentPrice"]',
            'span[class*="CurrentPrice"]',
            'span[class*="ProductPrice"]',
            'div[class*="PriceDisplay"] span',
            'div[class*="price-display"] span',
            # ARIA labels
            'span[aria-label*="current price"]',
            'span[aria-label*="sale price"]',
            # Generic but specific patterns
            "h3 span",  # Target often puts price in h3
            'div[class*="h-text-"] span',  # Target's text hierarchy classes
            # Last resort: any span with dollar sign
            "span",
        ]

        for selector in price_selectors:
            price_elements = soup.select(selector)

            for price_element in price_elements:
                if not price_element:
                    continue

                price_text = price_element.get_text(strip=True)

                # For the generic 'span' selector, only look for spans containing '$'
                if selector == "span" and "$" not in price_text:
                    continue

                # Skip if it's clearly not a price
                if len(price_text) > 20 or any(
                    word in price_text.lower()
                    for word in [
                        "per",
                        "fluid",
                        "ounce",
                        "shipping",
                        "tax",
                        "reg",
                        "when",
                        "with",
                        "circle",
                    ]
                ):
                    continue

                # Also check aria-label for price info
                aria_label = price_element.get("aria-label", "")
                if "price" in aria_label.lower() and "$" in aria_label:
                    price_text = aria_label

                cleaned_price = _clean_price_text(price_text)
                if cleaned_price and cleaned_price != "Not Found":
                    # Sanity check - price should be reasonable
                    try:
                        price_float = float(cleaned_price)
                        if 0.01 <= price_float <= 10000:  # Reasonable price range
                            price_info["sale_price"] = cleaned_price
                            # Default for Target
                            price_info["currency"] = "USD"
                            print(
                                f"Debug Target Price (HTML {selector}): Found '{cleaned_price}'"
                            )

                            # Continue searching for original price
                            break
                    except ValueError:
                        continue

            if price_info["sale_price"] != "Not Found":
                break

        # Look for original/was price
        original_price_selectors = [
            '[data-test="product-was-price"]',
            '[data-test="was-price"]',
            'span[class*="WasPrice"]',
            'span[class*="was-price"]',
            'span[class*="RegPrice"]',
            'span[class*="original-price"]',
            'span[aria-label*="was price"]',
            'span[aria-label*="regular price"]',
            "s span",  # strikethrough price
            'span[style*="text-decoration: line-through"]',
        ]

        for selector in original_price_selectors:
            orig_price_element = soup.select_one(selector)
            if orig_price_element:
                orig_price_text = orig_price_element.get_text(strip=True)

                # Check aria-label too
                aria_label = orig_price_element.get("aria-label", "")
                if "was" in aria_label.lower() and "$" in aria_label:
                    orig_price_text = aria_label

                cleaned_orig_price = _clean_price_text(orig_price_text)
                if cleaned_orig_price and cleaned_orig_price != "Not Found":
                    try:
                        if float(cleaned_orig_price) > float(
                            price_info.get("sale_price", "0")
                        ):
                            price_info["original_price"] = cleaned_orig_price
                            print(
                                f"Debug Target Price (HTML): Found original '{cleaned_orig_price}'"
                            )
                            break
                    except:
                        pass

    elif "amazon." in product_url.lower():
        # First, check if there's a deal/sale price
        deal_price_found = False

        # Check for deal price (red price)
        deal_price_selectors = [
            # Deal price
            'span.a-price[data-a-color="price"] span.a-offscreen',
            "span.a-price.a-text-price.a-size-medium.apexPriceToPay span.a-offscreen",  # Apex deal
            "span.priceToPay span.a-offscreen",  # Price to pay
        ]

        for selector in deal_price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                cleaned_price = _clean_price_text(price_text)
                if cleaned_price and cleaned_price != "Not Found":
                    price_info["sale_price"] = cleaned_price
                    deal_price_found = True
                    print(f"Debug Amazon Deal Price: Found '{cleaned_price}'")
                    break

        # If no deal price, get the regular price
        if not deal_price_found:
            regular_price_selectors = [
                "div.a-section.a-spacing-none.aok-align-center span.a-price-whole",  # Regular price whole
                "span.a-price.a-text-normal span.a-offscreen",  # Normal price
                "span.a-price-whole:first-of-type",  # First price whole
                'div[data-cy="price-recipe"] span.a-price-whole',  # Price recipe
            ]

            for selector in regular_price_selectors:
                price_element = soup.select_one(selector)
                if price_element:
                    price_text = price_element.get_text(strip=True)
                    cleaned_price = _clean_price_text(price_text)
                    # Sanity check
                    if (
                        cleaned_price
                        and cleaned_price != "Not Found"
                        and float(cleaned_price) < 10000
                    ):
                        price_info["sale_price"] = cleaned_price
                        print(f"Debug Amazon Regular Price: Found '{cleaned_price}'")
                        break

        # Only look for original price if we found a deal price
        if deal_price_found:
            original_price_selectors = [
                "span.a-price.a-text-price span.a-offscreen",  # Strikethrough price
                'span[data-a-strike="true"] span.a-offscreen',  # Strike price
                "div.a-section span.a-price.a-text-price span.a-offscreen",  # Section strike price
                "span.a-size-base.a-color-secondary.aok-align-center.basisPrice span.a-offscreen",  # Basis price
            ]

            for selector in original_price_selectors:
                price_elements = soup.select(selector)
                for price_element in price_elements:
                    price_text = price_element.get_text(strip=True)
                    cleaned_price = _clean_price_text(price_text)
                    if cleaned_price and cleaned_price != "Not Found":
                        # Make sure original price is higher than sale price
                        try:
                            if float(cleaned_price) > float(price_info["sale_price"]):
                                price_info["original_price"] = cleaned_price
                                print(
                                    f"Debug Amazon Original Price: Found '{cleaned_price}'"
                                )
                                break
                        except:
                            pass
                if price_info["original_price"] != "N/A":
                    break

    elif "canadiantire.ca" in product_url.lower():
        was_price_span = soup.select_one(
            ".price__was-value--format, .price__was .price-value"
        )
        if was_price_span:
            price_info["original_price"] = _clean_price_text(was_price_span.get_text())

        sale_price_span = soup.select_one(
            ".pdp-price__price--markdown .price__value--format, .pdp-price__price-value .price__value--format, .price__sale .price-value"
        )
        if sale_price_span:
            price_info["sale_price"] = _clean_price_text(sale_price_span.get_text())
        else:
            reg_price_span = soup.select_one(
                '.price__reg .price-value, .pdp-price__price-value span[itemprop="price"]'
            )
            if reg_price_span:
                price_info["sale_price"] = _clean_price_text(reg_price_span.get_text())

    elif "walmart." in product_url.lower():
        # First try __NEXT_DATA__ script tag (primary source for Walmart)
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if next_data_script and next_data_script.string:
            try:
                next_data = json.loads(next_data_script.string)
                product_data = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("initialData", {})
                    .get("data", {})
                    .get("product", {})
                )

                # Extract price info from product data
                price_info_data = product_data.get("priceInfo", {})
                current_price = price_info_data.get("currentPrice", {})
                was_price = price_info_data.get("wasPrice", {})

                # Get current/sale price
                if current_price:
                    price_value = current_price.get("price")
                    price_string = current_price.get("priceString", "")
                    currency = current_price.get("currencyUnit", "")

                    if price_value:
                        price_info["sale_price"] = str(price_value)
                        print(
                            f"Debug Walmart Price (NEXT_DATA): Found current price '{price_value}'"
                        )
                    elif price_string:
                        cleaned_price = _clean_price_text(price_string)
                        if cleaned_price:
                            price_info["sale_price"] = cleaned_price
                            print(
                                f"Debug Walmart Price (NEXT_DATA): Found current price string '{cleaned_price}'"
                            )

                    if currency:
                        price_info["currency"] = currency.upper()

                # Get was/original price
                if was_price:
                    was_price_value = was_price.get("price")
                    was_price_string = was_price.get("priceString", "")

                    if was_price_value:
                        price_info["original_price"] = str(was_price_value)
                        print(
                            f"Debug Walmart Price (NEXT_DATA): Found was price '{was_price_value}'"
                        )
                    elif was_price_string:
                        cleaned_was_price = _clean_price_text(was_price_string)
                        if cleaned_was_price:
                            price_info["original_price"] = cleaned_was_price
                            print(
                                f"Debug Walmart Price (NEXT_DATA): Found was price string '{cleaned_was_price}'"
                            )

            except (json.JSONDecodeError, AttributeError, KeyError) as e:
                print(f"Debug Walmart Price: Error parsing __NEXT_DATA__: {e}")

        # Fallback to HTML selectors
        if price_info["sale_price"] == "Not Found":
            # Get the current price
            current_price = soup.select_one('span[itemprop="price"]')
            if current_price:
                price_content = current_price.get("content")
                if price_content:
                    price_info["sale_price"] = _clean_price_text(price_content)
                else:
                    price_info["sale_price"] = _clean_price_text(
                        current_price.get_text()
                    )

            # Check for strikethrough price
            was_price = soup.select_one("span.w_C8 s")  # Strikethrough price
            if was_price:
                price_info["original_price"] = _clean_price_text(was_price.get_text())

    elif "ebay.com" in product_url.lower() or "ebay.ca" in product_url.lower():
        sale_price_tag = soup.select_one(
            "div.x-price-primary span.ux-textspans, span#prcIsum"
        )
        if sale_price_tag:
            price_info["sale_price"] = _clean_price_text(sale_price_tag.get_text())

        original_price_tag = soup.select_one(
            "div.x-price-was span.ux-textspans, span#orgPrc"
        )
        if original_price_tag:
            price_info["original_price"] = _clean_price_text(
                original_price_tag.get_text()
            )

    # --- 3. Infer Currency ---
    if price_info["currency"] == "Unknown":
        currency_text_element = soup.select_one('[itemprop="priceCurrency"]')
        if currency_text_element and currency_text_element.get("content"):
            price_info["currency"] = (
                currency_text_element.get("content").strip().upper()
            )
        else:
            # Check the price text itself for currency symbols
            if price_info["sale_price"] != "Not Found":
                if "CDN" in str(soup) or "CAD" in str(soup):
                    price_info["currency"] = "CAD"
                elif (
                    "amazon.ca" in product_url
                    or "walmart.ca" in product_url
                    or "canadiantire.ca" in product_url
                    or "ebay.ca" in product_url
                ):
                    price_info["currency"] = "CAD"
                elif "amazon.co.uk" in product_url or "ebay.co.uk" in product_url:
                    price_info["currency"] = "GBP"
                elif (
                    "amazon.com" in product_url
                    or "walmart.com" in product_url
                    or "ebay.com" in product_url
                    or "brandclub.com" in product_url
                ):
                    price_info["currency"] = "USD"

    # --- 4. Final Validation ---
    # If we only have one price and no indication of a sale, it's the regular price, not sale price
    if (
        price_info["sale_price"] != "Not Found"
        and price_info["original_price"] == "N/A"
    ):
        # Check if there are any sale indicators
        sale_indicators = soup.select(
            'span.savingsPercentage, div.dealBadge, span.a-color-price, [class*="deal"], [class*="save"], [class*="discount"]'
        )
        if not sale_indicators:
            # No sale indicators found, this is just the regular price
            # Don't report it as a sale price
            pass

    return price_info


def _find_generic_spec(soup, keywords):
    if not isinstance(keywords, list):
        keywords = [keywords]
    keywords_lower = [k.lower() for k in keywords]

    for dt_tag in soup.find_all("dt"):
        dt_text = dt_tag.get_text(strip=True).lower()
        if any(keyword in dt_text for keyword in keywords_lower):
            dd_tag = dt_tag.find_next_sibling("dd")
            if dd_tag:
                return dd_tag.get_text(strip=True)

    for th_tag in soup.find_all(["th", "td"]):
        th_text = th_tag.get_text(strip=True).lower()
        if any(keyword in th_text for keyword in keywords_lower):
            if th_tag.name == "th":
                next_cell = th_tag.find_next_sibling("td")
                if next_cell:
                    return next_cell.get_text(strip=True)
            elif th_tag.name == "td" and ":" in th_text:
                parts = th_text.split(":", 1)
                if len(parts) > 1 and parts[1].strip():
                    return parts[1].strip()
            elif th_tag.name == "td":
                next_cell = th_tag.find_next_sibling("td")
                if next_cell:
                    return next_cell.get_text(strip=True)

    for li_tag in soup.find_all("li"):
        text = li_tag.get_text(strip=True)
        text_lower = text.lower()
        if any(keyword in text_lower for keyword in keywords_lower):
            for keyword_in_text in keywords_lower:
                if keyword_in_text + ":" in text_lower:
                    parts = text.split(":", 1)
                    if len(parts) > 1 and parts[1].strip():
                        return parts[1].strip()
            if len(text.lower().replace(keywords_lower[0], "", 1).strip()) > 2:
                return text
    return None


def get_product_dimensions(soup, product_url):
    dimensions_str = "Product Dimensions Not Found"
    try:

        if "homedepot.ca" in product_url.lower():
            spec_text = _find_generic_spec(
                soup, ["dimensions", "depth", "width", "height"]
            )
            if spec_text:
                return spec_text
        if "brandclub.com" in product_url.lower():
            # Try to find dimensions in various formats
            dim_keywords = [
                "dimensions",
                "size",
                "length",
                "width",
                "height",
                "depth",
                "diameter",
            ]
            dimensions_str = _find_generic_spec(soup, dim_keywords)

            if dimensions_str:
                print(f"Debug BrandClub Dimensions: Found '{dimensions_str}'")
                return dimensions_str

            # Try looking in specification sections
            spec_sections = soup.select(
                'div[class*="specification"], div[class*="Specification"], div[class*="spec"], table'
            )
            for section in spec_sections:
                text = section.get_text(separator=" ", strip=True).lower()
                for keyword in dim_keywords:
                    if keyword in text:
                        # Try to extract dimension info
                        lines = section.get_text(separator="\n").split("\n")
                        for line in lines:
                            if keyword in line.lower():
                                # Clean up the line
                                dim_match = re.search(
                                    r"(\d+(?:\.\d+)?)\s*[x×]\s*(\d+(?:\.\d+)?)\s*[x×]?\s*(\d+(?:\.\d+)?)?",
                                    line,
                                )
                                if dim_match:
                                    dimensions_str = dim_match.group(0)
                                    print(
                                        f"Debug BrandClub Dimensions: Found '{dimensions_str}'"
                                    )
                                    return dimensions_str
                                elif ":" in line:
                                    parts = line.split(":", 1)
                                    if len(parts) > 1:
                                        dimensions_str = parts[1].strip()
                                        if dimensions_str and len(dimensions_str) > 2:
                                            print(
                                                f"Debug BrandClub Dimensions: Found '{dimensions_str}'"
                                            )
                                            return dimensions_str

        # First try __NEXT_DATA__ for Walmart
        elif "walmart." in product_url.lower():
            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if next_data_script and next_data_script.string:
                try:
                    next_data = json.loads(next_data_script.string)
                    product_data = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )

                    # Look for dimensions in specifications
                    specifications = product_data.get("specifications", {})
                    spec_groups = specifications.get("specificationGroups", [])

                    for group in spec_groups:
                        if isinstance(group, dict):
                            specifications_list = group.get("specifications", [])
                            for spec in specifications_list:
                                if isinstance(spec, dict):
                                    name = spec.get("name", "").lower()
                                    value = spec.get("value", "")
                                    if (
                                        any(
                                            keyword in name
                                            for keyword in [
                                                "dimension",
                                                "size",
                                                "length",
                                                "width",
                                                "height",
                                            ]
                                        )
                                        and value
                                    ):
                                        print(
                                            f"Debug Walmart Dimensions (NEXT_DATA): Found '{value}'"
                                        )
                                        return value

                except (json.JSONDecodeError, AttributeError, KeyError) as e:
                    print(f"Debug Walmart Dimensions: Error parsing __NEXT_DATA__: {e}")

        if "amazon" in product_url.lower():
            package_dims = _find_generic_spec(
                soup, ["package dimensions", "product dimensions"]
            )
            if package_dims:
                return package_dims

        generic_dims = _find_generic_spec(soup, ["dimensions", "size"])
        if generic_dims:
            return generic_dims

    except Exception as e:
        print(f"Error in get_product_dimensions: {e}")
    return dimensions_str


def get_product_weight(soup, product_url):
    weight_str = "Product Weight Not Found"
    try:

        if "homedepot.ca" in product_url.lower():
            weight_text = _find_generic_spec(soup, ["weight"])
            if weight_text:
                return weight_text
        if "brandclub.com" in product_url.lower():
            # Try to find weight in various formats
            weight_keywords = [
                "weight",
                "shipping weight",
                "item weight",
                "net weight",
                "gross weight",
            ]
            weight_str = _find_generic_spec(soup, weight_keywords)

            if weight_str:
                print(f"Debug BrandClub Weight: Found '{weight_str}'")
                return weight_str

            # Try looking in specification sections
            spec_sections = soup.select(
                'div[class*="specification"], div[class*="Specification"], div[class*="spec"], table'
            )
            for section in spec_sections:
                text = section.get_text(separator=" ", strip=True).lower()
                for keyword in weight_keywords:
                    if keyword in text:
                        # Try to extract weight info
                        lines = section.get_text(separator="\n").split("\n")
                        for line in lines:
                            if keyword in line.lower():
                                # Look for weight patterns
                                weight_match = re.search(
                                    r"(\d+(?:\.\d+)?)\s*(lb|lbs|pound|pounds|kg|kilogram|kilograms|g|gram|grams|oz|ounce|ounces)",
                                    line,
                                    re.I,
                                )
                                if weight_match:
                                    weight_str = weight_match.group(0)
                                    print(
                                        f"Debug BrandClub Weight: Found '{weight_str}'"
                                    )
                                    return weight_str
                                elif ":" in line:
                                    parts = line.split(":", 1)
                                    if len(parts) > 1:
                                        weight_str = parts[1].strip()
                                        if weight_str and len(weight_str) > 1:
                                            print(
                                                f"Debug BrandClub Weight: Found '{weight_str}'"
                                            )
                                            return weight_str

        # First try __NEXT_DATA__ for Walmart
        elif "walmart." in product_url.lower():
            next_data_script = soup.find("script", id="__NEXT_DATA__")
            if next_data_script and next_data_script.string:
                try:
                    next_data = json.loads(next_data_script.string)
                    product_data = (
                        next_data.get("props", {})
                        .get("pageProps", {})
                        .get("initialData", {})
                        .get("data", {})
                        .get("product", {})
                    )

                    # Look for weight in specifications
                    specifications = product_data.get("specifications", {})
                    spec_groups = specifications.get("specificationGroups", [])

                    for group in spec_groups:
                        if isinstance(group, dict):
                            specifications_list = group.get("specifications", [])
                            for spec in specifications_list:
                                if isinstance(spec, dict):
                                    name = spec.get("name", "").lower()
                                    value = spec.get("value", "")
                                    if (
                                        any(
                                            keyword in name
                                            for keyword in [
                                                "weight",
                                                "shipping weight",
                                                "item weight",
                                            ]
                                        )
                                        and value
                                    ):
                                        print(
                                            f"Debug Walmart Weight (NEXT_DATA): Found '{value}'"
                                        )
                                        return value

                except (json.JSONDecodeError, AttributeError, KeyError) as e:
                    print(f"Debug Walmart Weight: Error parsing __NEXT_DATA__: {e}")

        if "amazon" in product_url.lower():
            item_weight = _find_generic_spec(soup, ["item weight", "weight"])
            if item_weight:
                return item_weight

        generic_weight = _find_generic_spec(
            soup, ["weight", "item weight", "shipping weight"]
        )
        if generic_weight:
            return generic_weight

    except Exception as e:
        print(f"Error in get_product_weight: {e}")
    return weight_str


def save_processed_images_locally(
    image_urls, product_name="product", output_dir="processed_images", max_images=10
):
    """
    Process images and save them locally as PNG 800x800.
    Returns a list of local file paths.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Create a safe filename from product name
    safe_product_name = re.sub(r"[^\w\s-]", "", product_name)[:50]
    safe_product_name = re.sub(r"[-\s]+", "-", safe_product_name)

    processed_files = []
    urls_to_process = (
        image_urls[:max_images] if len(image_urls) > max_images else image_urls
    )

    print(f"Processing {len(urls_to_process)} images...")

    for index, url in enumerate(urls_to_process):
        try:
            print(f"  Processing image {index + 1}/{len(urls_to_process)}...")

            # Download the image
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            # Check if it's AVIF format (not supported by default PIL)
            content_type = response.headers.get("content-type", "").lower()
            if "avif" in content_type or url.lower().endswith(".avif"):
                print(f"    Skipping AVIF image (format not supported)")
                continue

            try:
                # Open and process the image
                img = Image.open(BytesIO(response.content))
            except Exception as img_error:
                # If PIL can't open it, skip this image
                print(f"    Warning: Could not open image format: {img_error}")
                continue

            # Convert RGBA to RGB if necessary
            if img.mode in ("RGBA", "LA"):
                background = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "RGBA":
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img, mask=img.split()[1])
                img = background
            elif img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            # Resize to 800x800 maintaining aspect ratio
            img.thumbnail((800, 800), Image.Resampling.LANCZOS)

            # Create a new 800x800 white image
            new_img = Image.new("RGB", (800, 800), (255, 255, 255))

            # Paste the resized image in the center
            x = (800 - img.width) // 2
            y = (800 - img.height) // 2
            new_img.paste(img, (x, y))

            # Save the processed image
            filename = f"{safe_product_name}_image_{index + 1}.png"
            filepath = os.path.join(output_dir, filename)
            new_img.save(filepath, "PNG", optimize=True)

            processed_files.append(filepath)
            print(f"    Saved: {filename}")

        except Exception as e:
            print(f"    Error processing image: {e}")
            continue

    print(f"Successfully processed {len(processed_files)} images.")
    return processed_files


def fetch_product_info(product_url_original):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,en-CA;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }
    product_data = {"Product URL": product_url_original}
    soup_parser = "lxml"
    source = "Unknown"
    final_url_for_processing = product_url_original

    # Determine if we should use Selenium based on the site
    use_selenium = False
    selenium_wait_selector = None

    # Check if it's a site that needs Selenium
    if any(
        site in product_url_original.lower()
        for site in ["target.com", "walmart.com", "walmart.ca", "brandclub.com"]
    ):
        use_selenium = True and SELENIUM_AVAILABLE

        if "target.com" in product_url_original.lower():
            selenium_wait_selector = (
                '[data-test="product-price"], h1[data-test="product-title"]'
            )
        elif "walmart." in product_url_original.lower():
            selenium_wait_selector = 'span[itemprop="price"], h1[itemprop="name"]'
        elif "brandclub.com" in product_url_original.lower():
            selenium_wait_selector = 'h1, div[class*="price"], div[class*="Price"]'

    try:
        print(f"Fetching URL: {product_url_original}")

        # Try Selenium first for JavaScript-heavy sites
        page_source = None
        if use_selenium:
            page_source = get_page_with_selenium(
                product_url_original, wait_for_selector=selenium_wait_selector
            )
            # Selenium doesn't give us redirects easily
            final_url_for_processing = product_url_original

        # Fall back to requests if Selenium fails or isn't needed
        if not page_source:
            session = requests.Session()
            response = session.get(product_url_original, headers=headers, timeout=60)
            final_url_for_processing = response.url
            print(f"  Redirected to/Final URL: {final_url_for_processing}")
            response.raise_for_status()
            page_source = response.content

        # Determine source
        if "brandclub.com" in final_url_for_processing.lower():
            source = "BrandClub"
        elif "target.com" in final_url_for_processing.lower():
            source = "Target"
        elif (
            "ebay.com" in final_url_for_processing.lower()
            or "ebay.ca" in final_url_for_processing.lower()
        ):
            source = "eBay"
        elif "amazon." in final_url_for_processing.lower():
            source = "Amazon"
        elif "walmart.ca" in final_url_for_processing.lower():
            source = "Walmart CA"
        elif "walmart.com" in final_url_for_processing.lower():
            source = "Walmart US"
        elif "kohls.com" in final_url_for_processing.lower():
            source = "Kohls"
        elif "whatnot.com" in final_url_for_processing.lower():
            source = "Whatnot"
        elif "rona.ca" in final_url_for_processing.lower():
            source = "Rona CA"
        elif "iherb.com" in final_url_for_processing.lower():
            source = "iHerb"
        elif "homedepot.ca" in final_url_for_processing.lower():
            source = "Home Depot CA"
        elif "canadiantire.ca" in final_url_for_processing.lower():
            source = "Canadian Tire"

        try:
            soup = BeautifulSoup(page_source, soup_parser)
        except Exception:
            print(f"Parser '{soup_parser}' failed. Trying 'html.parser'.")
            soup_parser = "html.parser"
            soup = BeautifulSoup(page_source, soup_parser)

        product_data[PRODUCT_NAME_FIELD] = get_product_name(
            soup, final_url_for_processing
        )
        product_data[DESCRIPTION_FIELD] = get_product_description(
            soup, final_url_for_processing
        )
        product_data[PHOTOS_FIELD] = get_product_images(soup, final_url_for_processing)
        price_data = get_product_price(soup, final_url_for_processing)
        product_data[SALE_PRICE_FIELD] = price_data["sale_price"]
        product_data[ORIGINAL_PRICE_FIELD] = price_data["original_price"]
        product_data[CURRENCY_FIELD] = price_data["currency"]
        product_data[DIMENSIONS_FIELD] = get_product_dimensions(
            soup, final_url_for_processing
        )
        product_data[WEIGHT_FIELD] = get_product_weight(soup, final_url_for_processing)
        product_data[SOURCE_FIELD] = source
        product_data["Scraping Process Status"] = "Success"

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error: {e.response.status_code}"
        product_data["Error Detail"] = str(e)
        product_data["Scraping Process Status"] = error_msg
    except requests.exceptions.Timeout as e:
        error_msg = "Failed (Timeout)"
        product_data["Error Detail"] = str(e)
        product_data["Scraping Process Status"] = error_msg
    except requests.exceptions.RequestException as e:
        error_msg = "Failed (Request Error)"
        product_data["Error Detail"] = str(e)
        product_data["Scraping Process Status"] = error_msg
    except Exception as e:
        error_msg = "Failed (Processing Error)"
        product_data["Error Detail"] = str(e)[:250]
        product_data["Scraping Process Status"] = error_msg
        import traceback

        traceback.print_exc()

    if SOURCE_FIELD not in product_data:
        product_data[SOURCE_FIELD] = source
    if CURRENCY_FIELD not in product_data:
        product_data[CURRENCY_FIELD] = "N/A"
    if SALE_PRICE_FIELD not in product_data:
        product_data[SALE_PRICE_FIELD] = "N/A"
    if ORIGINAL_PRICE_FIELD not in product_data:
        product_data[ORIGINAL_PRICE_FIELD] = "N/A"

    print(f"  Name: {product_data.get(PRODUCT_NAME_FIELD, 'N/A')}")
    print(f"  Sale Price: {product_data.get(SALE_PRICE_FIELD, 'N/A')}")
    print(f"  Original Price: {product_data.get(ORIGINAL_PRICE_FIELD, 'N/A')}")
    print(f"  Currency: {product_data.get(CURRENCY_FIELD, 'N/A')}")
    print(f"  Source: {product_data.get(SOURCE_FIELD, 'N/A')}")
    print(f"  Images: {len(product_data.get(PHOTOS_FIELD, []))} found")
    print(f"  Dimensions: {product_data.get(DIMENSIONS_FIELD, 'N/A')}")
    print(f"  Weight: {product_data.get(WEIGHT_FIELD, 'N/A')}")

    return product_data


def test_brandclub_scraping(
    url="https://brandclub.com/Ombre-Water-Blob-Sun-Squad8482/p/7YBN6333Y/product",
):
    """Test function specifically for BrandClub.com scraping"""
    print(f"\n=== Testing BrandClub URL: {url} ===")

    # Test the full scraping
    product_data = fetch_product_info(url)

    print("\n--- RESULTS ---")
    for key, value in product_data.items():
        if key == PHOTOS_FIELD and isinstance(value, list):
            print(f"{key}: {len(value)} images found")
            for i, img_url in enumerate(value[:3], 1):
                print(f"  Image {i}: {img_url}")
        else:
            print(f"{key}: {value}")

    return product_data


def debug_brandclub_structure(url):
    """Debug function to analyze BrandClub page structure"""
    print(f"\n=== DEBUGGING BRANDCLUB STRUCTURE ===")
    print(f"URL: {url}")

    # Get page with Selenium
    page_source = None
    if SELENIUM_AVAILABLE:
        page_source = get_page_with_selenium(url)

    if not page_source:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        page_source = response.content

    soup = BeautifulSoup(page_source, "lxml")

    print("\n1. Looking for product title:")
    # Find all h1 tags and their classes
    h1_tags = soup.find_all("h1")
    if not h1_tags:
        print("   No H1 tags found!")
        # Look for other heading tags
        for tag in ["h2", "h3", "h4"]:
            elements = soup.find_all(tag)
            if elements:
                print(f"   Found {len(elements)} {tag.upper()} tags:")
                for i, elem in enumerate(elements[:3]):
                    classes = " ".join(elem.get("class", []))
                    text = elem.get_text(strip=True)[:100]
                    print(
                        f"     {tag.upper()} #{i+1}: classes='{classes}', text='{text}'"
                    )
    else:
        for i, h1 in enumerate(h1_tags[:5]):
            classes = " ".join(h1.get("class", []))
            text = h1.get_text(strip=True)[:100]
            print(f"   H1 #{i+1}: classes='{classes}', text='{text}'")

    # Look for product_name container specifically
    product_name_divs = soup.find_all("div", class_="product_name")
    if product_name_divs:
        print(f"\n   Found {len(product_name_divs)} div.product_name elements:")
        for i, div in enumerate(product_name_divs[:3]):
            text = div.get_text(strip=True)[:200]
            print(f"     Product Name Div #{i+1}: text='{text}'")
            # Check what's inside
            inner_elements = div.find_all(
                ["h1", "h2", "h3", "h4", "h5", "h6", "p", "span", "a"]
            )
            if inner_elements:
                print(f"       Contains {len(inner_elements)} inner elements:")
                for elem in inner_elements[:5]:
                    inner_text = elem.get_text(strip=True)[:100]
                    inner_classes = " ".join(elem.get("class", []))
                    print(
                        f"       - {elem.name} (classes: '{inner_classes}'): '{inner_text}'"
                    )

    print("\n2. Looking for price elements:")
    # Find elements that might contain price
    price_candidates = soup.find_all(
        ["span", "div", "p"], string=re.compile(r"\$\d+\.?\d*")
    )
    for i, elem in enumerate(price_candidates[:10]):
        classes = " ".join(elem.get("class", []))
        text = elem.get_text(strip=True)
        parent_classes = " ".join(elem.parent.get("class", [])) if elem.parent else ""
        print(
            f"   Price #{i+1}: tag={elem.name}, classes='{classes}', parent_classes='{parent_classes}', text='{text}'"
        )

    # Also check for specific price containers
    price_divs = soup.find_all("div", class_="price")
    if price_divs:
        print(f"\n   Found {len(price_divs)} div.price elements:")
        for i, div in enumerate(price_divs[:3]):
            parent_classes = " ".join(div.parent.get("class", [])) if div.parent else ""
            text = div.get_text(strip=True)
            print(f"     Price Div #{i+1}: parent='{parent_classes}', text='{text}'")

    print("\n3. Looking for main product images:")
    # Find image containers
    img_containers = soup.find_all(
        ["div", "figure"], class_=re.compile(r"[Pp]roduct|[Ii]mage|[Gg]allery")
    )
    for i, container in enumerate(img_containers[:5]):
        classes = " ".join(container.get("class", []))
        imgs = container.find_all("img")
        print(f"   Container #{i+1}: classes='{classes}', contains {len(imgs)} images")
        for j, img in enumerate(imgs[:2]):
            src = img.get("src", "No src")[:80]
            alt = img.get("alt", "No alt")[:50]
            print(f"      Image: alt='{alt}', src='{src}...'")

    print("\n4. Checking for React/Next.js data:")
    # Look for __NEXT_DATA__ or React props
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        print("   Found __NEXT_DATA__ script tag")
        try:
            import json

            data = json.loads(next_data.string)
            # Try to find product data in the structure
            if "props" in data:
                print("   Contains props data")
        except:
            print("   Could not parse __NEXT_DATA__")

    # Look for data in script tags
    scripts_with_product = soup.find_all(
        "script", string=re.compile(r"product|price|image", re.I)
    )
    print(
        f"\n5. Found {len(scripts_with_product)} script tags with product-related content"
    )

    # Additional debug: look for any element containing product-like text
    print("\n6. Looking for any element with product-like text:")
    # Search for common product name patterns
    product_keywords = [
        "Pool Float",
        "Water Blob",
        "Cheetah",
        "Squad",
        "Kids",
        "Orange",
        "Sun",
    ]
    found_product_text = False

    for tag in ["div", "span", "p", "h1", "h2", "h3", "h4", "h5", "h6", "a"]:
        for keyword in product_keywords:
            elements = soup.find_all(tag, string=re.compile(rf"{keyword}", re.I))
            for elem in elements:
                text = elem.get_text(strip=True)
                # Filter out very short or very long text
                if 10 < len(text) < 200 and not any(
                    skip in text.lower()
                    for skip in ["reward", "cash back", "earn", "get", "price"]
                ):
                    classes = " ".join(elem.get("class", []))
                    parent = elem.parent
                    parent_classes = " ".join(parent.get("class", [])) if parent else ""
                    parent_tag = parent.name if parent else "None"

                    # Get grandparent for more context
                    grandparent = parent.parent if parent else None
                    grandparent_classes = (
                        " ".join(grandparent.get("class", [])) if grandparent else ""
                    )

                    print(f"   Found in {tag.upper()}: text='{text}'")
                    print(f"     - Classes: '{classes}'")
                    print(f"     - Parent: {parent_tag} (classes: '{parent_classes}')")
                    print(f"     - Grandparent classes: '{grandparent_classes}'")
                    found_product_text = True
                    break
            if found_product_text:
                break
        if found_product_text:
            break

    if not found_product_text:
        print("   No product text found with common keywords!")

    # Save the HTML for manual inspection
    with open("brandclub_debug.html", "w", encoding="utf-8") as f:
        f.write(str(soup.prettify()))
    print("\n7. Full HTML saved to 'brandclub_debug.html' for manual inspection")

    return soup


# --- Main Airtable Integration Logic ---
if __name__ == "__main__":
    import sys

    # Check if running in debug mode
    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        if len(sys.argv) > 2:
            debug_url = sys.argv[2]
        else:
            debug_url = "https://brandclub.com/Kids-Cheetah-Pool-Float-Orange-Sun-Sq/p/6R6Y2YWVG/product"

        print(f"Running in debug mode for URL: {debug_url}")
        debug_brandclub_structure(debug_url)
        sys.exit(0)

    # Check if running test mode
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        if len(sys.argv) > 2:
            test_url = sys.argv[2]
        else:
            test_url = "https://brandclub.com/Ombre-Water-Blob-Sun-Squad8482/p/7YBN6333Y/product"

        print(f"Running test for URL: {test_url}")
        test_brandclub_scraping(test_url)
        sys.exit(0)

    print("--- Script Starting ---")
    print(
        "Ensure 'Sale Price' and 'Original Price' fields exist in your Airtable as Currency or Number type."
    )
    print(
        "\n⚠️  SECURITY WARNING: Never hardcode API keys! Use environment variables instead."
    )

    # To debug BrandClub structure, run:
    # python fetch_me_url_airtable_v3.py debug
    # or
    # python fetch_me_url_airtable_v3.py debug "https://brandclub.com/your-product-url"

    # Check for Selenium
    if not SELENIUM_AVAILABLE:
        print(
            "\n⚠️  Selenium is not available. To scrape JavaScript-heavy sites like Target and BrandClub:"
        )
        print("   1. Install Selenium: pip install selenium")
        print("   2. Download ChromeDriver from: https://chromedriver.chromium.org/")
        print(
            "   3. Add ChromeDriver to your PATH or specify its location in the script"
        )
        print("   Without Selenium, some sites may not scrape properly.\n")

    if not Api:
        print("Airtable library (pyairtable) is not installed. Exiting.")
        exit()

    if (
        AIRTABLE_API_KEY == "YOUR_AIRTABLE_API_KEY_HERE"
        or AIRTABLE_BASE_ID == "YOUR_AIRTABLE_BASE_ID_HERE"
    ):
        print(
            "ERROR: Please set AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables."
        )
        print("Example: export AIRTABLE_API_KEY='your_key_here'")
        print("Or create a .env file with:")
        print("AIRTABLE_API_KEY=your_key_here")
        print("AIRTABLE_BASE_ID=your_base_id_here")
        exit()

    print(
        f"Connecting to Airtable. Base: {AIRTABLE_BASE_ID}, Table: {AIRTABLE_TABLE_NAME}"
    )
    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        print("Successfully connected to Airtable.")
    except Exception as e:
        print(f"Failed to connect to Airtable: {e}")
        exit()

    try:
        print(
            f"Fetching records from '{AIRTABLE_TABLE_NAME}' using view '{AIRTABLE_VIEW_NAME}'..."
        )
        formula_str = f"AND({{Status}} = 'Entered', NOT({{Product URL}} = ''), OR({{Scraping Status}} = '', NOT({{Scraping Status}})))"
        print(f"Using formula: {formula_str}")

        records_to_process = table.all(formula=formula_str, view=AIRTABLE_VIEW_NAME)

        if not records_to_process:
            print(
                f"No records to process. (Looking for records with '{URL_FIELD_NAME}' present and '{STATUS_FIELD}' empty in view '{AIRTABLE_VIEW_NAME}')."
            )
        else:
            print(
                f"Found {len(records_to_process)} records to process from view '{AIRTABLE_VIEW_NAME}'."
            )

        for record in records_to_process:
            record_id = record["id"]
            product_url_from_airtable = record["fields"].get(URL_FIELD_NAME)

            if not product_url_from_airtable:
                print(f"Skipping record {record_id}: '{URL_FIELD_NAME}' empty.")
                try:
                    table.update(record_id, {STATUS_FIELD: "Skipped (No URL)"})
                except:
                    pass
                continue

            print(
                f"\n--- Processing Record ID: {record_id}, URL: {product_url_from_airtable} ---"
            )
            scraped_info = fetch_product_info(product_url_from_airtable)
            fields_to_update = {}
            scraping_outcome_status = scraped_info.get(
                "Scraping Process Status", "Failed (Unknown)"
            )

            if scraping_outcome_status == "Success":
                fields_to_update[PRODUCT_NAME_FIELD] = scraped_info.get(
                    PRODUCT_NAME_FIELD, "N/A"
                )
                fields_to_update[DESCRIPTION_FIELD] = scraped_info.get(
                    DESCRIPTION_FIELD, "N/A"
                )
                fields_to_update[DIMENSIONS_FIELD] = scraped_info.get(
                    DIMENSIONS_FIELD, "N/A"
                )
                fields_to_update[WEIGHT_FIELD] = str(
                    scraped_info.get(WEIGHT_FIELD, "N/A")
                )

                # --- PRICE HANDLING ---
                try:
                    sale_price_val = float(scraped_info.get(SALE_PRICE_FIELD))
                    fields_to_update[SALE_PRICE_FIELD] = sale_price_val
                except (ValueError, TypeError):
                    print(
                        f"  Note: Could not convert Sale Price '{scraped_info.get(SALE_PRICE_FIELD)}' to a number. Skipping field."
                    )
                    pass

                try:
                    original_price_val = float(scraped_info.get(ORIGINAL_PRICE_FIELD))
                    fields_to_update[ORIGINAL_PRICE_FIELD] = original_price_val
                except (ValueError, TypeError):
                    print(
                        f"  Note: Could not convert Original Price '{scraped_info.get(ORIGINAL_PRICE_FIELD)}' to a number. Skipping field."
                    )
                    pass

                fields_to_update[CURRENCY_FIELD] = scraped_info.get(
                    CURRENCY_FIELD, "Unknown"
                )
                fields_to_update[SOURCE_FIELD] = scraped_info.get(
                    SOURCE_FIELD, "Unknown"
                )

                # --- IMAGE HANDLING WITH CONVERSION ---
                photos_url_list = scraped_info.get(PHOTOS_FIELD, [])

                # Store the original URLs for reference
                fields_to_update[PHOTOS_FIELD] = (
                    ", ".join(photos_url_list)
                    if photos_url_list
                    else "No Image URLs Found"
                )

                # Process and save images locally
                if photos_url_list:
                    print(f"Processing {len(photos_url_list)} images to PNG 800x800...")
                    product_name = scraped_info.get(PRODUCT_NAME_FIELD, "product")
                    processed_files = save_processed_images_locally(
                        photos_url_list, product_name=product_name, max_images=10
                    )

                    # For Airtable, we'll still use the original URLs
                    # The processed images are saved locally for your use
                    attachment_objects = [
                        {"url": img_url}
                        for img_url in photos_url_list[:10]
                        if isinstance(img_url, str) and img_url.startswith("http")
                    ]
                    fields_to_update[PHOTO_FILES_FIELD] = attachment_objects

                    if processed_files:
                        print(
                            f"  Processed images saved locally in 'processed_images' folder"
                        )
                        print(
                            f"  Files: {', '.join([os.path.basename(f) for f in processed_files])}"
                        )
                else:
                    fields_to_update[PHOTO_FILES_FIELD] = []

                # Check for missing critical fields
                critical_missing = (
                    not scraped_info.get(PRODUCT_NAME_FIELD)
                    or scraped_info[PRODUCT_NAME_FIELD]
                    in ["", "Product Name Not Found"]
                    or not scraped_info.get(DESCRIPTION_FIELD)
                    or scraped_info[DESCRIPTION_FIELD]
                    in ["", "Product Description Not Found"]
                    or not scraped_info.get(SALE_PRICE_FIELD)
                    or scraped_info[SALE_PRICE_FIELD] in ["", "Not Found"]
                    or not scraped_info.get(PHOTOS_FIELD)
                    or not isinstance(scraped_info[PHOTOS_FIELD], list)
                    or len(scraped_info[PHOTOS_FIELD]) == 0
                )

                if critical_missing:
                    final_airtable_status = "Needs Attention"
                    fields_to_update[STATUS_FIELD] = "Scraped"
                    fields_to_update[ASSIGNED_TO_FIELD] = "Naimish Silwal"
                    print(
                        f"⚠️ Critical fields missing, setting status to 'Needs Attention' and assigning to Naim."
                    )
                else:
                    final_airtable_status = "Scraped"
                    print(f"Scraping outcome for {product_url_from_airtable}: Success")
            else:
                fields_to_update[PRODUCT_NAME_FIELD] = scraped_info.get(
                    PRODUCT_NAME_FIELD, "Scraping Error"
                )
                fields_to_update[DESCRIPTION_FIELD] = scraped_info.get(
                    DESCRIPTION_FIELD, scraped_info.get("Error Detail", "N/A")
                )
                fields_to_update[SOURCE_FIELD] = scraped_info.get(SOURCE_FIELD, "N/A")
                final_airtable_status = scraping_outcome_status
                print(
                    f"Scraping outcome for {product_url_from_airtable}: {final_airtable_status}"
                )

            fields_to_update[STATUS_FIELD] = "Scraped"
            try:
                table.update(record_id, fields_to_update)
                print(
                    f"Airtable record {record_id} updated. Status: {final_airtable_status}"
                )
            except Exception as e_airtable_update:
                print(
                    f"ERROR updating Airtable record {record_id}: {e_airtable_update}"
                )
                error_str_lower = str(e_airtable_update).lower()
                fallback_status = "Airtable Update Error"
                if "invalid_attachment_object" in error_str_lower:
                    fallback_status = "Photos: Attachment Field Error"
                elif "invalid_value_for_column" in error_str_lower:
                    fallback_status = "Field Type: Value Error"
                elif "invalid_multiple_choice_options" in error_str_lower:
                    fallback_status = "Status: Select Option Error"
                try:
                    table.update(record_id, {STATUS_FIELD: fallback_status})
                except:
                    pass

        print("\n--- All specified records processed. ---")
    except Exception as e_main:
        print(f"An error occurred in the main Airtable processing loop: {e_main}")
        import traceback

        traceback.print_exc()

    print("\n--- Script Finished ---")



def parse_menagerieflower_com(driver):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(driver.page_source, "html.parser")
    result = {}

    # Product Name
    title_tag = soup.find("h1", class_="product-title")
    result["Product Name"] = title_tag.get_text(strip=True) if title_tag else "Product Name Not Found"

    # Description
    desc_tag = soup.find("div", class_="product-description")
    result["Product Description"] = desc_tag.decode_contents().strip() if desc_tag else "Product Description Not Found"

    # Price
    price_tag = soup.find("span", class_="price-item")
    result["Sale Price"] = price_tag.get_text(strip=True).replace("$", "") if price_tag else "Not Found"
    result["Original Price"] = result["Sale Price"]

    # Photos
    image_tags = soup.select("img.product__media-img")
    photos = []
    for img in image_tags:
        src = img.get("src") or img.get("data-src")
        if src and src.startswith("//"):
            src = "https:" + src
        if src:
            photos.append(src)
    result["Photos"] = photos if photos else []

    return result




def parse_cosmaroma_com(driver):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(driver.page_source, "html.parser")
    result = {}

    # Product Name
    title_tag = soup.find("h1", class_="product_title")
    result["Product Name"] = title_tag.get_text(strip=True) if title_tag else "Product Name Not Found"

    # Description
    desc_tag = soup.find("div", class_="woocommerce-Tabs-panel--description")
    result["Product Description"] = desc_tag.decode_contents().strip() if desc_tag else "Product Description Not Found"

    # Price
    price_tag = soup.find("p", class_="price")
    if price_tag:
        current_price = price_tag.find("ins")
        if current_price:
            result["Sale Price"] = current_price.get_text(strip=True).replace("€", "").strip()
            original_price = price_tag.find("del")
            result["Original Price"] = original_price.get_text(strip=True).replace("€", "").strip() if original_price else result["Sale Price"]
        else:
            result["Sale Price"] = price_tag.get_text(strip=True).replace("€", "").strip()
            result["Original Price"] = result["Sale Price"]
    else:
        result["Sale Price"] = "Not Found"
        result["Original Price"] = "Not Found"

    # Photos
    image_tags = soup.select("figure.woocommerce-product-gallery__wrapper img")
    photos = []
    for img in image_tags:
        src = img.get("src") or img.get("data-src")
        if src:
            photos.append(src)
    result["Photos"] = photos if photos else []

    return result




def parse_sail_ca(driver):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(driver.page_source, "html.parser")
    result = {}

    # Product Name
    title_tag = soup.find("h1", class_="product-name")
    result["Product Name"] = title_tag.get_text(strip=True) if title_tag else "Product Name Not Found"

    # Description
    desc_tag = soup.find("div", class_="product-description")
    result["Product Description"] = desc_tag.decode_contents().strip() if desc_tag else "Product Description Not Found"

    # Price
    price_tag = soup.find("span", class_="price")
    result["Sale Price"] = price_tag.get_text(strip=True).replace("$", "").strip() if price_tag else "Not Found"
    result["Original Price"] = result["Sale Price"]

    # Photos
    photos = []
    image_tags = soup.select("div.swiper-wrapper img.swiper-lazy")
    for img in image_tags:
        src = img.get("data-src") or img.get("src")
        if src and src.startswith("//"):
            src = "https:" + src
        if src:
            photos.append(src)
    result["Photos"] = photos if photos else []

    return result




def parse_notyourmothers_com(driver):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(driver.page_source, "html.parser")
    result = {}

    # Product Name
    title_tag = soup.find("h1", class_="product__title")
    result["Product Name"] = title_tag.get_text(strip=True) if title_tag else "Product Name Not Found"

    # Description
    desc_tag = soup.find("div", class_="product__description rte")
    result["Product Description"] = desc_tag.decode_contents().strip() if desc_tag else "Product Description Not Found"

    # Price
    price_tag = soup.find("div", class_="price__container")
    if price_tag:
        price_text = price_tag.get_text(strip=True).replace("$", "").strip()
        result["Sale Price"] = price_text
        result["Original Price"] = price_text
    else:
        result["Sale Price"] = "Not Found"
        result["Original Price"] = "Not Found"

    # Photos
    photos = []
    image_tags = soup.select("div.product__media-item img")
    for img in image_tags:
        src = img.get("src") or img.get("data-src")
        if src and src.startswith("//"):
            src = "https:" + src
        if src:
            photos.append(src)
    result["Photos"] = photos if photos else []

    return result

