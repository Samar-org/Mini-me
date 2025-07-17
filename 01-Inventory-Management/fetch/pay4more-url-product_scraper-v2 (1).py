import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import re
import json
from io import BytesIO
from PIL import Image
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
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = "Items-Pay4more"
AIRTABLE_VIEW_NAME = "Pay4more Sync View"

# --- Airtable Field Names ---
URL_FIELD_NAME = "Product URL"
PRODUCT_NAME_FIELD = "Product Name"
DESCRIPTION_FIELD = "Description"
DIMENSIONS_FIELD = "Dimensions"
WEIGHT_FIELD = "Weight"
PHOTOS_FIELD = "Photos URL"
PHOTO_FILES_FIELD = "Photo Files"
SALE_PRICE_FIELD = "Sale Price"
ORIGINAL_PRICE_FIELD = "Original Price"
CURRENCY_FIELD = "Currency"
SOURCE_FIELD = "Source"
STATUS_FIELD = "Scraping Status"


# --- Helper Functions for Scraping ---


def get_page_with_selenium(url, timeout=25):
    """
    Enhanced Selenium fetcher with better logic for dynamic sites.
    """
    if not SELENIUM_AVAILABLE:
        print("⚠️  Selenium not available, falling back to regular requests")
        return None

    driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        print(f"  Loading page with Selenium...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)

        # Site-specific waiting logic
        if "walmart." in url:
            print("  Special Walmart handling...")
            # Wait for the __NEXT_DATA__ script, which contains all product info
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, "__NEXT_DATA__"))
            )
            time.sleep(3)  # Extra wait for any client-side rendering to finish
            print("  ✓ Walmart page data loaded")

        elif "target.com" in url:
            print("  Special Target.com handling...")
            # Wait for a key element in the product display
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        'h1[data-test="product-title"], div[data-test*="hero"]',
                    )
                )
            )
            time.sleep(3)
            print("  ✓ Target page loaded")

        else:
            # Generic wait for other sites
            print("  Generic wait...")
            time.sleep(5)

        page_source = driver.page_source
        print("  ✓ Page source captured successfully with Selenium")
        return page_source

    except TimeoutException:
        print(f"  ❌ Selenium timeout waiting for page elements to load.")
        if driver:
            return driver.page_source  # Return what we have so far
        return None
    except WebDriverException as e:
        print(f"  ❌ Selenium error: {str(e)}")
        return None
    finally:
        if driver:
            driver.quit()


def _get_walmart_json(soup):
    """Helper to extract and parse the __NEXT_DATA__ JSON from Walmart pages."""
    next_data_script = soup.find("script", id="__NEXT_DATA__")
    if next_data_script and next_data_script.string:
        try:
            next_data = json.loads(next_data_script.string)
            # Navigate to the product data within the JSON structure
            product_data = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("initialData", {})
                .get("data", {})
                .get("product", {})
            )
            if product_data:
                return product_data
        except (json.JSONDecodeError, AttributeError, KeyError):
            return None
    return None


def get_product_name(soup, product_url):
    name_text = "Product Name Not Found"

    # --- Walmart (JSON First) ---
    if "walmart." in product_url.lower():
        product_json = _get_walmart_json(soup)
        if product_json and product_json.get("name"):
            name_text = product_json["name"]
            print(f"Debug Walmart Name (JSON): Found '{name_text}'")
            return name_text
        # Fallback to HTML
        name_tag = soup.find("h1", itemprop="name")
        if name_tag:
            name_text = name_tag.get_text(strip=True)

    # --- Amazon ---
    elif "amazon." in product_url.lower():
        name_tag = soup.find("span", id="productTitle")
        if name_tag:
            name_text = name_tag.get_text(strip=True)

    # --- Target ---
    elif "target.com" in product_url.lower():
        name_tag = soup.select_one('h1[data-test="product-title"]')
        if name_tag:
            name_text = name_tag.get_text(strip=True)

    # --- eBay ---
    elif "ebay.com" in product_url.lower() or "ebay.ca" in product_url.lower():
        name_tag = soup.select_one("h1.x-item-title__mainTitle span.ux-textspans--BOLD")
        if name_tag:
            name_text = name_tag.get_text(strip=True)
        else:
            name_tag = soup.select_one("h1.x-item-title__mainTitle")
            if name_tag:
                name_text = name_tag.get_text(strip=True)

    # --- Canadian Tire ---
    elif "canadiantire.ca" in product_url.lower():
        name_tag = soup.find("h1", class_="pdp-product-title__name")
        if name_tag:
            name_text = name_tag.get_text(strip=True)

    # --- Fallback for any other site ---
    else:
        name_tag = soup.find("h1")
        if name_tag:
            name_text = name_tag.get_text(strip=True)

    if name_text == "Product Name Not Found":
        print("Debug: Product name not found with primary selectors.")

    return name_text


def get_product_description(soup, product_url):
    description_text = "Product Description Not Found"

    # --- Walmart (JSON First) ---
    if "walmart." in product_url.lower():
        product_json = _get_walmart_json(soup)
        if product_json:
            long_desc = product_json.get("longDescription")
            short_desc = product_json.get("shortDescription")

            # Prefer long description, fallback to short
            desc_html = long_desc if long_desc else short_desc
            if desc_html:
                desc_soup = BeautifulSoup(desc_html, "html.parser")
                description_text = desc_soup.get_text(separator=" ", strip=True)
                return description_text
        # Fallback to HTML
        about_section = soup.find("div", class_=re.compile(r"about-item-description"))
        if about_section:
            description_text = about_section.get_text(separator=" ", strip=True)

    # --- Amazon ---
    elif "amazon." in product_url.lower():
        feature_bullets_div = soup.find("div", id="feature-bullets")
        if feature_bullets_div:
            bullets = feature_bullets_div.select(
                "ul.a-unordered-list li span.a-list-item"
            )
            bullet_texts = [b.get_text(strip=True) for b in bullets]
            description_text = ". ".join(bullet_texts)
        else:
            desc_tag = soup.find("div", id="productDescription")
            if desc_tag:
                description_text = desc_tag.get_text(separator=" ", strip=True)

    # --- Add other site-specific description logic here as elif blocks ---

    # --- Generic Fallback ---
    else:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.has_attr("content"):
            description_text = meta_desc["content"].strip()

    return description_text


def get_product_images(soup, product_url):
    images = []

    # --- Walmart (JSON First) ---
    if "walmart." in product_url.lower():
        product_json = _get_walmart_json(soup)
        if product_json:
            image_info = product_json.get("imageInfo", {})
            all_images_data = image_info.get("allImages", [])
            for img_data in all_images_data:
                # Get the highest resolution URL available
                img_url = img_data.get("url")
                if img_url:
                    # Ensure URL is clean and high-res
                    base_img_url = img_url.split("?")[0]
                    images.append(
                        f"{base_img_url}?odnHeight=2000&odnWidth=2000&odnBg=FFFFFF"
                    )
            if images:
                print(f"Debug Walmart Images (JSON): Found {len(images)} images.")
                return list(dict.fromkeys(images))  # Return unique images
        # Fallback to HTML
        thumb_container = soup.find("div", class_=re.compile(r"slider-wrapper"))
        if thumb_container:
            img_tags = thumb_container.find_all("img")
            for tag in img_tags:
                src = tag.get("src")
                if src and "default.jpg" not in src:
                    base_img_url = src.split("?")[0]
                    images.append(
                        f"{base_img_url}?odnHeight=2000&odnWidth=2000&odnBg=FFFFFF"
                    )

    # --- Amazon ---
    elif "amazon." in product_url.lower():
        image_container = soup.find("div", id="altImages")
        if image_container:
            img_tags = image_container.select("li.item span.a-button-text img")
            for tag in img_tags:
                src = tag.get("src")
                if src:
                    # Convert thumbnail URL to high-res URL
                    high_res_src = re.sub(r"\._.*?_\.", "._AC_SL1500_.", src)
                    images.append(high_res_src)

    # --- Add other site-specific image logic here as elif blocks ---

    if not images:
        # Generic fallback if no images found yet
        all_imgs = soup.find_all("img")
        for img in all_imgs:
            src = img.get("src") or img.get("data-src")
            if src and src.startswith("http"):
                # Simple filter to avoid logos/icons
                if (
                    "logo" not in src.lower()
                    and "icon" not in src.lower()
                    and "spinner" not in src.lower()
                ):
                    images.append(urljoin(product_url, src))

    # Return up to 10 unique images
    return list(dict.fromkeys(images))[:10]


def _clean_price_text(price_text):
    if not price_text:
        return None
    # Keep only digits and a single decimal point
    cleaned = re.sub(r"[^\d.]", "", str(price_text))
    return cleaned if cleaned else None


def get_product_price(soup, product_url):
    price_info = {
        "sale_price": "Not Found",
        "original_price": "N/A",
        "currency": "Unknown",
    }

    # --- Walmart (JSON First) ---
    if "walmart." in product_url.lower():
        product_json = _get_walmart_json(soup)
        if product_json:
            price_data = product_json.get("priceInfo", {})
            current_price_obj = price_data.get("currentPrice")
            was_price_obj = price_data.get("wasPrice")

            if current_price_obj and current_price_obj.get("price"):
                price_info["sale_price"] = _clean_price_text(current_price_obj["price"])
                price_info["currency"] = current_price_obj.get("currencyUnit", "CAD")

            if was_price_obj and was_price_obj.get("price"):
                price_info["original_price"] = _clean_price_text(was_price_obj["price"])

            return price_info
        # Fallback to HTML
        price_tag = soup.select_one('span[itemprop="price"]')
        if price_tag:
            price_info["sale_price"] = _clean_price_text(price_tag.get("content"))
            currency_tag = soup.select_one('span[itemprop="priceCurrency"]')
            if currency_tag:
                price_info["currency"] = currency_tag.get("content")

    # --- Amazon ---
    elif "amazon." in product_url.lower():
        price_tag = soup.select_one("span.a-price-whole")
        if price_tag:
            fraction_tag = soup.select_one("span.a-price-fraction")
            price_info["sale_price"] = price_tag.get_text(strip=True) + (
                fraction_tag.get_text(strip=True) if fraction_tag else ""
            )

        if "amazon.ca" in product_url:
            price_info["currency"] = "CAD"
        elif "amazon.com" in product_url:
            price_info["currency"] = "USD"

    # --- Add other site-specific price logic here as elif blocks ---

    return price_info


# --- The dimensions and weight functions can be simplified or kept as is if they work ---
def get_product_dimensions(soup, product_url):
    # This function can be improved with site-specific logic like the others
    return "Not Implemented"


def get_product_weight(soup, product_url):
    # This function can be improved with site-specific logic like the others
    return "Not Implemented"


def fetch_product_info(product_url_original):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    product_data = {"Product URL": product_url_original}
    source = "Unknown"
    final_url = product_url_original

    # Determine if Selenium is needed
    use_selenium = any(
        site in product_url_original.lower() for site in ["walmart.", "target."]
    )

    try:
        if use_selenium and SELENIUM_AVAILABLE:
            page_source = get_page_with_selenium(product_url_original)
            if not page_source:
                raise Exception("Selenium failed to retrieve page source.")
        else:
            print("  Fetching with standard requests...")
            response = requests.get(product_url_original, headers=headers, timeout=20)
            final_url = response.url  # Update URL after redirects
            response.raise_for_status()
            page_source = response.content

        print(f"  Final URL for processing: {final_url}")
        if "walmart.ca" in final_url:
            source = "Walmart CA"
        elif "walmart.com" in final_url:
            source = "Walmart US"
        elif "amazon.ca" in final_url:
            source = "Amazon CA"
        elif "amazon.com" in final_url:
            source = "Amazon US"
        # ... add other sources

        soup = BeautifulSoup(page_source, "lxml")

        product_data[PRODUCT_NAME_FIELD] = get_product_name(soup, final_url)
        product_data[DESCRIPTION_FIELD] = get_product_description(soup, final_url)
        product_data[PHOTOS_FIELD] = get_product_images(soup, final_url)
        price_data = get_product_price(soup, final_url)
        product_data[SALE_PRICE_FIELD] = price_data["sale_price"]
        product_data[ORIGINAL_PRICE_FIELD] = price_data["original_price"]
        product_data[CURRENCY_FIELD] = price_data["currency"]
        product_data[DIMENSIONS_FIELD] = get_product_dimensions(soup, final_url)
        product_data[WEIGHT_FIELD] = get_product_weight(soup, final_url)
        product_data[SOURCE_FIELD] = source
        product_data["Scraping Process Status"] = "Success"

    except Exception as e:
        product_data["Scraping Process Status"] = f"Failed: {type(e).__name__}"
        product_data["Error Detail"] = str(e)
        import traceback

        traceback.print_exc()

    print(f"  Name: {product_data.get(PRODUCT_NAME_FIELD, 'N/A')}")
    print(
        f"  Sale Price: {product_data.get(SALE_PRICE_FIELD, 'N/A')} {product_data.get(CURRENCY_FIELD, '')}"
    )
    print(f"  Images: {len(product_data.get(PHOTOS_FIELD, []))} found")

    return product_data


# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    if not Api or not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
        print(
            "ERROR: Airtable configuration is missing. Please set AIRTABLE_API_KEY and AIRTABLE_BASE_ID."
        )
        exit()

    try:
        api = Api(AIRTABLE_API_KEY)
        table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        print("✓ Successfully connected to Airtable.")
    except Exception as e:
        print(f"❌ Failed to connect to Airtable: {e}")
        exit()

    try:
        formula_str = f"AND({{URL}}, NOT({{{STATUS_FIELD}}}))"
        print(
            f"Fetching records from '{AIRTABLE_TABLE_NAME}' view '{AIRTABLE_VIEW_NAME}'..."
        )
        records_to_process = table.all(view=AIRTABLE_VIEW_NAME, formula=formula_str)

        if not records_to_process:
            print("No new records to process.")
        else:
            print(f"Found {len(records_to_process)} records to process.")

        for record in records_to_process:
            record_id = record["id"]
            product_url = record["fields"].get(URL_FIELD_NAME)

            if not product_url:
                table.update(record_id, {STATUS_FIELD: "Skipped (No URL)"})
                continue

            print(f"\n--- Processing Record: {record_id}, URL: {product_url} ---")
            scraped_info = fetch_product_info(product_url)

            fields_to_update = {}
            scraping_outcome_status = scraped_info.get(
                "Scraping Process Status", "Failed (Unknown)"
            )

            # Check if essential data was found
            product_name = scraped_info.get(PRODUCT_NAME_FIELD)
            if (
                scraping_outcome_status == "Success"
                and product_name != "Product Name Not Found"
            ):
                fields_to_update[PRODUCT_NAME_FIELD] = product_name
                fields_to_update[DESCRIPTION_FIELD] = scraped_info.get(
                    DESCRIPTION_FIELD, "N/A"
                )
                fields_to_update[DIMENSIONS_FIELD] = scraped_info.get(
                    DIMENSIONS_FIELD, "N/A"
                )
                fields_to_update[WEIGHT_FIELD] = str(
                    scraped_info.get(WEIGHT_FIELD, "N/A")
                )

                # --- Price Handling ---
                try:
                    fields_to_update[SALE_PRICE_FIELD] = float(
                        scraped_info.get(SALE_PRICE_FIELD)
                    )
                except (ValueError, TypeError):
                    pass  # Skip if price is not a valid number

                try:
                    fields_to_update[ORIGINAL_PRICE_FIELD] = float(
                        scraped_info.get(ORIGINAL_PRICE_FIELD)
                    )
                except (ValueError, TypeError):
                    pass

                fields_to_update[CURRENCY_FIELD] = scraped_info.get(
                    CURRENCY_FIELD, "Unknown"
                )
                fields_to_update[SOURCE_FIELD] = scraped_info.get(
                    SOURCE_FIELD, "Unknown"
                )

                # --- Image Handling ---
                photo_urls = scraped_info.get(PHOTOS_FIELD, [])
                fields_to_update[PHOTOS_FIELD] = (
                    ", ".join(photo_urls) if photo_urls else "No Image URLs Found"
                )
                if photo_urls:
                    attachment_objects = [{"url": url} for url in photo_urls]
                    fields_to_update[PHOTO_FILES_FIELD] = attachment_objects

                final_airtable_status = "Success"

            else:
                # If scraping failed or found no name, mark as failed.
                final_airtable_status = "Failed (No Data Found)"
                fields_to_update[DESCRIPTION_FIELD] = scraped_info.get(
                    "Error Detail", "Could not find product data on page."
                )

            fields_to_update[STATUS_FIELD] = final_airtable_status
            try:
                table.update(record_id, fields_to_update)
                print(
                    f"✓ Airtable record {record_id} updated. Status: {final_airtable_status}"
                )
            except Exception as e:
                print(f"❌ ERROR updating Airtable record {record_id}: {e}")
                # Try to at least update the status to avoid reprocessing
                table.update(record_id, {STATUS_FIELD: "Airtable Update Error"})

        print("\n--- All records processed. ---")
    except Exception as e:
        print(f"❌ An unexpected error occurred in the main loop: {e}")
        import traceback

        traceback.print_exc()

    print("\n--- Script Finished ---")
