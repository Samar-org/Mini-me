import os
from woocommerce import API
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment
load_dotenv()

# WooCommerce credentials
WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WC_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")

# Parse base URL
BASE_URL = f"{urlparse(WC_URL).scheme}://{urlparse(WC_URL).netloc}"

# Initialize WooCommerce API
wcapi = API(
    url=BASE_URL,
    consumer_key=WC_KEY,
    consumer_secret=WC_SECRET,
    version="wc/v3",
    timeout=30,
)

# Test the brands endpoint
response = wcapi.get("products/brands", params={"per_page": 1})
print("Status Code:", response.status_code)
print("Response:", response.json())
