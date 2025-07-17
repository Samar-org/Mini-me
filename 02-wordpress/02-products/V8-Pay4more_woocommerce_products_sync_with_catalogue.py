#!/usr/bin/env python3
import os
import sys
import logging
import requests
from woocommerce import API as WCAPI
from airtable import Airtable
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── ENVIRONMENT & TABLE SETUP ────────────────────────────────────────────────

AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
WC_CONSUMER_KEY = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY")
WC_CONSUMER_SECRET = os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET")

if not all(
    [AIRTABLE_BASE_ID, AIRTABLE_API_KEY, WC_URL, WC_CONSUMER_KEY, WC_CONSUMER_SECRET]
):
    logger.error(
        "Missing one of required env vars: AIRTABLE_BASE_ID, AIRTABLE_API_KEY, WC_URL, WC_CONSUMER_KEY, WC_CONSUMER_SECRET"
    )
    sys.exit(1)

api = Airtable(AIRTABLE_BASE_ID, api_key=AIRTABLE_API_KEY)

# Main tables
items_table = api.table(AIRTABLE_BASE_ID, "Items-Pay4more")
catalogue_table = api.table(AIRTABLE_BASE_ID, "Product Catalogue")  # <<< ADDED

# WooCommerce client
wcapi = WCAPI(
    url=WC_URL,
    consumer_key=WC_CONSUMER_KEY,
    consumer_secret=WC_CONSUMER_SECRET,
    wp_api=True,
    version="wc/v3",
    timeout=60,
)

# ─── FIELD & META MAPPINGS ───────────────────────────────────────────────────

FIELD_MAPPINGS = {
    "name": "4more-Product-Code",
    "description": "Description",
    "short_description": "Meta Description",
    "regular_price": "Unit Retail Price",
}

META_MAPPINGS = {
    "meta_title": "Meta Title",
    "focus_keyword": "Focus Keyword",
    "image_alt": "Image Alt Text",
}

# ─── SUPPORT FUNCTIONS ───────────────────────────────────────────────────────


def fetch_wc_categories():
    resp = wcapi.get("products/categories", params={"per_page": 100}).json()
    return [{"id": c["id"]} for c in resp]


def upload_and_attach_images(fields, payload):
    # Implementation from your V7 script goes here...
    return payload


# ─── PRODUCT PROCESSING ───────────────────────────────────────────────────────


def process_single_product(
    record: Dict, wc_cats: List[Dict], brand_method: Optional[str]
):
    # load item-level fields
    fields = record.get("fields", {})

    # if linked to a Product Catalogue, merge its fields first
    linked_ids = fields.get("4more-Product-Code-linked", [])
    if linked_ids:
        try:
            cat_rec = catalogue_table.get(linked_ids[0])
            if cat_rec and cat_rec.get("fields"):
                fields = {**cat_rec["fields"], **fields}
        except Exception as e:
            logger.warning(
                f"Failed to fetch Product Catalogue record {linked_ids[0]}: {e}"
            )

    # build basic payload
    payload = {
        "type": "simple",
        "name": fields.get("4more-Product-Code"),
        "status": "publish",
        "categories": wc_cats,
    }

    # apply FIELD_MAPPINGS
    for wc_field, at_field in FIELD_MAPPINGS.items():
        if fields.get(at_field) not in (None, ""):
            payload[wc_field] = str(fields[at_field])

    # apply META_MAPPINGS
    payload.setdefault("meta_data", [])
    for meta_key, at_field in META_MAPPINGS.items():
        if fields.get(at_field):
            payload["meta_data"].append({"key": meta_key, "value": fields[at_field]})

    # handle images, tags, shipping class, etc.
    payload = upload_and_attach_images(fields, payload)

    # upsert product by SKU
    sku = fields.get("4more-Product-Code")
    if sku:
        existing = wcapi.get("products", params={"sku": sku}).json()
        if existing:
            prod_id = existing[0]["id"]
            wcapi.put(f"products/{prod_id}", payload).json()
        else:
            wcapi.post("products", payload).json()


# ─── MAIN LOOP ───────────────────────────────────────────────────────────────


def main():
    wc_cats = fetch_wc_categories()
    brand_method = os.getenv("BRAND_METHOD", "by_name")

    for rec in items_table.get_all(view="Grid view"):
        try:
            process_single_product(rec, wc_cats, brand_method)
            logger.info(f"Processed SKU {rec['fields'].get('4more-Product-Code')}")
        except Exception as e:
            logger.error(f"Failed on record {rec['id']}: {e}")


if __name__ == "__main__":
    main()
