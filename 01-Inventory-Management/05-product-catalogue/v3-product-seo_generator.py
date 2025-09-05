#!/usr/bin/env python3
"""
Enhanced SEO Product Optimizer with Gemini AI
- Only processes records where SEO Ready is unchecked
- Uses Gemini AI to fetch missing descriptions
- Extracts brand from Product URL when missing
- Saves description, SEO titles, tags, and brand to Airtable
"""

import os
import json
import time
import logging
import sys
import re
import csv
import random
from datetime import datetime
from typing import Dict
import tldextract
from pyairtable import Api
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("seo_generator.log", encoding='utf-8'), logging.StreamHandler(sys.stdout)])

# Airtable configuration
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_PRODUCT_CATALOGUE_TABLE_NAME", "Product Catalogue")
AIRTABLE_CATEGORIES_TABLE_NAME = "Categories"
AIRTABLE_VIEW_NAME = "SEO-View"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Field names
FIELD_PRODUCT_NAME = "Product Name"
FIELD_DESCRIPTION = "Description"
FIELD_SEO_READY_FLAG = "SEO Ready"
FIELD_SEO_TITLE = "Product SEO Title"
FIELD_META_TITLE = "Meta Title"
FIELD_META_DESC = "Meta Description"
FIELD_IMAGE_ALT = "Image Alt Text"
FIELD_PRODUCT_SLUG = "Product Slug"
FIELD_KEYWORDS = "Focus Keyword"
FIELD_TAGS = "Product Tags"
FIELD_ECOMM_NAME = "ecommerce-Friendly Name"
FIELD_BRAND = "Brand"

class EnhancedSEOOptimizer:
    def __init__(self):
        if GEMINI_API_KEY:
            genai.configure(api_key=GEMINI_API_KEY)
            self.gemini_model = genai.GenerativeModel('gemini-1.5-flash')
        else:
            self.gemini_model = None
            logging.warning("Gemini API key not found - AI features disabled")

    def identify_brand(self, product_name: str, description: str = '', product_url: str = '') -> str:
        common_brands = ['apple', 'samsung', 'sony', 'lg', 'nike', 'adidas']
        text = f"{product_name} {description}".lower()
        for brand in common_brands:
            if brand in text:
                return brand.title()
        if product_url:
            domain_parts = tldextract.extract(product_url)
            if domain_parts.domain:
                return domain_parts.domain.title()
        return ''

    def fetch_description_with_ai(self, product_name: str, product_url: str = '') -> str:
        if not self.gemini_model:
            return ''
        prompt = f"Generate a concise product description for '{product_name}'. Use context from URL if provided: {product_url}"
        try:
            response = self.gemini_model.generate_content(prompt)
            if response and response.text:
                return response.text.strip()
        except Exception as e:
            logging.error(f"Gemini AI failed to fetch description: {e}")
        return ''

    def generate_seo_content_local(self, product_name: str, product_description: str, additional_data: Dict = None) -> Dict:
        if not product_description:
            product_description = self.fetch_description_with_ai(product_name, additional_data.get('product_url', ''))
        brand = (additional_data.get('Brand') or additional_data.get('brand') or 
                 self.identify_brand(product_name, product_description, additional_data.get('product_url', '')))
        seo_title = f"{brand} {product_name} | Save with 4more | Toronto"[:60]
        meta_description = f"Shop {product_name}. Save with 4more and free local delivery!"[:160]
        slug = re.sub(r'[^a-z0-9\s-]', '', f"{brand}-{product_name}".lower())
        slug = re.sub(r'\s+', '-', slug)[:50]
        return {
            'seo_title': seo_title,
            'meta_title': seo_title,
            'meta_description': meta_description,
            'product_slug': slug,
            'image_alt_text': f"{brand} {product_name}"[:125],
            'focus_keywords': [product_name.lower(), f"{product_name.lower()} toronto"],
            'product_tags': [product_name, brand, 'Local Store'],
            'ecommerce_name': f"{brand} {product_name}",
            'brand': brand,
            'description': product_description
        }

def main():
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    seo_optimizer = EnhancedSEOOptimizer()

    records_to_process = table.all(view=AIRTABLE_VIEW_NAME)
    logging.info(f"Fetched {len(records_to_process)} records")
    successful_updates = 0
    failed_updates = 0

    for index, record in enumerate(records_to_process, 1):
        fields = record['fields']
        if fields.get(FIELD_SEO_READY_FLAG, False) is True:
            logging.info(f"Skipping {record['id']} - already SEO Ready")
            continue
        product_name = fields.get(FIELD_PRODUCT_NAME)
        product_desc = fields.get(FIELD_DESCRIPTION, '')
        if not product_name:
            logging.warning(f"Skipping {record['id']} - missing Product Name")
            failed_updates += 1
            continue

        seo_data = seo_optimizer.generate_seo_content_local(
            product_name, product_desc, {**fields, "product_url": fields.get("Product URL", "")}
        )

        if not seo_data.get("brand"):
            logging.warning(f"Record {record['id']} ({product_name}) has no brand identified (URL: {fields.get('Product URL', 'N/A')})")

        update_payload = {
            FIELD_SEO_TITLE: seo_data.get("seo_title"),
            FIELD_META_TITLE: seo_data.get("meta_title"),
            FIELD_META_DESC: seo_data.get("meta_description"),
            FIELD_PRODUCT_SLUG: seo_data.get("product_slug"),
            FIELD_IMAGE_ALT: seo_data.get("image_alt_text"),
            FIELD_KEYWORDS: ", ".join(seo_data.get("focus_keywords", [])),
            FIELD_TAGS: ", ".join(seo_data.get("product_tags", [])),
            FIELD_ECOMM_NAME: seo_data.get("ecommerce_name"),
            FIELD_BRAND: seo_data.get("brand"),
            FIELD_DESCRIPTION: seo_data.get("description", product_desc),
            FIELD_SEO_READY_FLAG: True
        }
        try:
            table.update(record['id'], update_payload)
            successful_updates += 1
        except Exception as e:
            logging.error(f"Failed to update {record['id']}: {e}")
            failed_updates += 1
        time.sleep(2)

    logging.info(f"Done. Success: {successful_updates}, Failed: {failed_updates}")

if __name__ == "__main__":
    main()
