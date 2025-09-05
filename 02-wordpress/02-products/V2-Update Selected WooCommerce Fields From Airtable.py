#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V2 — Update Selected WooCommerce Fields From Airtable (Listed items only)

Supports:
- Interactive or CLI selection of fields via --fields
- Updating: name, slug, description, prices, stock, weight, dimensions
- Plus: Product Tags, Brand, Rank Math SEO (from linked Product Catalogue)

Requirements:
  pip install python-dotenv pyairtable woocommerce requests
"""

import os
import sys
import re
import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
from dotenv import load_dotenv
from pyairtable import Api
from woocommerce import API as WCAPI

# Fix Windows UTF-8 console
if sys.platform.startswith("win"):
    try:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
    except Exception:
        pass

# Load .env file
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("field_update.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pay4more_field_update")

# Environment check
REQUIRED_ENV = [
    "PAY4MORE_WOOCOMMERCE_STORE_URL",
    "PAY4MORE_WOOCOMMERCE_CONSUMER_KEY",
    "PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET",
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
]
missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

# Clients
api = Api(os.getenv("AIRTABLE_API_KEY"))
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
items_table = api.table(BASE_ID, "Items-Pay4more")
catalogue_table = api.table(BASE_ID, "Product Catalogue")

WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
wcapi = WCAPI(
    url=WC_URL.rstrip("/"),
    consumer_key=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY"),
    consumer_secret=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET"),
    version="wc/v3",
    timeout=40,
)

SUPPORTED = [
    "name", "slug", "description", "short_description", "regular_price", "sale_price",
    "stock_quantity", "weight", "dimensions", "tags", "brand",
    "rank_math_title", "rank_math_description", "rank_math_focus_keyword"
]

# Helpers
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def first(v, default=None):
    if isinstance(v, list):
        return v[0] if v else default
    return v if v is not None else default

def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    if not dimensions_str or not isinstance(dimensions_str, str):
        return {"length": "", "width": "", "height": ""}
    s = dimensions_str.replace('"', '').replace("'", "")
    m = re.search(r"(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)\s*[xX]\s*(\d+\.?\d*)", s)
    if m:
        return {"length": m.group(1), "width": m.group(2), "height": m.group(3)}
    logger.warning(f"Could not parse dimensions: '{dimensions_str}'")
    return {"length": "", "width": "", "height": ""}

def get_wc_product_id_by_sku(sku: str) -> Optional[int]:
    try:
        res = wcapi.get("products", params={"sku": sku})
        res.raise_for_status()
        data = res.json()
        return data[0]["id"] if data else None
    except Exception as e:
        logger.error(f"Failed to find product by SKU {sku}: {e}")
        return None

def get_catalogue_fields(linked_code: str) -> Dict[str, Any]:
    rec = catalogue_table.first(formula=f"{{4more-Product-Code}}='{linked_code}'")
    return rec.get("fields", {}) if rec else {}

def build_payload(selected: List[str], item_fields: Dict[str, Any]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    meta: List[Dict[str, Any]] = []

    code = first(item_fields.get("4more-Product-Code-linked"))
    cat_fields = get_catalogue_fields(code) if code else {}

    def get(name: str):
        return first(cat_fields.get(name)) or first(item_fields.get(name))

    if "name" in selected and (v := get("Product Name")):
        data["name"] = v

    if "slug" in selected and (v := get("Product Slug")):
        data["slug"] = v.strip().lower().replace(" ", "-")

    if "description" in selected and (v := get("Description")):
        data["description"] = v

    if "short_description" in selected and (v := get("Meta Description")):
        data["short_description"] = v

    if "regular_price" in selected and (v := get("Unit Retail Price")) not in (None, ""):
        data["regular_price"] = str(v)

    if "sale_price" in selected and (v := get("4more Price")) not in (None, ""):
        data["sale_price"] = str(v)

    if "stock_quantity" in selected and (v := get("Quantity")) not in (None, ""):
        try:
            data["stock_quantity"] = int(v)
            data["manage_stock"] = True
        except Exception:
            logger.warning(f"Invalid quantity: {v}")

    if "weight" in selected and (v := get("Weight")) not in (None, ""):
        data["weight"] = str(v)

    if "dimensions" in selected and (v := get("Dimensions")) not in (None, ""):
        data["dimensions"] = parse_dimensions(str(v))

    if "tags" in selected:
        tag_raw = get("Product Tags")
        tag_names = tag_raw if isinstance(tag_raw, list) else str(tag_raw).split(",")
        tag_ids = []
        for tag in tag_names:
            tag = tag.strip()
            if not tag:
                continue
            try:
                search = wcapi.get("products/tags", params={"search": tag}).json()
                match = next((x for x in search if x.get("name", "").lower() == tag.lower()), None)
                if match:
                    tag_ids.append({"id": match["id"]})
                else:
                    new_tag = wcapi.post("products/tags", {"name": tag}).json()
                    if "id" in new_tag:
                        tag_ids.append({"id": new_tag["id"]})
            except Exception as e:
                logger.warning(f"Tag error [{tag}]: {e}")
        if tag_ids:
            data["tags"] = tag_ids

    if "brand" in selected and (brand := get("Brand")):
        try:
            brands = wcapi.get("products/brands", params={"search": brand}).json()
            match = next((x for x in brands if x.get("name", "").lower() == brand.lower()), None)
            if match:
                data["brands"] = [match["id"]]
            else:
                new_brand = wcapi.post("products/brands", {"name": brand}).json()
                if "id" in new_brand:
                    data["brands"] = [new_brand["id"]]
        except Exception as e:
            logger.warning(f"Brand error [{brand}]: {e}")

    if "rank_math_title" in selected and (v := get("Meta Title")):
        meta.append({"key": "rank_math_title", "value": v})
    if "rank_math_description" in selected and (v := get("RM Meta Description") or get("Meta Description")):
        meta.append({"key": "rank_math_description", "value": v})
    if "rank_math_focus_keyword" in selected and (v := get("Focus Keyword")):
        meta.append({"key": "rank_math_focus_keyword", "value": v})

    if meta:
        data["meta_data"] = meta

    return data

def interactive_select_fields() -> List[str]:
    print("\nSelect fields to update (enter numbers or names, e.g., 1,3,slug):\n")
    for i, f in enumerate(SUPPORTED, 1):
        print(f"{i:>2}. {f}")
    raw = input("\nYour selection: ").strip()
    if not raw:
        sys.exit("No fields selected.")
    parts = [p.strip().lower() for p in raw.split(",")]
    selected = []
    for p in parts:
        if p.isdigit() and 1 <= int(p) <= len(SUPPORTED):
            selected.append(SUPPORTED[int(p)-1])
        elif p in SUPPORTED:
            selected.append(p)
        else:
            print(f"Unknown field: {p}")
    return list(dict.fromkeys(selected))

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync selected WooCommerce fields from Airtable")
    p.add_argument("--fields", help="Comma-separated WooCommerce fields to update")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    selected = (
        [s.strip().lower() for s in args.fields.split(",") if s.strip()]
        if args.fields else interactive_select_fields()
    )
    for s in selected:
        if s not in SUPPORTED:
            raise SystemExit(f"Unsupported field: {s}")

    records = items_table.all(view="Pay4more Sync View", formula="{Listing Status}='Listed'")
    if args.limit:
        records = records[:args.limit]

    success = skipped = error = 0
    for i, rec in enumerate(records, 1):
        fields = rec.get("fields", {})
        rid = rec.get("id", "?")
        sku = first(fields.get("SKU"))
        woo_id = first(fields.get("WooCommerce ID"))
        pid = int(woo_id) if woo_id else get_wc_product_id_by_sku(sku)
        if not pid:
            logger.warning(f"[{i}] No Woo ID or SKU match; skipping")
            skipped += 1
            continue
        payload = build_payload(selected, fields)
        if not payload:
            logger.info(f"[{i}] Empty payload; skipping.")
            skipped += 1
            continue
        logger.info(f"[{i}] Updating product #{pid} with: {payload}")
        if args.dry_run:
            success += 1
            continue
        try:
            wcapi.put(f"products/{pid}", payload).raise_for_status()
            items_table.update(rid, {"Last Sync": now_iso()})
            success += 1
        except Exception as e:
            logger.error(f"Update failed: {e}")
            error += 1

    logger.info(f"✅ Done: {success} updated, {skipped} skipped, {error} failed")

if __name__ == "__main__":
    main()
2,11