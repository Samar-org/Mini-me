#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V19 — Update Selected WooCommerce Fields From Airtable (Listed items only)
View locked to: "Pay4more Sync View"

- Reads Items-Pay4more where Listing Status = "Listed" from the "Pay4more Sync View"
- Lets you choose one or more WooCommerce fields to update (interactive menu or --fields)
- Matches Woo product by 'WooCommerce ID' (if present) or by 'SKU'
- Only updates the selected fields

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

# -------------- Windows console UTF-8 fix --------------
if sys.platform.startswith("win"):
    try:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
    except Exception:
        pass

# -------------- Load .env beside script --------------
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# -------------- Logging --------------
try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("field_update.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger("pay4more_field_update")

# -------------- Required ENV --------------
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

# -------------- Clients --------------
api = Api(os.getenv("AIRTABLE_API_KEY"))
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
items_table = api.table(BASE_ID, "Items-Pay4more")

WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
wcapi = WCAPI(
    url=WC_URL.rstrip("/"),
    consumer_key=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY"),
    consumer_secret=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET"),
    version="wc/v3",
    timeout=40,
)

# -------------- Small utils --------------
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def first(v, default=None):
    if isinstance(v, list):
        return v[0] if v else default
    return v if v is not None else default

def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    """Extract LxWxH numbers like '12 x 8 x 3 in' -> {'length':'12','width':'8','height':'3'}"""
    if not dimensions_str or not isinstance(dimensions_str, str):
        return {"length": "", "width": "", "height": ""}
    s = dimensions_str.replace('"', "").replace("'", "")
    m = re.search(r"(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)", s)
    if m:
        try:
            return {"length": m.group(1), "width": m.group(2), "height": m.group(3)}
        except Exception:
            pass
    logger.warning(f"Could not parse dimensions from: '{dimensions_str}'")
    return {"length": "", "width": "", "height": ""}

def get_wc_product_id_by_sku(sku: str) -> Optional[int]:
    try:
        res = wcapi.get("products", params={"sku": sku})
        res.raise_for_status()
        data = res.json() or []
        return data[0]["id"] if data else None
    except Exception as e:
        logger.error(f"Failed to find Woo product by SKU {sku}: {e}")
        return None

# -------------- Airtable → Woo field map --------------
# Airtable field names (left) that feed Woo fields/meta (right)
AT_FIELDS = {
    "Product Name": "name",
    "Description": "description",
    "Meta Description": "short_description",
    "Unit Retail Price": "regular_price",
    "4more Price": "sale_price",
    "Quantity": "stock_quantity",
    "Weight": "weight",
    "Dimensions": "dimensions",

    # Rank Math meta (kept in Woo meta_data)
    "Meta Title": "rank_math_title",
    # If you keep a separate RM-specific description, use this:
    "RM Meta Description": "rank_math_description",  # optional/alternative
    "Focus Keyword": "rank_math_focus_keyword",

    # Identity
    "SKU": "sku",
    "WooCommerce ID": "woo_id",
}

# Selectable “Woo fields” (right-hand keys above, deduped)
SUPPORTED = [
    "name",
    "description",
    "short_description",
    "regular_price",
    "sale_price",
    "stock_quantity",
    "weight",
    "dimensions",
    "rank_math_title",
    "rank_math_description",
    "rank_math_focus_keyword",
]

# -------------- Build payload for selected fields --------------
def build_payload(selected: List[str], at_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a WooCommerce product payload containing ONLY the selected fields.
    """
    data: Dict[str, Any] = {}
    meta: List[Dict[str, Any]] = []

    def get_at(name: str):
        return first(at_fields.get(name))

    # Core
    if "name" in selected:
        v = get_at("Product Name")
        if v:
            data["name"] = v

    if "description" in selected:
        v = get_at("Description")
        if v is not None:
            data["description"] = v

    if "short_description" in selected:
        v = get_at("Meta Description")
        if v is not None:
            data["short_description"] = v

    if "regular_price" in selected:
        v = get_at("Unit Retail Price")
        if v not in (None, ""):
            data["regular_price"] = str(v)

    if "sale_price" in selected:
        v = get_at("4more Price")
        if v not in (None, ""):
            data["sale_price"] = str(v)

    if "stock_quantity" in selected:
        v = get_at("Quantity")
        if v not in (None, ""):
            try:
                data["stock_quantity"] = int(v)
                data["manage_stock"] = True
            except Exception:
                logger.warning(f"Quantity is not an integer: {v}")

    if "weight" in selected:
        v = get_at("Weight")
        if v not in (None, ""):
            data["weight"] = str(v)

    if "dimensions" in selected:
        v = get_at("Dimensions")
        if v not in (None, ""):
            data["dimensions"] = parse_dimensions(str(v))

    # Rank Math
    rm_title_src = get_at("Meta Title")
    rm_desc_src = get_at("RM Meta Description") or get_at("Meta Description")
    rm_focus_src = get_at("Focus Keyword")

    if "rank_math_title" in selected and rm_title_src is not None:
        meta.append({"key": "rank_math_title", "value": rm_title_src})

    if "rank_math_description" in selected and rm_desc_src is not None:
        meta.append({"key": "rank_math_description", "value": rm_desc_src})

    if "rank_math_focus_keyword" in selected and rm_focus_src is not None:
        meta.append({"key": "rank_math_focus_keyword", "value": rm_focus_src})

    if meta:
        data["meta_data"] = meta

    return data

# -------------- Interactive selector --------------
def interactive_select_fields() -> List[str]:
    print("\nSelect which WooCommerce field(s) to update.")
    print("Enter numbers separated by commas (e.g., 1,3,5) or type names (e.g., name,regular_price).")
    print("Press Enter with no input to cancel.\n")

    # Pretty list
    for i, f in enumerate(SUPPORTED, start=1):
        print(f"{i:>2}. {f}")

    raw = input("\nYour selection: ").strip()
    if not raw:
        print("No fields selected. Exiting.")
        sys.exit(0)

    # Accept numbers and/or names
    chosen: List[str] = []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for p in parts:
        if p.isdigit():
            idx = int(p)
            if 1 <= idx <= len(SUPPORTED):
                chosen.append(SUPPORTED[idx - 1])
            else:
                print(f"  - Skipping invalid number: {p}")
        else:
            # name
            if p.lower() in SUPPORTED:
                chosen.append(p.lower())
            else:
                print(f"  - Skipping unknown field name: {p}")

    # Dedup, preserve order
    deduped = []
    for c in chosen:
        if c not in deduped:
            deduped.append(c)

    if not deduped:
        print("No valid fields selected. Exiting.")
        sys.exit(0)

    print(f"\nSelected fields: {', '.join(deduped)}\n")
    return deduped

# -------------- CLI --------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Update specific WooCommerce fields from Airtable (Listed items only, Pay4more Sync View)."
    )
    p.add_argument(
        "--fields",
        help=f"Comma-separated list of fields to update. If omitted, an interactive selector will appear.\n"
             f"Supported: {', '.join(SUPPORTED)}",
    )
    p.add_argument("--limit", type=int, default=0, help="Process at most N records (0 = no limit).")
    p.add_argument("--dry-run", action="store_true", help="Show what would be sent without updating Woo.")
    return p.parse_args()

# -------------- Main --------------
def main():
    args = parse_args()

    # Resolve fields (interactive if not provided)
    if args.fields:
        selected = [s.strip().lower() for s in args.fields.split(",") if s.strip()]
    else:
        selected = interactive_select_fields()

    # Validate
    unknown = [f for f in selected if f not in SUPPORTED]
    if unknown:
        raise SystemExit(f"Unsupported fields: {', '.join(unknown)}")
    if not selected:
        raise SystemExit("No fields selected.")

    # Airtable query: lock to "Pay4more Sync View" + enforce Listed filter
    params: Dict[str, Any] = {
        "formula": "{Listing Status}='Listed'",
        "page_size": 100,
        "view": "Pay4more Sync View",
    }

    logger.info("🔎 Fetching Airtable Items from view 'Pay4more Sync View' with Listing Status = 'Listed' ...")
    records: List[Dict[str, Any]] = items_table.all(**params)
    total = len(records)
    if args.limit and total > args.limit:
        records = records[:args.limit]
    logger.info(f"Found {total} Listed items in 'Pay4more Sync View'; processing {len(records)} record(s)...")

    success = skipped = errors = 0

    for i, rec in enumerate(records, 1):
        rid = rec.get("id", "?")
        fields = rec.get("fields", {})
        sku = first(fields.get("SKU"))
        woo_id = first(fields.get("WooCommerce ID"))

        label = f"[{i}/{len(records)}] Airtable:{rid} SKU:{sku or '-'}"
        if woo_id:
            label += f" WooID:{woo_id}"
        logger.info("-" * 60)
        logger.info(label)

        if not (woo_id or sku):
            logger.warning("Skipping: missing both WooCommerce ID and SKU")
            skipped += 1
            continue

        # Prepare payload
        payload = build_payload(selected, fields)
        if not payload:
            logger.info("Nothing to update for selected fields; skipping.")
            skipped += 1
            continue

        # Resolve Woo product ID
        pid = None
        if woo_id:
            try:
                pid = int(str(woo_id).strip())
            except Exception:
                pid = None

        if not pid and sku:
            pid = get_wc_product_id_by_sku(str(sku).strip())

        if not pid:
            logger.error("Could not resolve Woo product ID from WooCommerce ID or SKU; skipping.")
            errors += 1
            continue

        # Show payload
        logger.info(f"Prepared payload for product #{pid}: {payload}")

        if args.dry_run:
            logger.info("DRY RUN: not sending update to WooCommerce.")
            success += 1
            continue

        # Send update
        try:
            r = wcapi.put(f"products/{pid}", payload)
            r.raise_for_status()
            logger.info(f"✅ Updated product #{pid} successfully.")
            # Optional: stamp Last Sync
            try:
                items_table.update(rid, {"Last Sync": now_iso()})
            except Exception:
                pass
            success += 1
        except requests.exceptions.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:2000]
            except Exception:
                pass
            logger.error(f"HTTP {e.response.status_code} updating product #{pid}: {body}")
            errors += 1
        except Exception as e:
            logger.exception(f"Unexpected error updating product #{pid}: {e}")
            errors += 1

    logger.info("=" * 60)
    logger.info(f"Done. Success: {success} | Skipped: {skipped} | Errors: {errors}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
