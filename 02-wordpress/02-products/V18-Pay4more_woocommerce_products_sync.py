#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V18 — Pay4more WooCommerce Product Sync

Key updates vs V6:
- ENV: keeps your existing Pay4more env names (PAY4MORE_WOOCOMMERCE_* etc).
- SLUG: taken ONLY from Product Catalogue (never from Items table).
- COLLISIONS: checks if slug is used by another product; on collision writes
  "Loading Error" in Items and skips syncing that record.
- TAGS: robust upsert (search first, handle 'term_exists', reuse existing).
- STABILITY: per-record errors never crash the run; all errors written to Airtable.
- IMAGES: optional "delete old images" before attaching new ones (config flag).

Requirements:
  pip install python-dotenv pyairtable woocommerce Pillow requests
"""

import os
import sys
import re
import html
import base64
import time
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO
from datetime import datetime

import requests
from dotenv import load_dotenv
from PIL import Image
from pyairtable import Api
from woocommerce import API as WCAPI
from urllib.parse import urlparse

# -------------- Windows console fix --------------
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
            logging.FileHandler("sync.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger("pay4more_sync")

# -------------- Config --------------
@dataclass
class SyncConfig:
    image_size: Tuple[int,int] = (1200, 1200)
    jpeg_quality: int = 85
    webp_quality: int = 80
    upload_delay: float = 0.5
    timeout: int = 40
    per_page: int = 100
    delete_old_images: bool = True               # detach existing images before attaching new ones
    max_images_per_product: int = 12
    items_view: Optional[str] = os.getenv("AIRTABLE_ITEMS_VIEW") or "Pay4more Sync View"
    catalogue_table: str = os.getenv("AIRTABLE_CATALOGUE_TABLE") or "Product Catalogue"

config = SyncConfig()

# -------------- Required ENV --------------
REQUIRED_ENV = [
    "PAY4MORE_WOOCOMMERCE_STORE_URL",
    "PAY4MORE_WOOCOMMERCE_CONSUMER_KEY",
    "PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET",
    "WORDPRESS_USERNAME",
    "WORDPRESS_APPLICATION_PASSWORD",
    "AIRTABLE_API_KEY",
    "AIRTABLE_BASE_ID",
]

missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

# -------------- Clients --------------
api = Api(os.getenv("AIRTABLE_API_KEY"))
items_table = api.table(os.getenv("AIRTABLE_BASE_ID"), "Items-Pay4more")
catalogue_table = api.table(os.getenv("AIRTABLE_BASE_ID"), config.catalogue_table)

WC_URL = os.getenv("PAY4MORE_WOOCOMMERCE_STORE_URL")
BASE_URL = f"{urlparse(WC_URL).scheme}://{urlparse(WC_URL).netloc}"

wcapi = WCAPI(
    url=BASE_URL,
    consumer_key=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_KEY"),
    consumer_secret=os.getenv("PAY4MORE_WOOCOMMERCE_CONSUMER_SECRET"),
    version="wc/v3",
    timeout=config.timeout,
)

WP_USER = os.getenv("WORDPRESS_USERNAME")
WP_PASS = os.getenv("WORDPRESS_APPLICATION_PASSWORD")
WP_AUTH_HEADER = "Basic " + base64.b64encode(f"{WP_USER}:{WP_PASS}".encode()).decode("utf-8")

# -------------- Small utils --------------
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def first(v, default=None):
    if isinstance(v, list):
        return v[0] if v else default
    return v if v is not None else default

def clean_and_normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    unescaped_name = html.unescape(name)
    cleaned_name = re.sub(r"[\u200B-\u200D\uFEFF\s]+", " ", unescaped_name).strip()
    normalized = cleaned_name.lower().replace(" & ", " and ").replace("&", "and")
    return re.sub(r"\s+", " ", normalized)

def set_loading_error(record_id: str, msg: str):
    try:
        items_table.update(record_id, {"Loading Error": msg, "Last Sync": now_iso()})
    except Exception:
        pass

# -------------- Airtable: find Catalogue record for an Items record --------------
def get_catalogue_record_for_item(item_fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Tries multiple strategies to locate the matching Product Catalogue row:
      1) Linked record id in a field like 'Product Catalogue' or '4more-Product-Code-linked'
      2) Match by 4more-Product-Code value (string) against a field of same name in Catalogue
    """
    # 1) Common linked fields that hold Catalogue record IDs
    link_keys = ["Product Catalogue", "Catalogue", "4more-Product-Code-linked"]
    for k in link_keys:
        val = item_fields.get(k)
        if isinstance(val, list) and val and isinstance(val[0], str) and val[0].startswith("rec"):
            try:
                rec = catalogue_table.get(val[0])
                if rec: return rec
            except Exception:
                pass

    # 2) Try matching by the code value
    code_keys = ["4more-Product-Code", "4more Product Code", "Product Code", "Catalogue Code"]
    code_val = None
    for k in code_keys:
        v = first(item_fields.get(k))
        if isinstance(v, str) and v.strip():
            code_val = v.strip()
            break
    if code_val:
        try:
            rec = catalogue_table.first(formula=f"{{4more-Product-Code}}='{code_val}'")
            if rec: return rec
        except Exception:
            # Try alternative field names in Catalogue
            for field in ["Product Code", "Catalogue Code"]:
                try:
                    rec = catalogue_table.first(formula=f"{{{field}}}='{code_val}'")
                    if rec: return rec
                except Exception:
                    pass
    return None

# -------------- Slug logic --------------
SLUG_FIELDS = ("Product Slug", "Slug", "URL Slug", "Product URL Slug")

class SlugCollisionError(Exception):
    pass

def get_slug_from_catalogue(catalogue_fields: Dict[str, Any]) -> Optional[str]:
    for k in SLUG_FIELDS:
        v = first(catalogue_fields.get(k))
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def find_wc_product_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    if not slug: return None
    try:
        res = wcapi.get("products", params={"slug": slug})
        res.raise_for_status()
        data = res.json() or []
        return data[0] if data else None
    except Exception:
        return None

def assert_slug_unique_or_log(record_id: str, slug: str, current_product_id: Optional[int] = None):
    if not slug:
        return
    existing = find_wc_product_by_slug(slug)
    if not existing:
        return
    existing_id = existing.get("id")
    if current_product_id and str(existing_id) == str(current_product_id):
        return
    permalink = existing.get("permalink") or ""
    msg = f'Slug collision: "{slug}" already used by Woo product ID {existing_id}. {permalink}'
    set_loading_error(record_id, msg)
    raise SlugCollisionError(msg)

# -------------- Woo helpers --------------
def fetch_all_wc(endpoint: str) -> List[Dict[str, Any]]:
    data, page = [], 1
    while True:
        r = wcapi.get(endpoint, params={"per_page": config.per_page, "page": page})
        if r.status_code == 404:
            return []
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        data.extend(chunk)
        page += 1
    return data

_tag_cache: Dict[str,int] = {}
_brand_cache: Dict[str,int] = {}

def get_or_create_tag(name: str) -> Optional[int]:
    """Robust tag upsert: search first, handle term_exists, reuse existing id."""
    if not name or not isinstance(name, str) or not name.strip():
        return None
    name = name.strip()

    # Search first
    try:
        res = wcapi.get("products/tags", params={"search": name, "per_page": 100})
        res.raise_for_status()
        for t in res.json() or []:
            if t.get("name", "").strip().lower() == name.lower():
                return t.get("id")
    except Exception as e:
        logger.warning(f"Tag search failed for '{name}': {e}")

    # Create
    try:
        res = wcapi.post("products/tags", {"name": name})
        if res.status_code in (200, 201):
            return res.json().get("id")

        try:
            body = res.json()
        except Exception:
            body = {"raw": res.text}

        code = (body or {}).get("code", "")
        existing_id = (body or {}).get("data", {}).get("resource_id")
        if res.status_code == 400 and ("term_exists" in code or existing_id):
            if existing_id:
                return existing_id

        logger.warning(f"Tag upsert failed for '{name}': {res.status_code} {res.text}")
        return None
    except Exception as e:
        logger.warning(f"Tag create failed for '{name}': {e}")
        return None

def get_existing_product_id_by_sku(sku: str) -> Optional[int]:
    res = wcapi.get("products", params={"sku": sku})
    res.raise_for_status()
    products = res.json()
    return products[0]["id"] if products else None

def delete_old_images_associations(product_id: int):
    if not config.delete_old_images:
        return
    try:
        wcapi.put(f"products/{product_id}", {"images": []})
        logger.info(f"Cleared old image associations for product #{product_id}")
    except Exception as e:
        logger.warning(f"Could not clear images for product {product_id}: {e}")

# -------------- Media upload --------------
def generate_seo_filename(product_name: str, original_filename: str, index: int) -> str:
    clean_name = re.sub(r"[^\w\s-]", "", (product_name or "").lower()).strip()
    clean_name = re.sub(r"[-\s]+", "-", clean_name)
    clean_name = clean_name[:50].rsplit("-", 1)[0] if len(clean_name) > 50 else clean_name
    ext = original_filename.split(".")[-1].lower() if "." in original_filename else "jpg"
    return f"{clean_name}-{index + 1}.{ext}" if index > 0 else f"{clean_name}.{ext}"

def optimize_image(img: Image.Image) -> Tuple[BytesIO, str]:
    if img.mode not in ("RGB", "L"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "RGBA" and img.getbands() == ("R", "G", "B", "A"):
            bg.paste(img, mask=img.split()[3])
        else:
            bg.paste(img)
        img = bg
    img.thumbnail(config.image_size, Image.Resampling.LANCZOS)
    buf = BytesIO()
    try:
        img.save(buf, format="WEBP", quality=config.webp_quality)
        return buf, "webp"
    except Exception:
        img.save(buf, format="JPEG", quality=config.jpeg_quality, optimize=True)
        return buf, "jpeg"

def upload_image(image_url: str, product_name: str, index: int) -> Optional[int]:
    r = requests.get(image_url, timeout=30)
    r.raise_for_status()
    img = Image.open(BytesIO(r.content))
    buffer, fmt = optimize_image(img)
    filename = generate_seo_filename(product_name, image_url.split("/")[-1], index)
    filename = filename.rsplit(".", 1)[0] + f".{fmt}"

    headers = {
        "Authorization": WP_AUTH_HEADER,
        "Content-Disposition": f"attachment; filename={filename}",
        "Content-Type": f"image/{fmt}",
    }
    resp = requests.post(f"{BASE_URL}/wp-json/wp/v2/media", headers=headers, data=buffer.getvalue())
    resp.raise_for_status()
    media_id = resp.json()["id"]
    logger.info(f"Uploaded image '{filename}' (ID: {media_id})")
    time.sleep(config.upload_delay)
    return media_id

IMAGE_FIELDS = ["Featured Photos", "Photo Files", "Item Photos", "Inspection Photos"]

def build_images_payload_from_items(item_fields: Dict[str, Any], product_name: str) -> List[Dict[str, int]]:
    image_ids: List[Dict[str,int]] = []
    seen_urls: List[str] = []
    for field_name in IMAGE_FIELDS:
        images = item_fields.get(field_name)
        if not isinstance(images, list):
            continue
        for i, img in enumerate(images):
            url = img.get("url") if isinstance(img, dict) else None
            if not url or url in seen_urls:
                continue
            seen_urls.append(url)
            try:
                mid = upload_image(url, product_name, len(image_ids))
                if mid:
                    image_ids.append({"id": mid})
            except Exception as e:
                logger.warning(f"Image upload failed for {url}: {e}")
            if len(image_ids) >= config.max_images_per_product:
                break
        if len(image_ids) >= config.max_images_per_product:
            break
    return image_ids

# -------------- Field mappings --------------
FIELD_MAPPINGS = {
    "name": "Product Name",
    # "slug": (IGNORED — slug comes from Catalogue only)
    "featured": "Featured",
    "description": "Description",
    "short_description": "Meta Description",
    "sku": "SKU",
    "regular_price": "Unit Retail Price",
    "sale_price": "4more Price",
    "weight": "Weight",
    "dimensions": "Dimensions",
    "stock_quantity": "Quantity",
    "tags": "Product Tags",
    "brand": "Brand",
    "parent_category": "Parent Category Rollup (from Category)",
    "category": "Category Name",
}

ACF_MAPPINGS = {
    "inspected": "Inspected",
    "condition": "Inspection Condition",
    "box_condition": "Box Condition",
    "color": "Color",
    "model": "Model",
    "location": "Warehouse",
    "aisle_bin": "Shelf Location",
    "heavy": "Heavy",
    "fragile": "Fragile",
    "big": "Big",
}

META_MAPPINGS = {
    "rank_math_title": "Meta Title",
    "rank_math_description": "Meta Description",
    "rank_math_focus_keyword": "Focus Keyword",
}

# -------------- Dimensions parsing --------------
def parse_dimensions(dimensions_str: str) -> Dict[str, str]:
    if not dimensions_str or not isinstance(dimensions_str, str):
        return {"length":"", "width":"", "height":""}
    s = dimensions_str.replace('"', "").replace("'", "")
    m = re.search(r"(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)\s*[a-zA-Z]*\s*[xX]\s*(\d+\.?\d*)", s)
    if m:
        try:
            return {"length": m.group(1), "width": m.group(2), "height": m.group(3)}
        except Exception:
            pass
    logger.warning(f"Could not parse dimensions from: '{dimensions_str}'")
    return {"length":"", "width":"", "height":""}

# -------------- Brands plugin probe (optional) --------------
def brands_plugin_available() -> bool:
    try:
        r = wcapi.get("products/brands", params={"per_page": 1})
        logger.info(f"Brands endpoint status: {r.status_code}")
        return r.status_code == 200
    except Exception:
        return False

# -------------- Record processing --------------
def process_single_product(record: Dict[str, Any], wc_categories: List[Dict[str, Any]], brands_enabled: bool) -> bool:
    rid = record.get("id")
    fields = record.get("fields", {})
    sku = first(fields.get(FIELD_MAPPINGS["sku"]))
    name = first(fields.get(FIELD_MAPPINGS["name"])) or "Unknown Product"

    logger.info(f"Processing: {name} (SKU: {sku})")

    if not sku:
        set_loading_error(rid, "Missing SKU")
        return False

    try:
        # Find Catalogue record to pull SEO-only slug (and optionally meta)
        cat_rec = get_catalogue_record_for_item(fields)
        if not cat_rec:
            set_loading_error(rid, "Catalogue record not found")
            return False
        cfields = cat_rec.get("fields", {})

        # Build base payload
        data: Dict[str, Any] = {
            "status": "publish",
            "catalog_visibility": "visible",
            "tax_status": "taxable",
            "stock_status": "instock",
            "backorders": "no",
            "type": "simple",
            "manage_stock": True,
            "name": name,
            "sku": sku,
        }
        meta: List[Dict[str, Any]] = []
        data["meta_data"] = meta

        # Map Item fields (excluding slug)
        for key, at_field in FIELD_MAPPINGS.items():
            if key in ("slug", "tags", "brand", "category", "parent_category", "sku", "name"):
                continue
            val = fields.get(at_field)
            if val is None:
                continue
            if isinstance(val, list) and val:
                val = val[0]
            if key in ("regular_price", "sale_price"):
                data[key] = str(val)
            elif key == "dimensions":
                data[key] = parse_dimensions(str(val))
            elif key in ACF_MAPPINGS or key in META_MAPPINGS:
                meta.append({"key": key, "value": val})
            else:
                data[key] = val

        # SEO meta from Catalogue (fallback to Items if missing)
        seo_title = first(cfields.get("Meta Title")) or first(fields.get("Meta Title")) or name
        seo_desc  = first(cfields.get("Meta Description")) or first(fields.get("Meta Description")) or ""
        focus_kw  = first(cfields.get("Focus Keyword")) or first(fields.get("Focus Keyword")) or ""

        meta.extend([
            {"key": "rank_math_title", "value": seo_title},
            {"key": "rank_math_description", "value": seo_desc},
            {"key": "rank_math_focus_keyword", "value": focus_kw},
            {"key": "rank_math_pillar_content", "value": "off"},
            {"key": "rank_math_robots", "value": ["index","follow"]},
            {"key": "_yoast_wpseo_title", "value": seo_title},
            {"key": "_yoast_wpseo_metadesc", "value": seo_desc},
            {"key": "_yoast_wpseo_focuskw", "value": focus_kw},
        ])

        # Categories (match by name)
        parent_cats = fields.get("Parent Category Rollup (from Category)", [])
        child_cats  = fields.get("Category Name", [])
        at_cat_names = set((parent_cats or []) + (child_cats or []))
        cat_ids = set()
        for cat_name in at_cat_names:
            norm = clean_and_normalize(cat_name)
            for wc_cat in wc_categories:
                if norm == clean_and_normalize(wc_cat.get("name")):
                    cat_ids.add(wc_cat["id"])
                    break
        if cat_ids:
            data["categories"] = [{"id": cid} for cid in sorted(cat_ids)]

        # Tags
        tag_values = fields.get("Product Tags", [])
        if isinstance(tag_values, str):
            tag_values = [t.strip() for t in tag_values.split(",") if t.strip()]
        tag_ids: List[Dict[str,int]] = []
        for t in tag_values:
            tid = get_or_create_tag(t)
            if tid:
                tag_ids.append({"id": tid})
        if tag_ids:
            data["tags"] = tag_ids

        # Brand (if Brands plugin)
        if brands_enabled:
            brand_name = first(fields.get("Brand"))
            if brand_name:
                # Try create/reuse brand via products/brands endpoint
                try:
                    # search
                    res = wcapi.get("products/brands", params={"search": brand_name, "per_page": 100})
                    res.raise_for_status()
                    existing = None
                    for b in res.json() or []:
                        if b.get("name","").strip().lower() == brand_name.strip().lower():
                            existing = b.get("id")
                            break
                    if not existing:
                        res = wcapi.post("products/brands", {"name": brand_name})
                        if res.status_code in (200,201):
                            existing = res.json().get("id")
                    if existing:
                        data["brands"] = [{"id": existing}]
                except Exception as e:
                    logger.warning(f"Brand assignment failed for '{brand_name}': {e}")

        # Images (from Items attachments)
        images = build_images_payload_from_items(fields, name)

        # Existing product?
        existing_id = None
        try:
            existing_id = get_existing_product_id_by_sku(sku)
        except Exception:
            existing_id = None

        # Slug ONLY from Catalogue + collision check
        slug = get_slug_from_catalogue(cfields)
        if slug:
            assert_slug_unique_or_log(rid, slug, existing_id)
            data["slug"] = slug

        # Attach images
        if existing_id and config.delete_old_images and images:
            delete_old_images_associations(existing_id)

        if images:
            data["images"] = images

        # Create/Update
        if existing_id:
            r = wcapi.put(f"products/{existing_id}", data)
            r.raise_for_status()
            pid = r.json().get("id", existing_id)
            status_txt = "updated"
        else:
            r = wcapi.post("products", data)
            r.raise_for_status()
            pid = r.json().get("id")
            status_txt = "created"

        # Clear error + mark synced
        items_table.update(rid, {"Loading Error": "", "Listing Status": "Synced", "WooCommerce ID": str(pid), "Last Sync": now_iso()})
        logger.info(f"[{rid}] {name} ({sku}) {status_txt} as #{pid}")
        return True

    except SlugCollisionError as e:
        logger.error(f"[{rid}] {e}")
        return False

    except requests.exceptions.HTTPError as e:
        body = ""
        try:
            body = e.response.text[:2000]
        except Exception:
            pass
        msg = f"HTTP {e.response.status_code} for SKU {sku}: {body}"
        logger.error(msg)
        set_loading_error(rid, msg)
        return False

    except Exception as e:
        msg = f"Unexpected error for SKU {sku}: {e}"
        logger.exception(msg)
        set_loading_error(rid, msg)
        return False

# -------------- Main --------------
def main():
    logger.info("🚀 Starting product sync from Airtable to WooCommerce...")
    try:
        brands_enabled = brands_plugin_available()
        wc_categories = fetch_all_wc("products/categories")

        # Fetch Items to sync
        params = {}
        if config.items_view:
            params["view"] = config.items_view
        records = items_table.all(**params)
        total = len(records)
        logger.info(f"Starting sync for {total} items...")

        success = errors = 0
        for i, rec in enumerate(records, 1):
            try:
                logger.info(f"[{i}/{total}] -------------------------")
                if process_single_product(rec, wc_categories, brands_enabled):
                    success += 1
                else:
                    errors += 1
            except Exception as e:
                rid = rec.get("id","?")
                msg = f"Fatal per-record error [{rid}]: {e}"
                logger.exception(msg)
                set_loading_error(rid, msg)
                errors += 1

        logger.info("="*60)
        logger.info(f"🏁 SYNC COMPLETE — Success: {success}, Errors: {errors}")
        logger.info("="*60)

    except Exception as e:
        logger.exception(f"A fatal error occurred during the sync: {e}")

if __name__ == "__main__":
    main()
