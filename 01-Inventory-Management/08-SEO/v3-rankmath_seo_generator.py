#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rank Math 100% SEO Field Generator for Airtable using OpenAI
-----------------------------------------------------------
• Reads products from Airtable (default table: "Product Catalogue").
• Processes ONLY records where the checkbox field "SEO Ready" is UNCHECKED.
• Uses Product URL or Product Name (plus existing Airtable fields) to build SEO — no scraping.
• Generates Rank Math fields via OpenAI and writes them back to Airtable.
• After a successful update, sets "SEO Ready" to CHECKED (true).
• Also generates best‑practice Product Tags (6–10) and saves to "Product Tags".

Setup
=====
1) pip install -U openai pyairtable tenacity python-slugify python-dotenv
2) .env
   OPENAI_API_KEY=sk-...
   OPENAI_MODEL=gpt-5            # or o4-mini / o3-mini depending on access
   AIRTABLE_API_KEY=pat-...
   AIRTABLE_BASE_ID=appXXXXXXXXXX
   AIRTABLE_TABLE_NAME=Product Catalogue
   AIRTABLE_VIEW_NAME=
   BATCH_LIMIT=50
   OVERWRITE_EXISTING=0
   SEO_READY_FIELD=SEO Ready

Run
===
python v2-rankmath_seo_generator.py
"""
from __future__ import annotations

import os
import json
import re
from typing import Dict, Any, List
from dataclasses import dataclass

from tenacity import retry, stop_after_attempt, wait_exponential
from slugify import slugify
from pyairtable import Api
from dotenv import load_dotenv

# -------------------- Config / Env --------------------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")  # default to a valid model
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Product Catalogue")
AIRTABLE_VIEW_NAME = os.getenv("AIRTABLE_VIEW_NAME") or None
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "0") or 0)
OVERWRITE_EXISTING = os.getenv("OVERWRITE_EXISTING", "0") == "1"
SEO_READY_FIELD = os.getenv("SEO_READY_FIELD", "SEO Ready")

if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY in environment.")
if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
    raise SystemExit("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID in environment.")

print(f"[SEO-Gen] Model={OPENAI_MODEL} | No temperature param used.")

# -------------------- OpenAI client --------------------
from openai import OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# -------------------- Field config --------------------
REQUIRED_FIELDS = [
    "ecommerce-Friendly Name",
    "Product SEO Title",
    "Meta Description",
    "Meta Title",
    "Focus Keyword",
    "Product Slug",
    "Image Alt Text",
    "Product Tags",
]

# -------------------- Helpers --------------------
def coalesce(*vals) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def needs_update(current: Dict[str, Any]) -> bool:
    if OVERWRITE_EXISTING:
        return True
    return any(not coalesce(current.get(k, "")) for k in REQUIRED_FIELDS)

def optimize_slug(seed: str, focus_kw: str) -> str:
    """
    Build an SEO-friendly slug optimized for Google crawling:
    - Always include focus keyword (prefixed if absent).
    - Remove common stopwords.
    - Hyphenate using slugify.
    - Keep under 55 chars (safer than 60).
    """
    base = slugify((seed or "").lower())
    focus = slugify((focus_kw or "").lower())

    # Ensure focus keyword present
    if focus and focus not in base:
        base = f"{focus}-{base}" if base else focus

    # Remove common stop words
    stopwords = {
        "the", "and", "with", "for", "from", "by", "of", "in", "on",
        "to", "a", "an", "at", "as", "is", "are"
    }
    words = [w for w in base.split("-") if w and w not in stopwords]
    if not words and base:
        words = base.split("-")

    slug = "-".join(words)
    # Final clean + cap length to 55
    slug = slugify(slug)[:55].strip("-")
    # Avoid trailing/leading hyphens after trim
    return slug

def sanitize_output(data: Dict[str, Any], fallback_slug_seed: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out['ecommerce-Friendly Name'] = coalesce(data.get('ecommerce_friendly_name'))
    out['Product SEO Title'] = coalesce(data.get('product_seo_title'))
    out['Meta Description'] = coalesce(data.get('meta_description'))
    out['Meta Title'] = coalesce(data.get('meta_title'), out.get('Product SEO Title'))
    out['Focus Keyword'] = coalesce(data.get('focus_keyword'))

    # Optimized slug generation (Google-friendly + shorter if title is long)
    slug_seed = coalesce(data.get('product_slug'), fallback_slug_seed)
    out['Product Slug'] = optimize_slug(slug_seed, out['Focus Keyword'])

    alts = data.get('image_alt_text') or []
    if isinstance(alts, list):
        alts = "\n".join(a.strip() for a in alts if str(a).strip())
    out['Image Alt Text'] = coalesce(alts)

    # Product Tags: expect comma-separated string from the model
    out['Product Tags'] = coalesce(data.get('product_tags'))
    return out

@dataclass
class ProductCtx:
    record_id: str
    fields: Dict[str, Any]

    @property
    def display_title(self) -> str:
        return coalesce(self.fields.get('Title'), self.fields.get('Name'))

    @property
    def brand(self) -> str:
        return coalesce(self.fields.get('Brand'))

    @property
    def description(self) -> str:
        return coalesce(self.fields.get('Long Description'), self.fields.get('Short Description'))

    @property
    def sku(self) -> str:
        return coalesce(self.fields.get('SKU'))

SYSTEM_PROMPT = (
    "You are an elite ecommerce SEO copywriter and Rank Math expert. "
    "Given a product's raw details, produce fields that score 100/100 in Rank Math for WooCommerce. "
    "Do NOT browse or fetch remote pages. Use only the provided fields, including the literal Product URL string and the Product Name/Title as hints. "
    "Return only JSON with the required keys and avoid keyword stuffing (aim 1–2% density)."
)

USER_INSTRUCTIONS = (
    "Return JSON with ONLY these keys: \n"
    "ecommerce_friendly_name, product_seo_title, meta_title, meta_description, focus_keyword, product_slug, image_alt_text, product_tags.\n"
    "Rules: focus keyword FIRST in SEO title (50–60 chars) with one power word; meta title 50–60 chars; "
    "meta description 150–160 chars with the focus keyword once and a soft CTA; "
    "slug <=55 chars, hyphenated, includes the focus keyword; "
    "image_alt_text is an array of 4–6 strings including the focus keyword with unique angles; "
    "product_tags is a comma-separated list of 6–10 tags mixing brand, category, subcategory, and high-intent search terms.\n"
    "No extra prose—JSON only."
)

def build_product_payload(ctx: ProductCtx) -> Dict[str, Any]:
    return {
        "title": ctx.display_title,
        "brand": ctx.brand,
        "sku": ctx.sku,
        "description": ctx.description,
        "url": ctx.fields.get("Product URL"),
        "category": ctx.fields.get("Category"),
        "subcategory": ctx.fields.get("Subcategory"),
        "color": ctx.fields.get("Color"),
        "size": ctx.fields.get("Size"),
        "material": ctx.fields.get("Material"),
    }

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def call_openai_for_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Call OpenAI with fallback models; no temperature param."""
    candidates = [
        OPENAI_MODEL,        # from .env
        "gpt-5",
        "o4-mini",
        "o3-mini",
    ]
    last_err = None
    for m in candidates:
        if not m:
            continue
        try:
            resp = client.responses.create(
                model=m,
                reasoning={"effort": "medium"},
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": USER_INSTRUCTIONS
                        + "\n\nProduct Context JSON:\n"
                        + json.dumps(payload, ensure_ascii=False),
                    },
                ],
            )
            raw = resp.output_text
            # Extract JSON from any surrounding text just in case
            match = re.search(r"\{[\s\S]*\}$", raw.strip())
            text = match.group(0) if match else raw
            return json.loads(text)
        except Exception as e:
            last_err = e
            continue
    raise last_err if last_err else RuntimeError("Model calls failed with no exception detail.")

# -------------------- Airtable I/O --------------------
def fetch_products(table) -> List[Dict[str, Any]]:
    def at_field(name: str) -> str:
        return "{" + name + "}"

    formula = f"OR({at_field(SEO_READY_FIELD)}=BLANK(), {at_field(SEO_READY_FIELD)}=0, {at_field(SEO_READY_FIELD)}=FALSE())"

    records: List[Dict[str, Any]] = []
    kwargs: Dict[str, Any] = {"formula": formula}
    if AIRTABLE_VIEW_NAME:
        kwargs["view"] = AIRTABLE_VIEW_NAME

    try:
        iterator = table.iterate(page_size=100, **kwargs)
        for chunk in iterator:
            if isinstance(chunk, list):
                records.extend(chunk)
            else:
                records.append(chunk)
            if BATCH_LIMIT and len(records) >= BATCH_LIMIT:
                break
    except TypeError:
        page = table.all(**kwargs)
        records.extend(page)

    if BATCH_LIMIT:
        records = records[:BATCH_LIMIT]
    return records

# -------------------- Main per-record processing --------------------
def process_record(table, rec: Dict[str, Any]) -> None:
    if isinstance(rec, list):
        for r in rec:
            process_record(table, r)
        return

    ctx = ProductCtx(record_id=rec['id'], fields=rec.get('fields', {}))

    if not needs_update(ctx.fields):
        print(f"- {ctx.record_id} skipped (SEO fields complete)")
        return

    payload = build_product_payload(ctx)

    try:
        model_json = call_openai_for_fields(payload)
    except Exception as e:
        print(f"! {ctx.record_id} OpenAI error: {e}")
        return

    # Fallback seed for slug when AI doesn't provide a clean one
    seed = coalesce(ctx.sku, ctx.display_title, ctx.brand)
    out = sanitize_output(model_json, fallback_slug_seed=seed)

    try:
        table.update(ctx.record_id, {**out, SEO_READY_FIELD: True})
        print(f"✓ Updated {ctx.record_id} | SEO Ready -> True")
    except Exception as e:
        print(f"! {ctx.record_id} Airtable update failed: {e}")

def main():
    api = Api(AIRTABLE_API_KEY)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    records = fetch_products(table)
    print(f"Found {len(records)} pending SEO.")
    for rec in records:
        process_record(table, rec)

if __name__ == "__main__":
    main()
