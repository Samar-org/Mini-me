#!/usr/bin/env python3
"""
Airtable Image Downloader (robust, Hibid-only naming, conditional sequence)
- Reads from Airtable; does NOT update Airtable
- File naming: always based on "Hibid Lot No"
- Sequence:
    If "4more-Product-Code" is empty:
        1) Item Featured Photo
        2) Photo Files
        3) Inspection Photos
    If "4more-Product-Code" is not empty:
        1) Photo Files (from 4more-Product-Code)
        2) Inspection Photos
"""

import os

# Hardcoded fallback values
DEFAULTS = {
    "AIRTABLE_API_KEY": "patBj24RPrnf4x3DN.91f32ab12173eaf5ea44ab29034783550856b9192f65aa04e86bff1b176f0f66",
    "AIRTABLE_BASE_ID": "appI0EDHQVVZCxVZ9",
    "AIRTABLE_TABLE_NAME": "Items-Bid4more",
    "AIRTABLE_VIEW_NAME": "Hibid File Loading View",
    "OUTPUT_DIR": "downloaded_images",
    "MAX_WIDTH": "800",
    "MAX_HEIGHT": "800",
}

# Use env if set, otherwise fallback
for key, value in DEFAULTS.items():
    os.environ[key] = os.getenv(key, value)


import sys
import csv
import time
import logging
from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from PIL import Image, ImageFile
from dotenv import load_dotenv
from pyairtable import Api

ImageFile.LOAD_TRUNCATED_IMAGES = True

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("airtable-image-downloader")


def build_retrying_session(
    total=5,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    connect_retries=3,
    read_retries=3,
    pool_maxsize=10,
    timeout=30,
    user_agent="AirtableImageDownloader/1.0"
):
    sess = requests.Session()
    retry = Retry(
        total=total,
        read=read_retries,
        connect=connect_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=pool_maxsize)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": user_agent})
    sess.request_timeout = timeout
    return sess


def is_probable_image_response(resp):
    ctype = resp.headers.get("Content-Type", "").lower()
    return ctype.startswith("image/") or "octet-stream" in ctype


class AirtableImageDownloader:
    def __init__(self, api_key, base_id, table_name, output_base_dir=None, max_size=(800, 800)):
        self.api = Api(api_key)
        self.base = self.api.base(base_id)
        self.table = self.base.table(table_name)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = output_base_dir or os.getenv("OUTPUT_DIR", "downloaded_images")
        self.output_dir = f"{base_dir}_{timestamp}"

        self.download_log = []
        self.current_view = None
        self.session = build_retrying_session()
        self.max_size = max_size

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created output directory: {self.output_dir}")

    def download_image(self, url):
        try:
            with self.session.get(url, stream=True, timeout=self.session.request_timeout) as r:
                r.raise_for_status()
                if not is_probable_image_response(r):
                    logger.error(f"Not an image (Content-Type={r.headers.get('Content-Type')}) - {url}")
                    return None
                content = (
                    r.content
                    if r.headers.get("Content-Length") and int(r.headers["Content-Length"]) < 25_000_000
                    else r.raw.read()
                )
                return Image.open(BytesIO(content))
        except Exception as e:
            logger.error(f"Failed to download image from {url}: {e}")
            return None

    def resize_and_convert_image(self, image, size=None):
        size = size or self.max_size
        # Normalize to RGB
        if image.mode in ("RGBA", "LA", "P"):
            if image.mode == "P":
                image = image.convert("RGBA")
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[-1] if "A" in image.getbands() else None)
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        # Fit within size maintaining aspect; pad to canvas
        image.thumbnail(size, Image.Resampling.LANCZOS)
        new_image = Image.new("RGB", size, (255, 255, 255))
        x = (size[0] - image.width) // 2
        y = (size[1] - image.height) // 2
        new_image.paste(image, (x, y))
        return new_image

    @staticmethod
    def sanitize_filename_piece(s):
        return "".join(c for c in str(s) if c.isalnum() or c in ("-", "_")).strip() or "unknown"

    @staticmethod
    def _flatten_lookup(val):
        """
        Flattens Airtable lookup/array values:
        - [] or [ {url:...}, ... ]
        - [ [ {url:...}, ... ], [ {url:...}, ... ] ]
        - [ "https://...", "https://..." ]
        """
        if not isinstance(val, list):
            return []
        flat = []
        stack = list(val)
        while stack:
            item = stack.pop(0)
            if isinstance(item, list):
                stack = item + stack
            else:
                flat.append(item)
        return flat

    @staticmethod
    def _field_has_value(val):
        """True if a field value is 'non-empty' in Airtable terms."""
        if val is None:
            return False
        if isinstance(val, list):
            return len(val) > 0
        if isinstance(val, str):
            return val.strip() != ""
        return bool(val)

    def _choose_basename(self, fields, record_id):
        # ALWAYS use Hibid Lot No
        hibid_lot_no = fields.get("Hibid Lot No", f"unknown_{record_id}")
        if isinstance(hibid_lot_no, list):
            hibid_lot_no = hibid_lot_no[0] if hibid_lot_no else None
        return self.sanitize_filename_piece(hibid_lot_no)

    def process_record(self, record):
        fields = record.get("fields", {})
        record_id = record.get("id", "unknown")

        # Auction folder
        auction_name = fields.get("Name (from Auction-Code)", "Unknown Auction")
        if isinstance(auction_name, list) and auction_name:
            auction_name = auction_name[0]
        auction_folder = os.path.join(self.output_dir, self.sanitize_filename_piece(auction_name))
        os.makedirs(auction_folder, exist_ok=True)

        basename = self._choose_basename(fields, record_id)
        logger.info(f"Processing {record_id[:8]}… | Auction: {auction_name} | Base: {basename}")

        # Field names (env overrides allowed for the basic three)
        featured_photo_field = os.getenv("AIRTABLE_FEATURED_PHOTO_FIELD", "Item Featured Photo")
        item_photos_field = os.getenv("AIRTABLE_ITEM_PHOTOS_FIELD", "Photo Files")
        # Accept both correct and common-typo for "Inspection Photos"
        inspection_photos_field = os.getenv("AIRTABLE_INSPECTION_PHOTOS_FIELD", "Inspection Photos")
        inspection_photos_field_alt = "Incpestion Photos"  # typo safeguard
        product_catalogue_photos_field = "Photo Files (from 4more-Product-Code)"

        # Decide sequence based on "4more-Product-Code"
        product_code_field = "4more-Product-Code"
        has_product_code = self._field_has_value(fields.get(product_code_field))

        if has_product_code:
            sequence = [
                product_catalogue_photos_field,
                inspection_photos_field,
                inspection_photos_field_alt,  # try typo if present
            ]
        else:
            sequence = [
                featured_photo_field,
                item_photos_field,
                inspection_photos_field,
                inspection_photos_field_alt,  # try typo if present
            ]

        image_counter = 0

        for field_name in sequence:
            val = fields.get(field_name)
            if not val:
                continue

            # Flatten only for product catalogue images; others are normal attachment lists
            if field_name == product_catalogue_photos_field:
                attachments = self._flatten_lookup(val)
            else:
                attachments = val if isinstance(val, list) else [val]

            for attachment in attachments:
                url = None
                if isinstance(attachment, dict) and "url" in attachment:
                    url = attachment["url"]
                elif isinstance(attachment, str) and attachment.startswith("http"):
                    url = attachment

                if not url:
                    continue

                img = self.download_image(url)
                if not img:
                    continue

                try:
                    processed = self.resize_and_convert_image(img, self.max_size)
                except Exception as e:
                    logger.error(f"Failed to process image from {url}: {e}")
                    continue

                image_counter += 1
                filename = f"{basename}-{image_counter}.jpg"
                filepath = os.path.join(auction_folder, filename)

                try:
                    processed.save(filepath, "JPEG", quality=90, optimize=True, progressive=True)
                    logger.info(f"Saved: {filepath}")
                except Exception as e:
                    logger.error(f"Failed to save {filepath}: {e}")
                    image_counter -= 1
                    continue

                self.download_log.append(
                    {
                        "hibid_or_sku": basename,
                        "filename": filename,
                        "field_name": field_name,
                        "auction_name": auction_name,
                        "view_name": self.current_view,
                        "original_url": url,
                        "timestamp": datetime.now().isoformat(),
                        "source": "Product Catalogue" if field_name == product_catalogue_photos_field else "Direct",
                    }
                )

                time.sleep(0.35)

        return image_counter

    def save_download_report(self):
        if not self.download_log:
            return None

        report_path = os.path.join(self.output_dir, f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        fieldnames = [
            "hibid_or_sku",
            "filename",
            "field_name",
            "auction_name",
            "view_name",
            "original_url",
            "timestamp",
            "source",
        ]
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.download_log)

        logger.info(f"Download report saved to: {report_path}")
        return report_path

    def download_all_images(self, view_name="Hibid File Loading View"):
        self.current_view = view_name

        logger.info("=" * 50)
        logger.info(f"DOWNLOADING FROM VIEW: {view_name}")
        logger.info("=" * 50)

        total_images = 0
        total_records = 0
        failed_records = 0
        product_catalogue_images = 0

        try:
            for page in self.table.iterate(page_size=100, view=view_name):
                for record in page:
                    try:
                        count_before = len(self.download_log)
                        images_count = self.process_record(record)
                        total_images += images_count
                        total_records += 1

                        # Count how many added from Product Catalogue for this record
                        if images_count:
                            hibid_or_sku = self._choose_basename(record.get("fields", {}), record.get("id", "unknown"))
                            catalogue_added = sum(
                                1
                                for log in self.download_log[count_before:]
                                if log["source"] == "Product Catalogue" and log["hibid_or_sku"] == hibid_or_sku
                            )
                            product_catalogue_images += catalogue_added

                        if total_records % 10 == 0:
                            logger.info(f"Processed {total_records} records | {total_images} images so far...")
                    except Exception as e:
                        failed_records += 1
                        logger.error(f"Failed to process record {record.get('id', 'unknown')}: {e}")
                        continue

            if total_records == 0:
                logger.warning(f"No records found in view '{view_name}'")
                logger.info("Check: (1) view name (case-sensitive), (2) records exist, (3) API key has access.")
                return

            logger.info("\n" + "=" * 50)
            logger.info("DOWNLOAD COMPLETE!")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"Total images downloaded: {total_images}")
            if product_catalogue_images > 0:
                logger.info(f"  - From Product Catalogue: {product_catalogue_images}")
                logger.info(f"  - Direct uploads: {total_images - product_catalogue_images}")
            if failed_records > 0:
                logger.warning(f"Records failed: {failed_records}")
            logger.info(f"Images saved to: {os.path.abspath(self.output_dir)}")
            if total_images > 0:
                self.save_download_report()
            logger.info("=" * 50 + "\n")

        except Exception as e:
            logger.error(f"Error during download: {e}")
            if "view" in str(e).lower():
                logger.error(f"Could not find or access view '{view_name}'")
            raise


def main():
    load_dotenv()

    API_KEY = os.getenv("AIRTABLE_API_KEY")
    BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Items-Bid4more")
    VIEW_NAME = os.getenv("AIRTABLE_VIEW_NAME", "Hibid File Loading View")
    OUTPUT_BASE_DIR = os.getenv("OUTPUT_DIR", "downloaded_images")

    if len(sys.argv) > 1 and sys.argv[1].strip():
        VIEW_NAME = sys.argv[1].strip()

    if not API_KEY or not BASE_ID:
        logger.error("Missing required environment variables!")
        logger.info("Ensure your .env contains:")
        logger.info("  AIRTABLE_API_KEY=your_api_key_here")
        logger.info("  AIRTABLE_BASE_ID=your_base_id_here")
        logger.info("Optional:")
        logger.info("  AIRTABLE_TABLE_NAME=Items-Bid4more")
        logger.info("  AIRTABLE_VIEW_NAME=Hibid File Loading View")
        logger.info("  OUTPUT_DIR=downloaded_images")
        return

    max_w = int(os.getenv("MAX_WIDTH", "800"))
    max_h = int(os.getenv("MAX_HEIGHT", "800"))

    logger.info(f"Using Airtable Base ID: {BASE_ID}")
    logger.info(f"Using Table: {TABLE_NAME}")
    logger.info(f"Using View: {VIEW_NAME}")

    downloader = AirtableImageDownloader(
        API_KEY,
        BASE_ID,
        TABLE_NAME,
        output_base_dir=OUTPUT_BASE_DIR,
        max_size=(max_w, max_h),
    )
    downloader.download_all_images(view_name=VIEW_NAME)


if __name__ == "__main__":
    main()
