#!/usr/bin/env python3
"""
Airtable Image Downloader
Downloads and processes images from Airtable Items-Bid4more table
Only processes records from the "Hibid File Loading View"
"""

import os
import requests
from pyairtable import Api
from PIL import Image  #!/usr/bin/env python3

"""
Airtable Image Downloader
Downloads and processes images from Airtable Items-Bid4more table
Only processes records from the "Hibid File Loading View"
"""

import os
import requests
from pyairtable import Api
from PIL import Image
from io import BytesIO
import time
from urllib.parse import urlparse
import logging
from dotenv import load_dotenv
import csv
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AirtableImageDownloader:
    def __init__(self, api_key, base_id, table_name):
        self.api = Api(api_key)
        self.base = self.api.base(base_id)
        self.table = self.base.table(table_name)

        # Create a unique folder name with the current date and time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.getenv("OUTPUT_DIR", "downloaded_images")
        self.output_dir = f"{base_dir}_{timestamp}"

        self.download_log = []
        self.current_view = None

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")

    def download_image(self, url):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return Image.open(BytesIO(response.content))
        except Exception as e:
            logger.error(f"Failed to download image from {url}: {str(e)}")
            return None

    def resize_and_convert_image(self, image, size=(800, 800)):
        if image.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", image.size, (255, 255, 255))
            if image.mode == "P":
                image = image.convert("RGBA")
            background.paste(
                image, mask=image.split()[-1] if image.mode == "RGBA" else None
            )
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        image.thumbnail(size, Image.Resampling.LANCZOS)
        new_image = Image.new("RGB", size, (255, 255, 255))
        x = (size[0] - image.width) // 2
        y = (size[1] - image.height) // 2
        new_image.paste(image, (x, y))
        return new_image

    def process_record(self, record):
        fields = record["fields"]
        record_id = record["id"]

        sku_field = os.getenv("AIRTABLE_HIBID_LOT_FIELD", "Store Inventory No")
        inventory_no = fields.get(sku_field, f"unknown_{record_id}")
        inventory_no = "".join(
            c for c in str(inventory_no) if c.isalnum() or c in ("-", "_")
        )

        # Get and sanitize auction code
        auction_code = fields.get("Auction-code", "UnknownAuction")
        auction_code_clean = "".join(
            c for c in str(auction_code) if c.isalnum() or c in ("-", "_")
        )
        auction_folder = os.path.join(self.output_dir, f"Auction-{auction_code_clean}")
        os.makedirs(auction_folder, exist_ok=True)

        logger.info(
            f"Processing item: {inventory_no} | Auction: {auction_code_clean} (Record ID: {record_id[:8]}...)"
        )
        image_counter = 1

        featured_photo_field = os.getenv(
            "AIRTABLE_FEATURED_PHOTO_FIELD", "Item Featured Photo"
        )
        item_photos_field = os.getenv("AIRTABLE_ITEM_PHOTOS_FIELD", "Photo Files")
        inspection_photos_field = os.getenv(
            "AIRTABLE_INSPECTION_PHOTOS_FIELD", "Inspection Photos"
        )
        image_fields = [
            featured_photo_field,
            item_photos_field,
            inspection_photos_field,
        ]

        for field_name in image_fields:
            if field_name in fields and fields[field_name]:
                attachments = fields[field_name]

                for attachment in attachments:
                    if "url" in attachment:
                        image = self.download_image(attachment["url"])
                        if image:
                            processed_image = self.resize_and_convert_image(image)
                            filename = f"{inventory_no}-{image_counter}.jpg"
                            filepath = os.path.join(auction_folder, filename)

                            processed_image.save(
                                filepath, "JPEG", quality=90, optimize=True
                            )
                            logger.info(f"Saved: {filepath}")

                            self.download_log.append(
                                {
                                    "inventory_no": inventory_no,
                                    "filename": filename,
                                    "field_name": field_name,
                                    "auction_code": auction_code_clean,
                                    "view_name": self.current_view,
                                    "original_url": attachment["url"],
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                            image_counter += 1
                            time.sleep(0.5)

        return image_counter - 1

    def save_download_report(self):
        if not self.download_log:
            return

        report_path = os.path.join(
            self.output_dir,
            f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )

        with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "inventory_no",
                "filename",
                "field_name",
                "auction_code",
                "view_name",
                "original_url",
                "timestamp",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

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
        failed_downloads = 0

        try:
            for page in self.table.iterate(page_size=100, view=view_name):
                for record in page:
                    try:
                        images_count = self.process_record(record)
                        total_images += images_count
                        total_records += 1

                        if total_records % 10 == 0:
                            logger.info(
                                f"Processed {total_records} records, {total_images} images so far..."
                            )
                    except Exception as e:
                        failed_downloads += 1
                        logger.error(
                            f"Failed to process record {record.get('id', 'unknown')}: {str(e)}"
                        )
                        continue

            if total_records == 0:
                logger.warning(f"No records found in view '{view_name}'")
                logger.info("Please ensure:")
                logger.info("  1. The view name is correct (case-sensitive)")
                logger.info("  2. The view contains records")
                logger.info("  3. You have access to this view")
                return

            logger.info("\n" + "=" * 50)
            logger.info("DOWNLOAD COMPLETE!")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"Total images downloaded: {total_images}")
            if failed_downloads > 0:
                logger.warning(f"Failed downloads: {failed_downloads}")
            logger.info(f"Images saved to: {os.path.abspath(self.output_dir)}")

            if total_images > 0:
                self.save_download_report()

            logger.info("=" * 50 + "\n")

        except Exception as e:
            logger.error(f"Error during download: {str(e)}")
            if "view" in str(e).lower():
                logger.error(f"Could not find or access view '{view_name}'")
            raise


def main():
    load_dotenv()

    API_KEY = os.getenv("AIRTABLE_API_KEY")
    BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    TABLE_NAME = "Items-Bid4more"
    VIEW_NAME = "Hibid File Loading View"

    if not API_KEY or not BASE_ID:
        logger.error("Missing required environment variables!")
        logger.info("Please ensure your .env file contains:")
        logger.info("  AIRTABLE_API_KEY=your_api_key_here")
        logger.info("  AIRTABLE_BASE_ID=your_base_id_here")
        return

    logger.info(f"Using Airtable Base ID: {BASE_ID}")
    logger.info(f"Using Table: {TABLE_NAME}")
    logger.info(f"Using View: {VIEW_NAME}")

    downloader = AirtableImageDownloader(API_KEY, BASE_ID, TABLE_NAME)
    downloader.download_all_images(view_name=VIEW_NAME)


if __name__ == "__main__":
    main()
from io import BytesIO
import time
from urllib.parse import urlparse
import logging
from dotenv import load_dotenv
import csv
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AirtableImageDownloader:
    def __init__(self, api_key, base_id, table_name):
        self.api = Api(api_key)
        self.base = self.api.base(base_id)
        self.table = self.base.table(table_name)
        self.output_dir = os.getenv("OUTPUT_DIR", "downloaded_images")
        self.download_log = []
        self.current_view = None

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")

    def download_image(self, url):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return Image.open(BytesIO(response.content))
        except Exception as e:
            logger.error(f"Failed to download image from {url}: {str(e)}")
            return None

    def resize_and_convert_image(self, image, size=(800, 800)):
        if image.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", image.size, (255, 255, 255))
            if image.mode == "P":
                image = image.convert("RGBA")
            background.paste(
                image, mask=image.split()[-1] if image.mode == "RGBA" else None
            )
            image = background
        elif image.mode != "RGB":
            image = image.convert("RGB")

        image.thumbnail(size, Image.Resampling.LANCZOS)
        new_image = Image.new("RGB", size, (255, 255, 255))
        x = (size[0] - image.width) // 2
        y = (size[1] - image.height) // 2
        new_image.paste(image, (x, y))
        return new_image

    def process_record(self, record):
        fields = record["fields"]
        record_id = record["id"]

        sku_field = os.getenv("AIRTABLE_HIBID_LOT_FIELD", "Store Inventory No")
        inventory_no = fields.get(sku_field, f"unknown_{record_id}")
        inventory_no = "".join(
            c for c in str(inventory_no) if c.isalnum() or c in ("-", "_")
        )

        # Get and sanitize auction code
        auction_code = fields.get("Auction-code", "UnknownAuction")
        auction_code_clean = "".join(
            c for c in str(auction_code) if c.isalnum() or c in ("-", "_")
        )
        auction_folder = os.path.join(self.output_dir, f"Auction-{auction_code_clean}")
        os.makedirs(auction_folder, exist_ok=True)

        logger.info(
            f"Processing item: {inventory_no} | Auction: {auction_code_clean} (Record ID: {record_id[:8]}...)"
        )
        image_counter = 1

        featured_photo_field = os.getenv(
            "AIRTABLE_FEATURED_PHOTO_FIELD", "Item Featured Photo"
        )
        item_photos_field = os.getenv("AIRTABLE_ITEM_PHOTOS_FIELD", "Item Photos")
        inspection_photos_field = os.getenv(
            "AIRTABLE_INSPECTION_PHOTOS_FIELD", "Inspection Photos"
        )
        image_fields = [
            featured_photo_field,
            item_photos_field,
            inspection_photos_field,
        ]

        for field_name in image_fields:
            if field_name in fields and fields[field_name]:
                attachments = fields[field_name]

                for attachment in attachments:
                    if "url" in attachment:
                        image = self.download_image(attachment["url"])
                        if image:
                            processed_image = self.resize_and_convert_image(image)
                            filename = f"{inventory_no}-{image_counter}.jpg"
                            filepath = os.path.join(auction_folder, filename)

                            processed_image.save(
                                filepath, "JPEG", quality=90, optimize=True
                            )
                            logger.info(f"Saved: {filepath}")

                            self.download_log.append(
                                {
                                    "inventory_no": inventory_no,
                                    "filename": filename,
                                    "field_name": field_name,
                                    "auction_code": auction_code_clean,
                                    "view_name": self.current_view,
                                    "original_url": attachment["url"],
                                    "timestamp": datetime.now().isoformat(),
                                }
                            )

                            image_counter += 1
                            time.sleep(0.5)

        return image_counter - 1

    def save_download_report(self):
        if not self.download_log:
            return

        report_path = os.path.join(
            self.output_dir,
            f"download_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )

        with open(report_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "inventory_no",
                "filename",
                "field_name",
                "auction_code",
                "view_name",
                "original_url",
                "timestamp",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

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
        failed_downloads = 0

        try:
            for page in self.table.iterate(page_size=100, view=view_name):
                for record in page:
                    try:
                        images_count = self.process_record(record)
                        total_images += images_count
                        total_records += 1

                        if total_records % 10 == 0:
                            logger.info(
                                f"Processed {total_records} records, {total_images} images so far..."
                            )
                    except Exception as e:
                        failed_downloads += 1
                        logger.error(
                            f"Failed to process record {record.get('id', 'unknown')}: {str(e)}"
                        )
                        continue

            if total_records == 0:
                logger.warning(f"No records found in view '{view_name}'")
                logger.info("Please ensure:")
                logger.info("  1. The view name is correct (case-sensitive)")
                logger.info("  2. The view contains records")
                logger.info("  3. You have access to this view")
                return

            logger.info("\n" + "=" * 50)
            logger.info("DOWNLOAD COMPLETE!")
            logger.info(f"Total records processed: {total_records}")
            logger.info(f"Total images downloaded: {total_images}")
            if failed_downloads > 0:
                logger.warning(f"Failed downloads: {failed_downloads}")
            logger.info(f"Images saved to: {os.path.abspath(self.output_dir)}")

            if total_images > 0:
                self.save_download_report()

            logger.info("=" * 50 + "\n")

        except Exception as e:
            logger.error(f"Error during download: {str(e)}")
            if "view" in str(e).lower():
                logger.error(f"Could not find or access view '{view_name}'")
            raise


def main():
    load_dotenv()

    API_KEY = os.getenv("AIRTABLE_API_KEY")
    BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    TABLE_NAME = "Items-Bid4more"
    VIEW_NAME = "Hibid File Loading View"

    if not API_KEY or not BASE_ID:
        logger.error("Missing required environment variables!")
        logger.info("Please ensure your .env file contains:")
        logger.info("  AIRTABLE_API_KEY=your_api_key_here")
        logger.info("  AIRTABLE_BASE_ID=your_base_id_here")
        return

    logger.info(f"Using Airtable Base ID: {BASE_ID}")
    logger.info(f"Using Table: {TABLE_NAME}")
    logger.info(f"Using View: {VIEW_NAME}")

    downloader = AirtableImageDownloader(API_KEY, BASE_ID, TABLE_NAME)
    downloader.download_all_images(view_name=VIEW_NAME)


if __name__ == "__main__":
    main()
