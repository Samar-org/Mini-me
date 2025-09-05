#!/usr/bin/env python3
"""
WordPress Media Cleanup Tool
Safely removes media files that are not attached to any posts, pages, or WooCommerce products
Designed for 4more.org WordPress/WooCommerce installation
"""

import os
import sys
import json
import time
import logging
import argparse
import hashlib
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urlparse
import requests
import mysql.connector
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'wp_media_cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('WPMediaCleanup')


class WordPressDatabase:
    """Direct database connection for WordPress/WooCommerce"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.table_prefix = config.get('table_prefix', 'wp_')
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            logger.info("Successfully connected to WordPress database")
            return True
        except Error as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a SELECT query and return results"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            logger.error(f"Query execution error: {e}")
            return []
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute UPDATE/DELETE query and return affected rows"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            self.connection.commit()
            affected_rows = cursor.rowcount
            cursor.close()
            return affected_rows
        except Error as e:
            logger.error(f"Update execution error: {e}")
            self.connection.rollback()
            return 0


class WooCommerceAPI:
    """WooCommerce REST API client for backup verification"""
    
    def __init__(self, url: str, consumer_key: str, consumer_secret: str):
        self.url = url.rstrip('/')
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.api_url = f"{self.url}/wp-json/wc/v3"
    
    def get_all_product_ids(self) -> Set[int]:
        """Get all product IDs from WooCommerce"""
        product_ids = set()
        page = 1
        per_page = 100
        
        while True:
            try:
                response = requests.get(
                    f"{self.api_url}/products",
                    auth=(self.consumer_key, self.consumer_secret),
                    params={'page': page, 'per_page': per_page, 'status': 'any'}
                )
                response.raise_for_status()
                products = response.json()
                
                if not products:
                    break
                
                for product in products:
                    product_ids.add(product['id'])
                
                page += 1
                time.sleep(0.5)  # Rate limiting
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching products: {e}")
                break
        
        logger.info(f"Found {len(product_ids)} products via API")
        return product_ids


class MediaCleanup:
    """Main cleanup class for WordPress media"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.db = WordPressDatabase(config['database'])
        self.dry_run = config.get('dry_run', True)
        self.backup_dir = config.get('backup_dir', './media_backup')
        self.uploads_path = config.get('uploads_path', '/wp-content/uploads')
        self.site_url = config.get('site_url', '').rstrip('/')
        
        # Initialize WooCommerce API if configured
        self.wc_api = None
        if config.get('woocommerce'):
            self.wc_api = WooCommerceAPI(
                url=config['woocommerce']['url'],
                consumer_key=config['woocommerce']['consumer_key'],
                consumer_secret=config['woocommerce']['consumer_secret']
            )
        
        # Statistics
        self.stats = {
            'total_media': 0,
            'used_media': 0,
            'unused_media': 0,
            'deleted_media': 0,
            'backed_up': 0,
            'errors': 0,
            'space_freed': 0
        }
    
    def get_all_media_attachments(self) -> List[Dict]:
        """Get all media attachments from database"""
        query = f"""
            SELECT 
                p.ID,
                p.post_title,
                p.post_name,
                p.guid,
                p.post_parent,
                p.post_mime_type,
                pm1.meta_value as file_path,
                pm2.meta_value as file_meta
            FROM {self.db.table_prefix}posts p
            LEFT JOIN {self.db.table_prefix}postmeta pm1 
                ON p.ID = pm1.post_id AND pm1.meta_key = '_wp_attached_file'
            LEFT JOIN {self.db.table_prefix}postmeta pm2 
                ON p.ID = pm2.post_id AND pm2.meta_key = '_wp_attachment_metadata'
            WHERE p.post_type = 'attachment'
            ORDER BY p.ID
        """
        
        if not self.db.connect():
            return []
        
        attachments = self.db.execute_query(query)
        self.stats['total_media'] = len(attachments)
        logger.info(f"Found {len(attachments)} media attachments in database")
        
        return attachments
    
    def get_used_media_ids(self) -> Set[int]:
        """Get IDs of all media that are actually used"""
        used_ids = set()
        
        # 1. Get media attached to posts/pages
        logger.info("Checking media attached to posts and pages...")
        query_attached = f"""
            SELECT DISTINCT ID 
            FROM {self.db.table_prefix}posts 
            WHERE post_type = 'attachment' 
            AND post_parent > 0
            AND post_parent IN (
                SELECT ID FROM {self.db.table_prefix}posts 
                WHERE post_status IN ('publish', 'draft', 'private', 'pending')
            )
        """
        results = self.db.execute_query(query_attached)
        for row in results:
            used_ids.add(row['ID'])
        
        # 2. Get featured images (post thumbnails)
        logger.info("Checking featured images...")
        query_featured = f"""
            SELECT DISTINCT meta_value as attachment_id
            FROM {self.db.table_prefix}postmeta
            WHERE meta_key = '_thumbnail_id'
            AND meta_value != ''
            AND meta_value IS NOT NULL
        """
        results = self.db.execute_query(query_featured)
        for row in results:
            try:
                used_ids.add(int(row['attachment_id']))
            except (ValueError, TypeError):
                pass
        
        # 3. Get WooCommerce product images
        logger.info("Checking WooCommerce product images...")
        
        # Product featured images
        query_product_thumb = f"""
            SELECT DISTINCT pm.meta_value as attachment_id
            FROM {self.db.table_prefix}postmeta pm
            INNER JOIN {self.db.table_prefix}posts p ON pm.post_id = p.ID
            WHERE p.post_type = 'product'
            AND p.post_status IN ('publish', 'draft', 'private')
            AND pm.meta_key = '_thumbnail_id'
            AND pm.meta_value != ''
        """
        results = self.db.execute_query(query_product_thumb)
        for row in results:
            try:
                used_ids.add(int(row['attachment_id']))
            except (ValueError, TypeError):
                pass
        
        # Product gallery images
        query_gallery = f"""
            SELECT DISTINCT pm.meta_value as gallery_ids
            FROM {self.db.table_prefix}postmeta pm
            INNER JOIN {self.db.table_prefix}posts p ON pm.post_id = p.ID
            WHERE p.post_type = 'product'
            AND p.post_status IN ('publish', 'draft', 'private')
            AND pm.meta_key = '_product_image_gallery'
            AND pm.meta_value != ''
        """
        results = self.db.execute_query(query_gallery)
        for row in results:
            if row['gallery_ids']:
                gallery_ids = str(row['gallery_ids']).split(',')
                for gid in gallery_ids:
                    try:
                        used_ids.add(int(gid.strip()))
                    except (ValueError, TypeError):
                        pass
        
        # 4. Check media in post content (galleries, embedded images)
        logger.info("Checking media in post content...")
        query_content = f"""
            SELECT ID, post_content 
            FROM {self.db.table_prefix}posts 
            WHERE post_status IN ('publish', 'draft', 'private')
            AND post_type IN ('post', 'page', 'product')
            AND post_content LIKE '%wp-content/uploads%'
        """
        results = self.db.execute_query(query_content)
        
        for row in results:
            content = row['post_content']
            # Extract attachment IDs from various shortcodes and blocks
            
            # Gallery shortcode [gallery ids="1,2,3"]
            import re
            gallery_pattern = r'\[gallery[^\]]*ids=["\']([^"\']+)["\'][^\]]*\]'
            galleries = re.findall(gallery_pattern, content)
            for gallery in galleries:
                for aid in gallery.split(','):
                    try:
                        used_ids.add(int(aid.strip()))
                    except (ValueError, TypeError):
                        pass
            
            # wp:image blocks
            image_block_pattern = r'wp:image[^}]*"id":(\d+)'
            image_blocks = re.findall(image_block_pattern, content)
            for aid in image_blocks:
                try:
                    used_ids.add(int(aid))
                except (ValueError, TypeError):
                    pass
            
            # wp-image-XXX classes
            wp_image_pattern = r'wp-image-(\d+)'
            wp_images = re.findall(wp_image_pattern, content)
            for aid in wp_images:
                try:
                    used_ids.add(int(aid))
                except (ValueError, TypeError):
                    pass
        
        # 5. Check for media in widgets and theme customizer
        logger.info("Checking media in widgets and customizer...")
        query_options = f"""
            SELECT option_value 
            FROM {self.db.table_prefix}options 
            WHERE option_name LIKE 'widget_%'
            OR option_name LIKE 'theme_mods_%'
            OR option_name = 'sidebars_widgets'
        """
        results = self.db.execute_query(query_options)
        
        for row in results:
            try:
                value = row['option_value']
                # Look for attachment IDs in serialized data
                attachment_pattern = r'"attachment_id";i:(\d+)'
                attachments = re.findall(attachment_pattern, str(value))
                for aid in attachments:
                    used_ids.add(int(aid))
            except:
                pass
        
        # 6. Check ACF (Advanced Custom Fields) if present
        logger.info("Checking ACF fields...")
        query_acf = f"""
            SELECT meta_value 
            FROM {self.db.table_prefix}postmeta 
            WHERE meta_key LIKE '%image%' 
            OR meta_key LIKE '%gallery%'
            OR meta_key LIKE '%media%'
            OR meta_key LIKE '%file%'
        """
        results = self.db.execute_query(query_acf)
        
        for row in results:
            try:
                value = str(row['meta_value'])
                if value.isdigit():
                    used_ids.add(int(value))
            except:
                pass
        
        self.stats['used_media'] = len(used_ids)
        logger.info(f"Found {len(used_ids)} media files in use")
        
        return used_ids
    
    def get_unused_media(self, all_media: List[Dict], used_ids: Set[int]) -> List[Dict]:
        """Identify unused media files"""
        unused = []
        
        for media in all_media:
            if media['ID'] not in used_ids:
                unused.append(media)
        
        self.stats['unused_media'] = len(unused)
        logger.info(f"Identified {len(unused)} unused media files")
        
        return unused
    
    def backup_media(self, media: Dict) -> bool:
        """Backup media file before deletion"""
        if not media.get('file_path'):
            return False
        
        try:
            # Create backup directory
            os.makedirs(self.backup_dir, exist_ok=True)
            
            # Create subdirectory with current date
            date_dir = os.path.join(self.backup_dir, datetime.now().strftime('%Y%m%d'))
            os.makedirs(date_dir, exist_ok=True)
            
            # Source and destination paths
            source = os.path.join(self.uploads_path, media['file_path'])
            filename = os.path.basename(media['file_path'])
            dest = os.path.join(date_dir, f"{media['ID']}_{filename}")
            
            if os.path.exists(source):
                shutil.copy2(source, dest)
                
                # Also backup metadata
                meta_file = os.path.join(date_dir, f"{media['ID']}_metadata.json")
                with open(meta_file, 'w') as f:
                    json.dump({
                        'id': media['ID'],
                        'title': media['post_title'],
                        'original_path': media['file_path'],
                        'mime_type': media['post_mime_type'],
                        'guid': media['guid'],
                        'backup_date': datetime.now().isoformat()
                    }, f, indent=2)
                
                self.stats['backed_up'] += 1
                return True
            
        except Exception as e:
            logger.error(f"Error backing up media {media['ID']}: {e}")
            self.stats['errors'] += 1
        
        return False
    
    def delete_media_files(self, media: Dict) -> bool:
        """Delete media files from filesystem"""
        if not media.get('file_path'):
            return False
        
        try:
            main_file = os.path.join(self.uploads_path, media['file_path'])
            
            # Get file size for statistics
            if os.path.exists(main_file):
                self.stats['space_freed'] += os.path.getsize(main_file)
            
            # Delete main file
            if os.path.exists(main_file) and not self.dry_run:
                os.remove(main_file)
                logger.debug(f"Deleted file: {main_file}")
            
            # Delete thumbnails and intermediate sizes
            if media.get('file_meta'):
                try:
                    import phpserialize
                    meta_data = phpserialize.loads(media['file_meta'].encode())
                    
                    if b'sizes' in meta_data:
                        base_dir = os.path.dirname(main_file)
                        for size_name, size_data in meta_data[b'sizes'].items():
                            if b'file' in size_data:
                                thumb_file = os.path.join(base_dir, size_data[b'file'].decode())
                                if os.path.exists(thumb_file) and not self.dry_run:
                                    self.stats['space_freed'] += os.path.getsize(thumb_file)
                                    os.remove(thumb_file)
                                    logger.debug(f"Deleted thumbnail: {thumb_file}")
                except:
                    # If phpserialize is not available or parsing fails, try basic pattern matching
                    base_dir = os.path.dirname(main_file)
                    base_name = os.path.splitext(os.path.basename(main_file))[0]
                    
                    # Common WordPress thumbnail patterns
                    patterns = [
                        f"{base_name}-*x*.jpg",
                        f"{base_name}-*x*.jpeg",
                        f"{base_name}-*x*.png",
                        f"{base_name}-*x*.gif",
                        f"{base_name}-*x*.webp"
                    ]
                    
                    import glob
                    for pattern in patterns:
                        for thumb in glob.glob(os.path.join(base_dir, pattern)):
                            if os.path.exists(thumb) and not self.dry_run:
                                self.stats['space_freed'] += os.path.getsize(thumb)
                                os.remove(thumb)
                                logger.debug(f"Deleted thumbnail: {thumb}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting media files for {media['ID']}: {e}")
            self.stats['errors'] += 1
            return False
    
    def delete_media_record(self, media_id: int) -> bool:
        """Delete media record from database"""
        if self.dry_run:
            return True
        
        try:
            # Delete postmeta
            query_meta = f"DELETE FROM {self.db.table_prefix}postmeta WHERE post_id = %s"
            self.db.execute_update(query_meta, (media_id,))
            
            # Delete post
            query_post = f"DELETE FROM {self.db.table_prefix}posts WHERE ID = %s"
            affected = self.db.execute_update(query_post, (media_id,))
            
            if affected > 0:
                self.stats['deleted_media'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error deleting database record for media {media_id}: {e}")
            self.stats['errors'] += 1
        
        return False
    
    def cleanup_unused_media(self, min_age_days: int = 30, batch_size: int = 50):
        """Main cleanup process"""
        logger.info("="*60)
        logger.info("WordPress Media Cleanup Started")
        logger.info(f"Dry Run: {self.dry_run}")
        logger.info(f"Min Age: {min_age_days} days")
        logger.info("="*60)
        
        # Get all media
        all_media = self.get_all_media_attachments()
        if not all_media:
            logger.error("No media found or database connection failed")
            return
        
        # Get used media IDs
        used_ids = self.get_used_media_ids()
        
        # Get unused media
        unused_media = self.get_unused_media(all_media, used_ids)
        
        if not unused_media:
            logger.info("No unused media found. Everything is in use!")
            self.db.disconnect()
            return
        
        # Filter by age if specified
        if min_age_days > 0:
            cutoff_date = datetime.now() - timedelta(days=min_age_days)
            filtered_unused = []
            
            for media in unused_media:
                # Check media age
                query_date = f"""
                    SELECT post_date 
                    FROM {self.db.table_prefix}posts 
                    WHERE ID = %s
                """
                result = self.db.execute_query(query_date, (media['ID'],))
                
                if result and result[0]['post_date'] < cutoff_date:
                    filtered_unused.append(media)
            
            logger.info(f"Filtered to {len(filtered_unused)} media files older than {min_age_days} days")
            unused_media = filtered_unused
        
        # Process in batches
        total_batches = (len(unused_media) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(unused_media))
            batch = unused_media[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch)} items)")
            
            for media in batch:
                logger.info(f"Processing media ID {media['ID']}: {media['post_title']}")
                
                # Backup if enabled
                if self.config.get('backup_before_delete', True):
                    if not self.backup_media(media):
                        logger.warning(f"Failed to backup media {media['ID']}, skipping deletion")
                        continue
                
                # Delete files
                self.delete_media_files(media)
                
                # Delete database record
                self.delete_media_record(media['ID'])
                
                # Rate limiting
                time.sleep(0.1)
        
        # Disconnect database
        self.db.disconnect()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print cleanup summary"""
        logger.info("="*60)
        logger.info("CLEANUP SUMMARY")
        logger.info("="*60)
        logger.info(f"Total media files: {self.stats['total_media']}")
        logger.info(f"Media in use: {self.stats['used_media']}")
        logger.info(f"Unused media: {self.stats['unused_media']}")
        logger.info(f"Media backed up: {self.stats['backed_up']}")
        logger.info(f"Media deleted: {self.stats['deleted_media']}")
        logger.info(f"Errors: {self.stats['errors']}")
        
        # Convert bytes to human readable
        space_mb = self.stats['space_freed'] / (1024 * 1024)
        if space_mb > 1024:
            space_str = f"{space_mb/1024:.2f} GB"
        else:
            space_str = f"{space_mb:.2f} MB"
        
        logger.info(f"Space freed: {space_str}")
        
        if self.dry_run:
            logger.info("\n*** DRY RUN MODE - No actual changes were made ***")
        
        logger.info("="*60)


def load_config(config_file: str = 'wp_cleanup_config.json') -> Dict:
    """Load configuration from file or environment variables"""
    config = {
        'database': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', ''),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', ''),
            'table_prefix': os.getenv('DB_PREFIX', 'wp_')
        },
        'woocommerce': {
            'url': os.getenv('WC_URL', 'https://4more.ca'),
            'consumer_key': os.getenv('WC_CONSUMER_KEY', ''),
            'consumer_secret': os.getenv('WC_CONSUMER_SECRET', '')
        },
        'site_url': os.getenv('SITE_URL', 'https://4more.ca'),
        'uploads_path': os.getenv('UPLOADS_PATH', './wp-content/uploads'),
        'backup_dir': os.getenv('BACKUP_DIR', './media_backup'),
        'backup_before_delete': os.getenv('BACKUP_BEFORE_DELETE', 'true').lower() == 'true',
        'dry_run': os.getenv('DRY_RUN', 'true').lower() == 'true'
    }
    
    # Try to load from config file
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                # Merge file config with env config (env takes precedence)
                for key in file_config:
                    if isinstance(config.get(key), dict):
                        config[key].update(file_config[key])
                    elif key not in config or not config[key]:
                        config[key] = file_config[key]
        except Exception as e:
            logger.warning(f"Could not load config file: {e}")
    
    return config


def create_sample_config(filename: str = 'wp_cleanup_config.json'):
    """Create a sample configuration file"""
    sample_config = {
        "database": {
            "host": "localhost",
            "user": "your_db_user",
            "password": "your_db_password",
            "database": "wordpress_db",
            "table_prefix": "wp_"
        },
        "woocommerce": {
            "url": "https://4more.ca",
            "consumer_key": "ck_your_consumer_key",
            "consumer_secret": "cs_your_consumer_secret"
        },
        "site_url": "https://4more.ca",
        "uploads_path": "/path/to/wordpress/wp-content/uploads",
        "backup_dir": "./media_backup",
        "backup_before_delete": True,
        "dry_run": True
    }
    
    with open(filename, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"Sample configuration file created: {filename}")
    print("Please edit this file with your actual WordPress database credentials and paths.")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='WordPress Media Cleanup Tool - Remove unused media files safely'
    )
    parser.add_argument('--config', default='wp_cleanup_config.json', 
                       help='Configuration file path')
    parser.add_argument('--create-config', action='store_true',
                       help='Create a sample configuration file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Simulate cleanup without actual deletion')
    parser.add_argument('--no-dry-run', dest='dry_run', action='store_false',
                       help='Perform actual cleanup (DELETE FILES)')
    parser.add_argument('--min-age', type=int, default=30,
                       help='Minimum age in days for media to be deleted (default: 30)')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Number of media files to process in each batch (default: 50)')
    parser.add_argument('--no-backup', action='store_true',
                       help='Skip backup before deletion (not recommended)')
    parser.add_argument('--uploads-path', help='Path to WordPress uploads directory')
    parser.add_argument('--backup-dir', help='Directory for backing up deleted media')
    
    # Set default based on safety
    parser.set_defaults(dry_run=True)
    
    args = parser.parse_args()
    
    # Create sample config if requested
    if args.create_config:
        create_sample_config(args.config)
        sys.exit(0)
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.dry_run is not None:
        config['dry_run'] = args.dry_run
    
    if args.no_backup:
        config['backup_before_delete'] = False
    
    if args.uploads_path:
        config['uploads_path'] = args.uploads_path
    
    if args.backup_dir:
        config['backup_dir'] = args.backup_dir
    
    # Validate configuration
    if not config['database']['user'] or not config['database']['database']:
        logger.error("Database configuration is incomplete!")
        logger.error("Please create a config file with --create-config or set environment variables:")
        logger.error("  DB_HOST, DB_USER, DB_PASSWORD, DB_NAME")
        sys.exit(1)
    
    # Safety confirmation for non-dry-run mode
    if not config['dry_run']:
        print("\n" + "="*60)
        print("WARNING: You are about to DELETE media files!")
        print("="*60)
        print(f"Database: {config['database']['database']}")
        print(f"Uploads Path: {config['uploads_path']}")
        print(f"Backup: {'Enabled' if config['backup_before_delete'] else 'DISABLED'}")
        print(f"Min Age: {args.min_age} days")
        print("\nThis action cannot be undone!")
        
        confirmation = input("\nType 'DELETE' to confirm: ")
        if confirmation != 'DELETE':
            print("Cleanup cancelled.")
            sys.exit(0)
    
    # Initialize and run cleanup
    try:
        cleaner = MediaCleanup(config)
        cleaner.cleanup_unused_media(
            min_age_days=args.min_age,
            batch_size=args.batch_size
        )
    except KeyboardInterrupt:
        logger.info("\nCleanup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()