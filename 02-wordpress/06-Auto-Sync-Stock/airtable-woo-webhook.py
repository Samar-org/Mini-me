from flask import Flask, request, jsonify
import requests
import json
import hmac
import hashlib
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from woocommerce import API
import time
from threading import Thread, Lock
import queue
from typing import Dict, Any, Optional, List
import traceback

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_log.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'Products')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')

# WooCommerce Configuration
wcapi = API(
    url="https://4more.ca",
    consumer_key=os.getenv('WOO_CONSUMER_KEY'),
    consumer_secret=os.getenv('WOO_CONSUMER_SECRET'),
    version="wc/v3",
    timeout=30
)

# Queue for processing updates
airtable_queue = queue.Queue()
woo_queue = queue.Queue()

# Lock to prevent sync loops
sync_lock = Lock()
recently_synced = {}  # Track recently synced items to prevent loops

class SyncTracker:
    """Track recent syncs to prevent infinite loops"""
    def __init__(self, ttl_seconds=30):
        self.syncs = {}
        self.ttl = ttl_seconds
        self.lock = Lock()
    
    def add_sync(self, key: str, source: str):
        """Add a sync record"""
        with self.lock:
            self.syncs[key] = {
                'source': source,
                'timestamp': datetime.now()
            }
            self._cleanup()
    
    def should_sync(self, key: str, source: str) -> bool:
        """Check if we should sync (not recently synced from opposite source)"""
        with self.lock:
            self._cleanup()
            if key in self.syncs:
                last_sync = self.syncs[key]
                # Don't sync if this was recently synced from the opposite source
                if last_sync['source'] != source:
                    time_diff = (datetime.now() - last_sync['timestamp']).seconds
                    if time_diff < self.ttl:
                        return False
            return True
    
    def _cleanup(self):
        """Remove old sync records"""
        now = datetime.now()
        expired_keys = [
            key for key, value in self.syncs.items()
            if (now - value['timestamp']).seconds > self.ttl
        ]
        for key in expired_keys:
            del self.syncs[key]

sync_tracker = SyncTracker(ttl_seconds=30)

class AirtableWooSync:
    def __init__(self):
        self.airtable_headers = {
            'Authorization': f'Bearer {AIRTABLE_API_KEY}',
            'Content-Type': 'application/json'
        }
    
    # ============= AIRTABLE OPERATIONS =============
    
    def get_airtable_record(self, record_id: str) -> Optional[Dict]:
        """Fetch a single record from Airtable"""
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
        
        try:
            response = requests.get(url, headers=self.airtable_headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching Airtable record {record_id}: {str(e)}")
            return None
    
    def get_airtable_record_by_sku(self, sku: str) -> Optional[Dict]:
        """Find Airtable record by SKU"""
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
        params = {
            'filterByFormula': f"{{SKU}} = '{sku}'",
            'maxRecords': 1
        }
        
        try:
            response = requests.get(url, headers=self.airtable_headers, params=params)
            response.raise_for_status()
            data = response.json()
            records = data.get('records', [])
            return records[0] if records else None
        except Exception as e:
            logger.error(f"Error finding Airtable record by SKU {sku}: {str(e)}")
            return None
    
    def update_airtable_record(self, record_id: str, fields: Dict) -> bool:
        """Update an Airtable record"""
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}/{record_id}"
        
        # Add last sync timestamp
        fields['Last WooCommerce Sync'] = datetime.now().isoformat()
        
        data = {
            'fields': fields
        }
        
        try:
            response = requests.patch(url, headers=self.airtable_headers, json=data)
            response.raise_for_status()
            logger.info(f"Updated Airtable record {record_id}: {fields}")
            return True
        except Exception as e:
            logger.error(f"Error updating Airtable record {record_id}: {str(e)}")
            return False
    
    def create_airtable_record(self, fields: Dict) -> Optional[str]:
        """Create a new Airtable record"""
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
        
        # Add creation timestamp
        fields['Created From WooCommerce'] = datetime.now().isoformat()
        fields['Last WooCommerce Sync'] = datetime.now().isoformat()
        
        data = {
            'fields': fields
        }
        
        try:
            response = requests.post(url, headers=self.airtable_headers, json=data)
            response.raise_for_status()
            record = response.json()
            logger.info(f"Created Airtable record: {record.get('id')}")
            return record.get('id')
        except Exception as e:
            logger.error(f"Error creating Airtable record: {str(e)}")
            return None
    
    # ============= WOOCOMMERCE OPERATIONS =============
    
    def find_woo_product_by_sku(self, sku: str) -> Optional[Dict]:
        """Find WooCommerce product by SKU"""
        try:
            response = wcapi.get(f"products?sku={sku}")
            if response.status_code == 200:
                products = response.json()
                return products[0] if products else None
            return None
        except Exception as e:
            logger.error(f"Error finding product by SKU {sku}: {str(e)}")
            return None
    
    def find_woo_product_by_airtable_id(self, airtable_id: str) -> Optional[Dict]:
        """Find WooCommerce product by Airtable ID stored in meta_data"""
        try:
            # First try to search by meta_data (requires custom endpoint or workaround)
            page = 1
            per_page = 100
            
            while True:
                response = wcapi.get(f"products?per_page={per_page}&page={page}")
                if response.status_code != 200:
                    break
                    
                products = response.json()
                if not products:
                    break
                
                for product in products:
                    meta_data = product.get('meta_data', [])
                    for meta in meta_data:
                        if meta.get('key') == 'airtable_id' and meta.get('value') == airtable_id:
                            return product
                
                if len(products) < per_page:
                    break
                page += 1
                
            return None
        except Exception as e:
            logger.error(f"Error finding product by Airtable ID {airtable_id}: {str(e)}")
            return None
    
    # ============= MAPPING FUNCTIONS =============
    
    def map_airtable_to_woo(self, airtable_record: Dict) -> Dict:
        """Map Airtable fields to WooCommerce product structure"""
        fields = airtable_record.get('fields', {})
        
        woo_product = {
            'name': fields.get('Product Name', ''),
            'type': 'simple',
            'regular_price': str(fields.get('Price', 0)),
            'sale_price': str(fields.get('Sale Price', '')) if fields.get('Sale Price') else '',
            'description': fields.get('Description', ''),
            'short_description': fields.get('Short Description', ''),
            'sku': fields.get('SKU', ''),
            'manage_stock': True,
            'stock_quantity': fields.get('Stock Quantity', 0),
            'stock_status': 'instock' if fields.get('Stock Quantity', 0) > 0 else 'outofstock',
            'weight': str(fields.get('Weight', '')) if fields.get('Weight') else '',
            'categories': [],
            'images': [],
            'meta_data': [
                {
                    'key': 'airtable_id',
                    'value': airtable_record.get('id')
                },
                {
                    'key': 'last_airtable_sync',
                    'value': datetime.now().isoformat()
                }
            ]
        }
        
        # Handle categories
        if fields.get('Category'):
            categories = fields.get('Category')
            if isinstance(categories, list):
                woo_product['categories'] = [{'name': cat} for cat in categories]
            else:
                woo_product['categories'] = [{'name': categories}]
        
        # Handle images
        if fields.get('Images'):
            images = fields.get('Images', [])
            woo_product['images'] = [{'src': img.get('url')} for img in images if img.get('url')]
        
        # Handle channel-specific fields
        if fields.get('Channel'):
            woo_product['meta_data'].append({
                'key': 'channel',
                'value': fields.get('Channel')
            })
        
        if fields.get('Starting Bid'):
            woo_product['meta_data'].append({
                'key': 'starting_bid',
                'value': str(fields.get('Starting Bid'))
            })
        
        return woo_product
    
    def map_woo_to_airtable(self, woo_product: Dict) -> Dict:
        """Map WooCommerce product to Airtable fields"""
        fields = {
            'Product Name': woo_product.get('name', ''),
            'SKU': woo_product.get('sku', ''),
            'Price': float(woo_product.get('regular_price', 0)) if woo_product.get('regular_price') else 0,
            'Stock Quantity': woo_product.get('stock_quantity', 0),
            'Description': woo_product.get('description', ''),
            'Short Description': woo_product.get('short_description', ''),
            'WooCommerce ID': str(woo_product.get('id', '')),
            'Stock Status': woo_product.get('stock_status', 'outofstock'),
            'Last WooCommerce Sync': datetime.now().isoformat()
        }
        
        # Handle sale price
        if woo_product.get('sale_price'):
            fields['Sale Price'] = float(woo_product.get('sale_price'))
        
        # Handle weight
        if woo_product.get('weight'):
            fields['Weight'] = float(woo_product.get('weight'))
        
        # Handle categories
        categories = woo_product.get('categories', [])
        if categories:
            fields['Category'] = [cat.get('name') for cat in categories if cat.get('name')]
        
        # Extract channel from meta_data
        meta_data = woo_product.get('meta_data', [])
        for meta in meta_data:
            if meta.get('key') == 'channel':
                fields['Channel'] = meta.get('value')
            elif meta.get('key') == 'starting_bid':
                fields['Starting Bid'] = float(meta.get('value', 0))
        
        return fields
    
    # ============= SYNC OPERATIONS =============
    
    def sync_from_airtable_to_woo(self, airtable_record: Dict) -> bool:
        """Sync a single record from Airtable to WooCommerce"""
        try:
            airtable_id = airtable_record.get('id')
            fields = airtable_record.get('fields', {})
            sku = fields.get('SKU', '')
            
            # Check if we should sync (prevent loops)
            sync_key = f"at_{airtable_id}"
            if not sync_tracker.should_sync(sync_key, 'airtable'):
                logger.info(f"Skipping sync for {sku} - recently synced from WooCommerce")
                return True
            
            # Map Airtable to WooCommerce format
            woo_data = self.map_airtable_to_woo(airtable_record)
            
            # Find existing product
            existing_product = None
            if sku:
                existing_product = self.find_woo_product_by_sku(sku)
            
            if not existing_product:
                existing_product = self.find_woo_product_by_airtable_id(airtable_id)
            
            if existing_product:
                # Update existing product
                product_id = existing_product['id']
                response = wcapi.put(f"products/{product_id}", woo_data)
                action = "updated"
            else:
                # Create new product
                response = wcapi.post("products", woo_data)
                action = "created"
            
            if response.status_code in [200, 201]:
                # Mark as synced
                sync_tracker.add_sync(sync_key, 'airtable')
                logger.info(f"Product {action} in WooCommerce: {woo_data.get('name')} (SKU: {sku})")
                return True
            else:
                logger.error(f"Failed to {action} product in WooCommerce: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing from Airtable to WooCommerce: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def sync_from_woo_to_airtable(self, woo_product: Dict) -> bool:
        """Sync a single product from WooCommerce to Airtable"""
        try:
            woo_id = woo_product.get('id')
            sku = woo_product.get('sku', '')
            
            # Check if we should sync (prevent loops)
            sync_key = f"woo_{woo_id}"
            if not sync_tracker.should_sync(sync_key, 'woocommerce'):
                logger.info(f"Skipping sync for {sku} - recently synced from Airtable")
                return True
            
            # Map WooCommerce to Airtable format
            airtable_fields = self.map_woo_to_airtable(woo_product)
            
            # Check if product has an Airtable ID
            airtable_id = None
            meta_data = woo_product.get('meta_data', [])
            for meta in meta_data:
                if meta.get('key') == 'airtable_id':
                    airtable_id = meta.get('value')
                    break
            
            # If no Airtable ID, try to find by SKU
            if not airtable_id and sku:
                airtable_record = self.get_airtable_record_by_sku(sku)
                if airtable_record:
                    airtable_id = airtable_record.get('id')
            
            if airtable_id:
                # Update existing Airtable record
                success = self.update_airtable_record(airtable_id, airtable_fields)
                action = "updated"
            else:
                # Create new Airtable record
                new_id = self.create_airtable_record(airtable_fields)
                success = new_id is not None
                action = "created"
                
                # Update WooCommerce with the new Airtable ID
                if success and new_id:
                    woo_update = {
                        'meta_data': [
                            {'key': 'airtable_id', 'value': new_id}
                        ]
                    }
                    wcapi.put(f"products/{woo_id}", woo_update)
            
            if success:
                # Mark as synced
                sync_tracker.add_sync(sync_key, 'woocommerce')
                logger.info(f"Product {action} in Airtable: {woo_product.get('name')} (SKU: {sku})")
                return True
            else:
                logger.error(f"Failed to {action} product in Airtable")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing from WooCommerce to Airtable: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def delete_from_woo(self, airtable_id: str) -> bool:
        """Delete a product from WooCommerce"""
        try:
            product = self.find_woo_product_by_airtable_id(airtable_id)
            if product:
                response = wcapi.delete(f"products/{product['id']}", params={"force": True})
                if response.status_code == 200:
                    logger.info(f"Product deleted from WooCommerce: {product.get('name')}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error deleting product from WooCommerce: {str(e)}")
            return False

# Initialize sync handler
sync_handler = AirtableWooSync()

# ============= QUEUE PROCESSORS =============

def process_airtable_queue():
    """Process updates from Airtable"""
    while True:
        try:
            if not airtable_queue.empty():
                update = airtable_queue.get()
                action = update.get('action')
                record_id = update.get('record_id')
                
                logger.info(f"Processing Airtable {action} for record {record_id}")
                
                if action in ['create', 'update']:
                    record = sync_handler.get_airtable_record(record_id)
                    if record:
                        sync_handler.sync_from_airtable_to_woo(record)
                elif action == 'delete':
                    sync_handler.delete_from_woo(record_id)
                
                airtable_queue.task_done()
            else:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error processing Airtable queue: {str(e)}")
            logger.error(traceback.format_exc())

def process_woo_queue():
    """Process updates from WooCommerce"""
    while True:
        try:
            if not woo_queue.empty():
                update = woo_queue.get()
                product = update.get('product')
                
                logger.info(f"Processing WooCommerce update for product {product.get('id')}")
                sync_handler.sync_from_woo_to_airtable(product)
                
                woo_queue.task_done()
            else:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error processing WooCommerce queue: {str(e)}")
            logger.error(traceback.format_exc())

# ============= WEBHOOK ENDPOINTS =============

def verify_webhook_signature(data, signature):
    """Verify webhook signature for security"""
    if not WEBHOOK_SECRET:
        return True
    
    expected_signature = hmac.new(
        WEBHOOK_SECRET.encode(),
        data,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(signature, expected_signature)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'queues': {
            'airtable': airtable_queue.qsize(),
            'woocommerce': woo_queue.qsize()
        }
    })

@app.route('/webhook/airtable', methods=['POST'])
def handle_airtable_webhook():
    """Webhook endpoint for Airtable changes"""
    try:
        # Verify webhook signature
        signature = request.headers.get('X-Airtable-Signature', '')
        if WEBHOOK_SECRET and not verify_webhook_signature(request.data, signature):
            logger.warning("Invalid webhook signature from Airtable")
            return jsonify({'error': 'Invalid signature'}), 401
        
        data = request.json
        logger.info(f"Airtable webhook received: {json.dumps(data, indent=2)}")
        
        # Extract webhook type and record information
        webhook_type = data.get('type', '')
        base_id = data.get('base', {}).get('id')
        
        # Verify this is for our base
        if base_id and base_id != AIRTABLE_BASE_ID:
            return jsonify({'error': 'Invalid base ID'}), 400
        
        # Process single record
        if 'record' in data:
            record_id = data['record'].get('id')
            action = None
            
            if 'created' in webhook_type.lower():
                action = 'create'
            elif 'updated' in webhook_type.lower() or 'changed' in webhook_type.lower():
                action = 'update'
            elif 'deleted' in webhook_type.lower():
                action = 'delete'
            
            if action and record_id:
                airtable_queue.put({
                    'action': action,
                    'record_id': record_id,
                    'timestamp': datetime.now().isoformat()
                })
                
                return jsonify({
                    'status': 'queued',
                    'action': action,
                    'record_id': record_id
                }), 200
        
        # Process batch updates
        if 'records' in data:
            for record in data['records']:
                record_id = record.get('id')
                if record_id:
                    airtable_queue.put({
                        'action': 'update',
                        'record_id': record_id,
                        'timestamp': datetime.now().isoformat()
                    })
            
            return jsonify({
                'status': 'batch queued',
                'count': len(data['records'])
            }), 200
        
        return jsonify({'status': 'no action taken'}), 200
        
    except Exception as e:
        logger.error(f"Airtable webhook processing error: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/webhook/woocommerce', methods=['POST'])
def handle_woo_webhook():
    """Webhook endpoint for WooCommerce changes"""
    try:
        # Verify webhook signature (WooCommerce uses different signature method)
        signature = request.headers.get('X-WC-Webhook-Signature', '')
        # Add WooCommerce signature verification if needed
        
        data = request.json
        logger.info(f"WooCommerce webhook received: Product {data.get('id')} - {data.get('name')}")
        
        # Add to queue for processing
        woo_queue.put({
            'product': data,
            'timestamp': datetime.now().isoformat()
        })
        
        return jsonify({
            'status': 'queued',
            'product_id': data.get('id')
        }), 200
        
    except Exception as e:
        logger.error(f"WooCommerce webhook processing error: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/sync/manual', methods=['POST'])
def manual_sync():
    """Trigger a manual sync for specific records"""
    try:
        data = request.json
        source = data.get('source', 'airtable')  # 'airtable' or 'woocommerce'
        record_ids = data.get('record_ids', [])
        
        if source == 'airtable':
            for record_id in record_ids:
                airtable_queue.put({
                    'action': 'update',
                    'record_id': record_id,
                    'timestamp': datetime.now().isoformat()
                })
        elif source == 'woocommerce':
            for product_id in record_ids:
                # Fetch product from WooCommerce
                response = wcapi.get(f"products/{product_id}")
                if response.status_code == 200:
                    woo_queue.put({
                        'product': response.json(),
                        'timestamp': datetime.now().isoformat()
                    })
        
        return jsonify({
            'status': 'queued',
            'source': source,
            'count': len(record_ids)
        }), 200
        
    except Exception as e:
        logger.error(f"Manual sync error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/sync/full', methods=['POST'])
def full_sync():
    """Trigger a full sync in specified direction"""
    try:
        data = request.json
        direction = data.get('direction', 'airtable_to_woo')  # or 'woo_to_airtable' or 'bidirectional'
        
        if direction in ['airtable_to_woo', 'bidirectional']:
            # Fetch all records from Airtable
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            headers = {
                'Authorization': f'Bearer {AIRTABLE_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            all_records = []
            offset = None
            
            while True:
                params = {'pageSize': 100}
                if offset:
                    params['offset'] = offset
                
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                all_records.extend(data.get('records', []))
                
                offset = data.get('offset')
                if not offset:
                    break
            
            # Queue all Airtable records
            for record in all_records:
                airtable_queue.put({
                    'action': 'update',
                    'record_id': record['id'],
                    'timestamp': datetime.now().isoformat()
                })
        
        if direction in ['woo_to_airtable', 'bidirectional']:
            # Fetch all products from WooCommerce
            page = 1
            per_page = 100
            all_products = []
            
            while True:
                response = wcapi.get(f"products?per_page={per_page}&page={page}")
                if response.status_code != 200:
                    break
                
                products = response.json()
                if not products:
                    break
                
                all_products.extend(products)
                
                if len(products) < per_page:
                    break
                page += 1
            
            # Queue all WooCommerce products
            for product in all_products:
                woo_queue.put({
                    'product': product,
                    'timestamp': datetime.now().isoformat()
                })
        
        return jsonify({
            'status': 'full sync queued',
            'direction': direction,
            'airtable_count': airtable_queue.qsize(),
            'woocommerce_count': woo_queue.qsize()
        }), 200
        
    except Exception as e:
        logger.error(f"Full sync error: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Get current sync status"""
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now().isoformat(),
        'queues': {
            'airtable_pending': airtable_queue.qsize(),
            'woocommerce_pending': woo_queue.qsize()
        },
        'recent_syncs': len(sync_tracker.syncs)
    })

if __name__ == '__main__':
    # Start background worker threads
    airtable_worker = Thread(target=process_airtable_queue, daemon=True)
    airtable_worker.start()
    logger.info("Airtable background worker started")
    
    woo_worker = Thread(target=process_woo_queue, daemon=True)
    woo_worker.start()
    logger.info("WooCommerce background worker started")
    
    # Start Flask app
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)