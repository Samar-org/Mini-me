"""
WooCommerce Webhook Setup Script
This script creates webhooks in WooCommerce to notify your server of product changes
"""

import requests
from woocommerce import API
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# WooCommerce API configuration
wcapi = API(
    url="https://4more.ca",
    consumer_key=os.getenv('WOO_CONSUMER_KEY'),
    consumer_secret=os.getenv('WOO_CONSUMER_SECRET'),
    version="wc/v3"
)

# Your webhook server URL
WEBHOOK_SERVER_URL = "https://webhook.4more.ca"  # Update this to your deployed webhook server

def list_existing_webhooks():
    """List all existing webhooks"""
    print("\n=== Existing Webhooks ===")
    response = wcapi.get("webhooks")
    
    if response.status_code == 200:
        webhooks = response.json()
        if webhooks:
            for webhook in webhooks:
                print(f"ID: {webhook['id']}")
                print(f"Name: {webhook['name']}")
                print(f"Topic: {webhook['topic']}")
                print(f"Delivery URL: {webhook['delivery_url']}")
                print(f"Status: {webhook['status']}")
                print("-" * 50)
        else:
            print("No webhooks found")
        return webhooks
    else:
        print(f"Error fetching webhooks: {response.text}")
        return []

def create_webhook(name, topic, delivery_url, secret=""):
    """Create a new webhook"""
    webhook_data = {
        "name": name,
        "topic": topic,
        "delivery_url": delivery_url,
        "status": "active",
        "secret": secret
    }
    
    response = wcapi.post("webhooks", webhook_data)
    
    if response.status_code in [200, 201]:
        webhook = response.json()
        print(f"✅ Created webhook: {webhook['name']} (ID: {webhook['id']})")
        return webhook
    else:
        print(f"❌ Error creating webhook: {response.text}")
        return None

def delete_webhook(webhook_id):
    """Delete a webhook"""
    response = wcapi.delete(f"webhooks/{webhook_id}", params={"force": True})
    
    if response.status_code == 200:
        print(f"✅ Deleted webhook ID: {webhook_id}")
        return True
    else:
        print(f"❌ Error deleting webhook: {response.text}")
        return False

def setup_product_webhooks():
    """Set up all necessary product webhooks"""
    
    # Webhook configurations
    webhooks_to_create = [
        {
            "name": "Product Created - Sync to Airtable",
            "topic": "product.created",
            "delivery_url": f"{WEBHOOK_SERVER_URL}/webhook/woocommerce"
        },
        {
            "name": "Product Updated - Sync to Airtable",
            "topic": "product.updated",
            "delivery_url": f"{WEBHOOK_SERVER_URL}/webhook/woocommerce"
        },
        {
            "name": "Product Deleted - Sync to Airtable",
            "topic": "product.deleted",
            "delivery_url": f"{WEBHOOK_SERVER_URL}/webhook/woocommerce"
        },
        {
            "name": "Product Restored - Sync to Airtable",
            "topic": "product.restored",
            "delivery_url": f"{WEBHOOK_SERVER_URL}/webhook/woocommerce"
        }
    ]
    
    # Optional: Add secret for webhook signature verification
    webhook_secret = os.getenv('WEBHOOK_SECRET', '')
    
    print("\n=== Setting up WooCommerce Webhooks ===")
    print(f"Webhook Server URL: {WEBHOOK_SERVER_URL}")
    print("-" * 50)
    
    # Check existing webhooks
    existing_webhooks = list_existing_webhooks()
    
    # Check if our webhooks already exist
    existing_urls = [w['delivery_url'] for w in existing_webhooks]
    our_webhook_url = f"{WEBHOOK_SERVER_URL}/webhook/woocommerce"
    
    if our_webhook_url in existing_urls:
        print("\n⚠️  Webhooks may already exist for this URL")
        response = input("Do you want to delete existing webhooks and recreate them? (y/n): ")
        
        if response.lower() == 'y':
            # Delete existing webhooks for our URL
            for webhook in existing_webhooks:
                if webhook['delivery_url'] == our_webhook_url:
                    delete_webhook(webhook['id'])
        else:
            print("Keeping existing webhooks")
            return
    
    # Create new webhooks
    print("\n=== Creating New Webhooks ===")
    created_webhooks = []
    
    for webhook_config in webhooks_to_create:
        webhook = create_webhook(
            name=webhook_config['name'],
            topic=webhook_config['topic'],
            delivery_url=webhook_config['delivery_url'],
            secret=webhook_secret
        )
        if webhook:
            created_webhooks.append(webhook)
    
    print(f"\n✅ Successfully created {len(created_webhooks)} webhooks")
    
    # Test webhook connectivity
    print("\n=== Testing Webhook Connectivity ===")
    test_webhook_connection()

def test_webhook_connection():
    """Test if the webhook server is reachable"""
    try:
        response = requests.get(f"{WEBHOOK_SERVER_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"✅ Webhook server is reachable and healthy")
            print(f"Response: {response.json()}")
        else:
            print(f"⚠️  Webhook server returned status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to webhook server: {str(e)}")
        print("Make sure your webhook server is running and accessible")

def test_product_update():
    """Test webhook by updating a product"""
    print("\n=== Testing Webhook with Product Update ===")
    
    # Get first product
    response = wcapi.get("products?per_page=1")
    if response.status_code != 200:
        print("❌ Could not fetch products for testing")
        return
    
    products = response.json()
    if not products:
        print("❌ No products found for testing")
        return
    
    product = products[0]
    product_id = product['id']
    original_stock = product.get('stock_quantity', 0)
    
    print(f"Testing with product: {product['name']} (ID: {product_id})")
    print(f"Original stock: {original_stock}")
    
    # Update stock quantity
    new_stock = original_stock + 1 if original_stock is not None else 1
    update_data = {
        'stock_quantity': new_stock
    }
    
    response = wcapi.put(f"products/{product_id}", update_data)
    
    if response.status_code == 200:
        print(f"✅ Updated stock to: {new_stock}")
        print("Check your webhook server logs to verify the webhook was received")
        
        # Restore original stock
        input("\nPress Enter to restore original stock...")
        wcapi.put(f"products/{product_id}", {'stock_quantity': original_stock})
        print(f"✅ Restored stock to: {original_stock}")
    else:
        print(f"❌ Error updating product: {response.text}")

def remove_all_webhooks():
    """Remove all webhooks (cleanup utility)"""
    print("\n=== Removing All Webhooks ===")
    response = input("Are you sure you want to delete ALL webhooks? (y/n): ")
    
    if response.lower() != 'y':
        print("Cancelled")
        return
    
    webhooks = list_existing_webhooks()
    for webhook in webhooks:
        delete_webhook(webhook['id'])
    
    print(f"✅ Removed {len(webhooks)} webhooks")

if __name__ == "__main__":
    print("""
    ===================================
    WooCommerce Webhook Setup
    ===================================
    
    This script will set up webhooks in WooCommerce
    to sync product changes to Airtable.
    
    Options:
    1. Set up product webhooks
    2. List existing webhooks
    3. Test webhook connection
    4. Test with product update
    5. Remove all webhooks
    0. Exit
    """)
    
    while True:
        choice = input("\nEnter your choice (0-5): ")
        
        if choice == '1':
            setup_product_webhooks()
        elif choice == '2':
            list_existing_webhooks()
        elif choice == '3':
            test_webhook_connection()
        elif choice == '4':
            test_product_update()
        elif choice == '5':
            remove_all_webhooks()
        elif choice == '0':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")