import os
import requests
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import google.generativeai as genai
from PIL import Image
import io
import base64
import json
import csv
from dotenv import load_dotenv
import urllib.parse
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AirtableProductScraper:
    """
    A class to interact with Airtable and process products using Gemini AI for scraping.
    """
    
    def __init__(self, airtable_api_key: str, base_id: str, gemini_api_key: str):
        """
        Initialize the Airtable client and Gemini AI.
        
        Args:
            airtable_api_key (str): Airtable API key
            base_id (str): Airtable base ID
            gemini_api_key (str): Google Gemini AI API key
        """
        self.airtable_api_key = airtable_api_key
        self.base_id = base_id
        self.headers = {
            'Authorization': f'Bearer {airtable_api_key}',
            'Content-Type': 'application/json'
        }
        self.base_url = f'https://api.airtable.com/v0/{base_id}'
        
        # Configure Gemini AI
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')
    
    def get_product_catalogue_products(self) -> List[str]:
        """
        Get all product names from Product Catalogue table for comparison.
        
        Returns:
            List[str]: List of product names in Product Catalogue
        """
        try:
            logger.info("Fetching existing products from Product Catalogue...")
            
            url = f"{self.base_url}/Product Catalogue"
            params = {
                'fields[]': ['Product Name'],  # Only fetch product names for efficiency
                'maxRecords': 1000  # Adjust as needed
            }
            
            all_products = []
            
            while True:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                records = data.get('records', [])
                
                for record in records:
                    product_name = record.get('fields', {}).get('Product Name', '')
                    if product_name:
                        all_products.append(product_name.strip().lower())
                
                # Check if there are more records
                offset = data.get('offset')
                if offset:
                    params['offset'] = offset
                else:
                    break
            
            logger.info(f"Found {len(all_products)} products in Product Catalogue")
            return all_products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Product Catalogue: {e}")
            return []
    
    def get_records_from_all_tables(self) -> List[Dict]:
        """
        Fetch records with "Entered" status and empty scraping status from all tables using AI-Product-Fetch view.
        
        Returns:
            List[Dict]: List of records that need processing from all tables
        """
        all_records = []
        tables = ["Product Catalogue", "Items-Bid4more", "Items-Pay4more"]
        view_name = "AI-Product-Fetch"
        
        for table in tables:
            try:
                logger.info(f"Fetching records from table: {table}")
                
                # Build the filter formula for Airtable
                filter_formula = "AND({Status} = 'Entered', {Scraping Status} = '')"
                
                url = f"{self.base_url}/{table}"
                params = {
                    'view': view_name,
                    'filterByFormula': filter_formula,
                    'maxRecords': 100  # Adjust as needed
                }
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                records = data.get('records', [])
                
                # Add table information to each record
                for record in records:
                    record['table_name'] = table
                
                all_records.extend(records)
                logger.info(f"Found {len(records)} records in {table}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching records from {table}: {e}")
                continue
        
        logger.info(f"Total records found across all tables: {len(all_records)}")
        return all_records
    
    def download_image(self, image_url: str, folder_path: str, filename: str) -> str:
        """
        Download an image from URL to local folder.
        
        Args:
            image_url (str): URL of the image
            folder_path (str): Local folder path
            filename (str): Filename for the downloaded image
            
        Returns:
            str: Local path of downloaded image or empty string if failed
        """
        try:
            # Create folder if it doesn't exist
            Path(folder_path).mkdir(parents=True, exist_ok=True)
            
            # Get image extension from URL
            parsed_url = urllib.parse.urlparse(image_url)
            extension = Path(parsed_url.path).suffix
            if not extension:
                extension = '.jpg'  # Default extension
            
            # Create full local path
            local_path = os.path.join(folder_path, f"{filename}{extension}")
            
            # Download the image
            response = requests.get(image_url, timeout=30, stream=True)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded image: {local_path}")
            return local_path
            
        except Exception as e:
            logger.error(f"Error downloading image {image_url}: {e}")
            return ""
    
    def download_product_images(self, product_images: List[str], product_name: str, base_folder: str = "downloaded_images") -> List[str]:
        """
        Download all images for a product.
        
        Args:
            product_images (List[str]): List of image URLs
            product_name (str): Name of the product (for folder organization)
            base_folder (str): Base folder for downloads
            
        Returns:
            List[str]: List of local paths to downloaded images
        """
        if not product_images:
            return []
        
        # Create safe folder name from product name
        safe_product_name = "".join(c for c in product_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_product_name = safe_product_name.replace(' ', '_')[:50]  # Limit length
        
        product_folder = os.path.join(base_folder, safe_product_name)
        local_paths = []
        
        for i, image_url in enumerate(product_images):
            if image_url and image_url.startswith('http'):
                filename = f"image_{i+1}"
                local_path = self.download_image(image_url, product_folder, filename)
                if local_path:
                    local_paths.append(local_path)
        
        return local_paths
    
    def fetch_webpage_content(self, url: str) -> str:
        """
        Fetch the content of a webpage.
        
        Args:
            url (str): URL to fetch
            
        Returns:
            str: Webpage content
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching webpage content: {e}")
            return ""
    
    def load_image_from_url(self, image_url: str) -> Optional[Image.Image]:
        """
        Load an image from a URL.
        
        Args:
            image_url (str): URL of the image
            
        Returns:
            PIL.Image.Image or None: Loaded image
        """
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            return Image.open(io.BytesIO(response.content))
        except Exception as e:
            logger.error(f"Error loading image from URL: {e}")
            return None
    
    def extract_product_data_with_gemini(self, product_url: str, inspection_photo: str = None) -> Dict[str, Any]:
        """
        Extract product information using Gemini AI from URL and inspection photo.
        
        Args:
            product_url (str): URL of the product to scrape
            inspection_photo (str): Path or URL to inspection photo
            
        Returns:
            Dict: Extracted product data
        """
        try:
            logger.info(f"Processing product URL with Gemini AI: {product_url}")
            
            # Fetch webpage content
            webpage_content = self.fetch_webpage_content(product_url)
            
            # Prepare the prompt for Gemini
            prompt = f"""
            Analyze the following webpage content and extract product information. Return the data in JSON format with the following structure:
            {{
                "Product Name": "string",
                "Description": "string",
                "Retail Price (CAD)": "string (include currency symbol and amount)",
                "Weight": "string (with units)",
                "Dimension": "string (length x width x height with units)",
                "Brand": "string",
                "Good product images": ["array of image URLs found on the page"]
            }}
            
            Instructions:
            1. Extract the main product name/title
            2. Get a comprehensive product description (2-3 sentences)
            3. Find the retail price in Canadian dollars (convert if necessary)
            4. Look for weight specifications
            5. Find product dimensions
            6. Identify the brand/manufacturer
            7. Collect high-quality product image URLs
            8. If any field cannot be determined, use "Not found" as the value
            9. Ensure all prices include "CAD" or "$" symbol
            10. Be accurate and thorough
            
            Webpage URL: {product_url}
            
            Webpage Content:
            {webpage_content[:8000]}  # Limit content to avoid token limits
            """
            
            # If inspection photo is provided, include it in the analysis
            content_parts = [prompt]
            
            if inspection_photo:
                # Try to load the inspection photo
                if inspection_photo.startswith('http'):
                    image = self.load_image_from_url(inspection_photo)
                else:
                    # Assume it's a local file path
                    try:
                        image = Image.open(inspection_photo)
                    except Exception as e:
                        logger.warning(f"Could not load local image: {e}")
                        image = None
                
                if image:
                    content_parts.append(image)
                    prompt += f"\n\nAdditionally, analyze this inspection photo to verify or supplement the product information:"
            
            # Generate response using Gemini
            response = self.model.generate_content(content_parts)
            
            # Parse the JSON response
            try:
                # Extract JSON from the response
                response_text = response.text
                
                # Find JSON in the response (handle markdown code blocks)
                if '```json' in response_text:
                    json_start = response_text.find('```json') + 7
                    json_end = response_text.find('```', json_start)
                    json_text = response_text[json_start:json_end].strip()
                elif '```' in response_text:
                    json_start = response_text.find('```') + 3
                    json_end = response_text.find('```', json_start)
                    json_text = response_text[json_start:json_end].strip()
                else:
                    json_text = response_text.strip()
                
                extracted_data = json.loads(json_text)
                
                logger.info(f"Successfully extracted product data using Gemini AI")
                return extracted_data
                
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing Gemini AI JSON response: {e}")
                logger.error(f"Response text: {response.text}")
                
                # Return default structure with error info
                return {
                    'Product Name': 'Error parsing response',
                    'Description': 'Could not parse Gemini AI response',
                    'Retail Price (CAD)': 'Not found',
                    'Weight': 'Not found',
                    'Dimension': 'Not found',
                    'Brand': 'Not found',
                    'Good product images': []
                }
            
        except Exception as e:
            logger.error(f"Error with Gemini AI extraction: {e}")
            return {
                'Product Name': 'Error occurred',
                'Description': f'Error during extraction: {str(e)}',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found',
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Good product images': []
            }
    
    def save_unique_products_csv(self, unique_products: List[Dict], filename: str = None) -> str:
        """
        Save unique products (not in Product Catalogue) to a separate CSV file.
        
        Args:
            unique_products (List[Dict]): List of unique product data
            filename (str): Optional custom filename
            
        Returns:
            str: Path to the created CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"unique_products_{timestamp}.csv"
        
        try:
            # Define the CSV columns for unique products
            csv_columns = [
                'Source Table',
                'Record ID',
                'Product URL',
                'Inspection Photo',
                'Product Name',
                'Description',
                'Retail Price (CAD)',
                'Weight',
                'Dimension',
                'Brand',
                'Good product images URLs',
                'Downloaded Images Count',
                'Local Image Paths',
                'Processing Status',
                'Processing Timestamp'
            ]
            
            # Write CSV file using standard csv module
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                
                for product in unique_products:
                    # Handle lists - convert to comma-separated strings
                    if 'Good product images URLs' in product and isinstance(product['Good product images URLs'], list):
                        product['Good product images URLs'] = ', '.join(product['Good product images URLs'])
                    
                    if 'Local Image Paths' in product and isinstance(product['Local Image Paths'], list):
                        product['Local Image Paths'] = ', '.join(product['Local Image Paths'])
                    
                    # Ensure all columns exist in the product
                    row = {}
                    for column in csv_columns:
                        row[column] = product.get(column, '')
                    
                    writer.writerow(row)
            
            logger.info(f"Unique products data saved to CSV file: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving unique products to CSV: {e}")
            return ""
    
    def save_to_csv(self, results: List[Dict], filename: str = None) -> str:
        """
        Save the extracted product data to a CSV file.
        
        Args:
            results (List[Dict]): List of extracted product data
            filename (str): Optional custom filename
            
        Returns:
            str: Path to the created CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"product_data_{timestamp}.csv"
        
        try:
            # Define the CSV columns in the desired order
            csv_columns = [
                'Table Name',
                'Record ID',
                'Product URL',
                'Inspection Photo',
                'Product Name',
                'Description',
                'Retail Price (CAD)',
                'Weight',
                'Dimension',
                'Brand',
                'Good product images',
                'Processing Status',
                'Processing Timestamp',
                'Error Message'
            ]
            
            # Write CSV file using standard csv module
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                
                for result in results:
                    # Handle the 'Good product images' list - convert to comma-separated string
                    if 'Good product images' in result and isinstance(result['Good product images'], list):
                        result['Good product images'] = ', '.join(result['Good product images'])
                    
                    # Ensure all columns exist in the result
                    row = {}
                    for column in csv_columns:
                        row[column] = result.get(column, '')
                    
                    writer.writerow(row)
            
            logger.info(f"Data saved to CSV file: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return ""
    
    def update_record(self, record_id: str, data: Dict[str, Any], table_name: str = "AI-Product-Fetch") -> bool:
        """
        Update a record in Airtable with extracted product data.
        
        Args:
            record_id (str): Airtable record ID
            data (Dict): Data to update
            table_name (str): Name of the table to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            url = f"{self.base_url}/{table_name}/{record_id}"
            
            # Prepare the update payload
            payload = {
                'fields': data
            }
            
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            
            logger.info(f"Successfully updated record {record_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating record {record_id}: {e}")
            return False
    
    def update_scraping_status(self, record_id: str, status: str) -> bool:
        """
        Update the scraping status of a record.
        
        Args:
            record_id (str): Airtable record ID
            status (str): New scraping status
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            data = {
                'Scraping Status': status,
                'Last Processed': datetime.now().isoformat()
            }
            
            return self.update_record(record_id, data)
            
        except Exception as e:
            logger.error(f"Error updating scraping status: {e}")
            return False
    
    def process_products(self) -> tuple[str, str]:
        """
        Main processing function that fetches records from all tables and processes them using Gemini AI.
        Returns the paths to the generated CSV files (all products, unique products).
        """
        logger.info("Starting product processing with Gemini AI across all tables...")
        
        # Get existing products from Product Catalogue for comparison
        existing_products = self.get_product_catalogue_products()
        
        # Get records from all tables using AI-Product-Fetch view
        records = self.get_records_from_all_tables()
        
        if not records:
            logger.info("No records found for processing")
            return "", ""
        
        processed_count = 0
        failed_count = 0
        results = []
        unique_products = []
        
        for record in records:
            result_row = {
                'Table Name': '',
                'Record ID': '',
                'Product URL': '',
                'Inspection Photo': '',
                'Product Name': '',
                'Description': '',
                'Retail Price (CAD)': '',
                'Weight': '',
                'Dimension': '',
                'Brand': '',
                'Good product images': [],
                'Processing Status': '',
                'Processing Timestamp': datetime.now().isoformat(),
                'Error Message': ''
            }
            
            try:
                record_id = record['id']
                table_name = record.get('table_name', 'Unknown')
                fields = record.get('fields', {})
                
                # Fill in basic record info
                result_row['Table Name'] = table_name
                result_row['Record ID'] = record_id
                result_row['Product URL'] = fields.get('Product URL', '')
                result_row['Inspection Photo'] = fields.get('Inspection Photo', '')
                
                # Extract required fields
                product_url = fields.get('Product URL', '')
                inspection_photo = fields.get('Inspection Photo', '')
                
                if not product_url:
                    logger.warning(f"No product URL found for record {record_id} in {table_name}")
                    result_row['Processing Status'] = 'Failed - No URL'
                    result_row['Error Message'] = 'No product URL provided'
                    failed_count += 1
                    results.append(result_row)
                    continue
                
                logger.info(f"Processing record {record_id} from {table_name}...")
                result_row['Processing Status'] = 'Processing'
                
                # Extract product data using Gemini AI
                extracted_data = self.extract_product_data_with_gemini(product_url, inspection_photo)
                
                # Update result row with extracted data
                result_row.update(extracted_data)
                
                if extracted_data.get('Product Name') and extracted_data.get('Product Name') != 'Error occurred':
                    result_row['Processing Status'] = 'Completed'
                    processed_count += 1
                    logger.info(f"Successfully processed record {record_id} from {table_name}")
                    
                    # Check if this is a unique product (not in Product Catalogue)
                    product_name = extracted_data.get('Product Name', '').strip().lower()
                    
                    if (table_name != 'Product Catalogue' and 
                        product_name and 
                        product_name not in existing_products):
                        
                        logger.info(f"Found unique product: {extracted_data.get('Product Name')}")
                        
                        # Create unique product entry
                        unique_product = {
                            'Source Table': table_name,
                            'Record ID': record_id,
                            'Product URL': product_url,
                            'Inspection Photo': inspection_photo,
                            'Product Name': extracted_data.get('Product Name', ''),
                            'Description': extracted_data.get('Description', ''),
                            'Retail Price (CAD)': extracted_data.get('Retail Price (CAD)', ''),
                            'Weight': extracted_data.get('Weight', ''),
                            'Dimension': extracted_data.get('Dimension', ''),
                            'Brand': extracted_data.get('Brand', ''),
                            'Good product images URLs': extracted_data.get('Good product images', []),
                            'Downloaded Images Count': 0,
                            'Local Image Paths': [],
                            'Processing Status': 'Completed',
                            'Processing Timestamp': datetime.now().isoformat()
                        }
                        
                        # Download images for unique products
                        if extracted_data.get('Good product images'):
                            logger.info(f"Downloading images for: {extracted_data.get('Product Name')}")
                            local_paths = self.download_product_images(
                                extracted_data.get('Good product images', []),
                                extracted_data.get('Product Name', f'product_{record_id}')
                            )
                            unique_product['Downloaded Images Count'] = len(local_paths)
                            unique_product['Local Image Paths'] = local_paths
                        
                        unique_products.append(unique_product)
                    
                else:
                    result_row['Processing Status'] = 'Failed - Extraction Error'
                    result_row['Error Message'] = 'Could not extract product data'
                    failed_count += 1
                
                results.append(result_row)
                
            except Exception as e:
                error_msg = f"Error processing record {record.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                result_row['Processing Status'] = 'Failed - Processing Error'
                result_row['Error Message'] = str(e)
                failed_count += 1
                results.append(result_row)
        
        logger.info(f"Processing completed. Processed: {processed_count}, Failed: {failed_count}")
        logger.info(f"Found {len(unique_products)} unique products for downloading")
        
        # Save results to CSV files
        csv_filename = self.save_to_csv(results)
        unique_csv_filename = ""
        
        if unique_products:
            unique_csv_filename = self.save_unique_products_csv(unique_products)
        
        # Print summary by table
        if csv_filename and results:
            logger.info(f"All results saved to: {csv_filename}")
            if unique_csv_filename:
                logger.info(f"Unique products saved to: {unique_csv_filename}")
            
            # Create summary by table
            table_summary = {}
            for result in results:
                table = result.get('Table Name', 'Unknown')
                status = result.get('Processing Status', 'Unknown')
                
                if table not in table_summary:
                    table_summary[table] = {'Completed': 0, 'Failed': 0, 'Total': 0}
                
                table_summary[table]['Total'] += 1
                if 'Completed' in status:
                    table_summary[table]['Completed'] += 1
                else:
                    table_summary[table]['Failed'] += 1
            
            logger.info("\n=== PROCESSING SUMMARY BY TABLE ===")
            for table, stats in table_summary.items():
                logger.info(f"{table}: {stats['Completed']} completed, {stats['Failed']} failed, {stats['Total']} total")
            
            logger.info(f"\n=== OVERALL SUMMARY ===")
            logger.info(f"Total records processed: {len(results)}")
            logger.info(f"Successful: {processed_count}")
            logger.info(f"Failed: {failed_count}")
            logger.info(f"Unique products found: {len(unique_products)}")
            
            if unique_products:
                total_images = sum(p.get('Downloaded Images Count', 0) for p in unique_products)
                logger.info(f"Total images downloaded: {total_images}")
        
        return csv_filename, unique_csv_filename

def main():
    """
    Main function to run the product scraper with Gemini AI.
    """
    # Configuration - replace with your actual values
    AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY', 'your_airtable_api_key_here')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID', 'your_base_id_here')
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'your_gemini_api_key_here')
    
    if (AIRTABLE_API_KEY == 'your_airtable_api_key_here' or 
        AIRTABLE_BASE_ID == 'your_base_id_here' or 
        GEMINI_API_KEY == 'your_gemini_api_key_here'):
        logger.error("Please set your API keys and Base ID")
        logger.info("Set environment variables:")
        logger.info("- AIRTABLE_API_KEY")
        logger.info("- AIRTABLE_BASE_ID") 
        logger.info("- GEMINI_API_KEY")
        return
    
    # Initialize and run the scraper
    scraper = AirtableProductScraper(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, GEMINI_API_KEY)
    csv_file, unique_csv_file = scraper.process_products()
    
    if csv_file:
        print(f"\n✅ Processing completed successfully!")
        print(f"📁 All results saved to: {csv_file}")
        
        if unique_csv_file:
            print(f"🆕 Unique products saved to: {unique_csv_file}")
            print(f"📸 Product images downloaded to: downloaded_images/")
        
        print(f"🔍 Open the CSV files to review all extracted product data")
    else:
        print("❌ Processing failed or no data to process")

if __name__ == "__main__":
    main()