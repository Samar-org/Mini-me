import os
import requests
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import google.generativeai as genai
from PIL import Image, ImageFilter, ImageEnhance
import io
import json
import re
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
    
    def get_product_catalogue_products(self, view_name: str = "full-table-view") -> List[str]:
        """
        Get all product names from Product Catalogue table's full-table-view for comparison.
        """
        try:
            logger.info(f"Fetching existing products from Product Catalogue '{view_name}'...")
            
            url = f"{self.base_url}/Product Catalogue"
            params = {
                'view': view_name,
                'fields[]': ['Product Name'],
                'maxRecords': 1000
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
                
                offset = data.get('offset')
                if offset:
                    params['offset'] = offset
                else:
                    break
            
            logger.info(f"Found {len(all_products)} products in Product Catalogue '{view_name}'")
            return all_products
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Product Catalogue '{view_name}': {e}")
            return []
    
    def get_records_from_all_tables(self) -> List[Dict]:
        """
        Fetch records with "Entered" status, empty scraping status, and empty "4more-Product-Code" from all tables.
        """
        all_records = []
        tables = ["Product Catalogue", "Items-Bid4more", "Items-Pay4more"]
        view_name = "AI-Product-Fetch"
        
        for table in tables:
            try:
                logger.info(f"Fetching records from table: {table}")
                
                filter_formula = "AND({Status} = 'Entered', {Scraping Status} = '', {4more-Product-Code} = '')"
                
                url = f"{self.base_url}/{table}"
                params = {
                    'view': view_name,
                    'filterByFormula': filter_formula,
                    'maxRecords': 100
                }
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                records = data.get('records', [])
                
                for record in records:
                    record['table_name'] = table
                
                all_records.extend(records)
                logger.info(f"Found {len(records)} records in {table} (excluding records with filled 4more-Product-Code)")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching records from {table}: {e}")
                continue
        
        logger.info(f"Total records found across all tables: {len(all_records)}")
        return all_records
    
    def fetch_webpage_content(self, url: str) -> str:
        """
        Fetch the content of a webpage with redirect handling.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
            response.raise_for_status()
            logger.info(f"Resolved URL: {response.url}")
            return response.text
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
        product_url = self.resolve_shortlink(product_url)
Extract product information using Gemini AI from URL and inspection photo.
        """
        try:
            logger.info(f"Processing product URL with Gemini AI: {product_url}")
            
            # First, try to extract from webpage content
            webpage_content = self.fetch_webpage_content(product_url)
            url_extraction_successful = False
            
            if webpage_content and len(webpage_content.strip()) > 500:
                logger.info("Attempting extraction from webpage content...")
                
                url_prompt = f"""
                Analyze the following webpage content and extract product information. Return the data in JSON format:
                {{
                    "Product Name": "string",
                    "Description": "string", 
                    "Retail Price (CAD)": "string (include currency symbol and amount)",
                    "Weight": "string (with units)",
                    "Dimension": "string (length x width x height with units)", 
                    "Brand": "string",
                    "Photo Files": ["array of image URLs found on the page"]
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
                {webpage_content[:8000]}
                """
                
                try:
                    response = self.model.generate_content([url_prompt])
                    response_text = response.text
                    
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
                    
                    product_name = extracted_data.get('Product Name', '').strip()
                    if (product_name and 
                        product_name.lower() not in ['not found', 'error', 'unknown', 'n/a', ''] and
                        len(product_name) > 3):
                        
                        logger.info("Successfully extracted product data from webpage content")
                        url_extraction_successful = True
                        
                        if inspection_photo:
                            logger.info("Using inspection photo to enhance webpage data...")
                            enhanced_data = self.enhance_with_inspection_photo(extracted_data, inspection_photo, product_url)
                            return enhanced_data
                        
                        return extracted_data
                    else:
                        logger.warning("Webpage extraction failed - product name not found or invalid")
                        
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Failed to extract from webpage: {e}")
            
            else:
                logger.warning("Webpage content insufficient or empty")
            
            if not url_extraction_successful and inspection_photo:
                logger.info("URL extraction failed, attempting extraction from inspection photo...")
                return self.extract_from_inspection_photo(inspection_photo, product_url)
            
            logger.error("Both URL and inspection photo extraction failed")
            return {
                'Product Name': 'Extraction failed',
                'Description': 'Unable to extract data from URL and no inspection photo available',
                'Retail Price (CAD)': 'Not found',
                'Weight': 'Not found', 
                'Dimension': 'Not found',
                'Brand': 'Not found',
                'Photo Files': []
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
                'Photo Files': []
            }
    
    def enhance_with_inspection_photo(self, webpage_data: Dict[str, Any], inspection_photo: str, product_url: str) -> Dict[str, Any]:
        """
        Enhance webpage extraction data using inspection photo.
        """
        try:
            image = None
            if inspection_photo.startswith('http'):
                image = self.load_image_from_url(inspection_photo)
            else:
                try:
                    image = Image.open(inspection_photo)
                except Exception as e:
                    logger.warning(f"Could not load local image: {e}")
            
            if not image:
                logger.warning("Could not load inspection photo for enhancement")
                return webpage_data
            
            enhancement_prompt = f"""
            I have extracted the following product information from a webpage, but I want you to verify and enhance it using this inspection photo.
            
            Current extracted data:
            {json.dumps(webpage_data, indent=2)}
            
            Please analyze the inspection photo and:
            1. Verify the product name matches what you see in the image
            2. Enhance or correct the description based on visual details
            3. Verify brand information visible in the image
            4. Add any missing details you can see
            5. Correct any inconsistencies between webpage data and image
            
            Return the enhanced data in the same JSON format.
            
            Original URL: {product_url}
            """
            
            response = self.model.generate_content([enhancement_prompt, image])
            response_text = response.text
            
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
            
            enhanced_data = json.loads(json_text)
            logger.info("Successfully enhanced webpage data with inspection photo")
            return enhanced_data
            
        except Exception as e:
            logger.warning(f"Failed to enhance with inspection photo: {e}")
            return webpage_data
    
    def extract_from_inspection_photo(self, inspection_photo: str, product_url: str) -> Dict[str, Any]:
        """
        Extract product information primarily from inspection photo when URL fails.
        """
        try:
            logger.info("Extracting product data from inspection photo...")
            
            image = None
            if inspection_photo.startswith('http'):
                image = self.load_image_from_url(inspection_photo)
            else:
                try:
                    image = Image.open(inspection_photo)
                except Exception as e:
                    logger.error(f"Could not load local image: {e}")
                    return self.get_error_response("Could not load inspection photo")
            
            if not image:
                logger.error("Failed to load inspection photo")
                return self.get_error_response("Failed to load inspection photo")
            
            photo_prompt = f"""
            Analyze this product inspection photo and extract as much product information as possible. 
            
            Extract and return data in JSON format:
            {{
                "Product Name": "product name visible in image or best description",
                "Description": "detailed description based on what you see in the image",
                "Retail Price (CAD)": "price if visible on packaging/labels, otherwise 'Not found'",
                "Weight": "weight if visible on packaging/labels",
                "Dimension": "dimensions if visible or estimatable from image",
                "Brand": "brand name visible in image",
                "Photo Files": ["Note: Extracted from inspection photo - original URL: {product_url}"]
            }}
            
            Instructions:
            1. Look carefully at any text, labels, packaging, or branding visible in the image
            2. Describe the product based on its visual appearance
            3. Identify any brand names, logos, or text visible
            4. Look for any pricing, weight, or dimension information on packaging
            5. Provide a detailed description of the product's appearance and features
            6. If information is not visible in the image, use "Not found"
            7. Be as detailed and accurate as possible based on visual inspection
            
            Original URL (failed): {product_url}
            """
            
            response = self.model.generate_content([photo_prompt, image])
            response_text = response.text
            
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
            logger.info("Successfully extracted product data from inspection photo")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Failed to extract from inspection photo: {e}")
            return self.get_error_response(f"Inspection photo extraction failed: {str(e)}")
    
    def get_error_response(self, error_message: str) -> Dict[str, Any]:
        """
        Return a standardized error response.
        """
        return {
            'Product Name': 'Extraction failed',
            'Description': error_message,
            'Retail Price (CAD)': 'Not found',
            'Weight': 'Not found',
            'Dimension': 'Not found',
            'Brand': 'Not found',
            'Photo Files': []
        }
    
    def check_data_completeness(self, extracted_data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Check if all required product information was found.
        """
        required_fields = [
            'Product Name',
            'Description', 
            'Retail Price (CAD)',
            'Weight',
            'Dimension',
            'Brand',
            'Photo Files'
        ]
        
        missing_fields = []
        
        for field in required_fields:
            value = extracted_data.get(field, '')
            
            if (not value or 
                str(value).strip().lower() in ['not found', 'n/a', 'unknown', '', 'error', 'none'] or
                (isinstance(value, list) and len(value) == 0)):
                missing_fields.append(field)
        
        is_complete = len(missing_fields) == 0
        return is_complete, missing_fields
    
    def determine_status_and_scraping_status(self, extracted_data: Dict[str, Any]) -> tuple[str, str, str]:
        """
        Determine the appropriate Status and Scraping Status based on extraction results.
        """
        product_name = extracted_data.get('Product Name', '').strip()
        
        if (not product_name or 
            product_name.lower() in ['error occurred', 'extraction failed', 'error parsing response', 'not found']):
            return 'Needs Attention', 'Failed - Extraction Error', 'Could not extract product data from URL or inspection photo'
        
        is_complete, missing_fields = self.check_data_completeness(extracted_data)
        
        if is_complete:
            status = 'Scraped'
            scraping_status = 'Completed'
            processing_notes = 'All product information successfully extracted'
        else:
            status = 'Needs Attention'
            scraping_status = 'Partially Completed'
            missing_fields_str = ', '.join(missing_fields)
            processing_notes = f'Missing information for: {missing_fields_str}'
        
        return status, scraping_status, processing_notes
    
    def parse_price_to_number(self, price_string: str) -> Optional[float]:
        """
        Parse a price string and extract the numeric value for Airtable currency field.
        """
        if not price_string or price_string.lower().strip() in ['not found', 'n/a', 'unknown', '']:
            return None
        
        try:
            # Remove common currency symbols and text
            cleaned = price_string.replace('$', '').replace('CAD', '').replace('USD', '')
            cleaned = cleaned.replace(',', '').strip()
            
            # Extract first number found
            numbers = re.findall(r'\d+\.?\d*', cleaned)
            if numbers:
                return float(numbers[0])
            
            return None
            
        except (ValueError, AttributeError):
            logger.warning(f"Could not parse price: {price_string}")
            return None
    
    def get_field_mapping(self, table_name: str) -> Dict[str, str]:
        """
        Get field mapping for different tables.
        """
        base_mapping = {
            'Product Name': 'Product Name',
            'Description': 'Description', 
            'Retail Price (CAD)': 'Unit Retail Price',
            'Weight': 'Weight',
            'Dimension': 'Dimensions',
            'Brand': 'Brand',
            'Photo Files': 'Photo Files',
            'Status': 'Status',
            'Scraping Status': 'Scraping Status',
            'Processing Notes': 'Scraping Status'
        }
        
        return base_mapping
    
    def get_safe_field_names(self, table_name: str) -> List[str]:
        """
        Get a list of fields that are known to be safe to update.
        """
        safe_fields = [
            'Product Name',
            'Description',
            'Brand', 
            'Status',
            'Scraping Status',
            '4more-Product-Code',
            'Unit Retail Price',
            'Weight',
            'Dimensions',
            'Photo Files'
        ]
        
        return safe_fields
    
    def map_fields_for_table(self, data: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """
        Map standard field names to actual Airtable field names and handle special formatting.
        """
        field_mapping = self.get_field_mapping(table_name)
        safe_fields = self.get_safe_field_names(table_name)
        mapped_data = {}
        
        for standard_name, value in data.items():
            mapped_name = field_mapping.get(standard_name, standard_name)
            
            if mapped_name in safe_fields:
                if mapped_name == 'Unit Retail Price':
                    numeric_price = self.parse_price_to_number(str(value))
                    if numeric_price is not None:
                        mapped_data[mapped_name] = numeric_price
                else:
                    mapped_data[mapped_name] = value
            else:
                logger.debug(f"Skipping field '{standard_name}' -> '{mapped_name}' (not in safe fields)")
        
        return mapped_data
    
    def update_record(self, record_id: str, data: Dict[str, Any], table_name: str) -> bool:
        """
        Update a record in Airtable with extracted product data.
        """
        try:
            url = f"{self.base_url}/{table_name}/{record_id}"
            
            mapped_data = self.map_fields_for_table(data, table_name)
            
            cleaned_data = {}
            for key, value in mapped_data.items():
                if value is not None:
                    if isinstance(value, str):
                        if key in ['Product Name', 'Brand']:
                            cleaned_data[key] = value[:255] if value else ""
                        elif key in ['Description', 'Scraping Status']:
                            cleaned_data[key] = value[:1000] if value else ""
                        else:
                            cleaned_data[key] = value
                    else:
                        cleaned_data[key] = value
            
            payload = {'fields': cleaned_data}
            
            response = requests.patch(url, headers=self.headers, json=payload)
            
            if response.status_code == 422:
                error_details = response.json()
                logger.error(f"Validation error updating record {record_id}: {error_details}")
                return False
            
            response.raise_for_status()
            logger.info(f"Successfully updated record {record_id} in {table_name}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating record {record_id} in {table_name}: {e}")
            return False
    
    def format_photo_files_for_airtable(self, photo_files) -> List[Dict[str, str]]:
        """
        Convert photo URLs to proper Airtable attachment objects.
        """
        if not photo_files:
            return []
        
        attachments = []
        
        # Handle different input formats
        if isinstance(photo_files, str):
            # Single URL or comma-separated URLs
            urls = [url.strip() for url in photo_files.split(',') if url.strip()]
        elif isinstance(photo_files, list):
            urls = [str(url).strip() for url in photo_files if str(url).strip()]
        else:
            urls = [str(photo_files).strip()]
        
        # Convert each URL to Airtable attachment format
        for url in urls:
            if url.startswith('http'):
                # Extract filename from URL
                try:
                    filename = url.split('/')[-1]
                    # Remove query parameters if present
                    if '?' in filename:
                        filename = filename.split('?')[0]
                    # Ensure it has an extension
                    if '.' not in filename:
                        filename += '.jpg'
                except:
                    filename = 'product_image.jpg'
                
                attachments.append({
                    "url": url,
                    "filename": filename
                })
        
        return attachments

    def resolve_shortlink(self, url: str) -> str:
        """Follow redirects for short URLs like a.co and return final URL."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.head(url, headers=headers, allow_redirects=True, timeout=10)
            final_url = response.url
            logger.info(f"Resolved shortlink to: {final_url}")
            return final_url
        except Exception as e:
            logger.warning(f"Failed to resolve shortlink: {e}")
            return url

    def update_record_with_status(self, record_id: str, extracted_data: Dict[str, Any], table_name: str) -> bool:
        """
        Update a record with extracted data and appropriate status.
        """
        try:
            status, scraping_status, processing_notes = self.determine_status_and_scraping_status(extracted_data)
            
            combined_scraping_status = f"{scraping_status}"
            if processing_notes and processing_notes != scraping_status:
                combined_scraping_status += f" - {processing_notes}"
            
            update_data = {
                'Product Name': extracted_data.get('Product Name', ''),
                'Description': extracted_data.get('Description', ''),
                'Retail Price (CAD)': extracted_data.get('Retail Price (CAD)', ''),
                'Weight': extracted_data.get('Weight', ''),
                'Dimensions': extracted_data.get('Dimensions', ''),
                'Brand': extracted_data.get('Brand', ''),
                'Status': status,
                'Scraping Status': combined_scraping_status
            }
            
            # Handle photo files properly for Airtable attachments
            if extracted_data.get('Photo Files'):
                logger.info(f"🖼️ Processing images for record {record_id}...")
                
                # Format as Airtable attachment objects
                photo_attachments = self.format_photo_files_for_airtable(extracted_data['Photo Files'])
                
                if photo_attachments:
                    update_data['Photo Files'] = photo_attachments
                    logger.info(f"📸 Adding {len(photo_attachments)} photo attachment(s)")
                else:
                    logger.warning("⚠️ No valid photo URLs found to attach")
            
            success = self.update_record(record_id, update_data, table_name)
            
            if success:
                if status == 'Scraped':
                    logger.info(f"✅ Record {record_id} - All information found and updated (Status: Scraped)")
                elif status == 'Needs Attention':
                    logger.warning(f"⚠️ Record {record_id} - Missing some information (Status: Needs Attention)")
                    logger.info(f"   Details: {processing_notes}")
                
                price = extracted_data.get('Retail Price (CAD)', '')
                if price and price != 'Not found':
                    numeric_price = self.parse_price_to_number(price)
                    if numeric_price:
                        logger.info(f"💰 Price updated: ${numeric_price:.2f}")
                    else:
                        logger.warning(f"⚠️ Could not parse price: {price}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error updating record with status: {e}")
            return False

    
    def add_unique_product_to_catalogue(self, product_data: Dict[str, Any]) -> str:
        """
        Add a unique product to the Product Catalogue table with 4more-product-code as "NEW".
        """
        try:
            url = f"{self.base_url}/Product Catalogue"

            # Prepare and clean fields
            catalogue_data = {
                'Product Name': str(product_data.get('Product Name', '')).strip()[:255],
                'Description': str(product_data.get('Description', '')).strip()[:1000],
                'Weight': str(product_data.get('Weight', '')).strip()[:255],
                'Dimensions': str(product_data.get('Dimensions', '')).strip()[:255],
                'Brand': str(product_data.get('Brand', '')).strip()[:255],
                '4more-Product-Code': 'NEW',
                'Status': 'Code Generated',
                'Scraping Status': f"AI Processed - Auto-added from {product_data.get('Source Table', '')} - New product not found in Product Catalogue full-table-view",
                'Product URL': product_data.get('Product URL', '').strip()[:1000],
                'Category': product_data.get('Category', '').strip()[:255],
            }

            # Add price if valid
            price_numeric = self.parse_price_to_number(product_data.get('Retail Price (CAD)', ''))
            if price_numeric is not None:
                catalogue_data['Unit Retail Price'] = price_numeric

            # Add photo files as Airtable attachments
            photo_urls = product_data.get('Photo Files URLs', [])
            if isinstance(photo_urls, list) and photo_urls:
                photo_attachments = self.format_photo_files_for_airtable(photo_urls)
                if photo_attachments:
                    catalogue_data['Photo Files'] = photo_attachments

            payload = {'fields': catalogue_data}
            logger.warning(f"Payload being sent: {json.dumps(payload, indent=2)}")

            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()

            result = response.json()
            new_record_id = result.get('id', '')

            if new_record_id:
                logger.info(f"✅ Successfully added NEW product to catalogue: {product_data.get('Product Name', '')} (ID: {new_record_id}, Code: NEW)")
                if price_numeric:
                    logger.info(f"💰 Price set to: ${price_numeric:.2f}")
                return new_record_id
            else:
                logger.error("Failed to get new record ID after adding to catalogue")
                return ""

        except requests.exceptions.RequestException as e:
            logger.error(f"Error adding product to catalogue: {e}")
            return ""
    
    def process_products(self) -> tuple[int, int, int]:
        """
        Main processing function that fetches records from all tables and processes them using Gemini AI.
        """
        logger.info("Starting product processing with Gemini AI across all tables...")
        
        existing_products = self.get_product_catalogue_products("full-table-view")
        records = self.get_records_from_all_tables()
        
        if not records:
            logger.info("No records found for processing")
            return 0, 0, 0
        
        processed_count = 0
        failed_count = 0
        unique_products_added = 0
        table_summary = {}
        
        for record in records:
            try:
                record_id = record['id']
                table_name = record.get('table_name', 'Unknown')
                fields = record.get('fields', {})
                
                if table_name not in table_summary:
                    table_summary[table_name] = {'Completed': 0, 'Failed': 0, 'Total': 0}
                table_summary[table_name]['Total'] += 1
                
                product_code = fields.get('4more-Product-Code', '')
                if product_code and product_code.strip():
                    logger.info(f"⏭️ Skipping record {record_id} from {table_name} - 4more-Product-Code already filled: {product_code}")
                    table_summary[table_name]['Total'] -= 1
                    continue
                
                product_url = fields.get('Product URL', '')
                inspection_photo = fields.get('Inspection Photo', '')
                
                if not product_url:
                    logger.warning(f"❌ No product URL found for record {record_id} in {table_name}")
                    error_data = {
                        'Status': 'Needs Attention',
                        'Scraping Status': 'Failed - No URL - No product URL provided'
                    }
                    self.update_record(record_id, error_data, table_name)
                    failed_count += 1
                    table_summary[table_name]['Failed'] += 1
                    continue
                
                logger.info(f"🔄 Processing record {record_id} from {table_name}...")
                
                processing_data = {'Scraping Status': 'Processing'}
                self.update_record(record_id, processing_data, table_name)
                
                extracted_data = self.extract_product_data_with_gemini(product_url, inspection_photo)
                
                if (extracted_data.get('Product Name') and 
                    extracted_data.get('Product Name') not in ['Error occurred', 'Extraction failed', 'Error parsing response']):
                    
                    if self.update_record_with_status(record_id, extracted_data, table_name):
                        processed_count += 1
                        table_summary[table_name]['Completed'] += 1
                        
                        is_complete, missing_fields = self.check_data_completeness(extracted_data)
                        if is_complete:
                            logger.info(f"✅ Successfully processed record {record_id} from {table_name} - All data found")
                        else:
                            logger.info(f"⚠️ Partially processed record {record_id} from {table_name} - Some data missing")
                        
                        product_name = extracted_data.get('Product Name', '').strip().lower()
                        
                        if (table_name != 'Product Catalogue' and 
                            product_name and 
                            product_name not in existing_products):
                            
                            logger.info(f"🆕 Found NEW product not in full-table-view: {extracted_data.get('Product Name')}")
                            
                            unique_product_data = {
                                'Product Name': extracted_data.get('Product Name', ''),
                                'Description': extracted_data.get('Description', ''),
                                'Retail Price (CAD)': extracted_data.get('Retail Price (CAD)', ''),
                                'Weight': extracted_data.get('Weight', ''),
                                'Dimensions': extracted_data.get('Dimensions', ''),
                                'Brand': extracted_data.get('Brand', ''),
                                'Photo Files URLs': extracted_data.get('Photo Files', []),
                                'Source Table': table_name,
                                'Record ID': record_id,
                                'Product URL': product_url
                            }
                            
                            new_catalogue_id = self.add_unique_product_to_catalogue(unique_product_data)
                            if new_catalogue_id:
                                unique_products_added += 1
                                existing_products.append(product_name)
                                logger.info(f"➕ Added NEW product to catalogue with 4more-product-code: NEW (ID: {new_catalogue_id})")
                            else:
                                logger.warning(f"❌ Failed to add NEW product to catalogue: {extracted_data.get('Product Name')}")
                    
                    else:
                        error_data = {
                            'Status': 'Needs Attention',
                            'Scraping Status': 'Failed - Update Error - Could not update record with extracted data'
                        }
                        self.update_record(record_id, error_data, table_name)
                        failed_count += 1
                        table_summary[table_name]['Failed'] += 1
                
                else:
                    error_msg = f"Extraction failed: {extracted_data.get('Description', 'Unknown error')}"
                    error_data = {
                        'Status': 'Needs Attention',
                        'Scraping Status': f'Failed - Extraction Error - {error_msg}'
                    }
                    self.update_record(record_id, error_data, table_name)
                    failed_count += 1
                    table_summary[table_name]['Failed'] += 1
                
            except Exception as e:
                error_msg = f"Error processing record {record.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                
                try:
                    error_data = {
                        'Status': 'Needs Attention',
                        'Scraping Status': f'Failed - Processing Error - {str(e)}'
                    }
                    self.update_record(record_id, error_data, table_name)
                except:
                    logger.error(f"Could not update error status for record {record_id}")
                
                failed_count += 1
                if table_name in table_summary:
                    table_summary[table_name]['Failed'] += 1
        
        logger.info(f"\n=== PROCESSING COMPLETED ===")
        logger.info(f"Total records processed: {processed_count + failed_count}")
        logger.info(f"Successful extractions: {processed_count}")
        logger.info(f"Failed extractions: {failed_count}")
        logger.info(f"Unique products added to catalogue: {unique_products_added}")
        
        logger.info(f"\n=== SUMMARY BY TABLE ===")
        for table, stats in table_summary.items():
            logger.info(f"{table}: {stats['Completed']} completed, {stats['Failed']} failed, {stats['Total']} total")
        
        if unique_products_added > 0:
            logger.info(f"\n✅ {unique_products_added} NEW products were automatically added to Product Catalogue with 4more-product-code: NEW!")
            logger.info(f"🔍 Filter Product Catalogue by '4more-product-code = NEW' to review new products")
        
        return processed_count, failed_count, unique_products_added

def main():
    """
    Main function to run the product scraper with Gemini AI.
    """
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
    
    scraper = AirtableProductScraper(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, GEMINI_API_KEY)
    processed, failed, unique_added = scraper.process_products()
    
    if processed > 0 or failed > 0:
        print(f"\n✅ Processing completed!")
        print(f"📊 Successfully processed: {processed} records")
        if failed > 0:
            print(f"❌ Failed to process: {failed} records")
        if unique_added > 0:
            print(f"🆕 NEW products added to catalogue: {unique_added} (4more-product-code: NEW)")
            print(f"🔍 Filter Product Catalogue by '4more-product-code = NEW' to review new products")
        print(f"🔍 Check your Airtable tables to review the updated data")
    else:
        print("❌ No records were processed - check your Airtable configuration")

if __name__ == "__main__":
        main()