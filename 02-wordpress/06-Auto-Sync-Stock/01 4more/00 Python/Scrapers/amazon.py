from bs4 import BeautifulSoup
import requests
import csv
from urllib.parse import urljoin, quote
import pandas as pd
import re

def search_amazon_product_details(product_name):
    """
    Searches for a product on Amazon.ca, and retrieves the product title, price,
    description, image URLs, and ASIN.

    Args:
        product_name (str): The name of the product to search for.

    Returns:
        dict: A dictionary containing the product's details, including ASIN.
              Returns None if the product page cannot be accessed or details
              cannot be extracted.
    """

    base_url = "https://www.amazon.ca"
    search_url = f"{base_url}/s?k={quote(product_name)}"

    headers = {
        'authority': 'www.amazon.ca',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-CA,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(search_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the first product link
        first_product_link = soup.find('a', class_='a-link-normal')
        if first_product_link and first_product_link.has_attr('href'):
            product_url = urljoin(base_url, first_product_link['href'])

            # Extract ASIN from the URL
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', product_url)
            asin = asin_match.group(1) if asin_match else 'Not Found'

            # Fetch the product details page
            product_response = requests.get(product_url, headers=headers)
            product_response.raise_for_status()
            product_soup = BeautifulSoup(product_response.text, 'html.parser')

            # Extract product details
            title = product_soup.find('span', {'id': 'productTitle'}).text.strip() if product_soup.find('span', {'id': 'productTitle'}) else 'Not Found'
            price_element = product_soup.find('span', class_='a-offscreen')
            price = price_element.text.strip() if price_element else 'Not Found'

            description_div = product_soup.find('div', {'id': 'productDescription'})
            description = description_div.text.strip() if description_div else 'Not Found'

            image_elements = product_soup.find_all('img', class_='a-dynamic-image')
            image_urls = [img['src'] for img in image_elements] if image_elements else []

            return {
                'title': title,
                'price': price,
                'description': description,
                'image_urls': image_urls,
                'amazon_url': product_url,
                'asin': asin  # Include ASIN in the results
            }
        else:
            print(f"No product link found for {product_name}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Could not retrieve product page for {product_name}: {e}")
        return None
    except AttributeError as e:
        print(f"Could not extract details for {product_name}: {e}")
        return None

# --- Main execution ---
df = pd.read_csv("Auction-002.csv")
product_names = df['Name'].tolist()

csv_file_path = "amazon_products_data.csv"
csv_header = ['Product Name', 'Title', 'Price', 'Description', 'Image URLs', 'Amazon URL', 'ASIN']  # Added ASIN to header

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(csv_header)

    for product_name in product_names:
        print(f"Searching Amazon for: {product_name}")
        details = search_amazon_product_details(product_name)
        if details:
            writer.writerow([
                product_name,
                details['title'],
                details['price'],
                details['description'],
                '; '.join(details['image_urls']),
                details['amazon_url'],
                details['asin']  # Write ASIN to CSV
            ])
        else:
            writer.writerow([product_name, 'Not Found', 'Not Found', 'Not Found', 'Not Found', 'Not Found', 'Not Found'])

print(f"Product details saved to {csv_file_path}")