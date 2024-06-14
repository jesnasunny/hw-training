import json
from settings import headers, NEXT_THURSDAY_DATE
from pipeline import MongoConnection
import logging
import re
import requests
import time

class DmParser:
    def __init__(self):
        self.mongoconnection = MongoConnection()
        self.collection = self.mongoconnection.db["product_urls"]
        self.parsed_collection = self.mongoconnection.db['parsed_data']
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def dmparser(self, data, url):
        unique_id = data.get('gtin', '')
        competitor_name = "dm"
        extraction_date = NEXT_THURSDAY_DATE
        product_name = data.get('title', {}).get('headline', '')        
        brand = data.get('title', {}).get('brand', '')   
        match = re.search(r'(\d+)\s*(.*)', product_name)
        if match:
            number = match.group(1)
            text_after_number = match.group(2)
        else:
            number = ''
            text_after_number = ''     
        grammage_quantity = number
        grammage_unit = text_after_number
        breadcrumbs = data.get('breadcrumbs', [])
        producthierarchy_level1 = 'Startseite'
        producthierarchy_level2 = breadcrumbs[0] if len(breadcrumbs) > 0 else ''
        producthierarchy_level3 = breadcrumbs[1] if len(breadcrumbs) > 1 else ''
        producthierarchy_level4 = breadcrumbs[2] if len(breadcrumbs) > 2 else ''
        producthierarchy_level5 = breadcrumbs[3] if len(breadcrumbs) > 3 else ''
        producthierarchy_level6 = breadcrumbs[4] if len(breadcrumbs) > 4 else ''
        producthierarchy_level7 = ''                
        regular_price = data.get('price', {}).get('selloutPriceLocalized', '') 
        selling_price = data.get('price', {}).get('price', '')
        price_was = regular_price     
        promotion_valid_from = data.get('price', {}).get('priceNotIncreasedSince', '')        
        price_valid_from = data.get('price', {}).get('selloutFrom', '')
        pdp_url = url
        
        product_details = {
            "unique_id": unique_id,
            "competitor_name": competitor_name,
            "extraction_date": extraction_date,
            "product_name": product_name,
            "brand": brand,
            "grammage_quantity": grammage_quantity,
            "grammage_unit": grammage_unit,
            "producthierarchy_level1": producthierarchy_level1,
            "producthierarchy_level2": producthierarchy_level2,
            "producthierarchy_level3": producthierarchy_level3,
            "producthierarchy_level4": producthierarchy_level4,
            "producthierarchy_level5": producthierarchy_level5,
            "producthierarchy_level6": producthierarchy_level6,
            "producthierarchy_level7": producthierarchy_level7,
            "regular_price": regular_price,
            "selling_price": selling_price,
            "price_was": price_was,
            "promotion_valid_from": promotion_valid_from,
            "price_valid_from": price_valid_from,
            "pdp_url": url
        }

        self.parsed_collection.insert_one(product_details)

    def scrape_data(self, fixed_delay=5):
        while self.collection.count_documents({}) > 0:
            product_doc = self.collection.find_one_and_delete({})
            product_url = product_doc["url"]

            try:
                pattern = r'p(\d+)\.html'
                match = re.search(pattern, product_url)
                if match:
                    number = match.group(1)
                    json_url = f"https://products.dm.de/product/AT/products/detail/gtin/{number}"
                    response = requests.get(json_url, headers=headers)
                    if response.status_code == 200:
                        data = response.json()
                        self.dmparser(data, product_url)
                    else:
                        logging.error(f"Failed to retrieve data for {json_url}, status code: {response.status_code}")
                else:
                    logging.error(f"URL does not match expected pattern: {product_url}")

            except requests.exceptions.RequestException as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {product_url}. Error: {error_message}")

            time.sleep(fixed_delay)  

        logging.info(f"Scraped {self.parsed_collection.count_documents({})} Product URLs")

if __name__ == "__main__":
    parser = DmParser()
    parser.scrape_data()
