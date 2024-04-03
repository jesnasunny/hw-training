import csv
import re
import requests
from pymongo import MongoClient
from parsel import Selector
import logging

class OlxParser:
    def __init__(self):
        self.base_url = "https://www.olx.in"
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['olx_data']
        self.url_collection = self.db['property_urls']
        self.details_collection = self.db['property_details']
        self.log_collection = self.db['parsing_logs']
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger('olx_parser')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def parse_urls_and_store_details(self):
        urls = self.fetch_urls_from_db()
        if urls:
            for url in urls:
                html_content = self.crawl_webpage(url)
                if html_content:
                    try:
                        details = self.parse_html(html_content, url)
                        if details:
                            self.store_details_in_db(details)
                            self.delete_url_from_db(url)
                            self.logger.info(f"Parsing and storing details for URL: {url} successful")
                            self.store_details_in_csv(details)  # Save details to CSV
                            self.log_parsing(url, True)
                        else:
                            self.logger.warning(f"Failed to parse details for URL: {url}")
                            self.log_parsing(url, False)
                    except Exception as e:
                        self.logger.error(f"Error parsing details for URL: {url}, Error: {str(e)}")
                        with open('parsing_errors.txt', mode='a', encoding='utf-8') as error_file:
                            error_file.write(f"Error parsing details for URL: {url}, Error: {str(e)}\n")
                        self.log_parsing(url, False, error=str(e))
                else:
                    self.logger.error(f"Failed to fetch HTML content for URL: {url}")
                    self.log_parsing(url, False)

    def log_parsing(self, url, success, error=None):
        log_entry = {'url': url, 'success': success, 'error': error}
        self.log_collection.insert_one(log_entry)

    def fetch_urls_from_db(self):
        urls = [entry['url'] for entry in self.url_collection.find()]
        return urls

    def crawl_webpage(self, url):
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
            if response.status_code == 200:
                return response.text
            else:
                self.log_parsing(url, False)  # Pass 'success=False' to indicate failure
                return None
        except requests.exceptions.RequestException as e:
            self.log_parsing(url, False, error=str(e))  # Pass 'success=False' and 'error=e' to indicate failure with error message
            return None

    def parse_html(self, html_content, url):
        selector = Selector(text=html_content)
        product_name = selector.xpath("//h1[@data-aut-id='itemTitle']/text()").get()
        breadcrumbs = selector.xpath("//ol[@class='rui-2Pidb']/li/a/text()").getall()
        price_with_currency = selector.xpath("//span[@data-aut-id='itemPrice']/text()").get()
        price_info = None
        if price_with_currency:
            parts = price_with_currency.split()
            if len(parts) >= 2:
                currency = parts[0].strip()
                amount = ''.join(filter(str.isdigit, price_with_currency))
                price_info = {"amount": amount, "currency": currency}
        image_url = selector.xpath("//img[@data-aut-id='defaultImg']/@src").get()
        location = selector.xpath("//span[@class='_1RkZP']/text()").get()
        type_info = selector.xpath("//span[@data-aut-id='value_type']//text()").get()
        description = selector.xpath("//div[@data-aut-id='itemDescriptionContent']/p/text()").getall()
        description_text = '\n'.join(description)
        title = selector.xpath("//div[@class='eHFQs']/text()").get()
        propertyid = selector.xpath("//div[@class='_1-oS0']/strong").get()
        property_id = re.search(r'\d+', propertyid).group() if propertyid is not None else None
        bathroom = selector.xpath("//span[@data-aut-id='value_bathrooms']/text()").get()
        bedroom = selector.xpath("//span[@data-aut-id='value_rooms']/text()").get()
        return {
            'property_url': url,
            'property_name': product_name,
            'property_id': property_id,
            'breadcrumbs': breadcrumbs,
            'price': price_info,
            'image_url': image_url,
            'description_text': description_text,
            'location': location,
            'seller_name': title,
            'property_type': type_info.strip() if type_info else None,
            'bathrooms': bathroom,
            'bedrooms': bedroom,
        }

    def store_details_in_db(self, details):
        self.details_collection.create_index([('property_url', 1)], unique=True)
        self.details_collection.insert_one(details)

    def delete_url_from_db(self, url):
        self.url_collection.find_one_and_delete({'url': url})

    def store_details_in_csv(self, details):
        fieldnames = ['property_url', 'property_name', 'property_id', 'breadcrumbs', 'price', 'image_url',
                      'description_text', 'location', 'seller_name', 'property_type', 'bathrooms', 'bedrooms']

        details.pop('_id', None)

        with open('olx_details.csv', mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerow(details)

if __name__ == "__main__":
    parser = OlxParser()
    parser.parse_urls_and_store_details()
