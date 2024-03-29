import requests
from pymongo import MongoClient
import logging
from datetime import datetime
from parsel import Selector

class MongoHandler(logging.Handler):
    def __init__(self, collection):
        super().__init__()
        self.collection = collection

    def emit(self, record):
        log_entry = {
           
            "status": self.format(record)
        }
        self.collection.insert_one(log_entry)

class OlxScraper:
    def __init__(self, start_url):
        self.start_url = start_url
        self.base_url = "https://www.olx.in"
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['olx_data']
        self.url_collection = self.db['property_urls']
        self.details_collection = self.db['property_details']

        self.url_collection.create_index([('url', 1)], unique=True)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = MongoHandler(self.db['logs'])
        formatter = logging.Formatter('%(status)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def crawl_webpage(self, url):
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
            if response.status_code == 200:
                self.logger.info(f"Successfully fetched page: {url}")
                return response.text 
            else:
                self.logger.error(f"Failed to fetch page {url}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch page {url} due to error: {e}")
            return None

    def save_url_to_mongodb(self, url):
        try:
            self.url_collection.insert_one({'url': url})
            # self.logger.info(f"Property URL successfully saved to MongoDB: {url}")
        except Exception as e:
            self.logger.error(f"Error")

    def start_scraping(self):
        page_number = 1
        while True:
            page_url = f"{self.start_url}?page={page_number}"
            html_content = self.crawl_webpage(page_url)
            if html_content:
                urls = self.extract_urls(html_content)
                if not urls:
                    break
                for url in urls:
                    self.save_url_to_mongodb(url)
                    self.logger.info(f"Processed successfully: {url}")
                page_number += 1
            else:
                break

    def extract_urls(self, html_content):
        selector = Selector(text=html_content)
        products = selector.xpath("//li[@data-aut-id='itemBox']")
        urls = []
        for product in products:
            product_link = product.xpath(".//a/@href").get()
            if product_link:
                urls.append(self.base_url + product_link)
        return urls

if __name__ == "__main__":
    start_url = "https://www.olx.in/kozhikode_g4058877/for-rent-houses-apartments_c1723"
    scraper = OlxScraper(start_url)
    scraper.start_scraping()
