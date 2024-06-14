import requests
import xml.etree.ElementTree as ET
from pipeline import uri, db_name, MongoConnection
from settings import url
import logging

class DmCrawler:
    def __init__(self,sitemap_url) :
        self.sitemap_url = sitemap_url
        self.mongoconnection = MongoConnection()

    def save_urls(self):
        collection = self.mongoconnection.db["product_urls"]

        for sitemap_url in self.sitemap_url:
            response = requests.get(sitemap_url)
            if response.status_code == 200:
                root = ET.fromstring(response.content)    
                urls = [url.text for url in root.iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]           
                for url in urls:
                    try:
                        collection.insert_one({'url': url})
                        logging.info(f"Inserted URL into MongoDB: {url}")
                    except Exception as e:
                        logging.error(f"Failed to insert URL {url}: {e}")
            else:
                logging.error(f"Failed to retrieve the sitemap from {sitemap_url}. Status code: {response.status_code}")    

if __name__ == "__main__":
    sitemap_url=url
    sitemap_fetch = DmCrawler(sitemap_url)
    sitemap_fetch.save_urls()
