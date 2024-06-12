import requests
import curl_cffi.requests
from scrapy.selector import Selector
from pymongo import MongoClient
from settings import MONGO_SETTINGS


class SparScraper:
    def __init__(self, urls_to_scrape):
        self.urls_to_scrape = urls_to_scrape
        self.base_url = "https://www.spar.hu/onlineshop"
        self.mongo_uri = MONGO_SETTINGS['MONGO_URI']
        self.db_name = MONGO_SETTINGS['MONGO_DATABASE']
        self.collection_name = MONGO_SETTINGS['MONGO_COLLECTION']
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    
    def scrape(self):
        all_product_urls = []
        for base_url, base_path in self.urls_to_scrape:
            page = 1
            while True:
                url = base_url.format(page)
                response = curl_cffi.requests.get(url)
                if response.status_code == 200:
                    urls = self.extract_urls(response.json(), base_path)
                    if not urls:
                        break
                    all_product_urls.extend(urls)
                    page += 1
                else:
                    print(f"Failed to retrieve data from {url}: Status code {response.status_code}")
                    break
        self.insert_to_mongodb(all_product_urls)
        self.client.close()

    
    def extract_urls(self, data, base_path):
        hits = data.get('hits', [])
        urls = []
        for hit in hits:
            url = hit.get('masterValues', {}).get('url', '')
            if url:
                full_url = self.base_url + base_path + url 
                urls.append(full_url)
        return urls    
    def extract_section_urls(self):
        response = requests.get(self.base_url)
        if response.status_code == 200:
            selector = Selector(text=response.text)
            brand_urls = selector.xpath("//a[@class='flyout-categories__link']/@href").extract()
            return [self.base_url + url for url in brand_urls]
        else:
            print(f"Failed to retrieve data from {self.base_url}: Status code {response.status_code}")
            return []
    def insert_to_mongodb(self, urls):
        if urls:
            documents = [{'url': url} for url in urls]
            try:
                self.collection.insert_many(documents)
                print(f"Inserted {len(urls)} URLs into MongoDB.")
            except Exception as e:
                print(f"Error inserting documents into MongoDB: {e}")


if __name__ == "__main__":
    urls_to_scrape = [("https://search-spar.spar-ics.com/fact-finder/rest/v4/search/products_lmos_hu?query=*&q=*&hitsPerPage=36&page={}", '')]
    scraper = SparScraper(urls_to_scrape)
    scraper.scrape()
