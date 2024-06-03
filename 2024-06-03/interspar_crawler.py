import requests
from pymongo import MongoClient
from settings import MONGO_SETTINGS

class IntersparScraper:
    def __init__(self, base_urls_with_paths, max_pages=300):
        self.base_urls_with_paths = base_urls_with_paths
        self.max_pages = max_pages
        self.mongo_uri = MONGO_SETTINGS['MONGO_URI']
        self.db_name = MONGO_SETTINGS['MONGO_DATABASE']
        self.collection_name = MONGO_SETTINGS['MONGO_COLLECTION']
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
    
    def scrape(self):
        all_product_urls = []
        for base_url, base_path in self.base_urls_with_paths:
            for page in range(1, self.max_pages + 1):
                url = base_url.format(page)
                response = requests.get(url)
                if response.status_code == 200:
                    urls = self.extract_urls(response.json(), base_path)
                    if not urls:
                        break
                    all_product_urls.extend(urls)
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
                full_url = base_path + url
                urls.append(full_url)
        return urls
    
    def insert_to_mongodb(self, urls):
        if urls:
            documents = [{'url': url} for url in urls]
            try:
                self.collection.insert_many(documents)
                print(f"Inserted {len(urls)} URLs into MongoDB.")
            except Exception as e:
                print(f"Error inserting documents into MongoDB: {e}")

if __name__ == "__main__":
    base_urls_with_paths = [
        ("https://search-spar.spar-ics.com/fact-finder/rest/v4/search/products_lmos_at?query=*&page={}", "https://www.interspar.at/shop/lebensmittel"),
        ("https://search-spar.spar-ics.com/fact-finder/rest/v4/search/products_ww_at?query=*&page={}", "https://www.interspar.at/shop/weinwelt"),
        ("https://search-spar.spar-ics.com/fact-finder/rest/v4/search/products_nf2_at?query=*&page={}", "https://www.interspar.at/shop/haushalt")
    ]

    scraper = IntersparScraper(base_urls_with_paths)
    scraper.scrape()
