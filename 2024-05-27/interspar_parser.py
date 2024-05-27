import re
import cloudscraper
from parsel import Selector
from pymongo import MongoClient
import logging
import time

logging.basicConfig(filename='failed_data.log', level=logging.ERROR)

class IntersparParser:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["interspar_database"]
        self.collection = self.db['product_urls']
        self.parsed_data_collection = self.db["parsed_data"]
        self.parsed_data_collection.create_index("data_url", unique=True)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.scraper = cloudscraper.create_scraper()

    def parse_pages(self, data_url, selector):
        unique_id = selector.xpath("//label[@class='productDetailsArticleNumber']//text()").get()
        competitor_name = interspar
        store_addressline2 = ''
        store_suburb = ''
        store_state = ''
        store_postcode = ''
        store_addressid = ''
        extraction_date = ''
        product_name = selector.xpath("//h1[@class='productDetailsName']//text()[2]").get()
        data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
        json_data = json.loads(data[1])
        brand = json_data['brand'].get('name')
        brand_type = json_name.get("brand", {}).get("@type", "")

        weight = selector.xpath("//label[@class='productDetailsDescription']//text()").get()
        wei_ght = weight.split()
        grammage_quantity = wei_ght[0] 
        grammage_unit = wei_ght[1]
        drained_weight = ''
        breadcrumps = selector.xpath("//div[@class='breadcrumbScroll']//ul//li//a//text()").getall()
        producthierarchy_level1 = breadcrumps[0] 
        producthierarchy_level2 = breadcrumps[1]
        producthierarchy_level3 = breadcrumps[2]
        producthierarchy_level4 = breadcrumps[3]
        producthierarchy_level5 = breadcrumps[4]
        producthierarchy_level6 = breadcrumps[5]
        producthierarchy_level7 = breadcrumps[6]
        pdp_url = data_url
        variants = ''
        price = selector.xpath("//label[@class='productDetailsPrice ']//text()").get()
        perunitprice = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
        contact = selector.xpath("//ul[@class='descColumn pdpTabsColumn']//li//text()").getall()
        country = selector.xpath("//li[@class='tradegroupItemValue']//text()").getall()
        img = selector.xpath("//img[contains(@src, 'https://')]/@src").get()
        des = selector.xpath("//section[@class='productDescription__par breakWords']//text()").getall()
        colour = selector.xpath("//p[@class='modal-topic-text without-icon']//text()").get()

        property_data = {
            'data_url': data_url,
            'name': name,
            'article': article,
            'weight': weight,
            'price': price,
            'perunitprice': perunitprice,
            'contact': contact,
            'country': country,
            'img': img,
            'des': des,
            'colour': colour,
        }

        return property_data

    def scrape_data(self, fixed_delay=5):
        while self.collection.count_documents({}) > 0:
            agent_doc = self.collection.find_one_and_delete({})
            agent_url = agent_doc["url"]

            try:
                response = self.scraper.get(agent_url, headers=self.headers)
                response.raise_for_status() 
                html_content = response.text
                selector = Selector(text=html_content)
                agent_data = self.parse_pages(agent_url, selector)

                self.parsed_data_collection.insert_one(agent_data)
                logging.info(f"Successfully scraped data from URL: {agent_url}")

                time.sleep(fixed_delay)

            except Exception as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
                print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")

        logging.info(f"Scraped {self.parsed_data_collection.count_documents({})} Agent URLs")

if __name__ == "__main__":
    parser = IntersparParser()
    parser.scrape_data()
