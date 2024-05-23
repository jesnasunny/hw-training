import requests
from parsel import Selector
from pymongo import MongoClient
import logging

client = MongoClient("mongodb://localhost:27017/")
db = client["ewm_agents"]
url_collection = db["urls_collection"]
parsed_data_collection = db["parsed_data"]
parsed_data_collection.create_index("data_url", unique=True)
logging.basicConfig(filename='filed_data.log', level=logging.ERROR)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

class EwmParser:
    def parse_agent_page(self, selector, data_url):
        title = selector.xpath("//div[@class='widget']//div[3]//text()").get()
        office = selector.xpath("//div[@itemprop='name']//strong//text()").get()
        description = selector.xpath("//div[@itemprop='description']//p//text()").get()
        languages = selector.xpath("//div[@class='cell single-sidebar']//h3//text()").get()
        address = selector.xpath("//div[@itemprop='streetAddress']//text()").get()
        city = selector.xpath("//span[@itemprop='addressLocality']//text()").get()
        state = selector.xpath("//span[@itemprop='addressRegion']//text()").get()
        zipcode = selector.xpath("//span[@itemprop='postalCode']//text()").get()
        office_number = selector.xpath("//div[contains(text(), 'Office:')]/a/text()").get()
        personal_number = selector.xpath("//div[contains(text(), 'Mobile:')]/a/text()").get()
        website = selector.xpath("(//div[@class='mt-4']//a)[3]/@href").get()

        property_data = {
            'data_url': data_url,
            'title': title,
            'office': office,
            'description': description,
            'languages': languages,
            'address': address,
            'city': city,
            'state': state,
            'zipcode': zipcode,
            'office_number': office_number,
            'personal_number': personal_number,
            'website': website,
        }

        return property_data

if __name__ == "__main__":
    parser = EwmParser()

    while url_collection.count_documents({}) > 0:
        agent_doc = url_collection.find_one_and_delete({})
        agent_url = agent_doc["url"]
        data_url = agent_url
        response = requests.get(agent_url, headers=headers)

        if response.status_code != 200:
            logging.error(f"Failed to fetch URL: {agent_url}. Status code: {response.status_code}")
            print(f"Failed to fetch URL: {agent_url}. Status code: {response.status_code}")
            continue
        
        html_content = response.text
        selector = Selector(text=html_content)

        try:
            agent_data = parser.parse_agent_page(selector, data_url)
            if agent_data:
                parsed_data_collection.insert_one(agent_data)
        except Exception as e:
            error_message = str(e)
            logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
            print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")

    logging.info(f"Scraped {parsed_data_collection.count_documents({})} Agent URLs")
