import re
import requests
from parsel import Selector
from pymongo import MongoClient
import logging

client = MongoClient("mongodb://localhost:27017/")
db = client["emw_agents"]
collection = db["agent_urls"]
parsed_data_collection = db["parsed_data"]
parsed_data_collection.create_index("data_url", unique=True)
logging.basicConfig(filename='filed_data.log', level=logging.ERROR)

class Ewm_Parser:
    name = selector.xpath("//h1[@itemprop='name']//text()").get()
    first_name = name.split()[0]
    middle_name = name.split()[1]
    last_name = name.split()[2]
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
        'first_name': first_name,
        'middle_name': middle_name,
        'last_name': last_name,
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
    for key, value in property_data.items():
        print(f"{key}: {value if value else 'None'}")

if __name__ == "__main__":

        while collection.count_documents({}) > 0:
            agent_doc = property_urls_collection.find_one_and_delete({})
            agent_url = agent_doc["url"]
            data_url = room_url 
            html_content = requests.get(agent_url).text
            selector = Selector(text=html_content)
            parser = EwmParser()
            try:
                agent_data = parser.parse_agent_page(selector, room_url)
                collection.insert_one(agent_data)
                filtered_agent_data = {}
                for key in fieldnames:
                    if key in agent_data:
                        filtered_agent_data[key] = agent_data[key]
                    else:

                        filtered_agent_data[key] = None

                writer.writerow(filtered_agent_data)
            except Exception as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
                print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
    logging.info(f"Scraped {collection.count_documents({})} Agent URLs")