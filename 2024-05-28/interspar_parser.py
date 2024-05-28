import re
import json
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
        try:
            unique_id = selector.xpath("//label[@class='productDetailsArticleNumber']//text()").get()
            competitor_name = 'interspar'
            product_name = selector.xpath("//h1[@class='productDetailsName']//text()[2]").get()

            data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
            json_data = json.loads(data[1])
            brand = json_data['brand'].get('name')
            brand_type = json_data.get("brand", {}).get("@type", "")

            weight = selector.xpath("//label[@class='productDetailsDescription']//text()").get()
            weight_split = weight.split() if weight else ["", ""]
            grammage_quantity = weight_split[0]
            grammage_unit = weight_split[1]

            breadcrumps = selector.xpath("//div[@class='breadcrumbScroll']//ul//li//a//text()").getall()
            producthierarchy_levels = breadcrumps + [""] * (7 - len(breadcrumps))
            producthierarchy_level1, producthierarchy_level2, producthierarchy_level3, producthierarchy_level4, producthierarchy_level5, producthierarchy_level6, producthierarchy_level7 = producthierarchy_levels[:7]

            regular_price = selector.xpath("//label[@class='productDetailsPrice ']//text()").get()
            price_per_unit = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
            currency = json_data.get("offers", {}).get("priceCurrency", "")
            in_stock = "InStock" in json_data.get("offers", {}).get("availability", "")

            xpaths = [
                "//section[@class='productDescription__par breakWords']//text()",
                "//span[@class='productDescription__par infoAttribute']//text()",
                "//div[@class='mainDescription']//text()"
            ]

            product_description = None
            for xpath in xpaths:
                description = selector.xpath(xpath).getall()
                if description:
                    product_description = ' '.join(description).strip()
                    break

            instructions = selector.xpath("//span[contains(@class, 'productDescription__subtitle') and contains(text(), 'Hinweis')]/text()").get()

            country_of_origin = selector.xpath("//li[@class='tradegroupItemValue']//text()").getall()
            allergens = selector.xpath("//span[contains(@class, 'productDescription__subtitle') and contains(text(), 'Allergene')]/text()").get()

            color = selector.xpath("//p[@class='modal-topic-text without-icon']//text()").get()
            model_number = selector.xpath("//td[contains(text(), 'Modell-Nr')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()
            material = selector.xpath("//td[contains(text(), 'Material')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()

            file_name_1 = str(unique_id) + "1.png"

            image_url_1 =   json_data.get("image",'')[0]
            file_name_2 = str(unique_id) + "2.png"

            image_url_2 = json_data.get("image",'')[1]
            file_name_3 = str(unique_id) + "3.png"

            image_url_3 = json_data.get("image",'')[2]

            file_name_4 = str(unique_id) + "4.png"

            image_url_4 = json_data.get("image",'')[3]
            file_name_5  = str(unique_id) + "5.png"

            image_url_5 = json_data.get("image",'')[4]
            file_name_6 = ''

            image_url_6 = ''
            manufacturer_address = selector.xpath("//span[contains(text(),'Hersteller')]/following-sibling::text()").get()
            alcohol_by_volume = selector.xpath("//span[contains(text(),'Alkohol in vol. %')]/following-sibling::text()").get()
            grape_variety = selector.xpath("//span[contains(text(),'Rebsorte(n)')]/following-sibling::text()").get()

            # images = json_data.get("image", [])
            # image_urls = [images[i] if i < len(images) else '' for i in range(6)]
            # file_names = [f"{unique_id}{i+1}.png" for i in range(6)]

            property_data = {
                'data_url': data_url,
                'unique_id': unique_id,
                'competitor_name': competitor_name,
                'product_name': product_name,
                'brand': brand,
                'brand_type': brand_type,
                'grammage_quantity': grammage_quantity,
                'grammage_unit': grammage_unit,
                'producthierarchy_level1': producthierarchy_level1,
                'producthierarchy_level2': producthierarchy_level2,
                'producthierarchy_level3': producthierarchy_level3,
                'producthierarchy_level4': producthierarchy_level4,
                'producthierarchy_level5': producthierarchy_level5,
                'producthierarchy_level6': producthierarchy_level6,
                'producthierarchy_level7': producthierarchy_level7,
                'regular_price': regular_price,
                'price_per_unit': price_per_unit,
                'currency': currency,
                'in_stock': in_stock,
                'product_description': product_description,
                'instructions': instructions,
                'country_of_origin': country_of_origin,
                'allergens': allergens,
                'color': color,
                'model_number': model_number,
                'material': material,

                'file_name_1': file_name_1,
                'image_url_1': image_url_1,
                'file_name_2': file_name_2,
                'image_url_2': image_url_2,

                'file_name_3': file_name_3,
                'image_url_3': image_url_3,

                'file_name_4': file_name_4,
                'image_url_4': image_url_4,

                'file_name_5': file_name_5,
                'image_url_5': image_url_5,

                'file_name_6': file_name_6,
                'image_url_6': image_url_6,


                'manufacturer_address': manufacturer_address,
                'alcohol_by_volume': alcohol_by_volume,
                'grape_variety': grape_variety,
                # 'images': image_urls,
                # 'file_names': file_names,
            }

            return property_data
        except Exception as e:
            logging.error(f"Error parsing data from URL: {data_url}. Error: {str(e)}")
            return None

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

                if agent_data:
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
