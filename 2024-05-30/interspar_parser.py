import re
import json
import cloudscraper
import logging
import time
import random
from fake_useragent import UserAgent
from parsel import Selector
import pymongo
from pymongo import MongoClient
from settings import MONGO_SETTINGS,CUSTOM_HEADERS

logging.basicConfig(filename='failed_data.log', level=logging.ERROR)

class IntersparParser:
    def __init__(self):
        self.client = MongoClient(MONGO_SETTINGS['MONGO_URI'])
        self.db = self.client[MONGO_SETTINGS['MONGO_DATABASE']]
        self.collection = self.db[MONGO_SETTINGS['MONGO_COLLECTION']]
        self.parsed_data_collection = self.db["parsed_data"]
        self.parsed_data_collection.create_index("data_url", unique=True)

        self.user_agent = UserAgent()
        self.scraper = cloudscraper.create_scraper() 
    def parse_pages(self, data_url, selector):
        try:
            unique_id = re.search(r'\d+', selector.xpath("//label[@class='productDetailsArticleNumber']//text()").get() or '') or None
            unique_id = unique_id.group() if unique_id else None
            competitor_name = 'interspar'
            store_name = ''
            store_addressline1 = ''
            store_addressline2 = ''
            store_suburb = ''
            store_state = ''
            store_postcode = ''
            store_addressid = ''
            extraction_date = ''
            product_name = selector.xpath("//h1[@class='productDetailsName']//text()[2]").get()
            data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
            json_data = json.loads(data[1]) if len(data) > 1 else {}
            brand = json_data.get('brand', {}).get('name', '')
            brand_type = json_data.get('brand', {}).get('@type', '')

            weight = selector.xpath("//label[@class='productDetailsDescription']//text()").get()
            weight_split = weight.split() if weight else ['', '']
            grammage_quantity = weight_split[0]
            grammage_unit = weight_split[1] if len(weight_split) > 1 else ''

            breadcrumps = selector.xpath("//div[@class='breadcrumbScroll']//ul//li//a//text()").getall()
            producthierarchy = breadcrumps + [''] * (7 - len(breadcrumps))  
            regular_price = selector.xpath("//label[@class='productDetailsPrice ']//text()").get()
            price_per_unit = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
            currency = json_data.get("offers", {}).get("priceCurrency", "")
            
            product_description = self.extract_description(selector)

            country_of_origin = selector.xpath("//li[@class='tradegroupItemValue']//text()").getall()
            allergens = selector.xpath("//span[contains(@class, 'productDescription__subtitle') and contains(text(), 'Allergene')]/text()").get()

            color = selector.xpath("//p[@class='modal-topic-text without-icon']//text()").get()
            model_number = selector.xpath("//td[contains(text(), 'Modell-Nr')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()
            material = selector.xpath("//td[contains(text(), 'Material')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()
            manufacturer_address = selector.xpath("//span[contains(text(),'Hersteller')]/following-sibling::text()").get()
            alchol_by_volume = selector.xpath("//span[contains(text(),'Alkohol in vol. %')]/following-sibling::text()").get()
            grape_variety = selector.xpath("//span[contains(text(),'Rebsorte(n)')]/following-sibling::text()").get()
            
            images = json_data.get("image", [])
            image_urls = {f'file_name_{i+1}': f'{unique_id}{i+1}.png' for i in range(len(images))}
            image_urls.update({f'image_url_{i+1}': url for i, url in enumerate(images)})

            availability = json_data.get("offers", {}).get("availability", "")
            in_stock = "InStock" in availability

            item = {}
            item['unique_id'] = unique_id
            item['competitor_name'] = competitor_name
            item['store_name'] = store_name
            item['store_addressline1'] = store_addressline1
            item['store_addressline2'] = store_addressline2
            item['store_suburb'] = store_suburb
            item['store_state'] = store_state
            item['store_postcode'] = store_postcode
            item['store_addressid'] = store_addressid
            item['extraction_date'] = extraction_date
            item['product_name'] = product_name
            item['brand'] = brand
            item['brand_type'] = brand_type
            item['grammage_quantity'] = grammage_quantity
            item['grammage_unit'] = grammage_unit
            item['drained_weight'] = ''
            item['producthierarchy_level1'] = producthierarchy[0]
            item['producthierarchy_level2'] = producthierarchy[1]
            item['producthierarchy_level3'] = producthierarchy[2]
            item['producthierarchy_level4'] = producthierarchy[3]
            item['producthierarchy_level5'] = producthierarchy[4]
            item['producthierarchy_level6'] = producthierarchy[5]
            item['producthierarchy_level7'] = producthierarchy[6]
            item['regular_price'] = regular_price
            item['selling_price'] = ''
            item['price_was'] = regular_price
            item['promotion_price'] = ''
            item['promotion_valid_from'] = ''
            item['promotion_valid_upto'] = ''
            item['promotion_type'] = ''
            item['percentage_discount'] = ''
            item['promotion_description'] = ''
            item['package_sizeof_sellingprice'] = ''
            item['per_unit_sizedescription'] = ''
            item['price_valid_from'] = ''
            item['price_per_unit'] = price_per_unit
            item['multi_buy_item_count'] = ''
            item['multi_buy_items_price_total'] = ''
            item['currency'] = currency
            item['breadcrumb'] = ''
            item['pdp_url'] = data_url
            item['variants'] = ''
            item['product_description'] = product_description
            item['instructions'] = ''
            item['storage_instructions'] = ''
            item['preparationinstructions'] = ''
            item['instructionforuse'] = ''
            item['country_of_origin'] = country_of_origin
            item['allergens'] = allergens
            item['age_of_the_product'] = ''
            item['age_recommendations'] = ''
            item['flavour'] = ''
            item['nutritions'] = ''
            item['nutritional_information'] = ''
            item['vitamins'] = ''
            item['labelling'] = ''
            item['grade'] = ''
            item['region'] = ''
            item['packaging'] = ''
            item['receipies'] = ''
            item['processed_food'] = ''
            item['barcode'] = ''
            item['frozen'] = ''
            item['chilled'] = ''
            item['organictype'] = ''
            item['cooking_part'] = ''
            item['handmade'] = ''
            item['max_heating_temperature'] = ''
            item['special_information'] = ''
            item['label_information'] = ''
            item['dimensions'] = ''
            item['special_nutrition_purpose'] = ''
            item['feeding_recommendation'] = ''
            item['warranty'] = ''
            item['color'] = color
            item['model_number'] = model_number
            item['material'] = material
            item['usp'] = ''
            item['dosage_recommendation'] = ''
            item['tasting_note'] = ''
            item['food_preservation'] = ''
            item['size'] = ''
            item['rating'] = ''
            item['review'] = ''
            item['manufacturer_address'] = manufacturer_address
            item['importer_address'] = ''
            item['distributor_address'] = ''
            item['vinification_details'] = ''
            item['recycling_information'] = ''
            item['return_address'] = ''
            item['alchol_by_volume'] = alchol_by_volume
            item['beer_deg'] = ''
            item['netcontent'] = ''
            item['netweight'] = ''
            item['site_shown_uom'] = ''
            item['ingredients'] = ''
            item['random_weight_flag'] = ''
            # item['availability'] = availability
            # item['in_stock'] = in_stock
            item['instock'] = in_stock
            item['promo_limit'] = ''
            item['product_unique_key'] = unique_id
            item['multibuy_items_pricesingle'] = ''
            item['perfect_match'] = ''
            item['servings_per_pack'] = ''
            item['warning'] = ''
            item['suitable_for'] = ''
            item['standard_drinks'] = ''
            item['environmental'] = ''
            item['grape_variety'] = grape_variety
            item['retail_limit'] = ''

            try:

                self.parsed_data_collection.insert_one(item)
                logging.info(f"Data successfully parsed and stored from URL: {data_url}")
            except pymongo.errors.DuplicateKeyError:
                logging.error(f"Data already exists for URL: {data_url}. Skipping insertion.")
        except Exception as e:
            logging.error(f"Error parsing page: {e}")

        return None 
    def extract_description(self, selector):
        xpaths = [
            "//section[@class='productDescription__par breakWords']//text()",
            "//span[@class='productDescription__par infoAttribute']//text()",
            "//div[@class='mainDescription']//text()"
        ]

        for xpath in xpaths:
            description = selector.xpath(xpath).getall()
            if description:
                return ' '.join(description).strip()
        
        return ''
        

    def scrape_data(self, fixed_delay=5):
        while self.collection.count_documents({}) > 0:
            agent_doc = self.collection.find_one_and_delete({})
            agent_url = agent_doc["url"]

            try:
                headers = CUSTOM_HEADERS 

                response = self.scraper.get(agent_url, headers=headers)
                response.raise_for_status()
                selector = Selector(text=response.text)
                agent_data = self.parse_pages(agent_url, selector)

                if agent_data:
                    self.parsed_data_collection.insert_one(agent_data)
                    logging.info(f"Successfully scraped data from URL: {agent_url}")
                else:
                    logging.error(f"No data parsed from URL: {agent_url}")

                time.sleep(fixed_delay + random.uniform(1, 3))

            except Exception as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
                print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")

                if response.status_code == 403:
                    print("Encountered 403 Forbidden error. Trying again after some time.")
                    time.sleep(random.uniform(10, 30))

        logging.info(f"Scraped {self.parsed_data_collection.count_documents({})} Agent URLs")
if __name__ == "__main__":
    parser = IntersparParser()
    parser.scrape_data()