import re
import json
import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import time
import random
from curl_cffi import requests as curl_requests
from settings import MONGO_SETTINGS, CUSTOM_HEADERS

logging.basicConfig(filename='failed_data.log', level=logging.ERROR)

class IntersparParser:
    def __init__(self):
        self.mongo_uri = MONGO_SETTINGS['MONGO_URI']
        self.db_name = MONGO_SETTINGS['MONGO_DATABASE']
        self.collection_name = MONGO_SETTINGS['MONGO_COLLECTION']
        self.parsed_data_collection_name = MONGO_SETTINGS['parsed_data_collection']
        self.new_data_collection_name = MONGO_SETTINGS['NEW_DATA_COLLECTION']   
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.parsed_data_collection = self.db[self.parsed_data_collection_name]
        self.new_data_collection = self.db[self.new_data_collection_name] 
        self.headers = CUSTOM_HEADERS
    
    def parse_pages(self, data_url, selector):
        try:
            competitor_name = 'interspar'
            data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
            json_data = json.loads(data[1]) if len(data) > 1 else {}
            unique_id = json_data.get('sku', '')
            brand = json_data.get('brand', {}).get('name', '')
            brand_type = json_data.get('brand', {}).get('@type', '')
            product_name = json_data.get('name', '') if 'name' in json_data else ''
            regular_price = selector.xpath("//span[@class='productDetailsInsteadOfPrice']//text()").get()
            currency = json_data.get("offers", {}).get("priceCurrency", "")

            price_per_unit_text = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
            price_per_unit = re.sub(r'[()\s]', '', price_per_unit_text) if price_per_unit_text else ''

            match = re.search(r'(\d+)\s*(.*)', product_name)
            if match:
                number = match.group(1)
                text_after_number = match.group(2)
            else:
                number = ''
                text_after_number = ''

            grammage_quantity = number
            grammage_unit = text_after_number
            breadcrumbs = selector.xpath("//div[@class='breadcrumbContainer']//ul//li//a//text()").getall()
            producthierarchy_level1 = breadcrumbs[0] if len(breadcrumbs) > 0 else ''
            producthierarchy_level2 = breadcrumbs[1] if len(breadcrumbs) > 1 else ''
            producthierarchy_level3 = breadcrumbs[2] if len(breadcrumbs) > 2 else ''
            producthierarchy_level4 = breadcrumbs[3] if len(breadcrumbs) > 3 else ''
            producthierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) > 4 else ''
            producthierarchy_level6 = breadcrumbs[5] if len(breadcrumbs) > 5 else ''
            producthierarchy_level7 = ''
            selling_price = json_data.get("offers", {}).get("price", "")
            promotion_valid_upto = json_data.get("offers", {}).get("priceValidUntil", "")
            price_was = regular_price
            promotion_description = selector.xpath("//label[@class='productDetailsPromotion ']//text()").get()
            product_description = json_data.get("description", '')
            features = selector.xpath("//div[@class='detail__container']//h1[contains(text(), 'Jellemzők')]/following-sibling::text()").getall()
            storage_instructions = selector.xpath("//div[@class='detail__container ingredient-information']//span[contains(text(), 'Tárolása')]/following-sibling::span[contains(@class, 'detail__content')]//text()").getall()
            ingredients = selector.xpath("//div[@class='detail__container ingredient-information']//span[contains(text(), 'Összetevők')]/following-sibling::span[contains(@class, 'detail__content')]//text()").getall()
            netweight = selector.xpath("//span[contains(text(), 'Nettó mennyiség')]/following-sibling::span//text()").get()
            manufacturer_address = selector.xpath("//section[@class='detail__content']//b[contains(text(), 'Gyártó címe')]/following-sibling::text()").getall()
            distributor_address = selector.xpath("//section[@class='detail__content']//b[contains(text(), 'Forgalmazó címe')]/following-sibling::text()").getall()
            care_instructions = selector.xpath("//span[contains(text(), 'Használati utasítások')]/following-sibling::span//text()").getall()
            nutritional_information_raw = selector.xpath("//h1[contains(text(), 'Átlagos tápértékek')]/following-sibling::table//text()").getall()
            nutritional_information_raw = [item.strip() for item in nutritional_information_raw if item.strip()]

            header = nutritional_information_raw[:2]  
            elements = nutritional_information_raw[2:] 

            nutritional_information = {}
            if len(header) >= 2:
                header_key_part1 = header[0]
                header_key_part2 = header[1]

                for index, item in enumerate(elements):
                    if index % 2 == 0: 
                        current_key = f"{header_key_part1}_{item}_{header_key_part2}"
                    else:
                        nutritional_information[current_key] = item
            warning = selector.xpath("//span[contains(text(), 'Biztonsági figyelmeztetések')]/following-sibling::span//text()").getall()
            allergens = selector.xpath("//span[contains(text(),'Allergének')]/following-sibling::span//text()").get()
            alchol_by_volume = selector.xpath("//div[contains(@class, 'detail__container')]//span[contains(text(),'Alkoholtartalom')]/following-sibling::section//div//text()").get()
            organic_type_text = selector.xpath("//img[@class='detail__badge__image']/@alt").get()
            if organic_type_text == "BIO" :
                organictype = "organic"
            else:
                organictype = 'non-organic'
            country_of_origin = ''
            color = ''
            model_number = ''
            material = ''
            grape_variety = ''
            images = json_data.get("image", [])
            image_urls = images if images else []
            file_names = [f"{unique_id}_{i+1}.png" for i in range(len(image_urls))]
            availability = json_data.get("offers", {}).get("availability", "")
            in_stock = "InStock" in availability

            item = {
                'unique_id': unique_id,
                'competitor_name': competitor_name,
                'store_name': '',
                'store_addressline1': '',
                'store_addressline2': '',
                'store_suburb': '',
                'store_state': '',
                'store_postcode': '',
                'store_addressid': '',
                'extraction_date': '',
                'product_name': product_name,
                'brand': brand,
                'brand_type': brand_type,
                'grammage_quantity': grammage_quantity,
                'grammage_unit': grammage_unit,
                'drained_weight': '',
                'producthierarchy_level1': producthierarchy_level1,
                'producthierarchy_level2': producthierarchy_level2,
                'producthierarchy_level3': producthierarchy_level3,
                'producthierarchy_level4': producthierarchy_level4,
                'producthierarchy_level5': producthierarchy_level5,
                'producthierarchy_level6': producthierarchy_level6,
                'producthierarchy_level7': producthierarchy_level7,
                'regular_price': regular_price,
                'selling_price': selling_price,
                'price_was': price_was,
                'promotion_price': '',
                'promotion_valid_from': '',
                'promotion_valid_upto': promotion_valid_upto,
                'promotion_type': '',
                'percentage_discount': '',
                'promotion_description': promotion_description,
                'package_sizeof_sellingprice': '',
                'per_unit_sizedescription': '',
                'price_valid_from': '',
                'price_per_unit': price_per_unit,
                'multi_buy_item_count': '',
                'multi_buy_items_price_total': '',
                'currency': currency,
                'breadcrumbs': breadcrumbs,
                'pdp_url': data_url,
                'file_name_1': file_names[0] if len(file_names) > 0 else '',
                'image_url_1': image_urls[0] if len(image_urls) > 0 else '',
                'file_name_2': file_names[1] if len(file_names) > 1 else '',
                'image_url_2': image_urls[1] if len(image_urls) > 1 else '',
                'file_name_3': file_names[2] if len(file_names) > 2 else '',
                'image_url_3': image_urls[2] if len(image_urls) > 2 else '',
                'file_name_4': file_names[3] if len(file_names) > 3 else '',
                'image_url_4': image_urls[3] if len(image_urls) > 3 else '',
                'file_name_5': file_names[4] if len(file_names) > 4 else '',
                'image_url_5': image_urls[4] if len(image_urls) > 4 else '',
                'file_name_6': '',
                'image_url_6': '',
                'variants': '',
                'product_description': product_description,
                'instructions': '',
                'storage_instructions': storage_instructions,
                'preparationinstructions': '',
                'instructionforuse': '',
                'country_of_origin': country_of_origin,
                'allergens': allergens,
                'age_of_the_product': '',
                'age_recommendations': '',
                'flavour': '',
                'nutritions': '',
                'nutritional_information': nutritional_information,
                'vitamins': '',
                'labelling': '',
                'grade': '',
                'region': '',
                'packaging': '',
                'receipies': '',
                'processed_food': '',
                'barcode': '',
                'frozen': '',
                'chilled': '',
                'organictype': organictype,
                'cooking_part': '',
                'handmade': '',
                'max_heating_temperature': '',
                'special_information': '',
                'label_information': '',
                'dimensions': '',
                'special_nutrition_purpose': '',
                'feeding_recommendation': '',
                'warranty': '',
                'color': color,
                'model_number': model_number,
                'material': material,
                'usp': '',
                'dosage_recommendation': '',
                'tasting_note': '',
                'food_preservation': '',
                'size': '',
                'rating': '',
                'review': '',
                'manufacturer_address': manufacturer_address,
                'importer_address': '',
                'distributor_address': distributor_address,
                'vinification_details': '',
                'recycling_information': '',
                'return_address': '',
                'alchol_by_volume': alchol_by_volume,
                'beer_deg': '',
                'netcontent': '',
                'netweight': netweight,
                'site_shown_uom': product_name,
                'ingredients': ingredients,
                'random_weight_flag': '',
                'instock': in_stock,
                'promo_limit': '',
                'product_unique_key': unique_id,
                'multibuy_items_pricesingle': '',
                'perfect_match': '',
                'servings_per_pack': '',
                'warning': warning,
                'suitable_for': '',
                'standard_drinks': '',
                'environmental': '',
                'grape_variety': grape_variety,
                'retail_limit': '',
            }

            try:
                self.new_data_collection.insert_one(item)
                logging.info("Item information saved successfully!")
            except Exception as e:
                logging.error(f"Failed to save item information: {str(e)}")
        except Exception as e:
            logging.error(f"Error while parsing data from URL: {data_url}. Error: {str(e)}")

    def scrape_data(self, fixed_delay=5):
        while self.collection.count_documents({}) > 0:
            agent_doc = self.collection.find_one_and_delete({})
            agent_url = agent_doc["url"]

            try:
                response = curl_requests.get(agent_url, headers=CUSTOM_HEADERS, impersonate="safari15_3")

                response.raise_for_status()
                html_content = response.text
                selector = Selector(text=html_content)
                self.parse_pages(agent_url, selector)

                time.sleep(fixed_delay + random.uniform(1, 3))

            except requests.exceptions.RequestException as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
                print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")

        logging.info(f"Scraped {self.parsed_data_collection.count_documents({})} Agent URLs")

if __name__ == "__main__":
    parser = IntersparParser()
    parser.scrape_data()
