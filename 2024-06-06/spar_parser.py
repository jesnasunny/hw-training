import re
import json
from curl_cffi import requests as curl_requests
import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import time
from settings import MONGO_SETTINGS,CUSTOM_HEADERS
logging.basicConfig(filename='failed_data.log', level=logging.ERROR)

class IntersparParser:
    def __init__(self):
        self.mongo_uri = MONGO_SETTINGS['MONGO_URI']
        self.db_name = MONGO_SETTINGS['MONGO_DATABASE']
        self.collection_name = MONGO_SETTINGS['MONGO_COLLECTION']
        self.parsed_data_collection_name = MONGO_SETTINGS['parsed_data_collection'] 
        self.client = MongoClient(self.mongo_uri) 
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.parsed_data_collection = self.db[self.parsed_data_collection_name]
        self.headers = CUSTOM_HEADERS    
    def parse_pages(self, data_url, selector):
        try:
            competitor_name = 'interspar'
            store_name = ''
            store_addressline1 = ''
            store_addressline2 = ''
            store_suburb = ''
            store_state = ''
            store_postcode = ''
            store_addressid = ''
            extraction_date = ''
            data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
            json_data = json.loads(data[1]) if len(data) > 1 else {}
            unique_id = json_data.get('sku','')   
            brand = json_data.get('brand', {}).get('name', '')
            brand_type = json_data.get('brand', {}).get('@type', '')
            product_name = json_data.get('name','')  
            regular_price = json_data.get("offers", {}).get("price", "")
            currency = json_data.get("offers", {}).get("priceCurrency", "")

            price_per_unit_text = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
            price_per_unit =  re.sub(r'[()\s]', '', price_per_unit_text)
            match = re.search(r'(\d+)\s*(.*)', product_name)
            if match:
                number = match.group(1)
                text_after_number = match.group(2)
                return number, text_after_number
            else:
                return None, None


            grammage_quantity = number
            grammage_unit = text_after_number

            breadcrumbs = selector.xpath("//div[@class='breadcrumbContainer']//ul//li//a//text()").getall()
            producthierarchy_level1 =   breadcrumbs[0] if len(breadcrumbs) > 0 else ''
            producthierarchy_level2 =  breadcrumbs[1] if len(breadcrumbs) > 0 else ''
            producthierarchy_level3 =  breadcrumbs[2] if len(breadcrumbs) > 0 else ''
            producthierarchy_level4 =  breadcrumbs[3] if len(breadcrumbs) > 0 else ''
            producthierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) > 0 else ''
            producthierarchy_level6 =  breadcrumbs[5] if len(breadcrumbs) > 0 else ''
            producthierarchy_level7 =  ''
            regular_price = selector.xpath("//span[@class='productDetailsInsteadOfPrice']//text()").get()
            selling_price = json_data.get("offers", {}).get("price", "")
            currency = json_data.get("offers", {}).get("priceCurrency", "")
            promotion_valid_upto= json_data.get("offers", {}).get("priceValidUntil", "")
            price_was=regular_price
            promotion_description=selector.xpath("//label[@class='productDetailsPromotion ']//text()").get()
            product_description=json_data.get("description",'')
            features = urls =selector.xpath("//div[@class='detail__container']//h1[contains(text(), 'Jellemzők')]/following-sibling::text()").getall()
            storage_instructions = selector.xpath("//div[@class='detail__container ingredient-information']//h1[contains(text(), 'Tárolása')]/following-sibling::span[contains(@class, 'detail__content')]//text()").getall()
            ingredients = selector.xpath("//div[@class='detail__container ingredient-information']//h1[contains(text(), 'Összetevők')]/following-sibling::span[contains(@class, 'detail__content')]//text()").getall()
            netweight = selector.xpath("//span[contains(text(), 'Nettó mennyiség')]/following-sibling::span//text()").get()
            manufacturer_address = selector.xpath("//section[@class='detail__content']//b[contains(text(), 'Gyártó címe')]/following-sibling::text()").getall()
            distributor_address =  selector.xpath("//section[@class='detail__content']//b[contains(text(), 'Forgalmazó címe')]/following-sibling::text()").getall()
            ingredients = selector.xpath("//span[contains(text(), 'Összetevők')]/following-sibling::span//text()").get()
            care_instructions = selector.xpath("//span[contains(text(), 'Használati utasítások')]/following-sibling::span//text ()").getall()
            nutritional_information = selector.xpath("//h1[contains(text(), 'Átlagos tápértékek')]/following-sibling::table//text()").getall()
            warning = selector.xpath("//span[contains(text(), 'Biztonsági figyelmeztetések')]/following-sibling::span //text()").getall()
            allergens = selector.xpath("//span[contains(text(),'Allergének')]/following-sibling::span//text()").get()

            alchol_by_volume=selector.xpath("//div[contains(@class, 'detail__container')]//span[contains(text(),'Alkoholtartalom')]/following-sibling::section//div//text()").get()

            country_of_origin =''

            color = ''
            model_number = ''
            material = ''

            

            grape_variety = ''
            images = json_data.get("image", []) 
            file_name_1 = str(unique_id)+"_1.png"
            image_url_1 = images[0] if images else ''
            file_name_2 = str(unique_id)+'_2.png'
            image_url_2 = images[1] if images else ''
            file_name_3 = str(unique_id)+'_3.png'
            image_url_3 = images[2] if images else ''
            file_name_4 = str(unique_id)+'_4.png'
            image_url_4 = images[3] if images else ''
            file_name_5 = str(unique_id)+'_5.png'
            image_url_5 = images[4] if images else ''
            file_name_6 = ''
            image_url_6 = ''
            availability = json_data.get("offers", {}).get("availability", "")
            in_stock = "InStock" in availability
           
            retail_limit = ''

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
            item['producthierarchy_level1'] =  producthierarchy_level1
            item['producthierarchy_level2'] =  producthierarchy_level2
            item['producthierarchy_level3'] =  producthierarchy_level3
            item['producthierarchy_level4'] =  producthierarchy_level4
            item['producthierarchy_level5'] =  producthierarchy_level5
            item['producthierarchy_level6'] =  producthierarchy_level6
            item['producthierarchy_level7'] =  producthierarchy_level7
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
            item['breadcrumbs'] = breadcrumbs
            item['pdp_url'] = data_url
            item['file_name_1'] = file_name_1
            item['image_url_1'] = image_url_1
            item['file_name_2'] = file_name_2 
            item['image_url_2'] = image_url_2
            item['file_name_3'] = file_name_3 
            item['image_url_3' ]= image_url_3
            item['file_name_4'] = file_name_4 
            item['image_url_4'] = image_url_4
            item['file_name_5'] = file_name_5 
            item['image_url_5'] = image_url_5
            item['file_name_6' ]= file_name_6 
            item['image_url_6'] = image_url_6

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
            item['site_shown_uom'] = product_name
            item['ingredients'] = ''
            item['random_weight_flag'] = ''
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
            
            logging.info(item)
            try:
                self.parsed_data_collection.insert_one(item)
                logging.info("item information saved successfully!")
            except Exception as e:
                logging.error(f"Failed to save item information: {str(e)}")
        except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {str(e)}")


    def scrape_data(self, fixed_delay=5):
            while self.collection.count_documents({}) > 0:
                agent_doc = self.collection.find_one_and_delete({})
                agent_url = agent_doc["url"]

                try:
                    headers = {
                        "Accept": "*/*",
                        "Accept-Encoding": "gzip, deflate, br, zstd",
                        "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ml;q=0.6",
                        "If-None-Match": '"dda1f-qsY9kvw0LRKNqImtxdKscrDn4Wo-gzip"',
                        "Priority": "u=1, i",
                        "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": '"Linux"',
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "no-cors",
                        "Sec-Fetch-Site": "same-origin",
                        "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                    }

                    response = curl_requests.get(agent_url, headers=headers, impersonate="safari15_3")
                    response.raise_for_status()
                    html_content = response.text
                    selector = Selector(text=html_content)
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

            logging.info(f"Scraped {self.parsed_data_collection.count_documents({})} Agent URLs")

if __name__ == "__main__":
    parser = IntersparParser()
    parser.scrape_data()