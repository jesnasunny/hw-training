import re
import json
from curl_cffi import requests as curl_requests
import requests
from parsel import Selector
from pymongo import MongoClient
import logging
import time
import random
from fake_useragent import UserAgent
from settings import MONGO_SETTINGS
import js2py
from lxml import html

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
        self.user_agent = UserAgent()   
    # def fetch_js_from_html_url(self,url,xpath):
    #     response=requests.get(url)
    #     response.raise_for_status()
    #     tree= html.formstring(response.content)
    #     script_elements= tree.xpath(xpath)
    #     if script_elements:
    #         return script_elements[0].text
    #         return None
    # def extract_product_stock(self,js_code):
    #     context=js2py.EvalJs()
    #     context.execute(js_code)
    #     product_stock=context.spardtm.product[0].productStock
    #     return product_stock
    def parse_pages(self, data_url, selector):
        try:
            unique_id = None
            
            unique_id_match = re.search(r'\d+', selector.xpath("//label[@class='productDetailsArticleNumber']//text()").get() or '')
            unique_id = unique_id_match.group() if unique_id_match else None
            
            if not unique_id:
                unique_id_match = re.search(r'\d+',selector.xpath("//h3[@class='productDetail__subtitle']//text()").get() or '')
                unique_id = unique_id_match.group() if unique_id_match else None
            competitor_name = 'interspar'
            store_addressline2 = ''
            store_suburb = ''
            store_state = ''
            store_postcode = ''
            store_addressid = ''
            extraction_date = ''
            # product_name =  None
            # product_name_text = selector.xpath("//h1[@class='productDetailsName']//text()[2]").get()
            # if not product_name_text:
            #     product_name_text=  selector.xpath("//h1[@class='productDetail__title']//text()").get()
            # if product_name_text :
            #     product_name=product_name_text
            data = selector.xpath("//script[@type='application/ld+json']//text()").getall()
            json_data = json.loads(data[1]) if len(data) > 1 else {}
            brand = json_data.get('brand', {}).get('name', '')
            brand_type = json_data.get('brand', {}).get('@type', '')
            product_name = json_data.get('name','')            

            weight = selector.xpath("//label[@class='productDetailsDescription']//text()").get()
            weight_split = weight.split() if weight else ['', '']
            grammage_quantity = weight_split[0]
            grammage_unit = weight_split[1] if len(weight_split) > 1 else ''

            breadcrumbs = None 
            breadcrumbs_text = selector.xpath("//div[@class='breadcrumbScroll']//ul//li//a//text()").getall()
            if not breadcrumbs_text:
                breadcrumbs_text = selector.xpath("//ul[@class='breadcrumbs__list']//li//text()").getall()
            if breadcrumbs_text :
                breadcrumbs = breadcrumbs_text
            producthierarchy_level1 =   breadcrumbs[0] if len(breadcrumbs) > 0 else ''
            producthierarchy_level2 =  breadcrumbs[1] if len(breadcrumbs) > 0 else ''
            producthierarchy_level3 =  breadcrumbs[2] if len(breadcrumbs) > 0 else ''
            producthierarchy_level4 =  breadcrumbs[3] if len(breadcrumbs) > 0 else ''
            producthierarchy_level5 = breadcrumbs[4] if len(breadcrumbs) > 0 else ''
            producthierarchy_level6 =  breadcrumbs[5] if len(breadcrumbs) > 0 else ''
            producthierarchy_level7 =  ''

            regular_price = json_data.get("offers", {}).get("price", "")
            price_per_unit = selector.xpath("//label[@class='productDetailsPricePerUnit']//text()").get()
            currency = json_data.get("offers", {}).get("priceCurrency", "")
            
            # product_description = self.extract_description(selector)

            country_of_origin_list = selector.xpath("//li[@class='tradegroupItemValue']//text()").getall()
            country_of_origin = country_of_origin_list[2] if len(country_of_origin_list) > 2 else ''
            allergens = selector.xpath("//span[contains(@class, 'productDescription__subtitle') and contains(text(), 'Allergene')]/text()").get()

            color = None
            color_text = selector.xpath("//p[@class='modal-topic-text without-icon']//text()").get()
            if not color_text:
                color_text = selector.xpath("//td[contains(text(),'Farbe')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()
                color = color_text
            model_number = selector.xpath("//td[contains(text(), 'Modell-Nr')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()




            material = None

            material_text = selector.xpath("//td[contains(text(), 'Material')]/following-sibling::td[@class='productDescription__cellRight']/text()").get()
            if not material_text:
                material_text =  selector.xpath("//td[contains(text(), 'Material')]/following-sibling::td[@class='productDescription__cellLeft']/text()").get()     # //Material
# <td class="productDescription__cellLeft">
# Material</td>
            if material_text :
                material = material_text

            manufacturer_address = selector.xpath("//span[contains(text(),'Hersteller')]/following-sibling::text()").get()
            alchol_by_volume = selector.xpath("//span[contains(text(),'Alkohol in vol. %')]/following-sibling::text()").get()
            grape_variety = selector.xpath("//span[contains(text(),'Rebsorte(n)')]/following-sibling::text()").get()
            product_description=json_data.get("description",'')
            # Image URLs
            # images = json_data.get("image", [])
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
           
            # js_code=self.fetch_js_from_html_url(data_url,'//script[contains(text(),"spardtm.product")]/text()')
            # product_stock=None
            # if js_code:
            #     try:
            #         product_stock=self.extract_product_stock(js_code)
            #     except Exception as e:
            #         logging.error("error in executing:{str(e)}")
            retail_limit = ''

            item = {}
            item['unique_id'] = unique_id
            item['competitor_name'] = competitor_name
            # item['store_name'] = store_name
            # item['store_addressline1'] = store_addressline1
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
            
            logging.info(item)
            try:
                self.parsed_data_collection.insert_one(item)
                logging.info("item information saved successfully!")
            except Exception as e:
                logging.error(f"Failed to save item information: {str(e)}")
        except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {str(e)}")

    # def extract_description(self, selector):
    #     xpaths = [
    #         "//section[@class='productDescription__par breakWords']//text()",
    #         "//span[@class='productDescription__par infoAttribute']//text()",
    #         "//div[@class='mainDescription']//text()"
    #     ]

    #     for xpath in xpaths:
    #         description = selector.xpath(xpath).getall()
    #         if description:
    #             return ' '.join(description).strip()
        
    #     return ''

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