import re
import json
import cloudscraper
from parsel import Selector
from pymongo import MongoClient
import logging
import time
import random
from fake_useragent import UserAgent
from settings import MONGO_SETTINGS


logging.basicConfig(filename='failed_data.log', level=logging.ERROR)

class IntersparParser:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["intersparr_database"]
        self.collection = self.db['product_urls']
        self.parsed_data_collection = self.db["parsed_data"]
        self.parsed_data_collection.create_index("data_url", unique=True)

        self.user_agent = UserAgent()
        self.scraper = cloudscraper.create_scraper()

    def parse_pages(self, data_url, selector):
        try:
            unique_id = re.search(r'\d+', selector.xpath("//label[@class='productDetailsArticleNumber']//text()").get() or '') or None
            unique_id = unique_id.group() if unique_id else None
            competitor_name = 'interspar'
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
            producthierarchy = breadcrumps + [''] * (7 - len(breadcrumps))  # Ensure at least 7 levels

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
            
            # Image URLs
            images = json_data.get("image", [])
            image_urls = {f'file_name_{i+1}': f'{unique_id}{i+1}.png' for i in range(len(images))}
            image_urls.update({f'image_url_{i+1}': url for i, url in enumerate(images)})

            availability = json_data.get("offers", {}).get("availability", "")
            in_stock = "InStock" in availability

            property_data = {
                'data_url': data_url,
                'unique_id': unique_id,
                'competitor_name': competitor_name,
                'store_addressline2': store_addressline2,
                'store_suburb': store_suburb,
                'store_state': store_state,
                'store_postcode': store_postcode,
                'store_addressid': store_addressid,
                'extraction_date': extraction_date,
                'product_name': product_name,
                'brand': brand,
                'brand_type': brand_type,
                'grammage_quantity': grammage_quantity,
                'grammage_unit': grammage_unit,
                'drained_weight': '',
                'producthierarchy_level1': producthierarchy[0],
                'producthierarchy_level2': producthierarchy[1],
                'producthierarchy_level3': producthierarchy[2],
                'producthierarchy_level4': producthierarchy[3],
                'producthierarchy_level5': producthierarchy[4],
                'producthierarchy_level6': producthierarchy[5],
                'producthierarchy_level7': producthierarchy[6],
                'regular_price': regular_price,
                'selling_price': '',
                'price_was': regular_price,
                'promotion_price': '',
                'promotion_valid_from': '',
                'promotion_valid_upto': '',
                'promotion_type': '',
                'percentage_discount': '',
                'promotion_description': '',
                'package_sizeof_sellingprice': '',
                'per_unit_sizedescription': '',
                'price_valid_from': '',
                'price_per_unit': price_per_unit,
                'multi_buy_item_count': '',
                'multi_buy_items_price_total': '',
                'currency': currency,
                'breadcrumb': '',
                'pdp_url': data_url,
                'variants': '',
                'product_description': product_description,
                'instructions': '',
                'storage_instructions': '',
                'preparationinstructions': '',
                'instructionforuse': '',
                'country_of_origin': country_of_origin,
                'allergens': allergens,
                'age_of_the_product': '',
                'age_recommendations': '',
                'flavour': '',
                'nutritions': '',
                'nutritional_information': '',
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
                'organictype': '',
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
                'distributor_address': '',
                'vinification_details': '',
                'recycling_information': '',
                'return_address': '',
                'alchol_by_volume': alchol_by_volume,
                'beer_deg': '',
                'netcontent': '',
                'netweight': '',
                'site_shown_uom': '',
                'ingredients': '',
                'random_weight_flag': '',
                'availability': availability,
                'in_stock': in_stock,
                'instock': in_stock,
                'promo_limit': '',
                'product_unique_key': unique_id,
                'multibuy_items_pricesingle': '',
                'perfect_match': '',
                'servings_per_pack': '',
                'warning': '',
                'suitable_for': '',
                'standard_drinks': '',
                'environmental': '',
                'grape_variety': grape_variety,
                'retail_limit': '',
                **image_urls
            }

            return property_data
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
                # Rotate User-Agent for each request
                self.headers = {
                    "User-Agent": self.user_agent.random
                }

                # Fetch using scraper with rotated User-Agent
                response = self.scraper.get(agent_url, headers=self.headers)
                response.raise_for_status()
                html_content = response.text
                selector = Selector(text=html_content)
                agent_data = self.parse_pages(agent_url, selector)

                if agent_data:
                    self.parsed_data_collection.insert_one(agent_data)
                    logging.info(f"Successfully scraped data from URL: {agent_url}")
                else:
                    logging.error(f"No data parsed from URL: {agent_url}")

                # Randomize delay to avoid detection
                time.sleep(fixed_delay + random.uniform(1, 3))

            except Exception as e:
                error_message = f"{str(e)}"
                logging.error(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")
                print(f"Failed to scrape data from URL: {agent_url}. Error: {error_message}")

        logging.info(f"Scraped {self.parsed_data_collection.count_documents({})} Agent URLs")

if __name__ == "__main__":
    parser = IntersparParser()
    parser.scrape_data()
