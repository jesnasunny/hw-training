import csv
from pymongo import MongoClient
from settings import NEXT_THURSDAY_DATE
from pipeline import uri, db_name

class ExportData:
    def __init__(self, collection_name, output_csv_path):
        mongo_uri = uri
        database_name = db_name
        client = MongoClient(mongo_uri)
        database = client[database_name]
        self.collection = database[collection_name]
        self.documents = self.collection.find()
        self.output_csv_path = output_csv_path

    def data_document(self):
        headers = [
            'unique_id', 'competitor_name', 'store_name', 'store_addressline1',
            'store_addressline2', 'store_suburb', 'store_state', 'store_postcode',
            'store_addressid', 'extraction_date', 'product_name', 'brand', 'brand_type',
            'grammage_quantity', 'grammage_unit', 'drained_weight', 'producthierarchy_level1',
            'producthierarchy_level2', 'producthierarchy_level3', 'producthierarchy_level4',
            'producthierarchy_level5', 'producthierarchy_level6', 'producthierarchy_level7',
            'regular_price', 'selling_price', 'price_was', 'promotion_price',
            'promotion_valid_from', 'promotion_valid_upto', 'promotion_type',
            'percentage_discount', 'promotion_description', 'package_sizeof_sellingprice',
            'per_unit_sizedescription', 'price_valid_from', 'price_per_unit',
            'multi_buy_item_count', 'multi_buy_items_price_total', 'currency',
            'breadcrumbs', 'pdp_url', 'file_name_1', 'image_url_1', 'file_name_2', 'image_url_2',
            'file_name_3', 'image_url_3', 'file_name_4', 'image_url_4', 'file_name_5', 'image_url_5',
            'file_name_6', 'image_url_6', 'variants', 'product_description', 'instructions', 'storage_instructions',
            'preparationinstructions', 'instructionforuse', 'country_of_origin', 'allergens', 'age_of_the_product',
            'age_recommendations', 'flavour', 'nutritions', 'nutritional_information', 'vitamins', 'labelling', 'grade',
            'region', 'packaging', 'receipies', 'processed_food', 'barcode', 'frozen', 'chilled', 'organictype', 'cooking_part',
            'handmade', 'max_heating_temperature', 'special_information', 'label_information', 'dimensions',
            'special_nutrition_purpose', 'feeding_recommendation', 'warranty', 'color', 'model_number',
            'material', 'usp', 'dosage_recommendation', 'tasting_note', 'food_preservation', 'size', 'rating',
            'review', 'manufacturer_address', 'importer_address', 'distributor_address', 'vinification_details',
            'recycling_information', 'return_address', 'alchol_by_volume', 'beer_deg', 'netcontent', 'netweight',
            'site_shown_uom', 'ingredients', 'random_weight_flag', 'instock', 'promo_limit', 'product_unique_key',
            'multibuy_items_pricesingle', 'perfect_match', 'servings_per_pack', 'warning', 'suitable_for',
            'standard_drinks', 'environmental', 'grape_variety', 'retail_limit'
        ]
        
        with open(self.output_csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerow(headers)

            for doc in self.documents:
                for key, value in doc.items():
                    if isinstance(value, str):
                        doc[key] = value.replace('|', '/|')

                doc['extraction_date'] = NEXT_THURSDAY_DATE

                if 'brand' in doc:
                    doc['brand'] = doc['brand'].strip()

                if '_id' in doc:
                    del doc['_id']
                if 'product_name' in doc and isinstance(doc['product_name'], str):
                    doc['product_name'] = doc['product_name'].replace(",null", "").replace("null", "")                
                if 'grammage_unit' in doc:
                    doc['grammage_unit'] = doc['grammage_unit'].replace(",null", "").replace("null", "")
                    
                if 'grammage_quantity' not in doc or not doc['grammage_quantity']:
                    doc['grammage_quantity'] = 1
                if 'grammage_unit' not in doc or not doc['grammage_unit']:
                    doc['grammage_unit'] = 'stuck'      
                              
                if 'site_shown_uom' in doc:
                    doc['site_shown_uom'] = doc['site_shown_uom'].replace(",null", "").replace("null", "")
                if 'nutritional_information' in doc:
                    doc['nutritional_information'] = [item.strip() for item in doc['nutritional_information'] if item.strip()]
                    
                if 'storage_instructions' in doc:
                    storage_instructions_string = ', '.join(doc['storage_instructions'])                   
                    storage_instructions_string = storage_instructions_string.replace("\n", "").replace("null", "")                    
                    doc['storage_instructions'] = storage_instructions_string
                    
                if 'manufacturer_address' in doc:
                    doc['manufacturer_address'] = ', '.join(doc['manufacturer_address'])
                    
                if 'ingredients' in doc:                   
                    ingredients_string = ', '.join(doc['ingredients'])                    
                    ingredients_string = ingredients_string.replace("\n", "").replace("null", "")                    
                    doc['ingredients'] = ingredients_string
                    
                if 'warning' in doc:                   
                    warning_string = ', '.join(doc['warning'])                   
                    warning_string = warning_string.replace("\n", "").replace("null", "")                    
                    doc['warning'] = warning_string
                    
                # if 'nutritional_information' in doc:
                #     nutritional_information = doc['nutritional_information']
                #     nutritional_dict = {}
                #     for item in nutritional_information:
                #         key_value_pair = item.split('_', 1)
                #         if len(key_value_pair) == 2:
                #             key, value = key_value_pair
                #             key = key.strip()
                #             value = value.strip().rstrip(' :')
                #             nutritional_dict[key] = value
                #         else:
                #             print(f"Skipping invalid item: {item}")
                #     doc['nutritional_information'] = nutritional_dict    
                 
                
                price_keys = ['regular_price', 'selling_price', 'price_was', 'promotion_price']
                for key in price_keys:
                    if key in doc and doc[key] is not None and doc[key] != '':
                        try:
                            doc[key] = float(doc[key])
                        except ValueError:
                            doc[key] = ''

                if 'breadcrumbs' in doc and isinstance(doc['breadcrumbs'], list):
                    doc['breadcrumbs'] = ' > '.join(doc['breadcrumbs'])
                elif 'breadcrumbs' not in doc or not doc['breadcrumbs']:
                    doc['breadcrumbs'] = ''

                if doc.get('regular_price'):
                    doc['promotion_price'] = doc.get('selling_price')
                    doc['price_was'] = doc.get('regular_price')
                    doc['selling_price'] = doc.get('selling_price')
                else:
                    doc['regular_price'] = doc.get('selling_price')
                    doc['price_was'] = ""
                    doc['promotion_price'] = ''

                writer.writerow(doc.values())

        print(f"Data saved to {self.output_csv_path}")

if __name__ == "__main__":
    processor = ExportData("parsed_data", "spar_data.csv")
    processor.data_document()
