import csv
from pymongo import MongoClient
from settings import MONGO_SETTINGS
import datetime

def get_next_thursday():
    today = datetime.date.today()
    days_until_thursday = (3 - today.weekday() + 7) % 7
    next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
    return next_thursday_date.strftime("%Y-%m-%d")

def export_data_to_csv():
    mongo_uri = MONGO_SETTINGS['MONGO_URI']
    database_name = MONGO_SETTINGS['MONGO_DATABASE']
    collection_name = MONGO_SETTINGS['MONGO_COLLECTION']

    client = MongoClient(mongo_uri)
    database = client[database_name]
    collection = database[collection_name]

    documents = collection.find()
    output_csv_path = "interspar_data.csv"
    
    with open(output_csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='|')
        header_written = False
        
        for document in documents:
            document_dict = dict(document)        

            document_dict['extraction_date'] = get_next_thursday()

            if '_id' in document_dict:
                del document_dict['_id']

            if 'regular_price' in document_dict:
                document_dict['regular_price'] = float(document_dict['regular_price'])

            if 'selling_price' in document_dict:
                document_dict['selling_price'] = float(document_dict['selling_price'])

            if 'price_was' in document_dict and document_dict['price_was'] != '':
                document_dict['price_was'] = float(document_dict['price_was'])

            if 'promotion_price' in document_dict and document_dict['promotion_price'] != '':
                document_dict['promotion_price'] = float(document_dict['promotion_price'])      

            if 'feeding_recommendation' in document_dict:
                feeding_recommendation = document_dict['feeding_recommendation']
                if ('feeding_suggestion' not in feeding_recommendation or not feeding_recommendation['feeding_suggestion']) and \
                   ('table_data' not in feeding_recommendation or not feeding_recommendation['table_data']):
                    document_dict['feeding_recommendation'] = ''

            if 'promo_desc' not in document_dict:
                if 'regular_price' in document_dict and 'selling_price' in document_dict:
                    if document_dict['regular_price'] == document_dict['selling_price']:
                        document_dict['promo_price'] = document_dict['selling_price']
                        document_dict['price_was'] = ''
                    else:
                        document_dict['price_was'] = document_dict['regular_price']
                        document_dict['promo_price'] = document_dict['selling_price']
                else:
                    document_dict['regular_price'] = ''
                    document_dict['selling_price'] = ''
                    document_dict['promo_price'] = ''
                    document_dict['price_was'] = ''

            if 'promo_desc' in document_dict:
                if 'multibuy_items_pricesingle' in document_dict:
                    document_dict['selling_price'] = document_dict['multibuy_items_pricesingle']

                if 'regular_price' in document_dict and 'selling_price' in document_dict:
                    if document_dict['regular_price'] == document_dict['selling_price']:
                        document_dict['promo_price'] = document_dict['selling_price']
                        document_dict['price_was'] = ''
                    else:
                        document_dict['price_was'] = document_dict['regular_price']
                        document_dict['promo_price'] = document_dict['selling_price']
                else:
                    document_dict['regular_price'] = ''
                    document_dict['selling_price'] = ''
                    document_dict['promo_price'] = ''
                    document_dict['price_was'] = ''

            if 'grammage_quantity' not in document_dict or not document_dict['grammage_quantity']:
                document_dict['grammage_quantity'] = 1

            if 'grammage_unit' not in document_dict or not document_dict['grammage_unit']:
                document_dict['grammage_unit'] = 'stuck'
            
            if 'breadcrumbs' in document_dict and isinstance(document_dict['breadcrumbs'], list):
                document_dict['breadcrumbs'] = ' > '.join(document_dict['breadcrumbs'])
            elif 'breadcrumbs' not in document_dict or not document_dict['breadcrumbs']:
                document_dict['breadcrumbs'] = ''

            if not header_written:
                header = document_dict.keys()
                csv_writer.writerow(header)
                header_written = True

            csv_writer.writerow(document_dict.values())

    print(f"Data saved to {output_csv_path}")

export_data_to_csv()
