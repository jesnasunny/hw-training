from pymongo import MongoClient
import logging

uri = 'mongodb://localhost:27017/'
db_name = 'spar_database'

class MongoConnection:
    def __init__(self):
        self.client = MongoClient(uri)
        self.db = client[db_name]
        try:
            self.db[product_urls].create_index("url", unique=True)
            self.db[parsed_data].create_index("pdp_url", unique=True)
            self.db[new_parsed_data]
        except Exception as e:
            logging.warning(e)