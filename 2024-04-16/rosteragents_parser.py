import requests
from parsel import Selector
from pymongo import MongoClient
import logging

class ColdwellBankerParser:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        }
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['roster_agents']
        self.urls_collection = self.db['urls_collection']
        self.datas_collection = self.db['agent_details']
        self.errors_collection = self.db['datas_log']
        self.urls_collection.create_index([('url', 1)], unique=True)
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)
    def parse_agent_data(self, selector):
        agent_url= self.agent_url,
        full_name = selector.xpath("//p[@class='rng-agent-profile-contact-name']/text()").get()
        description= selector.xpath("//article[@class='rng-agent-profile-content']//div//p/text()").getall()

        agent_data = {
            'agents_url' : agent_url,
            'full_name': full_name,
            'description' : description
        }
        return agent_data
    def log_error(self,agent_url, error_message):
        error_log = {
            'agents_url': agent_url,
            'error_message': error_message
        }
        self.errors_collection.insert_one(error_log)

    def page(self):
        while self.urls_collection.count_documents({}) > 0:
            agent_doc = self.urls_collection.find_one_and_delete({})
            agent_url = agent_doc["url"]
            if not agent_url or not agent_url.strip():
                continue       
            try:
                self.agent_url = agent_url  
                html_content = requests.get(agent_url, headers=self.headers).text
                selector = Selector(text=html_content)
                agent_data = self.parse_agent_data(selector)
                self.datas_collection.insert_one(agent_data)               
            except Exception as e:
                self.logger.error(f"Error processing {agent_url}: {e}")
                self.log_error(agent_url, str(e))
if __name__ == "__main__":
    parser = ColdwellBankerParser()
    parser.page()