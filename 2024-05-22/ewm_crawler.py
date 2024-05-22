import requests
from parsel import Selector
from pymongo import MongoClient

base_url = "https://www.ewm.com"
referer_url = "https://www.ewm.com/agents/"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
headers = {
    'Referer': referer_url,
    'User-Agent': user_agent
}
client = MongoClient('mongodb://localhost:27017/')
db = client['ewm_agents']
collection = db['urls_collection']

def fetch_urls():
    url = "https://www.ewm.com/agents/"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        selector = Selector(response.text)
        urls = selector.xpath("//div[@class='cell cell-xs-12 cell-lg-4 grid--justifyCenter']//a/@href").getall()
        for url in urls:
            url_to_store = base_url + url
            if not collection.find_one({'url': url_to_store}):
                collection.insert_one({'url': url_to_store})
    else:
        print(f"Failed to retrieve the page, status code: {response.status_code}")

fetch_urls()
