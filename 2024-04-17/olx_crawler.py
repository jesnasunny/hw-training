import requests
from pymongo import MongoClient
from parsel import Selector

base_url = "https://www.olx.in"
referer_url = "https://www.olx.in/kozhikode_g4058877/for-rent-houses-apartments_c1723"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
headers = {
    'Referer': referer_url,
    'User-Agent': user_agent
}
client = MongoClient('mongodb://localhost:27017/')
db = client['olx_properties']
collection = db['urls_collection']

def starting_page(url):

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        selector = Selector(response.text)
        # urls=Selector.xpath("//li[@data-aut-id='itemBox']").get()
        products = selector.xpath("//li[@data-aut-id='itemBox']")

        urls = []

        for product in products:

            product_link = product.xpath(".//a/@href").get()

            if product_link:

                urls.append(base_url + product_link)

        return urls
    else:
        print("Failed to retrieve data from referral URL:", url, ". Status code:", response.status_code)
        return []


def fetch_agents(page_number):
    url = f"https://www.olx.in/api/relevance/v4/search?category=1723&facet_limit=100&lang=en-IN&location=4058877&location_facet_limit=20&page={page_number}&platform=web-desktop&relaxedFilters=true&size=40&user=0752705852478141"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            for item in data:
                title = item.get('title', '').replace(" ", "-")
                item_id = item.get('id', '')
                url_to_store = f"{base_url}/item/{title}-iid-{item_id}"
                collection.insert_one({'url': url_to_store})
        else:
            print("No matches found on page", page_number)
        return True
    else:
        print("Failed to retrieve data for page", page_number, ". Status code:", response.status_code)
        return False  

if __name__ == "__main__":
    referer_urls = starting_page(referer_url)
    for url in referer_urls:
        collection.insert_one({'url': url})
    

    page_number = 1
    while fetch_agents(page_number):
        page_number += 1
