import requests
import re
from pymongo import MongoClient

base_url = "https://www.reecenichols.com"
referer_url = "https://www.reecenichols.com/roster/agents"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
headers = {
    'Referer': referer_url,
    'User-Agent': user_agent
}
client = MongoClient('mongodb://localhost:27017/')
db = client['roster_agents']
collection = db['urls_collection']

def fetch_agents(page_number):
    url = f"https://www.reecenichols.com/CMS/CmsRoster/RosterSearchResults?layoutID=944&pageSize=10&pageNumber={page_number}&sortBy=firstname-asc"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.text
        matches = re.findall(r'\/bio\/([^"]+)', data)
        if matches:
            print("Found matches on page", page_number, ":")
            for match in matches:
                clean_match = match.replace("\\", "")
                url_to_store=(base_url + "/bio/" + clean_match)
                collection.insert_one({'url': url_to_store})

        else:
            print("No matches found on page", page_number)
        return True 
    else:
        print("Failed to retrieve data for page", page_number, ". Status code:", response.status_code)
        return False  

if __name__ == "__main__":
    page_number = 1
    while fetch_agents(page_number):
        page_number += 1
