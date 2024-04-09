import requests
import pymongo
import logging

BASE_URL = "https://www.coldwellbanker.com"
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MONGODB_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "coldwellbanker_agents"
COLLECTION_NAME = "agents"

client = pymongo.MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

collection.create_index([("canonicalURL", pymongo.ASCENDING)], unique=True)

def fetch_agents(base_url, sub_url):
    page = 1  
    while True:
        agent_url = f"{base_url}/_next/data/4dkge-vOkPyP5gSOU0Thy{sub_url}.json?page={page}"
        headers = {'User-Agent': USER_AGENT}

        try:
            agent_response = requests.get(agent_url, headers=headers)
            agent_response.raise_for_status()
            agent_data = agent_response.json()

            if 'pageProps' in agent_data and 'results' in agent_data['pageProps'] and 'agents' in agent_data['pageProps']['results']:
                agents = agent_data['pageProps']['results']['agents']
                if not agents:
                    break  
                for agent in agents:
                    last_url = agent.get('url')
                    agent_details = fetch_datas(base_url, last_url)
                    if agent_details:
                        
                        try:
                            collection.insert_one(agent_details)
                            logger.info(f"Inserted agent details for URL: {agent_details['canonicalURL']}")
                        except pymongo.errors.DuplicateKeyError:
                            logger.warning(f"Agent details for URL {agent_details['canonicalURL']} already exist in the database.")
            else:
                logger.info(f"No agents found for page {page} in {sub_url}")
                break  
            page += 1  
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {agent_url}: {e}")

def fetch_datas(base_url, last_url):
    data_url = f"{base_url}/_next/data/4dkge-vOkPyP5gSOU0Thy/{last_url}.json"
    headers = {'User-Agent': USER_AGENT}

    try:
        data_response = requests.get(data_url, headers=headers)
        data_response.raise_for_status()
        wellbanker_data = data_response.json()

        agent_details = wellbanker_data.get('pageProps', {}).get('detail', {}).get('agentDetails', {})
        if agent_details:
          
            agent_data = {
                'canonicalURL': agent_details.get('canonicalURL'),
                'first_name': agent_details.get('firstName'),
                'full_name': agent_details.get('fullName'),
                'email': agent_details.get('emailAccount'),
                'officenumber': agent_details.get('businessPhoneNumber'),
                'agentumber': agent_details.get('cellPhoneNumber'),
                'languages': agent_details.get('languages', [{'longDescription': 'Not specified'}])[0].get('longDescription'),
            }

            physical_address = agent_details.get('physicalAddress')
            if physical_address:
                agent_data.update({
                    'city': physical_address.get('city'),
                    'state': physical_address.get('state'),
                    'country': physical_address.get('country'),
                    'zipCode': physical_address.get('zipCode'),
                    'address': physical_address.get('address')
                })

            return agent_data
        else:
            logger.info(f"No agent details found for {last_url}")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from {data_url}: {e}")

    return None
def fetch_sub_urls(json_url):
    headers = {'User-Agent': USER_AGENT}

    try:
        response = requests.get(json_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        sitemap_content = data.get('pageProps', {}).get('sitemapTopCitiesListContent', {})
        if sitemap_content:
            return [link['url'] for link in sitemap_content.get('siteMapLinks', [])]
        else:
            logger.info("The structure of the JSON response is not as expected.")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from {json_url}: {e}")
    return []

def main():
    first_page_url = f"{BASE_URL}/_next/data/4dkge-vOkPyP5gSOU0Thy/sitemap/agents.json"

    try:
        response = requests.get(first_page_url, headers={'User-Agent': USER_AGENT})
        response.raise_for_status()
        data = response.json()

        sitemap_list_content = data.get('pageProps', {}).get('sitemapListContent', {})
        if sitemap_list_content:
            urls = [link['url'] for link in sitemap_list_content.get('siteMapLinks', [])]
            for url in urls:
                city = url.split('/')[-2] 
                json_url = f"{BASE_URL}/_next/data/4dkge-vOkPyP5gSOU0Thy/{url}.json"
                sub_urls = fetch_sub_urls(json_url)
                if sub_urls:
                    for sub_url in sub_urls:
                        logger.info(sub_url)
                        fetch_agents(BASE_URL, sub_url)
                else:
                    logger.info(f"No sub URLs found for {city.capitalize()}.")
        else:
            logger.info("The structure of the JSON response is not as expected.")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data from {first_page_url}: {e}")

if __name__ == "__main__":
    main()
