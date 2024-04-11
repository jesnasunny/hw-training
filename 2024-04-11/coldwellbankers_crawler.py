import requests
import xml.etree.ElementTree as ET

class ColdwellBanker:
    def __init__(self):
        self.base_url = "https://www.coldwellbanker.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        }
        self.session = requests.Session()
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['coldwellbanker_sitemapagents']
        self.urls_collection = self.db['urls']
        self.urls_collection.create_index([('url', 1)], unique=True)


    def get_robots_txt(self):
        url = "https://www.coldwellbanker.com/robots.txt"
        response = self.session.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.text
        else:
            return None

    def get_sitemap_urls(self, robots_txt):
        sitemap_urls = []
        for line in robots_txt.split('\n'):
            if line.startswith("Sitemap:"):
                sitemap_urls.append(line.split(": ")[1].strip())
        return sitemap_urls

    def parse_sitemap_xml(self, xml_content):
        urls = []
        if xml_content:
            root = ET.fromstring(xml_content)
            for elem in root.iter():
                if 'loc' in elem.tag:
                    url = elem.text
                    if not url.endswith('.jpg'): 
                        urls.append(url)
                    else:
                        break 

        return urls

    def get_xml_content(self, url):
        response = self.session.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.text
        else:
            return None

    def main(self):
        robots_txt = self.get_robots_txt()
        if robots_txt:
            sitemap_urls = self.get_sitemap_urls(robots_txt)
            if len(sitemap_urls) >= 4:
                agent_url = sitemap_urls[3]  # Index starts from 0
                print(f"Fourth sitemap URL found: {agent_url}")
                response = self.session.get(agent_url, headers=self.headers)
                if response.status_code == 200:
                    print("Successfully fetched XML page.")
                    xml_content = response.text
                    urls = self.parse_sitemap_xml(xml_content)
                    return urls
                else:
                    print("Failed to fetch XML page.")
            else:
                print("There are not enough sitemap URLs in the robots.txt")
        else:
            print("Failed to fetch robots.txt")
        return None

    def fetch_urls_and_parse_xml(self):
        urls = self.main()
        if urls:
            for url in urls:
                print("Accessing URL:", url)
                xml_content = self.get_xml_content(url)
                if xml_content:
                    print("Successfully fetched XML content from URL:", url)
                    parsed_urls = self.parse_sitemap_xml(xml_content)
                    if parsed_urls:
                        for parsed_url in parsed_urls:
                            print("URL from XML:", parsed_url)
                            self.urls_collection.insert_one({'url': parsed_url})
                    else:
                        print("No URLs found in XML content from URL:", url)
                else:
                    print("Failed to fetch XML content from URL:", url)

if __name__ == "__main__":
    scraper = ColdwellBanker()
    scraper.fetch_urls_and_parse_xml()
