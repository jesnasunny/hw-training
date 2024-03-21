import re
import json
import requests
from parsel import Selector
from time import sleep

class OlxScraper:
    def __init__(self, start_url):
        self.start_url = start_url
        self.base_url = "https://www.olx.in"
        # self.data=[]

    def fetch_page(self, url):
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch page {url}")
            return None

    def start_scraping(self):
        page_number = 1
        while page_number <= 3:
            print(f"Scraping page {page_number}")
            page_url = f"{self.start_url}?page={page_number}"
            html_content = self.fetch_page(page_url)
            if html_content:
                selector = Selector(html_content)
                self.extract_product_data(selector)
                sleep(5)  # Add a delay to avoid hitting the server too frequently
                page_number += 1
            else:
                
                break
        # self.save_to_json("olx_data.json")

    def extract_product_data(self, selector):
        products = selector.xpath("//li[@data-aut-id='itemBox']")
        for product in products:
            product_name = product.xpath("//span[@data-aut-id='itemTitle']/text()").get()
            property_id_href = product.css("a::attr(href)").get()
            property_id = re.search(r'\d+', property_id_href).group() if property_id_href is not None else None
            if property_id:
                breadcrumbs = product.xpath("//ol[@class='rui-2Pidb']/li/a/text()").getall()
                price = product.xpath("//span[@data-aut-id='itemPrice']/text()").get()
                image_url =product.xpath("//img[@class='_2hBzJ']/@src").get()
                location = product.xpath("//span[@data-aut-id='item-location']/text()").get()
                bathroom = product.xpath("//span[@data-aut-id='itemDetails']/text()").get()
                bedrooms = product.xpath("//span[@data-aut-id='itemDetails']/text()").get()

                if bathroom:
                    bathroom = bathroom[8]
                if bedrooms:
                    bedrooms = bedrooms[0]
                product_link = product.css('a::attr(href)').get()
                if product_link:
                    product_response = self.fetch_page(self.base_url + product_link)
                    if product_response:
                        product_selector = Selector(product_response)
                        type_info = product_selector.xpath("//span[@data-aut-id='value_type']//text()").get()  
                        description = product_selector.xpath("//div[@data-aut-id='itemDescriptionContent']/p/text()").getall()
                        description_text = '\n'.join(description)
                        title = product_selector.xpath("//div[@class='_3Yuv9.kI9QF']//div[@class='eHFQs']/text()").get()
                        # seller_name = title.strip() if title else None

                        print({
                            'property_name': product_name,
                            'property_id': property_id,
                            'breadcrumbs': breadcrumbs,
                            'price': price,
                            'image_url': image_url,
                            'description_text': description_text,
                            'location': location,
                            'seller_name': title,
                            'property_type': type_info.strip() if type_info else None,
                            'bathrooms': bathroom,
                            'bedrooms': bedrooms
                        })
                    else:
                        print(f"Failed to fetch product details for {product_name}")
    # def save_to_json(self, filename):
    #     with open(filename, 'w') as f:
    #         json.dump(self.data, f, indent=4)

if __name__ == "__main__":
    start_url = "https://www.olx.in/kozhikode_g4058877/for-rent-houses-apartments_c1723"
    scraper = OlxScraper(start_url)
    scraper.start_scraping()
print(f"Scraped {scraper.scraped_count} rooms.")