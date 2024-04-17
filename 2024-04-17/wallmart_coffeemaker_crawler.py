import requests
from parsel import Selector

def scrape_urls(url, base_url):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    headers = {'User-Agent': user_agent}

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        selector = Selector(response.text)

        product_urls = selector.xpath("//a[@class='absolute w-100 h-100 z-1 hide-sibling-opacity']/@href").getall()
        product_urls = [product_url for product_url in product_urls if not product_url.startswith('https://wrd.walmart.com/')]
        for product_url in product_urls:
            print(base_url + product_url)

def crawl(url):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    headers = {'User-Agent': user_agent}

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        selector = Selector(response.text)
        urls = []
        
        for i in range(6):
            column_url = selector.xpath(f"//div[@id='GridColumn-{i}']/a/@href").get()
            if column_url:
                urls.append(base_url + column_url)

        for url in urls:
            scrape_urls(url, base_url)
    else:
        print("Failed to fetch the page:", response.status_code)

if __name__ == "__main__":
    url = "https://www.walmart.com/cp/coffee-shop/1115306?povid=HomeTopNav_KitchenDining_CoffeeShop"
    base_url = "https://www.walmart.com" 
    crawl(url)
