import json
import requests
from scrapy import Selector

def extract_property_info(url):
    response = requests.get(url)
    if response.status_code == 200:
        selector = Selector(text=response.text)
        javascript_data = selector.xpath('//script[contains(text(), "window[\'dataLayer\']")]/text()').get()
        if javascript_data:
            start_index = javascript_data.find('{')
            end_index = javascript_data.rfind('}')
            json_string = javascript_data[start_index:end_index+1]
            data = json.loads(json_string)
            property_info = {
                "url": data.get('starting_page_url'),
                "reference_number": data.get("reference_id"),
                "property_name": data.get('listing_title'),
                "price": f"{data.get('price')} {data.get('currency_unit')}",
                "property_type": data.get('property_type'),
                "purpose": data.get("website_section"),
                "furnishing_status": data.get("furnishing_status"),
                "bathrooms": data.get("property_baths_list"),
                "bedrooms": data.get("property_beds_list"),
                "area": f"{data.get('property_land_area')} Sq. Ft.",
                "location": f"{data.get('loc_3_name').strip(';')}, {data.get('loc_2_name').strip(';')}, {data.get('loc_1_name').strip(';')}",
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude")
            }
            return property_info
    return None

url = "https://www.bayut.com/property/details-8724106.html"
property_info = extract_property_info(url)
if property_info:
    for key, value in property_info.items():
        print(f"{key}: {value}")
else:
    print("Failed to extract property information.")
