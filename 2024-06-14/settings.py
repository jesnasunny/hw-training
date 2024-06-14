import datetime
url = [
      
  'https://products.dm.de/productfeed/AT/sitemap.xml'
            
      ]
headers = {
  
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "x-dm-version": "2024.613.35773"
}

today = datetime.date.today()
days_until_thursday = (3 - today.weekday() + 7) % 7
next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
NEXT_THURSDAY_DATE = next_thursday_date.strftime("%Y-%m-%d")


