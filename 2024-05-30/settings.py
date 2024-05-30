MONGO_SETTINGS = {
    'MONGO_URI': 'mongodb://localhost:27017/',
    'MONGO_DATABASE': 'jes_database',
    'MONGO_COLLECTION': 'product_urls',
    'MONGO_INDEX_KEYS': ['url'],
    # 'MONGO_UNIQUE': True
}

CUSTOM_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ml;q=0.6",
    "If-None-Match": '"dda1f-qsY9kvw0LRKNqImtxdKscrDn4Wo-gzip"',
    "Priority": "u=1, i",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
