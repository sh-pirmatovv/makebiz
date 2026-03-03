import requests
from bs4 import BeautifulSoup
import lxml
import pandas
import time, random
import fake_useragent
from dotenv import load_dotenv

load_dotenv()


URL = 'https://www.yellowpages.uz/'
HOST = 'https://www.yellowpages.uz'

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/140.0.0.0 YaBrowser/25.10.0.0 Safari/537.36",
    "Accept": "application/json"
}

# response = requests.get(URL, headers=headers, timeout=10)
# html = response.text
# soup = BeautifulSoup(html, 'html.parser')
#
#
# block = soup.find('div', class_='custom-card')
# catalog = block.find('ul')
#
# print(catalog)
r = requests.get(URL, headers=headers, verify=False)
soup = BeautifulSoup(r.content, 'lxml')
block = soup.find('div', attrs={'class': 'custom-card'})
print(soup)

