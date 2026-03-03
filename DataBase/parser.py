# Импорт библиотек

from bs4 import BeautifulSoup
import requests
import os
import sqlite3
import re
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv('URL')
HOST = os.getenv('HOST')


db = sqlite3.connect('texnomart.db')
cursor = db.cursor()



class BaseParser():
    def __init__(self, url, name, category_id):
        self.url = url
        self.name = name
        self.category_id = category_id

    def get_img(self, url):
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        block = soup.find('div', class_='swiper-wrapper')
        slides = block.find_all('div', class_='swiper-slide')
        for slide in slides:
            image_link = HOST + slide.find('img', class_='swiper-slide__img').get('src')
            print(image_link)
def parsing():
    try:
        html = requests.get(URL).text
        soup = BeautifulSoup(html, 'html.parser')
        block = soup.find('div', class_='category__wrap')
        categories = block.find_all('div', class_='category__item')
        for category in categories:
            title = category.find('h2', class_='content__title').get_text(strip=True)
            link = HOST + category.find('a', class_='category__link').get('href')
            # Save to db
            db = sqlite3.connect('texnomart.db')
            cursor = db.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO categories(category_title, category_link) VALUES (?,?)
                ''', (title, link))
            db.commit()

            db.close()
            print('-------------------------------------------------------------------------------------------------')
            print(f'parsing ---> {title}')
            print(link)
            sub_html = requests.get(link).text
            sub_soup = BeautifulSoup(sub_html, 'html.parser')
            sub_block = sub_soup.find('div', class_='category__wrap')
            subcategories = sub_block.find_all('div', class_='category__item')
            for subcategory in subcategories:
                sub_title = subcategory.find('h2', class_='content__title').get_text(strip=True)
                try:
                    sub_title_span = re.findall(r'[0-90-9]+', sub_title)
                    sub_title_span = f'({sub_title_span[0]})'
                    sub_title = sub_title.replace(sub_title_span, '').replace('  ', '')
                    # print(sub_title)
                except:
                    pass

                sub_link = HOST + subcategory.find('a', class_='category__link').get('href')
                print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                print(f'parsing ---> {title} ---> {sub_title} ---> {sub_link}')
                # Брэнды\Типы
                try:
                    print('+')
                    brand_html = requests.get(sub_link).text
                    brand_soup = BeautifulSoup(brand_html, 'html.parser')
                    brand_block = brand_soup.find('div', class_='category__wrap')
                    brands = brand_block.find_all('div', class_='category__item')
                    for brand in brands:
                        brand_title = brand.find('h2', class_='content__title').get_text(strip=True)
                        brand_link = HOST + brand.find('a', class_='category__link').get('href')
                        print(f'parsing ---> {title} ---> {sub_title} ---> {brand_title} ---> {brand_link}')
                        try:
                            product_html = requests.get(brand_link).text
                            product_soup = BeautifulSoup(product_html, 'html.parser')
                            product_block = product_soup.find('div', class_='product-list__wrap')
                            products = product_block.find_all('div', class_='product-list__item')
                            for product in products:
                                # print(product)
                                product_title = product.find('h3', class_='product-name').get_text(strip=True)
                                product_price = product.find('div', class_='product-price').get_text(strip=True)
                                product_credit_price = product.find('div', class_='product-installment').get_text(strip=True)
                                product_link = HOST + product.find('a', class_='product-name').get('href')
                        except:
                            subbrand_html = requests.get(brand_link)
                            subbrand_soup = BeautifulSoup(subbrand_html, 'html.parser')
                            subbrand_block = subbrand_soup.find('div', class_='category__wrap')
                            subbrands = subbrand_block.find_all('div', class_='category__item')
                            for subbrand in subbrands:
                                subbrand_title = subbrand.find('h2', class_='content__title').get_text(strip=True)
                                subbrand_link = HOST + brand.find('a', class_='category__link').get('href')
                                print(f'parsing ---> {title} ---> {sub_title} ---> {brand_title} ---> {subbrand_title} ---> {subbrand_link}')
                                product_html = requests.get(brand_link).text
                                product_soup = BeautifulSoup(product_html, 'html.parser')
                                product_block = product_soup.find('div', class_='product-list__wrap')
                                products = product_block.find_all('div', class_='product-list__item')
                                for product in products:
                                    # print(product)
                                    product_title = product.find('h3', class_='product-name').get_text(strip=True)
                                    product_price = product.find('div', class_='product-price').get_text(strip=True)
                                    product_credit_price = product.find('div', class_='product-installment').get_text(
                                        strip=True)
                                    product_link = HOST + product.find('a', class_='product-name').get('href')


                                    print(f'parsing ---> {title} ---> {sub_title} ---> {brand_title} ---> {subbrand_title} ---> {product_title} ---> {product_price} ---> {product_credit_price} ---> {product_link}')
                except Exception as e:
                    print('2222222222222')
                    product_html = requests.get(sub_link).text
                    print(sub_link)
                    product_soup = BeautifulSoup(product_html, 'html.parser')
                    product_block = product_soup.find('div', class_='product-list__wrap')
                    products = product_block.find_all('div', class_='product-list__item')
                    for product in products:
                        product_title = product.find('h3', class_='product-name').get_text(strip=True)
                        product_price = product.find('div', class_='product-price').get_text(strip=True)
                        product_credit_price = product.find('div', class_='product-installment').get_text(strip=True)
                        product_link = HOST + product.find('a', class_='product-name').get('href')
                        print(product_link)
                        # BaseParser.get_img(product_link)
                        print(f'parsing ---> {title} ---> {sub_title} ---> -_- ---> {product_title} ---> {product_price} ---> {product_credit_price}')






    except Exception as e:
        print('Ошибка')
        print(e)


parsing()




