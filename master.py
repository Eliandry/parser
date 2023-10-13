import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

start_url = 'https://online.metro-cc.ru/category/chaj-kofe-kakao/kofe'


def fetch_url(url):
    with requests.Session() as session:
        response = session.get(url)
        return response.text


def parse_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    ul_tag = soup.find('ul', class_='product-attributes__list style--product-page-short-list')
    brand_name = ''
    if ul_tag:
        li_tag = ul_tag.find('li', class_='product-attributes__list-item')
        if li_tag:
            a_tag = li_tag.find('a', class_='product-attributes__list-item-link reset-link active-blue-text')
            if a_tag:
                brand_name = a_tag.get_text()

    return brand_name



def parse_product_card(product_card_content):
    data = {}

    data['id'] = product_card_content.get('data-sku')

    name_tag = product_card_content.find('span', class_='product-card-name__text')
    data['наименование'] = name_tag.get_text() if name_tag else ''

    new_price_tag = product_card_content.find('span',
                                              class_='product-price nowrap product-card-prices__actual style--catalog-2-level-product-card-major-actual color--red catalog--common offline-prices-sorting--best-level')
    price_tag = product_card_content.find('span',
                                          class_='product-price nowrap product-card-prices__old style--catalog-2-level-product-card-major-old catalog--common offline-prices-sorting--best-level')

    if price_tag:
        price_sum_tag = price_tag.find('span', class_="product-price__sum-rubles")
        data['регулярная цена'] = price_sum_tag.get_text() if price_sum_tag else ''

        new_price_sum_tag = new_price_tag.find('span', class_='product-price__sum-rubles') if new_price_tag else None
        data['промо цена'] = new_price_sum_tag.get_text() if new_price_sum_tag else ''
    else:
        price_sum_tag = product_card_content.find('span', class_="product-price__sum-rubles")
        data['регулярная цена'] = price_sum_tag.get_text() if price_sum_tag else ''
        data['промо цена'] = ''

    product_card_name_tag = product_card_content.find('div', class_="product-card__top")
    if product_card_name_tag:
        a_tag = product_card_name_tag.find('a',
                                           class_="product-card-name reset-link catalog-2-level-product-card__name style--catalog-2-level-product-card")
        if a_tag:
            href = a_tag.get('href')
            data['ссылка на товар'] = 'https://online.metro-cc.ru' + href
            data['бренд'] = ''
        else:
            data['ссылка на товар'] = ''
            data['бренд'] = ''
    else:
        data['ссылка на товар'] = ''
        data['бренд'] = ''

    return data

def fetch_and_parse_page(url):
    data_for_page = []


    html = fetch_url(url)
    soup = BeautifulSoup(html, 'lxml')


    products_inner = soup.find('div', id='products-inner', class_='subcategory-or-type__products')

    if products_inner:
        product_cards_content = products_inner.find_all('div',
                                                        class_='catalog-2-level-product-card product-card subcategory-or-type__products-item catalog--common offline-prices-sorting--best-level with-prices-drop')


        for product_card_content in product_cards_content:
            data = parse_product_card(product_card_content)
            data_for_page.append(data)

    return data_for_page

with requests.Session() as session:
    response = session.get(start_url)
    soup = BeautifulSoup(response.text, 'lxml')

    try:
        num_pages = soup.find('ul', class_='catalog-paginate v-pagination')
        last_page = int(num_pages.find_all('li')[-2].get_text()) + 1
    except:
        last_page = 1

urls = [start_url + f'?page={i}' for i in range(1, last_page)]


with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(fetch_and_parse_page, urls))


data = [item for sublist in results for item in sublist]


df = pd.DataFrame(data)
df.to_excel('ans.xlsx', index=False)