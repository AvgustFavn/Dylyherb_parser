import datetime

import requests
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup


def get_products(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    products = []

    for item in soup.find_all('div', class_='product_item'):
        product_url = f'https://deliherb.ru/{item.find("a")["href"]}'
        product_name = item.find('div', class_='product_title').text
        products.append((product_url, product_name))

    return products


def get_product_data(product_url):
    response = requests.get(product_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    product_data = {}
    h1 = soup.find('h1')
    try:
        product_data['sku'] = h1['data-product']
    except:
        product_data['sku'] = 0

    product_data['vendorarticle'] = soup.find('label', class_='lfeature').text
    if product_data['vendorarticle'] == "0":
        return None
    else:
        product_data['vendor'] = soup.find('div', class_='annot-brand hideable').find('a').text
        product_data['name'] = soup.find('h1').text
        product_data['url'] = product_url
        labels_names = soup.find_all('label', class_='featurename')
        labels = soup.find_all('label', class_='lfeature')
        try:
            num = 0
            product_data['barcode'] = 0
            for n in labels_names:
                if 'код' in n.text:
                    product_data['barcode'] = int(labels[num].text)
                    break
                num += 1
        except:
            product_data['barcode'] = 0

        if product_data['barcode'] == 0:
            return None

        product_data['price'] = int(soup.find('span', class_='price').text.replace(' ', '').replace('₽', ''))

        if 'США' in labels[1].text:
            product_data['available'] = 1
        else:
            product_data['available'] = 0

        return product_data


def generate_xml(products, file_name):
    try:
        tree = ET.parse(file_name)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError):
        root = ET.Element('offers')

    for product in products:
        product_xml = ET.SubElement(root, 'offer')

        for key, value in product.items():
            element = ET.SubElement(product_xml, key)
            element.text = str(value)

    # Записываем все обратно в файл
    tree = ET.ElementTree(root)
    tree.write(file_name, xml_declaration=True, encoding='utf-8')

def main():
    # Очистить файл перед началом записи
    open('products_deliherb.xml', 'w').close()
    print(f'Начало парсера {datetime.datetime.now()}')
    all_count = 0
    list_urls = [
        'https://deliherb.ru/catalog/sostoyaniya-zdorovya',
        'https://deliherb.ru/catalog/pischevye-dobavki',
        'https://deliherb.ru/catalog/tovary-dlya-detej',
        'https://deliherb.ru/catalog/produkty-pitaniya',
        'https://deliherb.ru/catalog/travy-i-naturalnye-sredstva',
        'https://deliherb.ru/catalog/sredstva-dlya-vanny-i-gigieny',
        'https://deliherb.ru/catalog/sport',
        'https://deliherb.ru/catalog/zootovary',
        'https://deliherb.ru/catalog/tovary-dlya-doma',
        'https://deliherb.ru/catalog/sredstva-lichnoj-gigieny-2',
        'https://deliherb.ru/catalog/travy-2',
        'https://deliherb.ru/catalog/naturalnye-sredstva-2',
        'https://deliherb.ru/catalog/krasota',
        'https://deliherb.ru/catalog/kollektsii-tovarov',
    ]

    for url_base in list_urls:
        page = 1
        while True:
            url = f'{url_base}?page={page}'
            products_data_l = []
            products = get_products(url)
            print(url)
            if len(products) == 0:
                print('Больше нет страниц')
                break
            else:
                for product_url, product_name in products:
                    try:
                        product_data = get_product_data(product_url)
                        if product_data:
                            products_data_l.append(product_data)
                    except:
                        print(f'Ошибочная ссылка - {product_url}')

                existing_skus = set()
                try:
                    tree = ET.parse('products_deliherb.xml')
                    root = tree.getroot()
                    for offer in root.findall('offer'):
                        sku = offer.find('vendorarticle').text
                        all_count += 1
                        existing_skus.add(sku)
                except (ET.ParseError, FileNotFoundError):
                    pass

                products_data_l = [
                    product_data
                    for product_data in products_data_l
                    if product_data['vendorarticle'] not in existing_skus
                ]

                print(f'{len(products_data_l)} товаров на странице {page}')
                generate_xml(products_data_l, 'products_deliherb.xml')
                page += 1

    print(f'Парсер закончил свою работу, все товары {all_count}')

main()
