from multiprocessing import Pool
import datetime
import concurrent.futures
import os
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from multiprocessing import Pool

all_count = 0
# cwd = os.getcwd()
cwd = os.path.dirname(os.path.abspath(__file__))
# file_path = f'/Dylyherb_parser/products_deliherb.xml'.replace('\\', '/')
file_path = f'{cwd}/products_deliherb.xml'.replace('\\', '/')
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
    tree = ET.ElementTree()
    root = ET.Element('offers')
    tree._setroot(root)
    for product in products:
        offer = ET.SubElement(root, 'offer')
        ET.SubElement(offer, 'sku').text = str(product["sku"])
        ET.SubElement(offer, 'vendorarticle').text = str(product["vendorarticle"])
        ET.SubElement(offer, 'vendor').text = str(product["vendor"])
        ET.SubElement(offer, 'name').text = product["name"]
        ET.SubElement(offer, 'url').text = str(product["url"])
        ET.SubElement(offer, 'barcode').text = str(product["barcode"])
        ET.SubElement(offer, 'price').text = str(product["price"])
        ET.SubElement(offer, 'available').text = str(product["available"])

    # tree = ET.ElementTree(root)
    with open(file_path, 'a', encoding='utf-8') as f:
        tree.write(f, encoding='unicode', xml_declaration=True)



def get_product_data_async(product_url):
    return get_product_data(product_url)

def walk_on_url(url_base):
    global all_count
    response = requests.get(url_base)
    page = 1
    while True:
        url = f'{url_base}?page={page}'
        products = get_products(url)
        # with open('/Dylyherb_parser/script.log', 'a') as file:
        with open(f'{cwd}/script.log', 'a') as file:
            file.write(f'{url}\n')
        if len(products) == 0:
            # with open('/Dylyherb_parser/script.log', 'a') as file:
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'Больше нет страниц по ссылке {url_base}\n')
            break
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(get_product_data_async, product_url) for product_url, product_name in
                       products]

            products_data_l = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    product_data = future.result()
                    if product_data:
                        products_data_l.append(product_data)
                except Exception as exc:
                    # with open('/Dylyherb_parser/script.log', 'a') as file:
                    with open(f'{cwd}/script.log', 'a') as file:
                        file.write(f'Ошибочная ссылка\n')



            products_data_l = [
                product_data
                for product_data in products_data_l
            ]

            # with open('/Dylyherb_parser/script.log', 'a') as file:
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'{len(products_data_l)} товаров на странице {page}\n')
            generate_xml(products_data_l, 'products_deliherb.xml')
            page += 1

def main():
    old_filename = f'{cwd}/products_deliherb.xml'
    new_filename = f'{cwd}/products_deliherb_last.xml'
    # Удаляем новый файл, если он уже существует
    if os.path.exists(new_filename):
        os.remove(new_filename)

    # Переименовываем старый файл
    if os.path.exists(old_filename):
        os.rename(old_filename, new_filename)

    # Создаем новый файл с названием старого
    open(old_filename, 'w', encoding='UTF-8').close()
    # with open('/Dylyherb_parser/script.log', 'a') as file:
    with open(f'{cwd}/script.log', 'a') as file:
        file.write(f'Начало парсера {datetime.datetime.now()}\n')

    list_urls = [
        f'https://deliherb.ru/catalog/sostoyaniya-zdorovya',
        f'https://deliherb.ru/catalog/pischevye-dobavki',
        f'https://deliherb.ru/catalog/tovary-dlya-detej',
        f'https://deliherb.ru/catalog/produkty-pitaniya',
        f'https://deliherb.ru/catalog/travy-i-naturalnye-sredstva',
        f'https://deliherb.ru/catalog/sredstva-dlya-vanny-i-gigieny',
        f'https://deliherb.ru/catalog/sport',
        f'https://deliherb.ru/catalog/zootovary',
        f'https://deliherb.ru/catalog/tovary-dlya-doma',
        f'https://deliherb.ru/catalog/sredstva-lichnoj-gigieny-2',
        f'https://deliherb.ru/catalog/travy-2',
        f'https://deliherb.ru/catalog/naturalnye-sredstva-2',
        f'https://deliherb.ru/catalog/krasota',
        f'https://deliherb.ru/catalog/kollektsii-tovarov',
    ]

    with Pool() as p:    # создаем пул процессов
        p.map(walk_on_url, list_urls)    # распределяем url'ы по процессам


if __name__ == '__main__':
    main()