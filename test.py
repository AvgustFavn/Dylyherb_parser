import random
import time
import datetime
import concurrent.futures
import os
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from multiprocessing import Pool

# session = requests.Session()
# session.proxies = {
#     'http': f'http//user125167:klw48b@194.61.234.185:8584',
# }
all_count = 0
cwd = os.getcwd()
# file_path = f'/Dylyherb_parser/products_deliherb.xml'.replace('\\', '/')
file_path = f'{cwd}/products_deliherb.xml'.replace('\\', '/')
users = [{
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36"
    }]

# headers = {
#         "User-Agent": (random.choice(users))['User-Agent'],
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'Accept-Encoding': 'gzip, deflate, br, zstd',
#     'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
#     'Cache-Control': 'max-age=0',
#     'Priority': 'u=0, i',
#     'Sec-Ch-Ua': random.choice(['"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', '"Google Chrome";v="124", "Chromium";v="124"', '"Not.A/Brand";v="22"']),
#     'Sec-Ch-Ua-Mobile': '?0',
#     'Sec-Ch-Ua-Platform': random.choice(['"macOS"', '"Windows"', '"Linux"']),
#     'Sec-Fetch-Dest': 'document',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-Site': 'none',
#     'Sec-Fetch-User': '?1',
#     'Upgrade-Insecure-Requests': '1'
#     }

proxies_dic = [
    {'https': 'http://user125167:klw48b@45.88.208.43:3485'},
    {'https': 'http://user125167:klw48b@45.128.131.173:3485'},
    {'https': 'http://user125167:klw48b@45.86.3.142:3485'},
    {'https': 'http://user125167:klw48b@45.86.3.161:2855'},
    {'https': 'http://user125167:klw48b@195.225.96.170:2855'},
    {'https': 'http://user125167:klw48b@45.88.208.168:2855'},
    {'https': 'http://user125167:klw48b@45.128.131.72:2855'},
    {'https': 'http://user125167:klw48b@185.21.140.123:2855'},
    {'https': 'http://user125167:klw48b@45.90.47.20:2855'},

]

def is_banned(resp, proxy):
    global proxies_dic
    time.sleep(5)
    resp.close()
    if len(resp.text) < 1:
        with open(f'{cwd}/script.log', 'a') as file:
            print(f'Прокси умер {proxy}\n')
            file.write(f'Прокси умер {proxy}\n')
            proxies_dic.remove(proxy)
            print(proxies_dic)

        return True
    else:
        return False



def get_products(url, sess, proxy):
    time.sleep(random.choice([26, 84, 54, 28]))
    headers = {
        "User-Agent": (random.choice(users))['User-Agent'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Priority': 'u=0, i',
        'Sec-Ch-Ua': random.choice(['"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                                    '"Google Chrome";v="124", "Chromium";v="124"', '"Not.A/Brand";v="22"']),
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': random.choice(['"macOS"', '"Windows"', '"Linux"']),
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }
    response = requests.request(
        'get',
        url=url,
        proxies=proxy,
        verify=False,
        headers=headers,
        stream=True
    )
    if is_banned(response, proxy):
        headers = {
            "User-Agent": (random.choice(users))['User-Agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Priority': 'u=0, i',
            'Sec-Ch-Ua': random.choice(['"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                                        '"Google Chrome";v="124", "Chromium";v="124"', '"Not.A/Brand";v="22"']),
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': random.choice(['"macOS"', '"Windows"', '"Linux"']),
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
        proxy = random.choice(proxies_dic)
        response = requests.request(
            'get',
            url=url,
            proxies=proxy,
            verify=False,
            headers=headers,
            stream=True
        )
        if is_banned(response, proxy):
            return None

    soup = BeautifulSoup(response.text, 'html.parser')
    products = []

    for item in soup.find_all('div', class_='product_item'):
        product_url = f'https://deliherb.ru/{item.find("a")["href"]}'
        product_name = item.find('div', class_='product_title').text
        products.append((product_url, product_name))

    return products


def get_product_data(product_url, sess, proxy):
    headers = {
        "User-Agent": (random.choice(users))['User-Agent'],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Priority': 'u=0, i',
        'Sec-Ch-Ua': random.choice(['"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                                    '"Google Chrome";v="124", "Chromium";v="124"', '"Not.A/Brand";v="22"']),
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': random.choice(['"macOS"', '"Windows"', '"Linux"']),
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }
    response = requests.request(
        'get',
        url=product_url,
        proxies=proxy,
        verify=False,
        headers=headers,
        stream=True
    )
    if is_banned(response, proxy):
        headers = {
            "User-Agent": (random.choice(users))['User-Agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Priority': 'u=0, i',
            'Sec-Ch-Ua': random.choice(['"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                                        '"Google Chrome";v="124", "Chromium";v="124"', '"Not.A/Brand";v="22"']),
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': random.choice(['"macOS"', '"Windows"', '"Linux"']),
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
        proxy = random.choice(proxies_dic)
        response = requests.request(
            'get',
            url=product_url,
            proxies=proxy,
            verify=False,
            headers=headers,
            stream=True
        )
        if is_banned(response, proxy):
            return None

    soup = BeautifulSoup(response.text, 'html.parser')
    product_data = {}
    h1 = soup.find('h1')
    try:
        product_data['sku'] = h1['data-product']
    except:
        product_data['sku'] = 0

    product_data['vendorarticle'] = soup.find('label', class_='lfeature').text
    if product_data['vendorarticle'] == "0":
        with open(f'{cwd}/script.log', 'a') as file:
            print(f'Товар без артикула {product_url}\n')
            file.write(f'Товар без артикула {product_url}\n')
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
            with open(f'{cwd}/script.log', 'a') as file:
                print(f'Товар без баркода {product_url}\n')
                file.write(f'Товар без баркода {product_url}\n')
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



def get_product_data_async(product_url, sess, proxy):
    time.sleep(random.choice([14, 23, 17, 10, 2]))
    return get_product_data(product_url, sess, proxy)

def walk_on_url(url_base):
    global all_count
    page = 1
    session = requests.Session()
    while True:
        time.sleep(random.choice([14, 44, 11, 20]))
        url = f'{url_base}?page={page}'
        proxy = random.choice(proxies_dic)
        products = get_products(url, session, proxy)

        # with open('/Dylyherb_parser/script.log', 'a') as file:
        with open(f'{cwd}/script.log', 'a') as file:
            file.write(f'{url}\n')
        if len(products) == 0:
            # with open('/Dylyherb_parser/script.log', 'a') as file:
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'Больше нет страниц по ссылке {url_base}\n')
            return
        else:
            # with concurrent.futures.ThreadPoolExecutor() as executor:
            #     futures = [executor.submit(get_product_data_async, product_url) for product_url, product_name in
            #            products]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for product_url, product_name in products:
                    future = executor.submit(get_product_data_async, product_url, session, proxy)
                    futures.append(future)

            products_data_l = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    product_data = future.result()
                    if product_data:
                        products_data_l.append(product_data)
                    else:
                        with open(f'{cwd}/script.log', 'a') as file:
                            file.write(f'Видимо, умер прокси...\n')
                            print(f'Видимо, умер прокси...\n')
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

    with Pool() as p:
        p.map(walk_on_url, list_urls)
    # walk_on_url(list_urls[0])


if __name__ == '__main__':
    main()