import random
import time
import datetime
import concurrent.futures
import os

import httpx
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from multiprocessing import Pool

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
    {'https': 'http://user125167:klw48b@194.32.240.8:4513'},
    {'https': 'http://user125167:klw48b@45.85.64.215:4513'},
    {'https': 'http://user125167:klw48b@194.32.240.141:4513'},
    {'https': 'http://user125167:klw48b@194.32.240.227:4513'},
    {'https': 'http://user125167:klw48b@45.128.131.91:4513'},
    {'https': 'http://user125167:klw48b@45.11.22.38:4513'},
    {'https': 'http://user125167:klw48b@45.88.208.246:4513'},
    {'https': 'http://user125167:klw48b@45.85.67.61:4513'},
    {'https': 'http://user125167:klw48b@45.85.64.145:4513'},
    {'https': 'http://user125167:klw48b@85.8.187.90:4513'},
    {'https': 'http://user125167:klw48b@195.225.96.250:4513'},
    {'https': 'http://user125167:klw48b@213.166.95.178:4513'},
    {'https': 'http://user125167:klw48b@212.107.24.251:4513'},
    {'https': 'http://user125167:klw48b@45.128.130.251:4513'},
    {'https': 'http://user125167:klw48b@45.88.208.74:4513'},
    {'https': 'http://user125167:klw48b@45.85.67.201:4513'},
    {'https': 'http://user125167:klw48b@85.8.187.146:4513'},
    {'https': 'http://user125167:klw48b@45.85.64.94:4513'},
    {'https': 'http://user125167:klw48b@45.86.3.136:4513'},
    {'https': 'http://user125167:klw48b@45.85.67.160:4513'},
    {'https': 'http://user125167:klw48b@45.86.3.102:4513'},
    {'https': 'http://user125167:klw48b@45.89.71.100:4513'},
    {'https': 'http://user125167:klw48b@45.128.131.177:4513'},
    {'https': 'http://user125167:klw48b@45.144.38.11:4513'},
    {'https': 'http://user125167:klw48b@45.90.47.110:4513'},
    {'https': 'http://user125167:klw48b@195.225.96.145:4513'},
    {'https': 'http://user125167:klw48b@185.234.8.202:4513'},
    {'https': 'http://user125167:klw48b@45.86.3.245:4513'},
    {'https': 'http://user125167:klw48b@45.86.3.37:4513'},
    {'https': 'http://user125167:klw48b@195.225.96.217:4513'}

]

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

products_data_l = []


def is_banned(url, proxy, tries=0):
    global proxies_dic
    proxy_right = {"http://": proxy['https'], "https://": proxy['https']}
    while True:
        try:
            client = httpx.Client(timeout=10, proxies=proxy_right)
            response = client.get(url)
        except:
            try:
                time.sleep(5)
                client = httpx.Client(timeout=10, proxies=proxy_right)
                response = client.get(url)
            except:
                proxies_dic.remove(proxy)
                client.close()
                print(f'{datetime.datetime.now()}: Прокси был удален как не рабочий - {proxy}\n')
                with open(f'{cwd}/script.log', 'a') as file:
                    file.write(f'{datetime.datetime.now()}: Прокси был удален как не рабочий - {proxy}\n')
                return is_banned(url, random.choice(proxies_dic))

        data = response.content
        content_length = len(data)

        if content_length > 0:
            client.close()
            return False
        else:
            try:
                proxies_dic.remove(proxy)
            except:
                with open(f'{cwd}/script.log', 'a') as file:
                    file.write(f"{datetime.datetime.now()}: Прокси закончились\n")
                print(f"{datetime.datetime.now()}: Прокси закончились\n")
                return True
            client.close()
            print(f'{datetime.datetime.now()}: Прокси был удален как не рабочий - {proxy}\n')
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'{datetime.datetime.now()}: Прокси был удален как не рабочий - {proxy}\n')
            return is_banned(url, random.choice(proxies_dic))


def get_products(url, sess, proxy):
    # time.sleep(random.choice([26, 84, 54, 28]))
    if is_banned(url, proxy) == False:
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
        try:
            proxy = random.choice(proxies_dic)
            response = requests.request(
                'get',
                url=url,
                proxies=proxy,
                verify=False,
                headers=headers,
                # stream=True
            )
        except:
            try:
                proxy = random.choice(proxies_dic)
                response = requests.request(
                    'get',
                    url=url,
                    proxies=proxy,
                    verify=False,
                    headers=headers,
                    # stream=True
                )
            except:
                return None

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
        except:
            return None
        products = []

        for item in soup.find_all('div', class_='product_item'):
            product_url = f'https://deliherb.ru/{item.find("a")["href"]}'
            try:
                product_name = item.find('div', class_='product_title').text
            except:
                continue
            products.append((product_url, product_name))

        return products


def get_product_data(product_url, sess, proxy):
    if is_banned(product_url, proxy) == False:
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
        try:
            response = requests.request(
                'get',
                url=product_url,
                proxies=proxy,
                verify=False,
                headers=headers,
                # stream=True
            )
        except:
            try:
                proxy = random.choice(proxies_dic)
                response = requests.request(
                    'get',
                    url=product_url,
                    proxies=proxy,
                    verify=False,
                    headers=headers,
                    # stream=True
                )
            except:
                return None

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
        except:
            return None
        product_data = {}
        h1 = soup.find('h1')
        try:
            product_data['sku'] = h1['data-product']
        except:
            product_data['sku'] = 0

        try:
            product_data['vendorarticle'] = soup.find('label', class_='lfeature').text
        except:
            with open(f'{cwd}/script.log', 'a') as file:
                print(f'{datetime.datetime.now()}: Товар без артикула {product_url}\n')
                file.write(f'{datetime.datetime.now()}: Товар без артикула {product_url}\n')
            return None
        if product_data['vendorarticle']:
            if product_data['vendorarticle'] == "0":
                with open(f'{cwd}/script.log', 'a') as file:
                    print(f'{datetime.datetime.now()}: Товар без артикула {product_url}\n')
                    file.write(f'{datetime.datetime.now()}: Товар без артикула {product_url}\n')
                return None
            else:
                try:
                    product_data['vendor'] = soup.find('div', class_='annot-brand hideable').find('a').text
                except:
                    product_data['vendor'] = ''
                product_data['name'] = soup.find('h1').text
                product_data['url'] = product_url
                labels_names = soup.find_all('label', class_='featurename')
                labels = soup.find_all('label', class_='lfeature')
                # try:
                #     num = 0
                #     product_data['barcode'] = 0
                #     for n in labels_names:
                #         if 'код' in n.text:
                #             product_data['barcode'] = int(labels[num].text)
                #             break
                #         num += 1
                # except:
                #     product_data['barcode'] = 0
                #
                # if product_data['barcode'] == 0:
                #     print(product_data)
                #     with open(f'{cwd}/script.log', 'a') as file:
                #         print(f'{datetime.datetime.now()}: Товар без баркода {product_url}\n')
                #         file.write(f'{datetime.datetime.now()}: Товар без баркода {product_url}\n')
                #     return None
                try:
                    product_data['price'] = int(
                        soup.find('span', class_='price').text.replace(' ', '').replace('₽', ''))
                except:
                    return None

                try:
                    is_av = soup.find('h4').text
                    if is_av:
                        product_data['available'] = 0
                    else:
                        product_data['available'] = 1
                except:
                    product_data['available'] = 1

                return product_data
        else:
            return None




def append_to_xml(products, file_path):
    for product in products:
        text = ('<offer>'
                f'<sku>{str(product["sku"])}</sku>'
                f'<vendorarticle>{product["vendorarticle"]}</vendorarticle>'
                f'<vendor>{product["vendor"]}</vendor>'
                f'<name>{product["name"]}</name>'
                f'<url>{product["url"]}</url>'
                f'<price>{product["price"]}</price>'
                f'<available>{product["available"]}</available>'
                '</offer>')
        with open('product.txt', 'a', encoding='utf-8') as f:
            f.write(text)


# def append_to_xml(products, file_path):
#     try:
#         tree = ET.parse(file_path)
#         root = tree.getroot()
#     except ET.ParseError:
#         # Если файл пустой или не является валидным XML-файлом,
#         # создаем корневой элемент offers
#         root = ET.Element('offers')
#     else:
#         # Если корневой элемент не является offers, создаем новый элемент offers
#         if 'offers' not in root.tag:
#             root = ET.Element('offers')
#
#         # Создаем элемент offers
#     offers_element = root
#
#     for product in products:
#         offer = ET.SubElement(offers_element, 'offer')
#         ET.SubElement(offer, 'sku').text = str(product["sku"])
#         ET.SubElement(offer, 'vendorarticle').text = str(product["vendorarticle"])
#         ET.SubElement(offer, 'vendor').text = str(product["vendor"])
#         ET.SubElement(offer, 'name').text = product["name"]
#         ET.SubElement(offer, 'url').text = str(product["url"])
#         ET.SubElement(offer, 'barcode').text = str(product["barcode"])
#         ET.SubElement(offer, 'price').text = str(product["price"])
#         ET.SubElement(offer, 'available').text = str(product["available"])
#
#     # Записываем обновленное дерево в файл
#     with open(file_path, 'w', encoding='utf-8') as f:
#         tree = ET.ElementTree(root)
#         tree.write(f, encoding='unicode')


def end_xml():
    with open('product.txt', 'r') as f:
        data = f.read()

    # Создаем корневой элемент XML-документа
    root = ET.Element('offers')

    # Создаем текстовый элемент и добавляем его в корневой элемент
    text_element = ET.Element('')
    text_element.text = data
    root.append(text_element)

    # Создаем XML-дерево и записываем его в файл
    tree = ET.ElementTree(root)
    tree.write('output.xml')



def get_product_data_async(product_url, sess, proxy):
    #     time.sleep(random.choice([14, 23, 17, 10, 2]))
    return get_product_data(product_url, sess, proxy)


def check_dict_in_list(lst, key, value):
    """
    Проверяет, существует ли в списке словарей словарь с заданным ключом и значением.
    """
    for d in lst:
        if key in d and d[key] == value:
            return True
    return False


def walk_on_url(url_base):
    global all_count
    global products_data_l
    list_urls.remove(url_base)
    page = 1
    session = requests.Session()
    while True:
        # while page < 3:
        #         time.sleep(random.choice([14, 44, 11, 20]))
        url = f'{url_base}?page={page}'
        proxy = random.choice(proxies_dic)
        products = get_products(url, session, proxy)
        if products is None:
            #             time.sleep(random.choice([14, 44, 11, 20]))
            proxy = random.choice(proxies_dic)
            products = get_products(url, session, proxy)
            if products is None:
                #                 time.sleep(random.choice([14, 44, 11, 20]))
                proxy = random.choice(proxies_dic)
                products = get_products(url, session, proxy)

        # with open('/Dylyherb_parser/script.log', 'a') as file:
        with open(f'{cwd}/script.log', 'a') as file:
            file.write(f'{url}\n')
        if len(products) == 0:
            # with open('/Dylyherb_parser/script.log', 'a') as file:
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'{datetime.datetime.now()}: Больше нет страниц по ссылке {url_base}\n')
            return
        else:
            url_prods = []
            for product_url, product_name in products:
                future = get_product_data_async(product_url, session, proxy)
                url_prods.append(future)

            for product_data in url_prods:
                try:
                    if product_data:
                        print(product_data)
                        if check_dict_in_list(products_data_l, 'sku', product_data['sku']):
                            continue
                        else:
                            products_data_l.append(product_data)
                    else:
                        with open(f'{cwd}/script.log', 'a') as file:
                            file.write(f'{datetime.datetime.now()}: Видимо, умер прокси...\n')
                            print(f'{datetime.datetime.now()}: Видимо, умер прокси...\n')
                except Exception as exc:
                    # with open('/Dylyherb_parser/script.log', 'a') as file:
                    with open(f'{cwd}/script.log', 'a') as file:
                        file.write(f'{datetime.datetime.now()}: Ошибочная ссылка\n')

            products_data_l = [
                product_data
                for product_data in products_data_l
            ]

            # with open('/Dylyherb_parser/script.log', 'a') as file:
            with open(f'{cwd}/script.log', 'a') as file:
                file.write(f'{datetime.datetime.now()}: На странице {page}\n')
            append_to_xml(products_data_l, 'products_deliherb.xml')
            # generate_xml(products_data_l, 'products_deliherb.xml')
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
    # start_xml()
    open('product.txt', 'w', encoding='UTF-8').close()
    # start_xml(old_filename)
    # with open('/Dylyherb_parser/script.log', 'a') as file:
    with open(f'{cwd}/script.log', 'a') as file:
        file.write(f'Начало парсера {datetime.datetime.now()}\n')

    with Pool() as p:
        p.map(walk_on_url, list_urls)
        p.join()
        end_xml()

    # walk_on_url(list_urls[0])


if __name__ == '__main__':
    main()
