import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


def get_products(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    products = []

    for item in soup.find_all('div', class_='product_item'):
        product_url = f'https://deliherb.ru/{item.find("a")["href"]}'
        product_name = item.find('div', class_='product_title').text
        products.append((product_url, product_name))
        print(product_url, product_name)

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

        print(product_data)
        return product_data


def generate_xml(products):
    root = ET.Element('offers')

    for product in products:
        product_xml = ET.SubElement(root, 'offer')

        for key, value in product.items():
            element = ET.SubElement(product_xml, key)
            element.text = str(value)

    tree = ET.ElementTree(root)
    tree.write('products_deliherb.xml')


if __name__ == '__main__':
    products_data_l = []
    page = 1
    list_urls = [f'https://deliherb.ru/catalog/sostoyaniya-zdorovya?page={page}',
                 f'https://deliherb.ru/catalog/pischevye-dobavki?page={page}',
                 f'https://deliherb.ru/catalog/tovary-dlya-detej?page={page}',
                 f'https://deliherb.ru/catalog/produkty-pitaniya?page={page}',
                 f'https://deliherb.ru/catalog/travy-i-naturalnye-sredstva?page={page}',
                 f'https://deliherb.ru/catalog/sredstva-dlya-vanny-i-gigieny?page={page}',
                 f'https://deliherb.ru/catalog/sport?page={page}',
                 f'https://deliherb.ru/catalog/zootovary?page={page}',
                 f'https://deliherb.ru/catalog/tovary-dlya-doma?page={page}',
                 f'https://deliherb.ru/catalog/sredstva-lichnoj-gigieny-2?page={page}',
                 f'https://deliherb.ru/catalog/travy-2?page={page}',
                 f'https://deliherb.ru/catalog/naturalnye-sredstva-2?page={page}',
                 f'https://deliherb.ru/catalog/krasota?page={page}',
                 f'https://deliherb.ru/catalog/kollektsii-tovarov?page={page}'
                 ]
    for url in list_urls:
        products = get_products(url)
        print(url)

        # if page < 2:
        if len(products) == 0:
            print('Больше нет страниц')
            page = 1
            continue
        else:

            for product_url, product_name in products[:3]:
                product_data = get_product_data(product_url)
                if product_data:
                    products_data_l.append(product_data)

            page += 1
    # else:
    #     break


    generate_xml(products_data_l)
