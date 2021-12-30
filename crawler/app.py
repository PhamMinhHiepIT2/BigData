import requests
import json


from crawler.config import HEADER, URL, PRODUCT_URL


def crawl_product_id(category: int):
    product_list = []
    i = 1
    while (True):
        print("Crawl page: ", i)
        print(URL.format(category, i))
        response = requests.get(URL.format(category, i), headers=HEADER)

        if (response.status_code != 200):
            break

        products = json.loads(response.text)["data"]

        if (len(products) == 0):
            break

        for product in products:
            product_id = str(product["id"])
            product_list.append(product_id)
        i += 1

    return product_list, i


def crawl_product(product_list=[]):
    product_detail_list = []
    for product_id in product_list:
        response = requests.get(PRODUCT_URL.format(product_id), headers=HEADER)
        if (response.status_code == 200):
            product_detail_list.append(response.text)
    return product_detail_list


flatten_field = ["badges", "inventory", "categories", "rating_summary",
                 "brand", "seller_specifications", "current_seller", "other_sellers",
                 "configurable_options",  "configurable_products", "specifications", "product_links",
                 "services_and_promotions", "promotions", "stock_item", "installment_info"]


def adjust_product(product):
    e = json.loads(product)
    if not e.get("id", False):
        return None

    for field in flatten_field:
        if field in e:
            e[field] = json.dumps(
                e[field],
                ensure_ascii=False).replace('\n', '')
    return e


def crawl_product_by_id(product_id: int) -> json:
    """
    Crawl product detail by product id

    Args:
        product_id : id of product

    Returns:
        json: product detail in json format
    """
    response = requests.get(PRODUCT_URL.format(product_id), headers=HEADER)
    if response.status_code != 200:
        print("Fail to crawl product id: {}".format(product_id))
        return -1
    # convert product detail in text format to json format
    product_detail = adjust_product(response.text)
    return product_detail


def crawler(category: int):
    product_list, page = crawl_product_id(category)
    print("No. Page: ", page)
    print("No. Product ID: ", len(product_list))
    # crawl detail for each product id
    product_list = crawl_product(product_list)
    # flatten detail before converting to csv
    product_json_list = [adjust_product(p) for p in product_list]
    return product_json_list
