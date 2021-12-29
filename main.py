import json


from crawler.app import crawler
from rabbitmq.publisher import publish


MIN_CATEGORY = 100
MAX_CATEGORY = 10000
CRAWL_STEP = 2


def main():
    # crawl data from tiki
    for category in range(MIN_CATEGORY, MAX_CATEGORY, CRAWL_STEP):
        product_list = crawler(category)
        # publish data to rabbitmq queue
        for product in product_list:
            publish(json.dumps(product))
