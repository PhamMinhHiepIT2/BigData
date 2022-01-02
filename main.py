import pandas as pd
from concurrent.futures import ProcessPoolExecutor

from crawler.app import crawl_product_id
from rabbitmq.publisher import publish
from rabbitmq.consumer import consume
from elasticSearch.get_data import get_product_id_and_name


MIN_CATEGORY = 100
MAX_CATEGORY = 100000
CRAWL_STEP = 2


def save_product_id():
    """
    Crawl product id from Tiki and publish to rabbitmq queue
    """
    # crawl product id from tiki
    for category in range(MIN_CATEGORY, MAX_CATEGORY, CRAWL_STEP):
        product_ids, _ = crawl_product_id(category)
        # publish product id to rabbitmq queue
        for product in product_ids:
            publish(str(product))
    print("Completely crawl product id and publish to rabbitmq queue!!!")


def write_crawled_data_to_parquet(parquet_file: str):
    data = get_product_id_and_name()
    col = ['id', 'name']
    df = pd.DataFrame(data, columns=col)
    df.to_parquet(parquet_file)


def main():
    with ProcessPoolExecutor(max_workers=3) as pool:
        saving_state = pool.submit(save_product_id, )
        consuming_state = pool.submit(consume, )
        if saving_state.result() is not None or consuming_state.result() is not None:
            print("Fail to consuming data from rabbitmq queue")


if __name__ == '__main__':
    save_data_crawled = "crawler/data/data_crawled.parquet"
    write_crawled_data_to_parquet(save_data_crawled)
    # main()
    # consume()
