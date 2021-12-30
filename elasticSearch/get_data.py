from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd

from elasticSearch.config import HOST, PORT, ES_INDEX

es = Elasticsearch(host=HOST, port=PORT)
# hardcode for elasticsearch index
es_index = "_".join([ES_INDEX, "30122021"])


def get_data_from_elastic():
    # query: The elasticsearch query.
    query = {
        "query": {
            "bool": {
                "filter": []
            }
        },
        "fields": ["name"],
        "_source": False
    }

    # Scan function to get all the data.
    rel = scan(client=es,
               query=query,
               scroll='5m',
               index=es_index,
               raise_on_error=True,
               preserve_order=False,
               clear_scroll=True)

    # Keep response in a list.
    result = list(rel)
    return result


def get_product_name():
    """
    Get all product names from elasticsearch data

    Returns:
        List name of products
    """
    res = []
    es_data = get_data_from_elastic()
    for hit in es_data:
        try:
            product_name = hit['fields']['name'][0]
            res.append(product_name)
        except Exception as e:
            print(e)

    return product_name
