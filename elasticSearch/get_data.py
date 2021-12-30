from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd

from elasticSearch.config import HOST, PORT, ES_INDEX

es = Elasticsearch(host=HOST, port=PORT)
# hardcode for elasticsearch index
es_index = "_".join([ES_INDEX, "30122021"])

# query: The elasticsearch query.
query = {
    "query": {
        "bool": {
            "filter": []
        }
    },
    "fields": ["name", "id"],
    "_source": False
}


def get_data_from_elastic(query):
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


def get_product_id_and_name(query=query):
    """
    Get all product ids and product names from elasticsearch data

    Returns:
        List mapping id and name of product
    """
    res = []
    es_data = get_data_from_elastic(query)
    for hit in es_data:
        try:
            # get product name and lowercase it
            product_name = str(hit['fields']['name'][0])
            product_name = product_name.lower().split(" ")
            product_id = hit['fields']['id'][0]
            res.append((product_id, product_name))
        except Exception as e:
            print(e)
    return res
