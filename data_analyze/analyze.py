import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, split, concat_ws
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans

from elasticSearch.get_data import get_product_id_and_name


spark = SparkSession \
    .builder \
    .appName('BigData') \
    .getOrCreate()


def write_data_to_parquet(parquet_file: str):
    data = get_product_id_and_name()
    col = ['id', 'name']
    df = pd.DataFrame(data, columns=col)
    df.to_parquet(parquet_file)


a = spark.read.csv(
    '/Users/admin/Development/Dev/BigData/BigData/crawler/data/product.csv')
print(a)
