import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, split, concat_ws
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans

from elasticSearch.get_data import get_product_id_and_name


spark = SparkSession \
    .builder \
    .appName('BigData') \
    .master('spark://13.67.92.248:7077') \
    .getOrCreate()


def process_data():
    data = get_product_id_and_name()
    col = ['id', 'name']
    df = pd.DataFrame(data, columns=col)
