import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, split, concat_ws, transform
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans

from elasticSearch.get_data import get_product_id_and_name


spark = SparkSession \
    .builder \
    .appName('BigData') \
    .getOrCreate()


def write_crawled_data_to_parquet(parquet_file: str):
    data = get_product_id_and_name()
    col = ['id', 'name']
    df = pd.DataFrame(data, columns=col)
    df.to_parquet(parquet_file)


def process_data(parquet_file: str):
    """
    Lowercase and split into word from product name

    Args:
        parquet_file (str): file save data in parquet format

    Returns:
        data processed
    """
    df = spark.read.parquet(parquet_file)
    # lowercase product name
    data = df.select('id', lower(col('name')))
    data = data.withColumnRenamed('lower(name)', 'name')
    # split word in product name
    data = data.select('id', split(data.name, " "))
    data = data.withColumnRenamed('split(name, )', 'name')
    return data


def analyze_data(data):
    word2Vec = Word2Vec(vectorSize=4, minCount=0,
                        inputCol='name', outputCol='features')
    model = word2Vec.fit(data)
    result = model.transform(data)
    kmeans = KMeans(k=10).setSeed(1)
    kmean_model = kmeans.fit(result.select('features'))
    data_transformed = kmean_model.transform(result)
    return data_transformed


if __name__ == '__main__':
    save_data_crawled = "crawler/data/data_crawled.parquet"
    save_transformed_data = "crawler/data/transformed_data.parquet"
    # save crawled data to parquet file
    write_crawled_data_to_parquet(save_data_crawled)
    # pre-process crawled data
    processed_data = process_data(save_data_crawled)
    # clustering
    transformed_data = analyze_data(process_data)
    transformed_data = transformed_data.withColumn(
        'name',
        concat_ws(" ", "name"))
    df = transformed_data.toPandas()
    # save data transformed to parquet file
    df.to_parquet(save_transformed_data)
