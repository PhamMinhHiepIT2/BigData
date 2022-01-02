from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans


spark = SparkSession \
    .builder \
    .appName('BigData') \
    .getOrCreate()


def analyze_data(data):
    word2Vec = Word2Vec(vectorSize=4, minCount=0,
                        inputCol='name', outputCol='features')
    model = word2Vec.fit(data)
    result = model.transform(data)
    kmeans = KMeans(k=10).setSeed(1)
    kmean_model = kmeans.fit(result.select('features'))
    data_transformed = kmean_model.transform(result)
    return data_transformed


def submit_job():
    data_crawled = "/opt/spark-apps/data_crawled.parquet"
    save_transformed_data = "/opt/spark-apps/transformed_data.parquet"
    # pre-process crawled data
    data = spark.read.parquet(data_crawled)
    # clustering
    transformed_data = analyze_data(data)
    transformed_data = transformed_data.withColumn(
        'name',
        concat_ws(" ", "name"))
    transformed_data.write.parquet(save_transformed_data)


if __name__ == '__main__':
    submit_job()
