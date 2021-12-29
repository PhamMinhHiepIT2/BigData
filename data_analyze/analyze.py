from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName('BigData') \
    .master('spark://13.67.92.248:7077') \
    .getOrCreate()
