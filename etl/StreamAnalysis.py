
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.conf import SparkConf

conf = SparkConf().set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "sensor-data") \
    .load("source-path")

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
