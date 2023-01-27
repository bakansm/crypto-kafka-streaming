from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from config import config, params
from pyspark.sql.functions import col
from config import config, params

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

spark = SparkSession \
    .builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()

reader = spark \
  .readStream \
  .format("kafka") \
  .option("header", "true") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", config["topic_1"]) \
  .option("startingOffsets", "latest") \
  .load()

ds = reader \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("checkpointLocation", "/home/bakansm/Documents/kafka-ml-test/checkpoint") \
  .option("topic", "topic_BTC") \
  .start()

ds.awaitTermination()