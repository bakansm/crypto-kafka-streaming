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

# finalDF = spark.read.option("mode", "PERMISSIVE").json(jsonDF)

# base_df = reader.selectExpr("CAST(value as STRING)", "timestamp")


# jsonDF = reader.selectExpr("CAST(value as STRING)")

# finalDF = jsonDF.toJSON().first()


# sample_schema = (StructType()
#   .add("col_a", StringType())
#   .add("col_b", StringType())
#   .add("col_c", StringType())
#   .add("col_d", StringType()))

# info_dataframe = base_df.select(from_json(col("value"), sample_schema).alias("sample"), "timestamp")


# info_df_fin = info_dataframe.select("sample.*", "timestamp")

# info_dataframe.printSchema()
# query = reader.writeStream \
#   .outputMode("append") \
#   .format("kafka") \
#   .option("checkpointLocation", "/home/bakansm/Documents/kafka-ml-test/checkpoint") \
#   .start()

# query.awaitTermination()

# query.explain()

ds = reader \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("checkpointLocation", "/home/bakansm/Documents/kafka-ml-test/checkpoint") \
  .option("topic", "topic_BTC") \
  .start()

ds.awaitTermination()