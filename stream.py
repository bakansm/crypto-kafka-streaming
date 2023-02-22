from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from config import config, params
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = config['topic_1']
CHECKPOINT_LOCATION = "./checkpoint"

### Khởi tạo spark
spark = SparkSession \
    .builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()


### Tạo khung dataframe
schema = StructType([ 
  StructField("Date", StringType(), True),
  StructField("Open", FloatType(), True),
  StructField("High", FloatType(), True),
  StructField("Low", FloatType(), True),
  StructField("Volume BTC", FloatType(), True),
  StructField("Vwap", FloatType(), True),
  StructField("Predict", FloatType(), True),
])


### Lấy dữ liệu từ kafka ( định dạng json )
data = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", "earliest") \
  .option("includeHeaders", "true") \
  .option("failOnDataLoss","false") \
  .load()

### Chuyển json sang datafram pyspark
dataframe = data.select(from_json(col("value").cast("string"), schema).alias("value")).select(col("value.*"))

############################# IMPORTANT #############################
### ---> Code mô hình máy học ở đây khúc này bằng dataframe  <--- ###
#####################################################################

# ### Chuyển dataframe ngược lại json để thêm vào topic
reward_data = dataframe.select(to_json(struct("*")).alias("value"))

# ### Thêm data vào lại topic
ds = reward_data \
  .writeStream \
  .format("kafka") \
  .trigger(processingTime="60 seconds") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("checkpointLocation", CHECKPOINT_LOCATION) \
  .outputMode("update") \
  .option("topic", "result") \
  .start()

ds.awaitTermination()

# result_ds = result \
#   .writeStream \
#   .format(“csv”) \
#   .outputMode(“append”) \
#   .option(“path”, "/home/bakansm/Code/kafka-ml-test/result.csv") \
#   .option(“checkpointLocation”, CHECKPOINT_LOCATION) \
#   .start()