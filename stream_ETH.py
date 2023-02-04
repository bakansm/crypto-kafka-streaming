from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from config import config, params
from pyspark.sql.functions import col
from config import config, params

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
INPUT_KAFKA_TOPIC = config["topic_2"]
OUTPUT_KAFKA_TOPIC = 'result'
CHECKPOINT_LOCATION = "./checkpoint"


### Khởi tạo spark
spark = SparkSession \
    .builder \
    .appName("Kafka streaming test") \
    .master('local[*]') \
    .getOrCreate()


### Tạo khung dataframe
schema = StructType([ 
  StructField("timestamp", StringType(), True),
  StructField("currency" , StringType(), True),
  StructField("amount" , StringType(), True),
])


### Lấy dữ liệu từ kafka ( định dạng json )
jsonData = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("subscribe", INPUT_KAFKA_TOPIC) \
  .option("startingOffsets", "latest") \
  .option("includeHeaders", "true") \
  .option("failOnDataLoss","false") \
  .load()

### Chuyển json sang datafram pyspark
dataframe = jsonData.select(from_json(col("value").cast("string"), schema).alias("value"))


############################# IMPORTANT #############################
### ---> Code mô hình máy học ở đây khúc này bằng dataframe  <--- ###
#####################################################################


### Chuyển dataframe ngược lại json để thêm vào topic
forward_data = dataframe.select(to_json(struct("value.*")).alias("value"))

### Thêm data vào lại topic
ds = forward_data \
  .writeStream \
  .format("kafka") \
  .trigger(processingTime="5 seconds") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
  .option("checkpointLocation", CHECKPOINT_LOCATION) \
  .outputMode("append") \
  .option("topic", OUTPUT_KAFKA_TOPIC) \
  .start()

ds.awaitTermination()