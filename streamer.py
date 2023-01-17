from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import config, params

spark = (
    SparkSession.builder.appName("Kafka PySpark Streaming")
    .master("local")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:8083")
        .option("subscribe", config['topic_1'])
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df.printSchema()

