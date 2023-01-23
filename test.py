import findspark
findspark.init('F:\spark-3.3.1-bin-hadoop3')

import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from config import config,params

n_secs=1
topic=config['topic_1']

conf=SparkConf().setAppName("Bigdata").setMaster("local[*]")
sc=SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc=StreamingContext(sc, n_secs)

kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {
    bootstrap.servers'
})