** Implement Guide **

* Step 1: Start zookeeper, kafka-server
- bin/zookeeper-server-start.sh config/zookeeper.properties
- bin/kafka-server-start.sh config/server.properties

* Step 2: Create topic
- bin/kafka-topics.sh --create --topic result --bootstrap-server localhost:9092

* Step 3: Start questdb and questdb-kafka-connector
- bin/connect-standalone.sh config/connect-standalone.properties config/questdb-connector.properties

* Step 4: Run producer and streaming by spark-submit
- python3 producer.py
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 stream.py
  
* Step 5: Visualize data from questdb
- run getData.ipynb
- bin/kafka-console-consumer.sh --topic result --bootstrap-server localhost:9092
