import sys
import requests
from config import config, params
from kafkaHelper import produceRecord, consumeRecord, initConsumer, initProducer
from config import config, params

consumer_1 = initConsumer(config['topic_1'])

while True:
    # consume data from Kafka
    # topic 1 --> 4
    records_1 = consumeRecord(consumer_1)
    for r in records_1:
        host = 'http://localhost:9000'

        sql_query = "select * from topic_BTC latest by currency"

        try:
            response = requests.get(
                host + '/exec',
                params={'query': sql_query}).json()
            for row in response['dataset']:
                print(row[0])
        except requests.exceptions.RequestException as e:
            print(f'Error: {e}', file=sys.stderr)