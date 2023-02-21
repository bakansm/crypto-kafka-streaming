params = {
  'param': {
    'symbol': 'BTCUSDT',
    'windowSize': '1h',
    'type': 'FULL'
  },
  'api_call_period': 10,
}

config = {
  # kafka
  'kafka_broker': 'localhost:9092',
  # topics
  'topic_1': 'topic_{0}'.format('BTC')
}