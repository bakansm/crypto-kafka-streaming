params = {
  # crypto setup
  'currency_1': 'BTC', # Bitcoin
  'currency_2': 'ETH', # Ethereum
  'currency_3': 'LINK', # Chainlink
  'currency_4': 'NEAR',
  'currency_5': 'SOL',
  'currency_6': 'DOGE',
  'currency_7': 'MATIC',

  'ref_currency': 'USD',
  'ma': 25,
  # api setup
  'api_call_period': 10,
}

config = {
  # kafka
  'kafka_broker': 'localhost:9092',
  # topics
  'topic_1': 'topic_{0}'.format(params['currency_1']),
  'topic_2': 'topic_{0}'.format(params['currency_2']),
  'topic_3': 'topic_{0}'.format(params['currency_3']),
  'topic_4': 'topic_{0}'.format(params['currency_4']),
  'topic_5': 'topic_{0}'.format(params['currency_5']),
  'topic_6': 'topic_{0}'.format(params['currency_6']),
  'topic_7': 'topic_{0}'.format(params['currency_7']),
  'topic_8': 'topic_{0}_ma_{1}'.format(params['currency_1'], params['ma']),
  'topic_9': 'topic_{0}_ma_{1}'.format(params['currency_2'], params['ma']),
  'topic_10': 'topic_{0}_ma_{1}'.format(params['currency_3'], params['ma']),
  'topic_11': 'topic_{0}_ma_{1}'.format(params['currency_4'], params['ma']),
  'topic_12': 'topic_{0}_ma_{1}'.format(params['currency_5'], params['ma']),
  'topic_13': 'topic_{0}_ma_{1}'.format(params['currency_6'], params['ma']),
  'topic_14': 'topic_{0}_ma_{1}'.format(params['currency_7'], params['ma']),
}