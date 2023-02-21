import json, requests, time, asyncio
import datetime as dt
import pandas as pd
import numpy as np
import os

from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import load_model
from kafkaHelper import initProducer, produceRecord
from config import config, params

os.system('clear')

# real time data collector
async def async_getCryptoRealTimeData(producer, topic, param, time_inverval):
    while True:
        t_0 = time.time()
        # call API
        uri = 'https://api.binance.com/api/v3/ticker'
        # print('URI: ' + uri )


        # Store 1 hour API data
        datetime=[]
        open=[]
        high=[]
        low=[]
        volume=[]
        vwap=[]

        for i in range(0,5):
            res = requests.get(uri,params=param)
            if (res.status_code==200):            # read json response
                # read json response
                data = json.loads(res.content)
                date = dt.datetime.fromtimestamp(data['openTime']/1000)
                # add schema
                datetime.append(str(date))
                open.append(float(data['openPrice']))
                high.append(float(data['highPrice']))
                low.append(float(data['lowPrice']))
                volume.append(float(data['volume']))
                vwap.append(float(data['weightedAvgPrice']))
                print('Get {0}/60 Data'.format(i+1))
                time.sleep(60)
            else:
                # debug / print message
                print('Failed API request at time {0}'.format(dt.datetime.utcnow()))
                print('======================================')
                time.sleep(60)
        df = pd.DataFrame({ 'Date': datetime,
                            'Open': open,
                            'High': high,
                            'Low': low,
                            'Volume BTC': volume,
                            'Vwap': vwap
                        })
        inputData = df.set_index("Date")


        # Create X, Y scaler
        scalerInput = MinMaxScaler(feature_range=(0,1))
        scalerOuput = MinMaxScaler(feature_range=(0,1))

        scalerOuput_features = inputData[['Open']]
        scalerOuput_features = scalerOuput_features.values
        scalerOuput_features = scalerOuput.fit_transform(scalerOuput_features)

        inputData = scalerInput.fit_transform(inputData)

        inputData=np.array(inputData)
        inputData=np.reshape(inputData,(inputData.shape[0],inputData.shape[1],1))
        model = load_model('btc-predict-model.h5')
        pred = model.predict(inputData)
        pred = scalerOuput.inverse_transform(pred)
        df['Predict'] = pred
        print(df)
        df.reset_index()
        for index, row in df.iterrows():
            message =   {   
                            'Date': row['Date'],
                            'Open': row['Open'],
                            'High': row['High'],
                            'Low': row['Low'],
                            'Volume BTC': row['Volume BTC'],
                            'Vwap': row['Vwap'],
                            'Predict': row['Predict']
                        }
            # produce record to kafka
            produceRecord(message, producer, topic)
        
        # wait
        await asyncio.sleep(time_inverval - (time.time() - t_0))

# initialize kafka producer
producer = initProducer()

# define async routine
async def main():
    await asyncio.gather(
    async_getCryptoRealTimeData(producer, config['topic_1'], params['param'], params['api_call_period'])
)

# run async routine
asyncio.run(main())