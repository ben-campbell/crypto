#!/usr/bin/python
import websocket
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import types

engine = sqlalchemy.create_engine('mysql+mysqlconnector://crypto:bitcoin@127.0.0.1:3306/crypto')
engine.execute('DROP TABLE IF EXISTS Ticker')

ticker_types = {'product_id': types.VARCHAR(10), 
                'sequence': types.BigInteger(), 
                'price': types.Float(),
                'open_24h': types.Float(),
                'volume_24h': types.Float(),
                'low_24h': types.Float(),
                'high_24h': types.Float(),
                'volume_30d': types.Float(),
                'best_bid': types.Float(),
                'best_ask': types.Float(),
		'side': types.VARCHAR(10),
                'time': sqlalchemy.dialects.mysql.DATETIME(fsp=6),
                'trade_id': types.Integer(),
                'last_size': types.Float()}

create_table_statement = """CREATE TABLE `Ticker` (
                            `type` text,
                            `sequence` bigint DEFAULT NULL,
                            `product_id` varchar(10),
                            `price` float DEFAULT NULL,
                            `open_24h` float DEFAULT NULL,
                            `volume_24h` float DEFAULT NULL,
                            `low_24h` float DEFAULT NULL,
                            `high_24h` float DEFAULT NULL,
                            `volume_30d` float DEFAULT NULL,
                            `best_bid` float DEFAULT NULL,
                            `best_ask` float DEFAULT NULL,
                            `side` varchar(10) DEFAULT NULL,
                            `time` datetime(6) DEFAULT NULL,
                            `trade_id` int,
                            `last_size` float DEFAULT NULL,
                            PRIMARY KEY(`product_id`, `trade_id`))"""

engine.execute(create_table_statement)

URI = 'wss://ws-feed.pro.coinbase.com'
with open('request.json', 'r') as file:
    req = file.read().replace('\n', '')

ws = websocket.create_connection(URI)
ws.send(req)

ticker_df = None
batch_size = 5
while True:
   result = ws.recv()
   result_dict = json.loads(result)
   if result_dict['type'] == 'ticker':
      print(result)
   for key in result_dict.keys():
      result_dict[key] = [result_dict[key]]

   if result_dict['type'][0] == 'ticker':
      result_dict['time'][0] = result_dict['time'][0].replace('T',' ').replace('Z','');
      if ticker_df is None:
         ticker_df = pd.DataFrame(data = result_dict)
      else:
         ticker_df = ticker_df.append(pd.DataFrame(data = result_dict))
         if ticker_df.shape[0] % batch_size == 0:             
            ticker_df.to_sql('Ticker', engine, index = False, if_exists = 'append', dtype = ticker_types)
            ticker_df = None
