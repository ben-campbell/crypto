# crypto

The purpose of this project is to:

1. demonstrate streaming data from websockets via Python
2. demonstrate Python-MySQL connectivity
3. demonstrate time series data exploration using Python/SQL interactions

Towards this end we have chosen a cryptocurrency data set, from [coinbase.com](http://coinbase.com). Documentation of the websocket feed can be found [here](https://docs.pro.coinbase.com/#websocket-feed).

## Files

| file | description |
| ---- | ----------- |
| requirements.txt | contains the Python modules to install, via ```pip3 install requirements.txt```|
| request.json | the subscribe request to send to the Coinbase websocket API |
| coinbase_client.py | a python script that subscribes to the Coinbase websocket feed and ingests incoming data to a MySQL database |
| prices.ipynb | a Jupyter notebook that pulls data from the database to analyze the cryptocurrency prices |

## Prerequisites

- Python3
- Jupyter
- Python modules installed via:
```pip3 install requirements.txt```
- MySQL: [install](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/) and create a new database and user
```
CREATE DATABASE crypto;
CREATE USER 'crypto'@'localhost' IDENTIFIED BY 'bitcoin';
GRANT ALL PRIVILEGES ON crypto . * TO 'crypto'@'localhost';
```
