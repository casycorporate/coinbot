import requests as requests
import requests
import hmac
import hashlib
import json
from urllib import parse
from types import SimpleNamespace
from datetime import datetime
import config
import psycopg2
from multiprocessing import Pool
import os
import time
from DBprocess import DBObject  as dbo
import logging
logging.basicConfig(filename='logger.log', level=logging.INFO)

API_KEY = 'mx0sC5LYhgEPV1YpKw'
SECRET_KEY = '1f644e42950b4a7db799bafde2b5ddb1'
ROOT_URL = 'https://www.mexc.com'

class ticker:
    def __init__(self, symbol, state, price_scale):
        self.symbol, self.state, self.price_scale = symbol, state, price_scale

def _get_server_time():
    return int(time.time())


def _sign(method, path, original_params=None):
    params = {
        'api_key': API_KEY,
        'req_time': _get_server_time(),
    }
    if original_params is not None:
        params.update(original_params)
    params_str = '&'.join('{}={}'.format(k, params[k]) for k in sorted(params))
    to_sign = '\n'.join([method, path, params_str])
    params.update({'sign': hmac.new(SECRET_KEY.encode(), to_sign.encode(), hashlib.sha256).hexdigest()})
    return params


def get_symbols():
    """marget data"""
    method = 'GET'
    path = '/open/api/v2/market/symbols'
    url = '{}{}'.format(ROOT_URL, path)
    params = {'api_key': API_KEY}
    response = requests.request(method, url, params=params)
    return response


def get_rate_limit():
    """rate limit"""
    method = 'GET'
    path = '/open/api/v2/common/rate_limit'
    url = '{}{}'.format(ROOT_URL, path)
    params = {'api_key': API_KEY}
    response = requests.request(method, url, params=params)
    print(response.json())


def get_timestamp():
    """get current time"""
    method = 'GET'
    path = '/open/api/v2/common/timestamp'
    url = '{}{}'.format(ROOT_URL, path)
    params = {'api_key': API_KEY}
    response = requests.request(method, url, params=params)
    print(response.json())


def get_ticker(symbol):
    """get ticker information"""
    method = 'GET'
    path = '/open/api/v2/market/ticker'
    url = '{}{}'.format(ROOT_URL, path)
    params = {
        'api_key': API_KEY,
        'symbol': symbol,
    }
    response = requests.request(method, url, params=params)
    print(response.json())


def get_depth(symbol, depth):
    """获market depth"""
    method = 'GET'
    path = '/open/api/v2/market/depth'
    url = '{}{}'.format(ROOT_URL, path)
    params = {
        'api_key': API_KEY,
        'symbol': symbol,
        'depth': depth,
    }
    response = requests.request(method, url, params=params)
    print(response.json())


def get_deals(symbol, limit):
    """get deals records"""
    method = 'GET'
    path = '/open/api/v2/market/deals'
    url = '{}{}'.format(ROOT_URL, path)
    params = {
        'api_key': API_KEY,
        'symbol': symbol,
        'limit': limit,
    }
    response = requests.request(method, url, params=params)
    print(response.json())


def get_kline(symbol, interval,limit):
    """k-line data"""
    method = 'GET'
    path = '/open/api/v2/market/kline'
    url = '{}{}'.format(ROOT_URL, path)
    params = {
        'api_key': API_KEY,
        'symbol': symbol,
        'interval': interval,
        'limit': limit,
    }
    response = requests.request(method, url, params=params)
    # print(response.json())
    return response


def get_account_info():
    """account information"""
    method = 'GET'
    path = '/open/api/v2/account/info'
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path)
    response = requests.request(method, url, params=params)
    print(response.json())


def place_order(symbol, price, quantity, trade_type, order_type):
    """place order"""
    method = 'POST'
    path = '/open/api/v2/order/place'
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path)
    data = {
        'symbol': symbol,
        'price': price,
        'quantity': quantity,
        'trade_type': trade_type,
        'order_type': order_type,
    }
    response = requests.request(method, url, params=params, json=data)
    print(response.json())


def batch_orders(orders):
    """batch order"""
    method = 'POST'
    path = '/open/api/v2/order/place_batch'
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path)
    response = requests.request(method, url, params=params, json=orders)
    print(response.json())


def cancel_order(order_id):
    """cancel in batch"""
    origin_trade_no = order_id
    if isinstance(order_id, list):
        origin_trade_no = parse.quote(','.join(order_id))
    method = 'DELETE'
    path = '/open/api/v2/order/cancel'
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path, original_params={'order_ids': origin_trade_no})
    if isinstance(order_id, list):
        params['order_ids'] = ','.join(order_id)
    response = requests.request(method, url, params=params)
    print(response.json())


def get_open_orders(symbol):
    """current orders"""
    method = 'GET'
    path = '/open/api/v2/order/open_orders'
    original_params = {
        'symbol': symbol,
    }
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path, original_params=original_params)
    response = requests.request(method, url, params=params)
    print(response.json())


def get_all_orders(symbol, trade_type):
    """order list"""
    method = 'GET'
    path = '/open/api/v2/order/list'
    original_params = {
        'symbol': symbol,
        'trade_type': trade_type,
    }
    url = '{}{}'.format(ROOT_URL, path)
    params = _sign(method, path, original_params=original_params)
    response = requests.request(method, url, params=params)
    print(response.json())


def query_order(order_id):
    """query order"""
    origin_trade_no = order_id
    if isinstance(order_id, list):
        origin_trade_no = parse.quote(','.join(order_id))
    method = 'GET'
    path = '/open/api/v2/order/query'
    url = '{}{}'.format(ROOT_URL, path)
    original_params = {
        'order_ids': origin_trade_no,
    }
    params = _sign(method, path, original_params=original_params)
    if isinstance(order_id, list):
        params['order_ids'] = ','.join(order_id)
    response = requests.request(method, url, params=params)
    print(response.json())


def get_deal_orders(symbol):
    """account deal records"""
    method = 'GET'
    path = '/open/api/v2/order/deals'
    url = '{}{}'.format(ROOT_URL, path)
    original_params = {
        'symbol': symbol,
    }
    params = _sign(method, path, original_params=original_params)
    response = requests.request(method, url, params=params)
    print(response.json())


def get_deal_detail(order_id):
    """deal detail"""
    method = 'GET'
    path = '/open/api/v2/order/deal_detail'
    url = '{}{}'.format(ROOT_URL, path)
    original_params = {
        'order_id': order_id,
    }
    params = _sign(method, path, original_params=original_params)
    response = requests.request(method, url, params=params)
    print(response.json())


db=dbo()
db.createSchema()
db.createCoinListTable()



def checkTables():
    list = json.loads(get_symbols().content, object_hook=lambda d: SimpleNamespace(**d))
    print(len(list.data))
    dbTables=db.getAllTableList()


    for obj in list.data:
        if "_USDT" in obj.symbol and (not "3L_" in obj.symbol)and (not "3S_" in obj.symbol) and(not obj.symbol[0].isnumeric()) and  obj.symbol not in dbTables:
            db.create_table(obj.symbol)
            db.insert2_coinList(obj.symbol)

def getcoinprice(symbol):
    try:
        data_kline = json.loads(get_kline(symbol, '1m', 1).content, object_hook=lambda d: SimpleNamespace(**d))
        if hasattr(data_kline, 'data'):
            for objJ in data_kline.data:
                 db.insert_value(datetime.fromtimestamp(objJ[0]), objJ[1], objJ[2], objJ[3], objJ[4], objJ[5], objJ[6],
                              symbol.lower())

    except Exception as e:  # work on python 3.x
        print(e)
        print(symbol)
        logging.error('Failed to upload to ftp: ' + str(e))


def main():
    # CREATE A PSYCOPG2 CONNECTION

    # CONVERT DICT OBJECT TO JSON STRING
    coinList=db.getAllTableList()
    logging.info('Started')


    for i in range(0,25):
        p = Pool(25)
        pool_output = p.map(getcoinprice, coinList)
        time.sleep(60)
        print(i)

        # for obj in list.data:
        #
        #     print(obj.symbol)
        #     if "_USDT" in obj.symbol and not obj.symbol[0].isnumeric():
        #         getcoinprice(obj)

        """    while True:
                for i in coinList:
                    getcoinprice(i)
                    print(i)
                time.sleep(60)"""

if __name__ == '__main__':
    main()
