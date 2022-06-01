#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time
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


API_KEY = config.MEXC_CONFIG['API_KEY']
SECRET_KEY = config.MEXC_CONFIG['SECRET_KEY']
ROOT_URL = config.MEXC_CONFIG['ROOT_URL']

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




def get_connection():
    # RETURN THE CONNECTION OBJECT
    return psycopg2.connect(
        database=config.DATABASE_CONFIG['database'],
        user=config.DATABASE_CONFIG['user'],
        password=config.DATABASE_CONFIG['password'],
        host=config.DATABASE_CONFIG['host'],
        port=config.DATABASE_CONFIG['port']
    )

CONNECTION = get_connection()

def dict_to_json(value: dict):
    # CONVERT DICT TO A JSON STRING AND RETURN
    return json.dumps(value)


def insert_value( timestmp, open_,close_,high,low,vol,amount,tablename, conn):
    # CREATE A CURSOR USING THE CONNECTION OBJECT
    curr = conn.cursor()

    # EXECUTE THE INSERT QUERY
    curr.execute(f'''
        INSERT INTO
            mexc.{tablename}(time_, open_,close_,high,low,vol,amount) 
        VALUES
            ('{timestmp}', '{open_}', '{close_}', '{high}', '{low}', '{vol}', '{amount}')
    ''')

    # COMMIT THE ABOVE REQUESTS
    conn.commit()

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

def insert_table(tableName,conn):
    curr = conn.cursor()

    if not checkTableExists(conn, tableName):
        print(tableName)
    # EXECUTE THE INSERT QUERY
        curr.execute(f'''
        CREATE TABLE mexc.{tableName} (
        time_ timestamp NULL,
        open_ float8 NULL,
        close_ float8 NULL,
        high float8 NULL,
        low float8 NULL,
        vol float8 NULL,
        amount float8 NULL
    )
            ''')

    # COMMIT THE ABOVE REQUESTS
    conn.commit()


def checkTables():
    conn = get_connection()
    list = json.loads(get_symbols().content, object_hook=lambda d: SimpleNamespace(**d))
    print(len(list.data))
    for obj in list.data:
        if "_USDT" in obj.symbol and (not "3L_" in obj.symbol)and (not "3S_" in obj.symbol) and(not obj.symbol[0].isnumeric()):
            insert_table(obj.symbol,conn)
    conn.close()


def getAllTables():
    curr = get_connection().cursor()
    curr.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
    tables = [i[0] for i in curr.fetchall()]
    return tables


def main():
    # CREATE A PSYCOPG2 CONNECTION
    conn = get_connection()

    # CONVERT DICT OBJECT TO JSON STRING
    list = json.loads(get_symbols().content, object_hook=lambda d: SimpleNamespace(**d))
    while True:
        p = Pool(25)
        pool_output = p.map(getcoinprice, list.data)
        print(pool_output)
        time.sleep(60)
        # for obj in list.data:
        #
        #     print(obj.symbol)
        #     if "_USDT" in obj.symbol and not obj.symbol[0].isnumeric():
        #         getcoinprice(obj)
    conn.close()


def getcoinprice(obj):
    if "_USDT" in obj.symbol and not obj.symbol[0].isnumeric():
        conn = CONNECTION
        data_kline = json.loads(get_kline(obj.symbol, '1m', 1).content, object_hook=lambda d: SimpleNamespace(**d))
        if hasattr(data_kline, 'data'):
            for objJ in data_kline.data:
                print(objJ)
                insert_value(datetime.fromtimestamp(objJ[0]), objJ[1], objJ[2], objJ[3], objJ[4], objJ[5], objJ[6],
                             obj.symbol, conn=conn)


if __name__ == '__main__':
    # checkTables()
    main()
    # get_symbols()
    # conn = psycopg2.connect(
    #     database="postgres", user='postgres', password='12345', host='127.0.0.1', port='5432'
    # )
    # # Creating a cursor object using the cursor() method
    # cursor = conn.cursor()
    #
    # # Executing an MYSQL function using the execute() method
    # cursor.execute("select version()")
    #
    # # Fetch a single row using fetchone() method.
    # data = cursor.fetchone()
    # print("Connection established to: ", data)
    #
    # # Closing the connection
    # conn.close()
    # list = json.loads(get_symbols().content, object_hook=lambda d: SimpleNamespace(**d))
    # for obj in list.data:
    #     print(obj.symbol)
    #     get_kline(obj.symbol, '1m')
    #     # get_ticker(obj.symbol)
    #     # print(obj.symbol)
    # get_rate_limit()
    # get_timestamp()
    # get_ticker('BTC_USDT')
    # get_depth('BTC_USDT', 5)
    # get_deals('BTC_USDT', 5)
    # get_kline('BTC_USDT', '1m')
    # get_account_info()
    # place_order('BTC_USDT', 7900, 0.1, 'BID', 'LIMIT_ORDER')
    # place_order('WEMIX_USDTttt', 3.2, 3.1, 'BID', 'LIMIT_ORDER')
    # cancel_order('cfc5a95618f****6d751dd04b2')
    # cancel_order(['cfc5a95618f****d751dd04b2', 'b956dfc923d***31b383c9d'])
    # batch_orders([
    #     {
    #         'symbol': 'BTC_USDT',
    #         'price': '7900',
    #         'quantity': '1',
    #         'trade_type': 'BID',
    #         'order_type': 'LIMIT_ORDER',
    #     },
    #     {
    #         'symbol': 'BTC_USDT',
    #         'price': '7901',
    #         'quantity': '1',
    #         'trade_type': 'ASK',
    #         'order_type': 'LIMIT_ORDER',
    #     },
    # ])
    # get_open_orders('BTC_USDT')
    # get_all_orders('BTC_USDT', 'BID')
    # query_order('ccbd62471d***dd109903e')
    # query_order(['ec72970d2****8264d7e86e', 'fd4d614ee4cf46***c7c82c0'])
    # get_deal_orders('BTC_USDT')
    # get_deal_detail('ccbd62471d*****ddd109903e')


