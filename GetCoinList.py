
import json

from types import SimpleNamespace

import requests as requests

from DBprocess import DBObject  as dbo
import logging
logging.basicConfig(filename='logger.log', level=logging.DEBUG)

API_KEY = 'mx0sC5LYhgEPV1YpKw'
SECRET_KEY = '1f644e42950b4a7db799bafde2b5ddb1'
ROOT_URL = 'https://www.mexc.com'


def get_symbols():
    """marget data"""
    method = 'GET'
    path = '/open/api/v2/market/symbols'
    url = '{}{}'.format(ROOT_URL, path)
    params = {'api_key': API_KEY}
    response = requests.request(method, url, params=params)
    return response
def checkTables():
    list = json.loads(get_symbols().content, object_hook=lambda d: SimpleNamespace(**d))
    print(len(list.data))
    dbTables=db.getAllTableList()


    for obj in list.data:
#TODO Burada '2L_ ,3L ..  ve 2S_ ,3S_ .. gibi coinleri de regexle engellemek lazım
        if "_USDT" in obj.symbol and (not "3L_" in obj.symbol) and (not "3S_" in obj.symbol) and(not obj.symbol[0].isnumeric()) and  obj.symbol not in dbTables:
            db.create_table(obj.symbol)
            db.insert2_coinList(obj.symbol)

def createTableFromList():
    tableList=db.getAllTableList()
    for i in tableList:
        db.create_table(i)

if __name__ == '__main__':
    logging.info('GetCoinList started')

    db = dbo()
    db.createSchema()
    db.createCoinListTable()
    createTableFromList()
    #checkTables()