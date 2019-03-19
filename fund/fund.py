#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

__author__ = 'lidc'

# PYTHONUNBUFFERED=1

import gc
import getpass
import json
import logging
import os
import re
import sys
from pprint import pprint
from random import randint, random
from time import monotonic, sleep, strftime, time, localtime
# from collections import OrderedDict
from urllib.parse import quote_plus, urlencode

import demjson
import MySQLdb
import requests
from sqlalchemy import (Boolean, Column, DateTime, Integer, String, create_engine, desc, func, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

s_time = monotonic()
today = strftime('%Y%m%d', localtime())

logdir = os.path.join(sys.path[0], 'log')

if not os.path.exists(logdir):
    os.mkdir(logdir)

FORMAT = '[%(asctime)s %(levelname)s]<%(process)d> %(message)s'
logfile = os.path.join(logdir, '{}_{}.log'.format(os.path.basename(__file__)[:-3], today))

fh = logging.FileHandler(filename=logfile, encoding='UTF-8')
logging.basicConfig(handlers=[logging.StreamHandler(), fh], format=FORMAT, level=logging.INFO)


Base = declarative_base()


class Fund(Base):

    __tablename__ = 'fund'

    code = Column(String(6), primary_key=True, comment='基金代码')
    hb_name = Column(String(32), comment='好买基金名')
    em_name = Column(String(32), comment='天天基金名')
    same = Column(Boolean, comment='是否相同')

    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='更新时间')

    def __init__(self, code, hb_name, em_name, same):
        self.code = code
        self.hb_name = hb_name
        self.em_name = em_name
        self.same = same

    def __repr__(self):
        return "<Fund(code={}, hb_name={}, em_name={}, same={}>".format(self.code, self.hb_name, self.em_name, self.same)


db_session = ''
db_funds = {}


def init(db_user, db_passwd, db_host, db_name):
    engine = create_engine("mysql+mysqldb://{}:{}@{}:3306/{}?charset=utf8mb4&binary_prefix=true".format(
        db_user, quote_plus(db_passwd), db_host, db_name))

    Base.metadata.create_all(engine)
    # engine.execute('TRUNCATE TABLE fund')

    Session = sessionmaker(bind=engine)

    global db_session
    db_session = Session()

    funds = db_session.query(Fund.code, Fund).all()

    global db_funds
    db_funds = dict(funds)

    logging.info('db count:%d', len(db_funds))


headers = {'Content-Type': 'application/json; charset=utf-8',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'}

s = requests.Session()
s.headers.update(headers)


def do_howbuy():

    hb_url = 'https://www.howbuy.com/fund/fundranking/ajax.htm'

    req = requests.Request('POST', hb_url)
    prepped = s.prepare_request(req)
    prepped.headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'

    payload = {}
    payload['orderField'] = ''
    payload['orderType'] = ''
    payload['glrm'] = ''
    payload['keyword'] = ''
    payload['bd'] = ''
    payload['ed'] = ''
    payload['radio'] = ''
    payload['page'] = 1
    payload['cat'] = 'index.htm'

    items = []
    i = 0

    while True:

        i = i + 1

        payload['page'] = i

        prepped.prepare_body(payload, None)
        r = s.send(prepped)

        online_funds = r.json()['list']

        if not online_funds:
            break

        for j in online_funds:
            if j['jjdm'] in db_funds:
                db_funds[j['jjdm']].hb_name = j['jjjc']
            else:
                fund = Fund(j['jjdm'], j['jjjc'], None, None)
                items.append(fund)
                db_funds[j['jjdm']] = fund

        if len(items) == 1000:
            db_session.add_all(items)
            db_session.commit()
            items.clear()
            gc.collect()

            logging.info('hb commit 1k')

        sleep(random())

    db_session.add_all(items)
    db_session.commit()

    logging.info('hb upsert over!')


def do_eastmoney_web():

    em_url = 'http://fund.eastmoney.com/js/fundcode_search.js'

    req = requests.Request('GET', em_url)
    prepped = s.prepare_request(req)

    r = s.send(prepped)

    obj_array = json.loads(r.text[8:-1])

    items = []

    for j in obj_array:
        if j[0] in db_funds:
            db_funds[j[0]].em_name = j[2]
        else:
            fund = Fund(j[0], None, j[2], None)
            items.append(fund)
            db_funds[j[0]] = fund

    db_session.add_all(items)
    db_session.commit()

    logging.info('em_web upsert over!')


def do_eastmoney_wap():

    em_url = 'https://m.1234567.com.cn/data/FundSuggestList.js'

    req = requests.Request('GET', em_url)
    prepped = s.prepare_request(req)

    r = s.send(prepped)

    obj = json.loads(r.text[16:-3])

    items = []

    for o in obj['Datas']:
        j = o.split('|')
        if j[0] in db_funds:
            db_funds[j[0]].em_name = j[2]
        else:
            fund = Fund(j[0], None, j[2], None)
            items.append(fund)
            db_funds[j[0]] = fund

    db_session.add_all(items)
    db_session.commit()

    logging.info('em_wap upsert over!')


if __name__ == "__main__":

    db_user = getpass.getuser()
    db_passwd = getpass.getpass('数据库密码:')
    db_host = os.environ.get('db_host')
    db_name = 'fintech'

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    init(db_user, db_passwd, db_host, db_name)

    do_howbuy()

    # do_eastmoney_web()
    do_eastmoney_wap()

    db_session.execute('UPDATE {} SET same=1 WHERE hb_name=em_name;'.format(Fund.__tablename__))
    db_session.execute('UPDATE {} SET same=0 WHERE same<>1 or same is null;'.format(Fund.__tablename__))

    db_session.commit()
    db_session.close()

    logging.info('cost {:.2f} seconds!'.format(monotonic() - s_time))
