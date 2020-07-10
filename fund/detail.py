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
from time import localtime, monotonic, sleep, strftime, time
# from collections import OrderedDict
from urllib.parse import quote_plus, urlencode

# import demjson
import MySQLdb
import requests
from sqlalchemy import Boolean, Column, DateTime, Integer, String, create_engine, desc, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from fund import Fund

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


class Fund_Detail(Base):

    __tablename__ = 'fund_detail'

    fcode = Column(String(8), primary_key=True, comment='基金代码')
    feature = Column(String(32), comment='')
    cycle = Column(String(4), comment='')
    webbackcode = Column(String(8), comment='基金代码(后端)')
    shortname = Column(String(32), comment='基金简称')
    fullname = Column(String(64), comment='基金全称')
    ftype = Column(String(8), comment='基金类型')
    estabdate = Column(String(16), comment='成立日期')
    endnav = Column(String(16), comment='资产规模')
    fegmrq = Column(String(16), comment='规模截止日期')
    rlevel_sz = Column(String(4), comment='基金评级')
    risklevel = Column(String(4), comment='风险等级')
    jjgs = Column(String(16), comment='基金管理人')
    tgyh = Column(String(8), comment='基金托管人')
    jjgsid = Column(String(8), comment='基金管理人代码')
    jjjl = Column(String(32), comment='基金经理人')
    netnav = Column(String(16), comment='成立规模')
    bench = Column(String(160), comment='业绩比较基准')
    indexcode = Column(String(16), comment='跟踪标的代码')
    indexname = Column(String(64), comment='跟踪标的')
    prsvperiod = Column(String(4), comment='')
    prsvdate = Column(String(32), comment='')
    prsvtype = Column(String(4), comment='')
    buytime = Column(String(8), comment='')
    mgrexp = Column(String(8), comment='管理费率')
    trustexp = Column(String(8), comment='托管费率')
    salesexp = Column(String(8), comment='销售服务费率')

    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='更新时间')

    # def __eq__(self,other):
    #     return self.__dict__ == other.__dict__


def __fund_detail(fund_detail, data):

    fund_detail.fcode = data['FCODE']
    fund_detail.feature = data['FEATURE']
    fund_detail.cycle = data['CYCLE']
    fund_detail.webbackcode = data['WEBBACKCODE']
    fund_detail.shortname = data['SHORTNAME']
    fund_detail.fullname = data['FULLNAME']
    fund_detail.ftype = data['FTYPE']
    fund_detail.estabdate = data['ESTABDATE']
    fund_detail.endnav = data['ENDNAV']
    fund_detail.fegmrq = data['FEGMRQ']
    fund_detail.rlevel_sz = data['RLEVEL_SZ']
    fund_detail.risklevel = data['RISKLEVEL']
    fund_detail.jjgs = data['JJGS']
    fund_detail.tgyh = data['TGYH']
    fund_detail.jjgsid = data['JJGSID']
    fund_detail.jjjl = data['JJJL']
    fund_detail.netnav = data['NETNAV']
    fund_detail.bench = data['BENCH']
    fund_detail.indexcode = data['INDEXCODE']
    fund_detail.indexname = data['INDEXNAME']
    fund_detail.prsvperiod = data['PRSVPERIOD']
    fund_detail.prsvdate = data['PRSVDATE']
    fund_detail.prsvtype = data['PRSVTYPE']
    fund_detail.buytime = data['BUYTIME']
    fund_detail.mgrexp = data['MGREXP']
    fund_detail.trustexp = data['TRUSTEXP']
    fund_detail.salesexp = data['SALESEXP']


db_session = ''
db_funds = {}
db_fund_detail = {}


def db_init(db_user, db_passwd, db_host, db_name):
    # engine = create_engine("mysql+mysqldb://{}:{}@{}:3306/{}?charset=utf8mb4&binary_prefix=true".format(db_user, quote_plus(db_passwd), db_host, db_name))
    engine = create_engine("mysql+mysqldb://{}:{}@{}:21852/{}?charset=utf8mb4&binary_prefix=true".format(db_user, quote_plus(db_passwd), db_host, db_name))

    Base.metadata.create_all(engine)
    # engine.execute('TRUNCATE TABLE fund_detail')

    Session = sessionmaker(bind=engine)

    global db_session
    db_session = Session()

    funds = db_session.query(Fund.code, Fund).all()
    fund_detail = db_session.query(Fund_Detail.fcode, Fund_Detail).all()

    global db_funds, db_fund_detail

    db_funds = dict(funds)
    db_fund_detail = dict(fund_detail)

    logging.info('fund count:%d    fund_detail count:%d',len(db_funds), len(db_fund_detail))


em_url = 'https://fundmobapi.eastmoney.com/FundMApi/FundDetailInformation.ashx?FCODE={}&deviceid=Wap&plat=Wap&product=EFund&version=2.0.0'
headers = {'Content-Type': 'application/json; charset=utf-8',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'}

s = requests.Session()
s.headers.update(headers)


def do_em_dt(update):

    items = []
    i = 0

    for code in db_funds.keys():

        i = i + 1

        req = requests.Request('GET', em_url.format(code))
        prepped = s.prepare_request(req)

        if code not in db_fund_detail.keys():

            r = s.send(prepped)
            j = r.json()

            data = j['Datas']

            if data:

                fund_detail = Fund_Detail()

                __fund_detail(fund_detail, data)

                items.append(fund_detail)

            sleep(random())

        else:

            if update:

                r = s.send(prepped)
                j = r.json()

                data = j['Datas']

                if data:

                    fund_detail = db_fund_detail[data['FCODE']]

                    __fund_detail(fund_detail, data)

                sleep(random())

        if i == 1000:
            db_session.add_all(items)
            db_session.commit()
            # db_session.rollback()

            items.clear()
            i = 0

            gc.collect()

            logging.info('detail commit 1k')

    db_session.add_all(items)
    db_session.commit()

    logging.info('detail upsert over!')


if __name__ == "__main__":

    db_user = getpass.getuser()
    db_passwd = getpass.getpass('数据库密码:')
    db_host = os.environ.get('db_host')
    db_name = 'fintech'

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    db_init(db_user, db_passwd, db_host, db_name)

    # update= True
    # do_em_dt(update)

    do_em_dt(False)

    logging.info('detai cost {:.2f} seconds!'.format(monotonic() - s_time))
