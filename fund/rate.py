#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

__author__ = 'lidc'

# PYTHONUNBUFFERED=1

import gc
import getpass
import logging
import os
import sys
from collections import OrderedDict
from random import random
from time import localtime, monotonic, sleep, strftime
from urllib.parse import quote_plus

import requests
from detail import Fund_Detail
from sqlalchemy import Boolean, Column, String, DateTime, text, create_engine
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

class Fund_Rate(Base):

    __tablename__ = 'fund_rate'

    fcode = Column(String(8), primary_key=True, comment='基金代码')
    sgzt = Column(String(4), comment='申购状态')
    shzt = Column(String(4), comment='赎回状态')
    dtzt = Column(Boolean, comment='定投状态')
    minsg = Column(String(16), comment='申购起点')
    mindt = Column(String(16), comment='定投起点')
    maxsg = Column(String(16), comment='日累计申购限额')
    minssg = Column(String(16), comment='首次购买')
    minsbsg = Column(String(16), comment='追加购买')
    ssbcfmdata = Column(String(4), comment='买入确认日')
    rdmcfmdata = Column(String(4), comment='卖出确认日')
    mgrexp = Column(String(8), comment='管理费率')
    trustexp = Column(String(8), comment='托管费率')
    salesexp = Column(String(8), comment='销售服务费率')

    sg_money1 = Column(String(16), comment='申购金额1')
    sg_rate1 = Column(String(8), comment='申购费率1')
    sg_money2 = Column(String(32), comment='申购金额2')
    sg_rate2 = Column(String(8), comment='申购费率2')
    sg_money3 = Column(String(32), comment='申购金额3')
    sg_rate3 = Column(String(8), comment='申购费率3')
    sg_money4 = Column(String(32), comment='申购金额4')
    sg_rate4 = Column(String(8), comment='申购费率4')
    sg_money5 = Column(String(16), comment='申购金额5')
    sg_rate5 = Column(String(8), comment='申购费率5')

    sh_time1 = Column(String(512), comment='持有期限1')
    sh_rate1 = Column(String(8), comment='赎回费率1')
    sh_time2 = Column(String(512), comment='持有期限2')
    sh_rate2 = Column(String(8), comment='赎回费率2')
    sh_time3 = Column(String(128), comment='持有期限3')
    sh_rate3 = Column(String(8), comment='赎回费率3')
    sh_time4 = Column(String(64), comment='持有期限4')
    sh_rate4 = Column(String(8), comment='赎回费率4')
    sh_time5 = Column(String(32), comment='持有期限5')
    sh_rate5 = Column(String(8), comment='赎回费率5')
    sh_time6 = Column(String(32), comment='持有期限6')
    sh_rate6 = Column(String(8), comment='赎回费率6')
    sh_time7 = Column(String(16), comment='持有期限7')
    sh_rate7 = Column(String(8), comment='赎回费率7')

    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='更新时间')


def __fund_rate(fund_rate, data):

    dtzt = data.get("DTZT", '0')

    data["dtzt"] = int(dtzt.strip()) if dtzt else 0

    fund_rate.sgzt = data["SGZT"]
    fund_rate.shzt = data["SHZT"]
    fund_rate.dtzt = bool(data["dtzt"])
    fund_rate.minsg = data["MINSG"]
    fund_rate.mindt = data["MINDT"]
    fund_rate.maxsg = data["MAXSG"]
    fund_rate.minssg = data["MINSSG"]
    fund_rate.minsbsg = data["MINSBSG"]
    fund_rate.ssbcfmdata = data["SSBCFMDATA"]
    fund_rate.rdmcfmdata = data["RDMCFMDATA"]
    fund_rate.mgrexp = data["MGREXP"]
    fund_rate.trustexp = data["TRUSTEXP"]
    fund_rate.salesexp = data["SALESEXP"]

    sg_prefix = 'sg'

    sg_list = data[sg_prefix]

    if sg_list is not None:
        sg_dict = handler_param(sg_list, 'money', 'rate', sg_prefix, 5)
        for k, v in sg_dict.items():
            setattr(fund_rate, k, v)

    sh_prefix = 'sh'

    sh_list = data[sh_prefix]

    if sh_list is not None:
        sh_dict = handler_param(sh_list, 'time', 'rate', sh_prefix, 7)
        for k, v in sh_dict.items():
            setattr(fund_rate, k, v)


db_session = ''
db_fund_detail = {}
db_fund_rate = {}


def db_init(db_user, db_passwd, db_host, db_name):
    # engine = create_engine("mysql+mysqldb://{}:{}@{}:3306/{}?charset=utf8mb4&binary_prefix=true".format(db_user, quote_plus(db_passwd), db_host, db_name))
    engine = create_engine("mysql+mysqldb://{}:{}@{}:21852/{}?charset=utf8mb4&binary_prefix=true".format(db_user, quote_plus(db_passwd), db_host, db_name))

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    global db_session
    db_session = Session()

    fund_detail = db_session.query(Fund_Detail.fcode, Fund_Detail).all()
    fund_rate = db_session.query(Fund_Rate.fcode, Fund_Rate).all()

    global db_fund_detail, db_fund_rate

    db_fund_detail = dict(fund_detail)
    db_fund_rate = dict(fund_rate)

    logging.info('fund_detail count:%d    fund_rate count:%d', len(db_fund_detail), len(db_fund_rate))


url = 'https://fundmobapi.eastmoney.com/FundMApi/FundRateInfo.ashx?FCODE={}&deviceid=Wap&plat=Wap&product=EFund&version=2.0.0'
headers = {'Content-Type': 'application/json; charset=utf-8',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'}

s = requests.Session()
s.headers.update(headers)


def getRate(update):

    items = []
    i = 0

    for code in db_fund_detail.keys():

        i = i + 1

        req = requests.Request('GET', url.format(code))
        prepped = s.prepare_request(req)

        if code not in db_fund_rate.keys():

            r = s.send(prepped)

            # if r.encoding == 'ISO-8859-1':
            #     r.encoding = None
            j = r.json()

            data = j['Datas']

            if data:

                fund_rate = Fund_Rate()
                fund_rate.fcode = code

                __fund_rate(fund_rate, data)

                items.append(fund_rate)

            sleep(random())

        else:
            if update and data:

                r = s.send(prepped)
                j = r.json()

                data = j['Datas']

                if data:

                    fund_rate = db_fund_rate[code]

                    __fund_rate(fund_rate, data)

                sleep(random())

        if i == 100:
            db_session.add_all(items)
            db_session.commit()
            # db_session.rollback()

            items.clear()
            i = 0

            gc.collect()

            logging.info('commit 1k')

    db_session.add_all(items)
    db_session.commit()

    logging.info('rate upsert over!')


def handler_param(dataList, name1, name2, prefix, maxLen):

    list_len = len(dataList) if len(dataList) < maxLen else maxLen

    data_dict = OrderedDict()

    for i in range(list_len):
        data_name1 = prefix + '_' + name1 + str(i + 1)
        data_name2 = prefix + '_' + name2 + str(i + 1)

        data_dict[data_name1] = dataList[i][name1]
        data_dict[data_name2] = dataList[i][name2]

    return data_dict


if __name__ == "__main__":

    db_user = getpass.getuser()
    db_passwd = getpass.getpass('数据库密码:')
    db_host = os.environ.get('db_host')
    db_name = 'fintech'

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    db_init(db_user, db_passwd, db_host, db_name)

    # update= True
    # getRate(update)

    getRate(False)

    logging.info('detai cost {:.2f} seconds!'.format(monotonic() - s_time))
