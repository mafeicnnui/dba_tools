#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/25 9:48
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import json
import pymysql
import warnings
import logging
from decrypt2chk.HandleLog import HandleLog

def initlogger():
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    BASIC_FORMAT='%(asctime)s:%(levelname)s:%(message)s'
    DATE_FORMAT='%T-%m-%d %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT,DATE_FORMAT)
    chlr = logging.StreamHandler() #输出到控制台的handler
    chlr.setFormatter(formatter)
    chlr.setLevel('INFO') # 也可以不设置，不设置默认用logger的level
    fhlr = logging.FileHandler('encrypt.log') #输出到文件的handle
    fhlr.setFormatter(formatter)
    logger.addHandler(chlr)
    logger.addHandler(fhlr)
    return logger

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_ds_mysql(host,port ,user,password,db):
    conn = pymysql.connect(host=host,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=db,
                           charset='utf8',
                           autocommit=True,
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_cfg():
    cfg = read_json('./encrypt.json')
    cfg['meta_db'] = get_ds_mysql(
                        host=cfg['meta_db']['db_ip'],
                        port=int(cfg['meta_db']['db_port']),
                        user=cfg['meta_db']['db_user'],
                        password=cfg['meta_db']['db_pass'],
                        db=cfg['meta_db']['db_service'])
    cfg['encrypt_db'] = get_ds_mysql(
                        host=cfg['encrypt_db']['db_ip'],
                        port=int(cfg['encrypt_db']['db_port']),
                        user=cfg['encrypt_db']['db_user'],
                        password=cfg['encrypt_db']['db_pass'],
                        db=cfg['encrypt_db']['db_service'])

    cfg['meta_cur'] = cfg['meta_db'].cursor()
    cfg['encrypt_cur'] = cfg['encrypt_db'].cursor()
    return cfg

def get_check_statements(cfg):
    st = cfg['statement']
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    return rs

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    #logger = initlogger()
    logger = HandleLog()
    cfg = get_cfg()
    for st in get_check_statements(cfg):
        st['env'] = cfg['env']
        logger.error('[{env}]:checking {table_schema}.{table_name} column `{column_name}` ...'.format(**st))
        if cfg['debug']:
           logger.info(st['statement'].replace('$env', cfg['env']))
        cfg['encrypt_cur'].execute(st['statement'].replace('$env', cfg['env']))