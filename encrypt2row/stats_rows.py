#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/5/6 15:34
# @Author : ma.fei
# @File : find.py.py
# @Software: PyCharm

import json
import pymysql
import warnings
import traceback

def get_cfg():
    cfg = read_json('stats_rows.json')
    cfg['meta_db'] = get_ds_mysql(
                        host=cfg['meta_db']['db_ip'],
                        port=int(cfg['meta_db']['db_port']),
                        user=cfg['meta_db']['db_user'],
                        password=cfg['meta_db']['db_pass'],
                        db=cfg['meta_db']['db_service'])
    cfg['prod_db'] = get_ds_mysql(
                        host=cfg['prod_db']['db_ip'],
                        port=int(cfg['prod_db']['db_port']),
                        user=cfg['prod_db']['db_user'],
                        password=cfg['prod_db']['db_pass'],
                        db=cfg['prod_db']['db_service'])
    cfg['prod_db_hft'] = get_ds_mysql(
                        host=cfg['prod_db_hft']['db_ip'],
                        port=int(cfg['prod_db_hft']['db_port']),
                        user=cfg['prod_db_hft']['db_user'],
                        password=cfg['prod_db_hft']['db_pass'],
                        db=cfg['prod_db_hft']['db_service'])

    cfg['meta_cur'] = cfg['meta_db'].cursor()
    cfg['prod_cur'] = cfg['prod_db'].cursor()
    cfg['prod_db_hft'] = cfg['prod_db_hft'].cursor()
    return cfg

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

def get_statements(cfg):
    cfg['meta_cur'].execute(cfg['statement'])
    rs = cfg['meta_cur'].fetchall()
    if cfg['debug'] == True:
        for r in rs:
            print(r)
    return rs

def update_notnull_rows(cfg,s):
    print('prod exec statement:{}'.format(s['statement']))
    try:
        cfg['prod_cur'].execute(s['statement'])
        rs = cfg['prod_cur'].fetchone()
    except:
        cfg['prod_db_hft'].execute(s['statement'])
        rs = cfg['prod_db_hft'].fetchone()
    print('meta db update rows:{}'.format(cfg['update_rows'].format(rs['rec'],s['table_schema'],s['table_name'],s['column_name'])))
    cfg['meta_cur'].execute(cfg['update_rows'].format(rs['rec'],s['table_schema'],s['table_name'],s['column_name']))

def update_total_rows(cfg,s):
    print('prod exec statement:{}'.format(s['statement_total']))
    try:
        cfg['prod_cur'].execute(s['statement_total'])
        rs = cfg['prod_cur'].fetchone()
    except:
        cfg['prod_db_hft'].execute(s['statement_total'])
        rs = cfg['prod_db_hft'].fetchone()
    print('meta db update_rows_total:{}'.format(cfg['update_rows_total'].format(rs['rec'],s['table_schema'],s['table_name'],s['column_name'])))
    cfg['meta_cur'].execute(cfg['update_rows_total'].format(rs['rec'],s['table_schema'],s['table_name'],s['column_name']))


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug'] == True:
        for k, v in cfg.items():
            print('{}={}'.format(k, v))
    try:
        for s in get_statements(cfg):
          print(s)
          try:
             update_notnull_rows(cfg,s)
             update_total_rows(cfg, s)
          except:
             pass
    except:
        traceback.print_exc()