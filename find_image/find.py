#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/5/6 15:34
# @Author : ma.fei
# @File : find.py.py
# @Software: PyCharm

'''
SELECT
  CONCAT('select count(0) as rec from `',
  table_schema,'`.`',table_name,'` where `',column_name,'` like \'%hopsontong%\'') AS statement,
  CONCAT('update `',
  table_schema,'`.`',table_name,'` set `',column_name,'`= replace(`',column_name,'`,\'%hopsontong%\',\'%hopsontone%\') where `',column_name,'` like \'%hopsontong%\'') AS update_statement
 FROM information_schema.columns
  WHERE column_name LIKE '%img%' OR column_name LIKE '%logo%'
   AND table_schema NOT IN ('test')

 {'statement': "select count(0) as rec from `hopsonone_catering`.`food` where `food_img` like '%hopsontong%'", 'row': 2288}
{'statement': "select count(0) as rec from `hopsonone_cms`.`home_page_setting` where `img` like '%hopsontong%'", 'row': 14}
{'statement': "select count(0) as rec from `hopsonone_park`.`pf_car_info` where `logo_brand` like '%hopsontong%'", 'row': 1}
{'statement': "select count(0) as rec from `hopsonone_spider`.`wx_spider` where `head_img` like '%hopsontong%'", 'row': 948}
{'statement': "select count(0) as rec from `hopsonone_verification`.`verification_log` where `goods_img` like '%hopsontong%'", 'row': 10}
{'statement': "select count(0) as rec from `hopsonone_verification`.`verification_log_detail` where `goods_img` like '%hopsontong%'", 'row': 10}
{'statement': "select count(0) as rec from `hopsonone_workorder`.`workorder_resource` where `img` like '%hopsontong%'", 'row': 1}
{'statement': "select count(0) as rec from `mall_goods_v2`.`goods` where `img` like '%hopsontong%'", 'row': 46}
{'statement': "select count(0) as rec from `mall_goods_v2`.`goods_info` where `imgs` like '%hopsontong%'", 'row': 47}
{'statement': "select count(0) as rec from `mall_goods_v2`.`goods_sku` where `img` like '%hopsontong%'", 'row': 6}
{'statement': "select count(0) as rec from `mall_merchant_shop`.`merchant_shop` where `shop_logo` like '%hopsontong%'", 'row': 3}
{'statement': "select count(0) as rec from `mall_merchant_shop`.`online_store` where `logo` like '%hopsontong%'", 'row': 3}
{'statement': "select count(0) as rec from `merchant_auth`.`bank` where `logo` like '%hopsontong%'", 'row': 20}
{'statement': "select count(0) as rec from `merchant_entity`.`entity_store` where `store_logo` like '%hopsontong%'", 'row': 109}
{'statement': "select count(0) as rec from `merchant_entity`.`entity_store_market` where `logo` like '%hopsontong%'", 'row': 112}
{'statement': "select count(0) as rec from `shop_side_operation`.`business_power` where `img` like '%hopsontong%'", 'row': 1}

'''
import json
import pymysql
import warnings
import traceback

def get_cfg():
    cfg = read_json('./find.json')
    cfg['db'] = get_ds_mysql(
                        host=cfg['db']['db_ip'],
                        port=int(cfg['db']['db_port']),
                        user=cfg['db']['db_user'],
                        password=cfg['db']['db_pass'],
                        db=cfg['db']['db_service'])
    cfg['cur'] = cfg['db'].cursor()

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
    cfg['cur'].execute(cfg['statement'])
    rs = cfg['cur'].fetchall()
    if cfg['debug'] == True:
        for r in rs:
            print(r)
    return rs

def get_result(cfg,s):
    cfg['cur'].execute(s['statement'])
    rs = cfg['cur'].fetchone()
    return rs['rec']

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug'] == True:
        for k, v in cfg.items():
            print('{}={}'.format(k, v))
    try:
        for s in get_statements(cfg):
           r = get_result(cfg,s)
           s['row'] = r
           if r > 0:
              print(json.dumps(s, ensure_ascii=False, indent=4, separators=(',', ':')))
    except:
        traceback.print_exc()