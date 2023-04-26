#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/25 9:48
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

'''
 print(db_encrypt(cfg, '1', '11050166580000000718'))
 print(db_encrypt(cfg, '1', '15810420106'))
'''

import json
import pymysql
import warnings
import traceback

def get_cfg():
    cfg = read_json('./encrypt.json')
    cfg['meta_db'] = get_ds_mysql(
                        host=cfg['meta_db']['db_ip'],
                        port=int(cfg['meta_db']['db_port']),
                        user=cfg['meta_db']['db_user'],
                        password=cfg['meta_db']['db_pass'],
                        db=cfg['meta_db']['db_service'])
    cfg['proxy_db'] = get_ds_mysql(
                        host=cfg['proxy_db']['db_ip'],
                        port=int(cfg['proxy_db']['db_port']),
                        user=cfg['proxy_db']['db_user'],
                        password=cfg['proxy_db']['db_pass'],
                        db=cfg['proxy_db']['db_service'])
    cfg['encrypt_db'] = get_ds_mysql(
                        host=cfg['encrypt_db']['db_ip'],
                        port=int(cfg['encrypt_db']['db_port']),
                        user=cfg['encrypt_db']['db_user'],
                        password=cfg['encrypt_db']['db_pass'],
                        db=cfg['encrypt_db']['db_service'])

    cfg['meta_cur'] = cfg['meta_db'].cursor()
    cfg['proxy_cur'] = cfg['proxy_db'].cursor()
    cfg['encrypt_cur'] = cfg['encrypt_db'].cursor()
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

def get_pk_names(cfg,schema,tab):
    cl=''
    st="""select column_name 
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' 
                and column_key='PRI' order by ordinal_position""".format(schema,tab)
    #print('st=',st)
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    for i in list(rs):
        cl=cl+i['column_name']+','
    #print('pk=',cl[0:-1])
    return cl[0:-1]

def get_encrypt_columns(cfg):
    cfg['encrypt_cur'].execute(cfg['statement'])
    rs = cfg['encrypt_cur'].fetchall()
    if cfg['debug'] == True:
        for r in rs:
            print(r)
    return rs

def get_encrypt_column_data(cfg,c):
    st = """select {pk_name},{column_name} 
            from {table_schema}.{table_name} 
             where {column_name} is not null 
              and  {column_name} !=''
               order by {pk_name}""".format(**c)
    print('get_encrypt_column_data=',st)
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    return rs

def db_encrypt(cfg,env,plain):
    if env in ('1', '2', '3', '4'):
       cfg['proxy_cur'].execute(cfg['encrypt_statement'].format(plain))
       cfg['proxy_cur'].execute(cfg['decrypt_statement'])
       rs =cfg['proxy_cur'].fetchone()
       return rs['dev_cipher']

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug'] == True:
        for k,v in cfg.items():
           print('{}={}'.format(k,v))

    cfg['encrypt_columns'] = get_encrypt_columns(cfg)
    try:
        for c in cfg['encrypt_columns']:
            c['pk_name'] = get_pk_names(cfg,c['table_schema'],c['table_name'])
            for d in get_encrypt_column_data(cfg, c):
                u = cfg['update_encrypt_column'].format(**c)
                s = u.format(db_encrypt(cfg, '1', d[c['column_name']]), d[c['pk_name']])
                print(s)
                try:
                   cfg['encrypt_cur'].execute(s)
                except:
                   pass
    except:
        traceback.print_exc()

