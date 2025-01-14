#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/25 9:48
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import json
import pymysql
import warnings
import traceback

DB_ENV = {'dev':1,'test':2,'pre':3,'pro':4}

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
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    for i in list(rs):
        cl=cl+i['column_name']+','
    return cl[0:-1]

def get_encrypt_columns(cfg):
    st = cfg['statement']
    print('get_encrypt_columns=',st.format(**cfg))
    cfg['encrypt_cur'].execute(st.format(**cfg))
    rs = cfg['encrypt_cur'].fetchall()
    if cfg['debug'] == True:
        for r in rs:
            print(r)
    return rs

def get_encrypt_column_data(cfg,c):
    st = """select {pk_name},{column_name}_cipher 
             from {table_schema}.{table_name} 
             where {column_name}_cipher is not null 
                and  {column_name}_cipher !=''
               order by {pk_name}""".format(**c)
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    return rs

def db_encrypt(cfg,env,plain):
    if env  == DB_ENV['dev']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['dev'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['dev'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['dev_cipher']

    if env  == DB_ENV['test']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['test'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['test'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['test_cipher']

    if env  == DB_ENV['pre']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['pre'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['pre'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pre_cipher']

    if env == DB_ENV['pro']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['pro'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement']['pro'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pro_cipher']

def db_decrypt(cfg, env, cipher):
    if env  == DB_ENV['dev']:
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['dev'][0].format(cipher))
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['dev'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['dev_plain']

    if env == DB_ENV['test']:
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['test'][0].format(cipher))
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['test'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['test_plain']

    if env == DB_ENV['pre']:
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['pre'][0].format(cipher))
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['pre'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pre_plain']

    if env == DB_ENV['pro']:
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['pro'][0].format(cipher))
        cfg['proxy_cur'].execute(cfg['decrypt_statement']['pro'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pro_plain']

def db_encrypt_like(cfg,env,plain):
    if env  == DB_ENV['dev']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['dev'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['dev'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['dev_cipher'],rs['dev_like']

    if env  == DB_ENV['test']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['test'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['test'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['test_cipher'],rs['test_like']

    if env  == DB_ENV['pre']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['pre'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['pre'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pre_cipher'],rs['pre_like']

    if env == DB_ENV['pro']:
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['pro'][0].format(plain))
        cfg['proxy_cur'].execute(cfg['encrypt_statement_like']['pro'][1])
        rs = cfg['proxy_cur'].fetchone()
        return rs['pro_cipher'],rs['pro_like']

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug'] == True:
        for k,v in cfg.items():
           print('{}={}'.format(k,v))

    cfg['encrypt_columns'] = get_encrypt_columns(cfg)
    for c in cfg['encrypt_columns']:
        c['pk_name'] = get_pk_names(cfg,c['table_schema'],c['table_name'])
        for d in get_encrypt_column_data(cfg, c):
            from_ = cfg['convert'].split('2')[0]
            to_ = cfg['convert'].split('2')[1]
            decrypt_text = db_decrypt(cfg, DB_ENV[from_], d[c['column_name']+'_cipher'])
            if c['like'] == '1':
                u = cfg['update_encrypt_column_like'].format(**c)
                s = u.format(*db_encrypt_like(cfg, DB_ENV[to_], decrypt_text), d[c['pk_name']])
            else:
                u = cfg['update_encrypt_column'].format(**c)
                s = u.format(db_encrypt(cfg, DB_ENV[to_], decrypt_text), d[c['pk_name']])
            print(s)
            cfg['encrypt_cur'].execute(s)