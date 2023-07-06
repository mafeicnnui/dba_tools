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
import logging
import datetime

logging.basicConfig(filename='encrypt.log'.format(datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

DB_ENV = {'dev':1,'test':2,'pre':3,'pro':4}

def get_cfg():
    cfg = read_json('./encrypt.json')
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
    cfg['proxy_cur'] = cfg['proxy_db'].cursor()
    cfg['encrypt_cur'] = cfg['encrypt_db'].cursor()
    return cfg

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

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
                and column_key='PRI' 
                 order by ordinal_position""".format(schema,tab)
    cfg['encrypt_cur'].execute(st)
    rs = cfg['encrypt_cur'].fetchall()
    for i in list(rs):
        cl=cl+i['column_name']+','
    return cl[0:-1]

def get_encrypt_columns(cfg):
    cfg['proxy_cur'].execute(cfg['statement'])
    rs = cfg['proxy_cur'].fetchall()
    if cfg['debug'] == True:
        for r in rs:
            print(r)
    return rs

def get_encrypt_column_data(cfg,c):
    st = """select {pk_name},
                   concat({column_name},'') as  {column_name}
            from {table_schema}.{table_name} 
             where {column_name} is not null 
               and  {column_name} !=''
                 -- and {column_name}_cipher is null 
                  order by {pk_name} """.format(**c)
    if cfg['debug']:
       print('get_encrypt_column_data=',st)
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

    env = cfg['env']
    cfg['encrypt_columns'] = get_encrypt_columns(cfg)
    try:
        for c in cfg['encrypt_columns']:
            c['pk_name'] = get_pk_names(cfg,c['table_schema'],c['table_name'])
            update_values = []
            update_values_like = []
            encrypt_column_data = get_encrypt_column_data(cfg, c)
            update_rows = 0
            for d in encrypt_column_data:
                if c['like'] == '1':
                    u = cfg['update_encrypt_column_like'].format(**c)
                    update_values_like.append(
                        (d[c['pk_name']], *db_encrypt_like(cfg, DB_ENV[env], format_sql(d[c['column_name']]))))
                else:
                    u = cfg['update_encrypt_column'].format(**c)
                    update_values.append(
                        (db_encrypt(cfg, DB_ENV[env], format_sql(d[c['column_name']])),str(d[c['pk_name']])))

                if len(update_values)>0 and len(update_values) % int(cfg['batch_size']) == 0:
                    u = cfg['update_encrypt_column'].format(**c)
                    try:
                        update_rows += len(update_values)
                        cfg['encrypt_cur'].executemany(u, update_values)
                        log = 'Table:`{}.{}`,total:{}/update:{} - {}% complete.'. \
                            format(c['table_schema'],
                                   c['table_name'],
                                   str(len(encrypt_column_data)),
                                   str(update_rows),
                                   str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6])
                        print('\r{}'.format(log), end='')
                        logging.info(log)
                        update_values = []
                    except:
                       traceback.print_exc()

                if len(update_values_like)>0 and len(update_values_like) % int(cfg['batch_size']) == 0:
                    u = cfg['update_encrypt_column'].format(**c)
                    try:
                        update_rows += len(update_values_like)
                        cfg['encrypt_cur'].executemany(u, update_values_like)
                        log = 'Table:`{}.{}`,total:{}/update:{} - {}% complete.'. \
                            format(c['table_schema'],
                                   c['table_name'],
                                   str(len(encrypt_column_data)),
                                   str(update_rows),
                                   str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6])
                        print('\r{}'.format(log), end='')
                        logging.info(log)
                        update_values_like = []
                    except:
                        traceback.print_exc()

            # last batch
            if len(update_values) > 0:
                u = cfg['update_encrypt_column'].format(**c)
                try:
                    update_rows += len(update_values)
                    cfg['encrypt_cur'].executemany(u, update_values)
                    log = 'Table:`{}.{}`,total:{}/update:{} - {}% complete.'.\
                             format(c['table_schema'],
                                    c['table_name'],
                                    str(len(encrypt_column_data)),
                                    str(update_rows),
                                    str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6])
                    print('\r{}'.format(log),end='')
                    logging.info(log)
                    update_values = []
                except:
                    traceback.print_exc()

            if len(update_values_like)> 0:
                u = cfg['update_encrypt_column'].format(**c)
                try:
                    update_rows += len(update_values_like)
                    cfg['encrypt_cur'].executemany(u, update_values_like)
                    log = 'Table:`{}.{}`,total:{}/update:{} - {}% complete.'. \
                        format(c['table_schema'],
                               c['table_name'],
                               str(len(encrypt_column_data)),
                               str(update_rows),
                               str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6])
                    print('\r{}'.format(log), end='')
                    logging.info(log)
                    update_values_like = []
                except:
                    traceback.print_exc()

            print('\n')

    except:
        traceback.print_exc()

