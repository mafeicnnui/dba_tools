#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/4/25 9:48
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import time
import json
import pymysql
import warnings
import traceback
import logging
import datetime
import threading

logging.basicConfig(filename='encrypt.log'.format(datetime.datetime.now().strftime("%Y-%m-%d")),
                    format='[%(asctime)s-%(levelname)s:%(message)s]',
                    level=logging.INFO,
                    filemode='a',
                    datefmt='%Y-%m-%d %I:%M:%S')

DB_ENV = {'dev':1,'test':2,'pre':3,'pro':4}

def get_cfg():
    cfg = read_json('./encrypt.json')
    cfg['proxy_db_init'] = get_ds_mysql(
                        host=cfg['proxy_db']['db_ip'],
                        port=int(cfg['proxy_db']['db_port']),
                        user=cfg['proxy_db']['db_user'],
                        password=cfg['proxy_db']['db_pass'],
                        db=cfg['proxy_db']['db_service'])
    cfg['encrypt_db_init'] = get_ds_mysql(
                        host=cfg['encrypt_db']['db_ip'],
                        port=int(cfg['encrypt_db']['db_port']),
                        user=cfg['encrypt_db']['db_user'],
                        password=cfg['encrypt_db']['db_pass'],
                        db=cfg['encrypt_db']['db_service'])
    cfg['proxy_cur_init'] = cfg['proxy_db_init'].cursor()
    cfg['encrypt_cur_init'] = cfg['encrypt_db_init'].cursor()
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
    cfg['encrypt_cur_init'].execute(st)
    rs = cfg['encrypt_cur_init'].fetchall()
    for i in list(rs):
        cl=cl+i['column_name']+','
    return cl[0:-1]

def get_encrypt_columns(cfg):
    cfg['proxy_cur_init'].execute(cfg['statement'])
    rs = cfg['proxy_cur_init'].fetchall()
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
                 and {query_filter}
                  order by {pk_name} """.format(**c)
    if cfg['debug'] == True:
       print('get_encrypt_column_data=',st)
    cfg['encrypt_cur_init'].execute(st)
    rs = cfg['encrypt_cur_init'].fetchall()
    return rs

def db_encrypt(cfg,thread_obj,env,plain):
    if env  == DB_ENV['dev']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['dev'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['dev'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['dev_cipher']

    if env  == DB_ENV['test']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['test'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['test'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['test_cipher']

    if env  == DB_ENV['pre']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['pre'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['pre'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['pre_cipher']

    if env == DB_ENV['pro']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['pro'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement']['pro'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['pro_cipher']

def db_encrypt_like(cfg,thread_obj,env,plain):
    if env  == DB_ENV['dev']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['dev'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['dev'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['dev_cipher'],rs['dev_like']

    if env  == DB_ENV['test']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['test'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['test'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['test_cipher'],rs['test_like']

    if env  == DB_ENV['pre']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['pre'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['pre'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['pre_cipher'],rs['pre_like']

    if env == DB_ENV['pro']:
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['pro'][0].format(plain,thread_obj['thread_id']))
        thread_obj['thread_proxy_cur'].execute(cfg['encrypt_statement_like']['pro'][1].format(thread_obj['thread_id']))
        rs = thread_obj['thread_proxy_cur'].fetchone()
        return rs['pro_cipher'],rs['pro_like']

def data_split(thread_number,data):
    task_data =[]
    start = 0
    end = int(len(data)/thread_number)
    step = int(len(data)/thread_number)
    while len(task_data) < thread_number:
        task_data.append(list(data[start:end]))
        start = end
        if len(task_data) == thread_number-1:
            end = len(data)
        else:
            end=end+step
    return task_data

def init_thread(cfg,thread_obj):
    if cfg['debug'] == True:
       print('initialize thread `{}`...'.format(thread_obj['thread_id']))
       print("insert into t_cipher(id) values('{}')".format(thread_obj['thread_id']))
    thread_obj['thread_proxy_cur'].execute("insert into t_cipher(id) values('{}')".format(thread_obj['thread_id']))

def clear_thread(cfg,thread_obj):
    if cfg['debug'] == True:
       print('clear thread `{}`...'.format(thread_obj['thread_id']))
       print("delete from t_cipher where id = '{}'".format(thread_obj['thread_id']))
    thread_obj['thread_proxy_cur'].execute("delete from t_cipher where id = '{}'".format(thread_obj['thread_id']))

def get_total_update_rows(thread_update_rows):
    r = 0
    for k,v in thread_update_rows.items():
        r =r + int(v)
    return r

def multi_thread_update(cfg,c,data,thread_id):
    thread_proxy_conn =  get_ds_mysql(
                    host=cfg['proxy_db']['db_ip'],
                    port=int(cfg['proxy_db']['db_port']),
                    user=cfg['proxy_db']['db_user'],
                    password=cfg['proxy_db']['db_pass'],
                    db=cfg['proxy_db']['db_service'])

    thread_encrypt_conn = get_ds_mysql(
                    host=cfg['encrypt_db']['db_ip'],
                    port=int(cfg['encrypt_db']['db_port']),
                    user=cfg['encrypt_db']['db_user'],
                    password=cfg['encrypt_db']['db_pass'],
                    db=cfg['encrypt_db']['db_service'])

    thread_obj = {
        'thread_id':thread_id,
        'thread_proxy_cur' :thread_proxy_conn.cursor(),
        'thread_encrypt_cur': thread_encrypt_conn.cursor()
    }

    update_values = []
    update_values_like = []
    update_rows = 0

    init_thread(cfg, thread_obj)
    for d in data:
        if c['like'] == '1':
            update_values_like.append(
             (*db_encrypt_like(cfg,thread_obj,DB_ENV[env],format_sql(d[c['column_name']])),str(d[c['pk_name']])))
        else:
            update_values.append(
              (db_encrypt(cfg,thread_obj,DB_ENV[env],format_sql(d[c['column_name']])), str(d[c['pk_name']])))

        if len(update_values) > 0 and len(update_values) % int(cfg['batch_size']) == 0:
            u = cfg['update_encrypt_column'].format(**c)
            try:
                update_rows += len(update_values)
                thread_update_rows[thread_obj['thread_id']] += len(update_values)
                thread_obj['thread_encrypt_cur'].executemany(u, update_values)
                log = 'Thread:{},Table:`{}.{}`,total:{}/param:{}/update:{}/{} - thread:{}% ,total:{}%'. \
                    format(thread_obj['thread_id'],
                           c['table_schema'],
                           c['table_name'],
                           str(len(encrypt_column_data)),
                           str(len(data)),
                           str(len(update_values)),
                           str(update_rows),
                           str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6],
                           str(round(get_total_update_rows(thread_update_rows) / len(encrypt_column_data), 4) * 100)[0:6],
                           )
                print(log)
                logging.info(log)
                update_values = []
            except:
                traceback.print_exc()


        if len(update_values_like) > 0 and len(update_values_like) % int(cfg['batch_size']) == 0:
            u = cfg['update_encrypt_column_like'].format(**c)
            try:
                update_rows += len(update_values_like)
                thread_update_rows[thread_obj['thread_id']] += len(update_values_like)
                thread_obj['thread_encrypt_cur'].executemany(u, update_values_like)
                log = 'Thread:{},Table:`{}.{}`,total:{}/param:{}/update:{}/{} - thread:{}% ,total:{}%'. \
                    format(thread_obj['thread_id'],
                           c['table_schema'],
                           c['table_name'],
                           str(len(encrypt_column_data)),
                           str(len(data)),
                           str(len(update_values_like)),
                           str(str(update_rows)),
                           str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6],
                           str(round(get_total_update_rows(thread_update_rows) / len(encrypt_column_data), 4) * 100)[0:6],
                           )
                print(log)
                logging.info(log)
                update_values_like = []
            except:
                traceback.print_exc()

    # last batch
    if len(update_values) > 0:
        u = cfg['update_encrypt_column'].format(**c)
        try:
            update_rows += len(update_values)
            thread_update_rows[thread_obj['thread_id']] += len(update_values)
            thread_obj['thread_encrypt_cur'].executemany(u, update_values)
            log = 'Thread:{},Table:`{}.{}`,total:{}/param:{}/update:{}/{} - thread:{}% ,total:{}%'. \
                format(thread_obj['thread_id'],
                       c['table_schema'],
                       c['table_name'],
                       str(len(encrypt_column_data)),
                       str(len(data)),
                       str(len(update_values)),
                       str(update_rows),
                       str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6],
                       str(round(get_total_update_rows(thread_update_rows) / len(encrypt_column_data), 4) * 100)[0:6],
                       )
            print(log)
            logging.info(log)
            update_values = []
        except:
            traceback.print_exc()
            print('update_values=',update_values)

    if len(update_values_like) > 0:
        u = cfg['update_encrypt_column_like'].format(**c)
        try:
            update_rows += len(update_values_like)
            thread_update_rows[thread_obj['thread_id']] += len(update_values_like)
            thread_obj['thread_encrypt_cur'].executemany(u, update_values_like)
            log = 'Thread:{},Table:`{}.{}`,total:{}/param:{}/update:{}/{} - thread:{}% ,total:{}%'. \
                format(thread_obj['thread_id'],
                       c['table_schema'],
                       c['table_name'],
                       str(len(encrypt_column_data)),
                       str(len(data)),
                       str(len(update_values_like)),
                       str(update_rows),
                       str(round(update_rows / len(encrypt_column_data), 4) * 100)[0:6],
                       str(round(get_total_update_rows(thread_update_rows) / len(encrypt_column_data), 4) * 100)[0:6],
                       )
            print(log)
            logging.info(log)
            update_values_like = []
        except:
            traceback.print_exc()
            print('u=',u)
            print('update_values_like=', update_values_like)

    #print('\n')
    clear_thread(cfg, thread_obj)

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
            encrypt_column_data = get_encrypt_column_data(cfg, c)
            threads = []
            thread_update_rows = {}
            for d in data_split(cfg['thread_number'],encrypt_column_data):
                time.sleep(0.1)
                thread_id = str(int(round(time.time()*1000)))
                thread_update_rows[thread_id] = 0
                print('prepare thread id:`{}` for `{}.{}`...'.format(thread_id,c['table_schema'],c['table_name']))
                logging.info('prepare thread id:`{}` for `{}.{}`...'.format(thread_id,c['table_schema'],c['table_name']))
                thread = threading.Thread(target=multi_thread_update, args=(cfg, c, d,thread_id))
                threads.append({
                    'thread_number':thread_id,
                    'thread_obj':thread
                })

            for i in range(0, len(threads)):
                print('start thread id :{} for {}.{}...'.format(threads[i]['thread_number'],c['table_schema'],c['table_name']))
                logging.info('start thread id :{} for {}.{}...'.format(threads[i]['thread_number'],c['table_schema'],c['table_name']))
                threads[i]['thread_obj'].start()
                time.sleep(0.5)

            for i in range(0, len(threads)):
                threads[i]['thread_obj'].join()
        print('\n')

    except:
        traceback.print_exc()

