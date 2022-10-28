#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/10/25 9:45
# @Author : ma.fei
# @File : query_market_id.py
# @Software: PyCharm

import time
import datetime
import pymysql
import warnings
import logging
import traceback

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False,cursorclass = pymysql.cursors.DictCursor)
    return conn

if __name__ == "__main__":
    # db = get_ds_mysql_dict('10.2.39.76',
    #                        '3306',
    #                        'information_schema',
    #                        'hopsonone',
    #                        'hopsonone123')

    # db = get_ds_mysql_dict('rr-2zekl959654j1k49r6o.mysql.rds.aliyuncs.com',
    #                        '3306',
    #                        'information_schema',
    #                        'apptong',
    #                        '4pH^2gp&amp;n3N6dgiRAlhs0fhnvq0xQ2&amp;D')
    n_market_id=123456

    db = get_ds_mysql_dict('rr-2ze8nqixl9wq6ei041o.mysql.rds.aliyuncs.com',
                           '3306',
                           'information_schema',
                           'hft_ro',
                           '8SezZdtuoUTJ5MBLXdcD6XJv5czwBkD')

    logging.basicConfig(filename='query_market_id.log'.format(datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    #warnings.filterwarnings("ignore")
    cr = db.cursor()
    cr.execute("SELECT table_schema,table_name FROM information_schema.columns WHERE column_name='market_id' ")
    rs= cr.fetchall()
    for r in rs:
        try:
            #print(r['table_schema'], r['table_name'])
            sql = "select count(0) as rec from `{}`.`{}` where market_id=30300".format(r['table_schema'], r['table_name'])
            cr.execute(sql)
            rs=cr.fetchone()
            if rs['rec'] >0:
                #logging.info('{}.{}'.format(r['table_schema'], r['table_name']))
                print(r['table_schema'], r['table_name'],rs['rec'])
        except:
            traceback.print_exc()
            logging.error('ERROR:{}.{}'.format(r['table_schema'], r['table_name']))
        #time.sleep(1)