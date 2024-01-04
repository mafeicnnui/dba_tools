#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/9/19 16:26
# @Author : ma.fei
# @File : market.py.py
# @Software: PyCharm

import datetime
import pymysql
import traceback
import warnings

settings = {
    "host"         : "39.107.70.55",
    "port"         : "3306",
    "user"         : "root",
    "passwd"       : "root321HOPSON",
    "db"           : "test",
    "db_charset"   : "utf8"
}

def get_db():
    conn = pymysql.connect(
       host = settings['host'],
       port=int(settings['port']),
       user=settings['user'],
       passwd=settings['passwd'],
       db=settings['db'],
       charset='utf8',
       cursorclass=pymysql.cursors.DictCursor,
       autocommit=True)
    return conn

def get_statements(db):
    st="""SELECT 
     CONCAT('select count(0) as cnt from ',table_schema,'.',table_name,' where market_id!=218') AS ct,
     CONCAT('delete from ',table_schema,'.',table_name,' where market_id!=218;') AS st   
FROM information_schema.`COLUMNS` 
WHERE column_name='market_id' 
  AND table_schema NOT IN('sync','puppet','hopson_pms')
  and table_schema not like '%_real_time%'
  and table_name not like '%tc_record%'
ORDER BY table_schema"""
    cr=db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    print('rs=',rs)
    return rs

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    db = get_db()
    cr = db.cursor()
    for s in get_statements(db):
       try:
           cr.execute(s['ct'])
           rs=cr.fetchone()
           if rs['cnt'] > 0 :
               print(s['st'])
       except:
          print('s=', s)
          #traceback.print_exc()