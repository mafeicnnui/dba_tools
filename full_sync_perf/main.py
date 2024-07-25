#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/7/16 16:21
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import datetime
import pymysql
import decimal

def get_db(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4',autocommit=True)
    return conn

def fmt_val(obj):
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d")
    elif isinstance(obj, datetime.timedelta):
        return str(obj)
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, int):
        return obj
    else:
        return str(obj).replace("\\","\\\\").replace("'","\\'")

def process_rs(rs):
    return [[ fmt_val(i)  for i in r]  for r in rs]

def get_cols():
    db = get_db('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr = db.cursor()
    st="""select concat('`',column_name,'`') from information_schema.columns
              where table_schema='puppet' and table_name='t_user'  order by ordinal_position"""
    cr.execute(st)
    rs = cr.fetchall()
    v_col = ''
    for i in list(rs):
        v_col = v_col + i[0] + ','
    return v_col[0:-1]

if __name__ == '__main__':
    db = get_db('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr = db.cursor()
    cr.execute('select * from puppet.t_user limit 0,20')
    rs=cr.fetchall()
    for r in process_rs(rs):
       print(r)

    cols=get_cols()
    vals=','.join(['%s' for i in cols.split(',')])
    st ='insert into test.t_user({}) values({})'.format(cols,vals)
    print('st=',st)
    data = rs #process_rs(rs)
    print('data=',data)
    cr.executemany(st,data)


