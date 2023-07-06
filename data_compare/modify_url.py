#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/1 11:06
# @Author : ma.fei
# @File : modify_url.py
# @Software: PyCharm

import pymysql

def get_db():
    conn = pymysql.connect(host= 'rr-2zekl959654j1k49r6o.mysql.rds.aliyuncs.com',
                           port= 3306,
                           user = 'puppet',
                           passwd = 'Puppet@123',
                           db  = 'information_schema',
                           charset = 'utf8mb4',
                           autocommit = True,
                           cursorclass = pymysql.cursors.DictCursor,
                           read_timeout = 60,
                           write_timeout = 60)
    return conn

if __name__ == '__main__':
    db = get_db()
    cr=db.cursor()
    cr.execute('SELECT id,function_url FROM `hopsonone_downcenter`.`function_model`   ORDER BY id')
    rs=cr.fetchall()
    for i in rs:
        n = []
        # n = i['function_url'].split(':')
        # u = ':'.join([ u.lower() if u.count('//')>0 else u for u in i['function_url'].split(':') ])
        # print(u)
        # n[1] =n[1].lower()
        # v = ':'.join(n)
        # s = "update `hopsonone_downcenter`.`function_model` set `function_url`='{}' where id={};".format(v,i['id'])
        # print(s)
        f = i['function_url'].split('//')
        s = f[1].split('/')
        s[0] = s[0].lower()
        s = '/'.join(s)
        v = '//'.join([f[0],s])
        s = "update `hopsonone_downcenter`.`function_model` set `function_url`='{}' where id={};".format(v,i['id'])
        print(s)