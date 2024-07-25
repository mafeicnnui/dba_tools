#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/7/23 11:52
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import pymysql
import pandas as pd

def get_db(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4',autocommit=True)
    return conn

cfg = {
    "host"      : "10.2.39.18",
    "port"      : 3306,
    "user"      : "puppet",
    "passwd"    : "Puppet@123",
    "db"        : "puppet",
    "charset"   : "utf8mb4",
    "autocommit": True
}
db = pymysql.connect(**cfg)
cr = db.cursor()
cr.execute('truncate table puppet.t_bbgl_imp')

df = pd.read_excel('上海静安MOHO销售积分0506~0512-2.xlsx')
typ ='1'  # 数据类型(1：支付宝，2：微信)
cols=df.columns.values.tolist()
tmp = ','.join(['`v{}`'.format(str(i+1)) for i in range(len(cols))])
header = "insert into puppet.t_bbgl_imp(`type`,{}) values ('{}',".format(tmp,typ)

for _, item in df.iterrows():
    values = ''
    for col in df.columns.values.tolist():
        values=values+"'"+str(item[col])+"',"
    sql = header+values[0:-1]+')'
    cr.execute(sql)