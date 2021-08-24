#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/1 16:49
# @Author : ma.fei
# @File : 通过python连接MySQL.py
# @Software: PyCharm

# 说明：本机先安装python3.6，再安装pymysql驱动
# pip install pymysql

import pymysql

cfg = {
    'ip':'10.2.39.18',
    'port':3306,
    'user':'puppet',
    'password':'Puppet@123',
    'db':'test',
    'charset':'utf8'
}


# 建立mysql连接对象
db = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     cursorclass = pymysql.cursors.DictCursor,  # 驱动会以字典返回行记录
                     autocommit=True)

# 获取游标对象
cr = db.cursor()

# 查询表
cr.execute('select * from user')
rs = cr.fetchall()

# 输出结果
for i in rs:
    print(i)

# 关闭游标
cr.close()

# 关闭连接
db.close()




