#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 13:44
# @Author : ma.fei
# @File : db_user.py.py
# @Software: PyCharm

import pymysql
import traceback
from .settings import cfg


''' 
CREATE TABLE xs(
     id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
     NAME VARCHAR(40),
     age INT,
     gender VARCHAR(3)
 )
'''

#返回字典
db = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     cursorclass = pymysql.cursors.DictCursor,
                     autocommit=True)

#返回元组
db2 = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     autocommit=True)

def save_user(name,age,gender):
    try:
        st = "insert into xs(name,age,gender) values('{}','{}','{}')".format(name,age,gender)
        cr = db.cursor()
        print('Execute sql:{}'.format(st))
        cr.execute(st)
        return {'code':0,'msg':'保存成功!'}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':'保存失败!({})'.format(str(e))}


def query_user(name,gender):
    try:
        st = "select name,age,gender from xs where name like '%{}%' and gender='{}'".format(name,gender)
        cr = db2.cursor()
        print('Execute sql:{}'.format(st))
        cr.execute(st)
        rs = cr.fetchall()
        return {'code':0,'msg':rs}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':'查询失败!({})'.format(str(e))}


def get_genders():
    st = "select distinct gender,gender from xs"
    cr = db2.cursor()
    print('Execute sql:{}'.format(st))
    cr.execute(st)
    rs = cr.fetchall()
    return rs
