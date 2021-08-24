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


db = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     cursorclass = pymysql.cursors.DictCursor,
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