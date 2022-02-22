#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/20 15:44
# @Author : ma.fei
# @File : test.py.py
# @Software: PyCharm

'''
  测试数据库API，返回字典
'''
import datetime
from study.day0420.utils import MySQLManager

def test_dict():

    # 查询单行数据
    with MySQLManager('dict') as db:
        rs = db.get_one('select * from user where id=75')
        print(rs)

    # 查询多行数据
    with MySQLManager('dict') as db:
        rs = db.get_list('select * from user order by id')
        print(rs)

    # 修改数据
    with MySQLManager('dict') as db:
        db.modify("update user set name='china' where id=75")
        rs = db.get_one('select * from user where id=75')
        print(rs)

   # 创建查看表
    with MySQLManager('dict') as db:
        tm = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        db.modify("create table tm{}(i int)".format(tm))
        rs = db.get_list("show tables like 'tm%'")
        print(rs)

'''
  测试数据库API，返回元组
'''
def test_tuple():
    # 查询单行数据
    with MySQLManager() as db:
        rs = db.get_one('select * from user where id=75')
        print(rs)

    # 查询多行数据
    with MySQLManager() as db:
        rs = db.get_list('select * from user order by id')
        print(rs)

    # 修改数据
    with MySQLManager() as db:
        db.modify("update user set name='china' where id=75")
        rs = db.get_one('select * from user where id=75')
        print(rs)

   # 创建查看表
    with MySQLManager() as db:
        tm = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        db.modify("create table tm{}(i int)".format(tm))
        rs = db.get_list("show tables like 'tm%'")
        print(rs)

if __name__=='__main__':
    # test_dict()
    test_tuple()

