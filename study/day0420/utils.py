#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/20 15:39
# @Author : ma.fei
# @File : utils.py.py
# @Software: PyCharm

import pymysql

'''
   数据库操作通用 API
'''

class MySQLManager(object):

    # 初始化实例的时候调用connect方法连接数据库
    def __init__(self,curType=None):
        self.conn = None
        self.cursor = None
        self.curType = curType
        self.connect()

    # 连接数据库
    def connect(self):
        self.conn = pymysql.connect(
            host ='10.2.39.18',
            port = 3306,
            database = 'test',
            user = 'puppet',
            password = 'Puppet@123',
            charset = 'utf8'
        )
        if self.curType is None:
            self.cursor = self.conn.cursor()
        else:
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)



    # 查询多条数据sql是sql语句，args是sql语句的参数
    def get_list(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchall()
        return result

    # 查询单条数据
    def get_one(self, sql, args=None):
        self.cursor.execute(sql, args)
        result = self.cursor.fetchone()
        return result

    # 执行单条SQL语句
    def modify(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()

    # 执行多条SQL语句
    def multi_modify(self, sql, args=None):
        self.cursor.executemany(sql, args)
        self.conn.commit()

    # 创建单条记录的语句
    def create(self, sql, args=None):
        self.cursor.execute(sql, args)
        self.conn.commit()
        last_id = self.cursor.lastrowid
        return last_id

    # 关闭数据库
    def close(self):
        self.cursor.close()
        self.conn.close()

    # 进入with语句自动执行
    def __enter__(self):
        return self

    # 退出with语句自动执行
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

