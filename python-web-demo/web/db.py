#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/9/17 14:44
# @Author : ma.fei
# @File : db.py
# @Software: PyCharm

import json
import warnings
from peewee import *

db = MySQLDatabase('demo',host ='10.2.39.18',port=3306,user='puppet',passwd='Puppet@123')

class BaseModel(Model):
    def __str__(self):
        r = {}
        for k in self._data.keys():
          try:
             r[k] = str(getattr(self, k))
          except:
             r[k] = json.dumps(getattr(self, k))
        return str(r)

    class Meta:
        database = db

class T_User(BaseModel):
    name       = CharField(max_length=20)
    gender     = CharField(max_length=10)
    age        = IntegerField()
    email      = CharField(max_length=40)
    phone      = CharField(max_length=20)
    password   = CharField(max_length=20)


def main():
    warnings.filterwarnings("ignore")
    print('建立连接...')
    db.connect()

    print('删除表...')
    db.drop_tables([T_User])

    print('创建表...')
    db.create_tables([T_User])

if __name__=='__main__':
    main()