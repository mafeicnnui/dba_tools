#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/2 2:05
# @Author : 马飞
# @File : models.py.py
# @Software: PyCharm

from datetime import datetime
import pymysql
from peewee import *
from peewee import Model

db = MySQLDatabase('test', host="10.2.39.40", port=3306, user="puppet", password="Puppet@123")


class Message(Model):
    id = AutoField(verbose_name="id")
    name = CharField(max_length=10, verbose_name="姓名")
    email = CharField(max_length=30, verbose_name="邮箱")
    address = CharField(max_length=30, verbose_name="地址")
    message = TextField(verbose_name="留言")

    class Meta:
        database = db
        table_name = "message"

if __name__ == "__main__":
    db.create_tables([Message])