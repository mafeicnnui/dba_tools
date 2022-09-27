#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/9/21 14:06
# @Author : ma.fei
# @File : mongo_test.py
# @Software: PyCharm

import pymongo

def get_ds_mongo(ip,port,service):
    conn = pymongo.MongoClient(host=ip, port=int(port))
    return conn[service]


def test1():
    db = get_ds_mongo('172.17.194.51', '27016', 'hft_settle_center')
    cr = db["SETTLEMENT_SETTING"]
    rs   = cr.find({})
    for i in rs:
        print(i)

def test2():
    db = get_ds_mongo('172.17.194.51', '27016', 'hopson_hft')
    cr = db["monitorLog"]
    rs   = cr.find({}).limit(10)
    for i in rs:
        print(i)

if __name__=="__main__":
     test2()
