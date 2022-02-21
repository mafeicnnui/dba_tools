#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/7/27 14:49
# @Author : ma.fei
# @File : client.py
# @Software: PyCharm

'''
  1. 开发，测试环境先测
  2. 查询命令支持，解析find()中的内容
  3. db.getCollection('menu').find({})
  4. db.menu.find({})
  5. db.getCollection('menu').find({"_id":ObjectId("5d40ead88505cf5bdbb376e8")})

'''

import re
import sys
import json
import pymongo
from bson.objectid import ObjectId

def mongo_connecter():
    conn = pymongo.MongoClient(host='39.106.184.57', port=int('27016'))
    db = conn['posB']
    db.authenticate('root', 'JULc9GnEuNHYUTBG')
    return db

def parse_name(p_sql):
    pattern = re.compile(r'(db.getCollection\(\'.*\'\))', re.I)
    if pattern != []:
        tab = pattern.findall(p_sql)[0].split('(')[1].split(')')[0].replace("'", '')
        res =tab
    else:
       res ='Parse Error:Not Found collection name!'
    return res

def parser_find(p_sql):
    pattern = re.compile(r'(find\(.*\))',re.I)
    if pattern !=[]:
       where = pattern.findall(p_sql)[0]
       return where
    else:
       print('Parse Error:Not Found find keyword!')
       return None

def parser_objectId(p_where):
    pattern = re.compile(r'(find\(.*\))', re.I)
    if pattern != []:
        id = p_where.split('ObjectId(')[1].split(')')[0].replace('"','')
        return id
    else:
        print('Parse Error:Not Found collection where!')
        return None


def parser_where(p_sql):
    t_name  = parse_name(p_sql)
    t_where = parser_find(p_sql)
    if t_where.find('_id')>=0:
       t_objId = parser_objectId(t_where)
       return t_name,None,t_objId
    else:
       v_where= t_where.split('(')[1].split(')')[0]
       return t_name,v_where,None

if __name__ == "__main__":
     # st = """db.getCollection('menu').find({})"""
     # st = """db.getCollection('menu').find({"_id": ObjectId("5d40ead88505cf5bdbb376e8")})"""
     #
     # n,v,id = parser_where(st)
     # print('value=',n,v,id)
     db = mongo_connecter()
     # print('------------------------------------------')
     # myjs = "function(){return db.getCollectionNames()}"
     # print('res0=', db.eval(myjs))
     # myjs = "function(){ res=[];doc=db.posInfo.find().limit(10); for i in print('');return 123;}"
     # res = db.eval(myjs)
     # print('res1=', db.eval(myjs))
     # print('res2=',res)
     # print(type(res))

     # st = db['menu'].find({})
     # print('st=',st)
     # for i in st:
     #     print(i)

     print('---------------------------------')
     for i in db.list_collection_names(session=None):
         print(i)