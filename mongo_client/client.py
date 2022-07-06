#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/7/27 14:49
# @Author : ma.fei
# @File : client.py
# @Software: PyCharm

import re
import json
import pymongo
from bson.objectid import ObjectId

def mongo_connecter():
    conn = pymongo.MongoClient(host='39.106.184.57', port=int('27016'))
    db = conn['posB']
    db.authenticate('root', 'JULc9GnEuNHYUTBG')
    return db

def mongo_connecter2():
    conn = pymongo.MongoClient(host='39.96.14.108', port=27018)
    db = conn['posB']
    db.authenticate('admin', 'admin')
    return db

def eval():
    db = mongo_connecter2()
    #myjs = "function(){ return 123;}"
    #myjs = "function(){res=db.posInfo.find().limit(10); return res;}"
    #myjs = "function(){ var v='';for(var c=db.posInfo.find().limit(10);c.hasNext();){v=v+c.next();printjson(c.next())}}"
    myjs = "function(){ var v='';for(var c=db.posInfo.find().limit(1);c.hasNext();){ v=printjson(c.next());} return v;}"
    print('res=', db.eval(myjs))
    res = db.eval(myjs)
    print('res=',type(res))

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
       v_where= t_where.split('(')[1].split(')')[0].replace("'",'"')
       return t_name,json.loads(v_where),None

def parser_limit(p_sql):
    if p_sql.find('.limit')>=0:
       return p_sql.split('.limit(')[1].split(')')[0]
    else:
       return None

def queryDatabases():
    db = mongo_connecter()
    print('---------------------------------')
    for i in db.list_collection_names(session=None):
        print(i)

def findAll(st):
    db = mongo_connecter()
    n,v,id = parser_where(st)
    res = db[n].find()
    return res

def findById(st):
    db = mongo_connecter()
    n,v,id = parser_where(st)
    res = db[n].find({'_id': ObjectId(id)})
    return res

def findByFilter(st):
    db = mongo_connecter()
    n,v,id = parser_where(st)
    l = parser_limit(st)
    print(n,v,id)
    if id is not None:
        if l is not None:
           return  findById(st).limit(int(l))
        else:
           return findById(st)
    else:
        if l is not None:
          return db[n].find(v).limit(int(l))
        else:
          return db[n].find(v)


if __name__ == "__main__":
     st = """db.getCollection('menu').find({}).limit(5)"""
     #st = """db.getCollection('menu').find({"_id": ObjectId("5d40ead88505cf5bdbb376e8")})"""
     #st = """db.getCollection('menu').find({"menuId": 'm16'})"""
     res = findByFilter(st)
     for r in res:
         print(r)


