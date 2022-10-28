#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/10/24 17:06
# @Author : ma.fei
# @File : mongo_query.py
# @Software: PyCharm

import re
import json
import sys

import pymongo
from bson.objectid import ObjectId

config = {
    'ip':'10.2.39.41',
    'port':'27016',
    'db':'hopson_hft'
}

class mongo_client:

    def __init__(self,ip,port,db=None,user=None,password=None):
        self.ip = ip
        self.port =port
        self.user = user
        self.password = password
        self.db = db
        self.client,self.conn = self.get_db()
        self.where = None
        self.limit = None
        self.columns =None
        self.collection_name=None

    def __repr__(self):
            return '''
    mongo_query attribute:
    --------------------------------
        ip:{}
        port:{}
        user:{}
        password:{}
        db:{}
        conn:{}
        client:{}
    --------------------------------
    '''.format(self.ip,
               self.port,
               '' if self.user is None else self.user,
               '' if self.password is None else self.password,
               self.db,
               self.conn,
               self.client
               )

    def get_db(self):
        conn = pymongo.MongoClient('mongodb://{0}:{1}/'.format(self.ip, int(self.port)))
        if self.db is not None:
           db = conn[self.db]
           if self.user is not None and self.password is not None:
               db.authenticate(self.user, self.password)
               return conn,db
           else:
               return conn,db
        else:
          return conn,conn['admin']

    def get_databases(self):
        return self.client.list_database_names()

    def get_collection_name(self,p_sql):
        pattern1 = re.compile(r'(db\..*\.)', re.I)
        pattern2 = re.compile(r'(db.getCollection\(\'.*\'\))', re.I)
        if pattern1.findall(p_sql) != [] and pattern2.findall(p_sql) == []:
            self.collection_name = pattern1.findall(p_sql)[0].split('.')[1]

        if pattern2.findall(p_sql) != []:
            self.collection_name = pattern2.findall(p_sql)[0].split('(')[1].split(')')[0].replace("'", '')

    def get_where(self,p_sql):
        pattern = re.compile(r'(find\(.*\))', re.I)
        if pattern.findall(p_sql) != [] :
            print('get_where=',pattern.findall(p_sql))


            if pattern.findall(p_sql)[0].find('_id') >= 0 :
                if pattern.findall(p_sql)[0].count('{') == 1 :
                    id = pattern.findall(p_sql)[0].split('ObjectId(')[1].split(')')[0].replace('"', '')
                    self.where = {'_id': ObjectId(id)}
                    print('_id=', self.where)
                elif pattern.findall(p_sql)[0].count('{') > 1 and pattern.findall(p_sql)[0].count('$in') == 0 :
                    temp = pattern.findall(p_sql)[0].split('.limit')[0]
                    id = pattern.findall(p_sql)[0].split('ObjectId(')[1].split(')')[0].replace('"', '')
                    pattern = re.compile(r'(\}\s*,\s*\{)', re.I)
                    if pattern.findall(temp) != []:
                        temp1 = re.split(r'(\}\s*,\s*\{)', temp, flags=re.I)
                        cols = '{' + temp1[2][0:-1]
                        print('id=',id,'cols=',cols)
                        self.where = {'_id': ObjectId(id)}
                        self.columns = json.loads(cols)
                    else:
                       print('not found },{')
                elif pattern.findall(p_sql)[0].count('{') > 1 and pattern.findall(p_sql)[0].count('$in') == 1 :
                    print('$in && _id...')
                    temp = re.split(r'(\$in\s*:\s*)',pattern.findall(p_sql)[0],flags=re.I)
                    objd = temp[2].split('[')[1].split(']')[0]
                    inid = []
                    for o in objd.split(','):
                       id =  o.replace('ObjectId','').replace('(','').replace(')','').replace("'","").replace('"','').strip()
                       inid.append(ObjectId(id))
                    print('inid=',inid)
                    self.where = {'_id': {'$in' : inid }}

                    temp = pattern.findall(p_sql)[0].split('.limit')[0]
                    pattern = re.compile(r'(\}\s*,\s*\{)', re.I)
                    if pattern.findall(temp) != []:
                        temp1 = re.split(r'(\}\s*,\s*\{)', temp, flags=re.I)
                        cols = '{' + temp1[2][0:-1]
                        print('cols=',json.loads(cols))
                        self.columns = json.loads(cols)
                    else:
                        print('not found },{')
            elif pattern.findall(p_sql)[0].count('{') > 1 :
                if pattern.findall(p_sql)[0].count('$in') == 0:
                    temp = pattern.findall(p_sql)[0].split('(')[1].split(')')[0]
                    print('temp=', temp)
                    pattern = re.compile(r'(\}\s*,\s*\{)', re.I)
                    if pattern.findall(temp) != []:
                        temp1 = re.split(r'(\}\s*,\s*\{)', temp, flags=re.I)
                        v = temp1[0] + '}'
                        c = '{' + temp1[2]
                        self.where = json.loads(v)
                        self.columns = json.loads(c)
                    else:
                        print('func:get_where,error:1000')
                elif  pattern.findall(p_sql)[0].count('$in') == 1:
                    print('$in && not _id...')
                    temp = re.split(r'(\$in\s*:\s*)', pattern.findall(p_sql)[0], flags=re.I)
                    objd = temp[2].split('[')[1].split(']')[0]
                    print('temp=', temp)
                    print('objd=', objd)
                    sys.exit(0)
                    # inid = []
                    # for o in objd.split(','):
                    #    id =  o.replace('ObjectId','').replace('(','').replace(')','').replace("'","").replace('"','').strip()
                    #    inid.append(ObjectId(id))
                    # print('inid=',inid)
                    # self.where = {'_id': {'$in' : inid }}

                    # temp = pattern.findall(p_sql)[0].split('.limit')[0]
                    # pattern = re.compile(r'(\}\s*,\s*\{)', re.I)
                    # if pattern.findall(temp) != []:
                    #     temp1 = re.split(r'(\}\s*,\s*\{)', temp, flags=re.I)
                    #     cols = '{' + temp1[2][0:-1]
                    #     print('cols=',json.loads(cols))
                    #     self.columns = json.loads(cols)
                    # else:
                    #     print('not found },{')
                else:
                   pass
            elif pattern.findall(p_sql)[0].count('{') == 1 :
                temp = pattern.findall(p_sql)[0].split('(')[1].split(')')[0]
                self.where = json.loads(temp)
            else:
               pass
        else:
          print('parser find error:2000')

    def get_limits(self,p_sql):
        if p_sql.find('.limit') >= 0:
           self.limit =  p_sql.split('.limit(')[1].split(')')[0]

    def find_by_where(self,p_query):
        print('p_query=',p_query)
        self.get_collection_name(p_query)
        print('get_collection_name=',self.collection_name)
        self.get_where(p_query)
        print('where=',self.where,type(self.where))
        print('column=', self.columns, type(self.columns))
        self.get_limits(p_query)
        print('limit=', self.limit)

        if self.limit is not None:
           if self.columns is not None:
             print(' limit not & columns not  ')
             rs = self.conn[self.collection_name].find(self.where,self.columns).limit(int(self.limit))
           else:
             print('limit not  & columns null ')
             rs = self.conn[self.collection_name].find(self.where).limit(int(self.limit))
        else:
           if self.columns is not None:
              print('limit null  & columns not ')
              rs = self.conn[self.collection_name].find(self.where,self.columns)
           else:
              print('limit null  & columns null ')
              rs = self.conn[self.collection_name].find(self.where)
        print('-----------------------------------')
        for r in rs:
            print(r)

'''
  1.一个条件，所有列
  √ db.monitorLog.find({terminalNo:"10000201"}}).limit(10)
  √ db.getCollection('monitorLog').find({terminalNo:"10000201"}).limit(10)
  
  2.一个条件，显示某些列
  √ db.monitorLog.find({"terminalNo":"10000201"},{"terminalNo":1}).limit(5)
  √ db.monitorLog.find({"terminalNo":"10000201"},{"terminalNo":1,"ip":1}).limit(5)
  √ db.getCollection('monitorLog').find({"terminalNo":"10000201"},{"terminalNo":1}).limit(5)
  √ db.getCollection('monitorLog').find({"terminalNo":"10000201"},{"terminalNo":1,"ip":1}).limit(5)
  
  3.查询所有数据
  √ db.monitorLog.find({}).limit(10)
  √ db.getCollection('monitorLog').find({}).limit(10) 
  
  4.查询所有数据,显示部分列
  √ db.getCollection('monitorLog').find({},{"terminalNo":1}).limit(5)
  √ db.getCollection('monitorLog').find({},{"terminalNo":1,"ip":1}).limit(5) 
  
  5.支持ObjectID列查询
  √ db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")})
  √ db.monitorLog.find({"_id" : {$in:[ObjectId("5d5e5f338488d5000145b343"),ObjectId("5d5e5fc88488d5000145b344")]}})
  √ db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")},{"terminalNo":1}).limit(5)
  √ db.monitorLog.find({"_id" : {$in:[ObjectId("5d5e5f338488d5000145b343"),ObjectId("5d5e5fc88488d5000145b344")]}},{"terminalNo":1}).limit(5)  
  √ db.getCollection('monitorLog').find({'_id':ObjectId("5d5e5f338488d5000145b343")})
  √ db.monitorLog.find({"_id":{$in:[ObjectId("5d5e5f338488d5000145b343"), ObjectId("5d5e5fc88488d5000145b344")]}}).limit(3)
  √ db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")},{"terminalNo":1}).limit(5)
  √ db.monitorLog.find({"_id" : {$in:[ObjectId("5d5e5f338488d5000145b343"),ObjectId("5d5e5fc88488d5000145b344")]}},{"terminalNo":1}).limit(5)  
  
  √ db.monitorLog.find({ $and : [{"receiveLogDt" : { $gte : ISODate("2019-08-22 00:00:00.000") }}, {"receiveLogDt" : { $lte : ISODate("2019-08-22 23:59:59.000") }}]})
  √ db.monitorLog.find({"logType" : {$in:[1,2]}},{"terminalNo":1,"logType":1}).limit(5)  

  
  6.支持$and操作
  
  7.不支持_id，其它条件组合查询 
  
'''

if __name__ == "__main__":
    mongo = mongo_client(config['ip'], config['port'],config['db'])
    print('database=',mongo.get_databases())
    #mongo.find_by_where('''db.monitorLog.find({"terminalNo":"10000201"}).limit(5)''')
    #mongo.find_by_where('''db.getCollection('monitorLog').find({"terminalNo":"10000201"}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({"terminalNo":"10000201"},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({"terminalNo":"10000201"},{"terminalNo":1,"ip":1}).limit(5)''')
    #mongo.find_by_where('''db.getCollection('monitorLog').find({"terminalNo":"10000201"},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.getCollection('monitorLog').find({"terminalNo":"10000201"},{"terminalNo":1,"ip":1}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({}).limit(10)''')
    #mongo.find_by_where('''db.getCollection('monitorLog').find({}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({},{"terminalNo":1}).limit(10)''')
    #mongo.find_by_where('''db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")})''')
    #mongo.find_by_where('''db.monitorLog.find({"_id":{$in:[ObjectId("5d5e5f338488d5000145b343"), ObjectId("5d5e5fc88488d5000145b344")]}}).limit(3)''')
    #mongo.find_by_where('''db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({"_id" : {$in:[ObjectId("5d5e5f338488d5000145b343"),ObjectId("5d5e5fc88488d5000145b344")]}},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.getCollection('monitorLog').find({'_id':ObjectId("5d5e5f338488d5000145b343")})''')
    #mongo.find_by_where('''db.monitorLog.find({"_id":{$in:[ObjectId("5d5e5f338488d5000145b343"), ObjectId("5d5e5fc88488d5000145b344")]}}).limit(3)''')
    #mongo.find_by_where('''db.monitorLog.find({'_id':ObjectId("5d5e5f338488d5000145b343")},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.monitorLog.find({"_id" : {$in:[ObjectId("5d5e5f338488d5000145b343"),ObjectId("5d5e5fc88488d5000145b344")]}},{"terminalNo":1}).limit(5)''')
    #mongo.find_by_where('''db.getCollection("monitorLog").find({ $and : [{"receiveLogDt" : { $gte : ISODate("2019-08-22 00:00:00.000") }}, {"receiveLogDt" : { $lte : ISODate("2019-08-22 23:59:59.000") }}]})''')
    mongo.find_by_where('''db.monitorLog.find({"logType" : {$in:[1,2]}},{"terminalNo":1,"logType":1}).limit(5)''')