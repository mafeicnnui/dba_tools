#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/10/24 17:06
# @Author : ma.fei
# @File : mongo_query.py
# @Software: PyCharm

import re
import json
import pickle
import pymongo
from bson.objectid import ObjectId

# config = {
#     'ip':'dds-2ze39d52c51a85f42163-pub.mongodb.rds.aliyuncs.com',
#     'port':'3717',
#     'db':'local',
#     'auth_db':'admin',
#     'user':'root',
#     'password':'HstmonGo@2021'
# }

'''
db.dropUser("hopsonone_ro");
db.createUser(
{
   user:'hopsonone_ro',
   pwd:'lvOD4ljLBkREapZd',
   roles:[
     { role:"readAnyDatabase",db:"admin"},
     { role : "clusterAdmin", "db" : "admin"},
  ]
}); 
'''

config = {
    'ip':'dds-2ze39d52c51a85f42163-pub.mongodb.rds.aliyuncs.com',
    'port':'3717',
    'db':'local',
    'auth_db':'admin',
    'user':'hopsonone_ro',
    'password':'lvOD4ljLBkREapZd'
}

class mongo_client:

    def __init__(self,ip,port,auth_db=None,db=None,user=None,password=None):
        self.ip = ip
        self.port =port
        self.user = user
        self.password = password
        self.auth_db=auth_db
        self.db = db
        self.client,self.conn = self.get_db()
        self.where = None
        self.limit = None
        self.columns =None
        self.collection_name=None
        self.find_string=None
        self.find_contents=None

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
        if self.auth_db is not None:
           db = conn[self.auth_db]
           if self.user is not None  and self.user !='' and self.password is not None and self.password !='':
               db.authenticate(self.user, self.password)
               return conn,conn[self.auth_db]
           else:
               return conn,db
        else:
          return conn,conn[self.db]

    def get_databases(self):
        return self.client.list_database_names()

    def get_collections(self,p_db):
        return self.client[p_db].list_collection_names()



def A():
    mongo = mongo_client(ip=config['ip'],
                         port=config['port'],
                         db=config['db'],
                         auth_db=config['auth_db'],
                         user=config['user'],
                         password=config['password'])
    print('[打印数据库名列表]....')
    print(mongo.get_databases())
    print('[打印集合列表]....')
    for db in mongo.get_databases():
        rs = mongo.client[db].system.profile.find(
               {'$and': [{"op":"query"},
                         {"millis":{'$gt':1000}}]},
               {'ns': 1, 'docsExamined':1,'nreturned':1,
                'millis':1,'client':1,'ts':1,'query': 1,}
             ).sort('ts')
        for r in rs:
            print(str(r))

    # #db = mongo.client['local']
    # db = mongo.conn
    # rs = db.system.profile.find({""}).limit(3)


if __name__ == "__main__":
    A()
