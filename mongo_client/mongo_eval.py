#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/1/4 10:47
# @Author : ma.fei
# @File : mongo_eval.py.py
# @Software: PyCharm


import os
import json
import re

def repl_str(matched):
    value = matched.group()
    return value.replace('ObjectId(','').replace(')','')


cmd = '/home/hopson/apps/usr/webserver/dbops/tools/mongo 10.2.39.43:27016/posB --eval "db.menu.find().limit(5)"'
r = os.popen(cmd).read()
c = r.split('\n')
print('r=',r,type(r))
print('c=',c)
print('c2=',c[3:])
c3 = c[3:]

pattern = re.compile(r'(ObjectId(.*))', re.I)
for i in range(len(c3)-1):
    c3[i]= eval(pattern.sub(repl_str, c3[i]))
    print('c3[{}]='.format(i), c3[i], type(c3[i]))

print('c3=',c3)

c4 = json.dumps(c3)
print('c4=',c4,type(c4))

c5=json.loads(c4)
print('c5=',c5,type(c5))