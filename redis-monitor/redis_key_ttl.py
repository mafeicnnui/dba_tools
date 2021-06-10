#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/6/4 9:20
# @Author : 马飞
# @File : redis_monitor.py
# @Software: PyCharm

import redis

def write_file(msg):
    with open('./redis_ttl.log', 'a') as f:
        f.write(msg)

def main():
    db   = [0,1,2,6,7,8,9,10,11,30,31,32]
    #db = [1,2]
    for d in db:
        r = redis.StrictRedis(host='r-2ze9f53dad8419b4.redis.rds.aliyuncs.com', port=6379, password='WXwk2018', db=d)
        keys = r.keys()
        keys.sort()
        for key in keys:
            remainTime = r.ttl(key)
            if remainTime==-1:
               write_file('db={},key={}\n'.format(d,key))

if __name__ == "__main__":
     main()



