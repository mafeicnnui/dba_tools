#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/6/4 9:20
# @Author : 马飞
# @File : redis_monitor.py
# @Software: PyCharm

import time
import redis
import json
import re

def print_dict(r,config):
    print('-'.ljust(150,'-'))
    print(' '.ljust(3,' ')+"name".ljust(60,' ')+"type".ljust(10,' ')+'value'.ljust(80,' '))
    print('-'.ljust(150,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(60,' ')+r.type(key).decode('UTF-8').ljust(10,' '),config[key].ljust(80,' '))
    print('-'.ljust(150,'-'))
    print('合计:{0}'.format(str(len(config))))


def timestamp_datetime(value):
    format = '%Y-%m-%d %H:%M:%S'
    value = time.localtime(value)
    dt = time.strftime(format, value)
    return dt


def main():
    #r   = redis.Redis(host='10.2.39.70', port=6379,db=1)
    r   = redis.StrictRedis(host='r-2ze9f53dad8419b4pd.redis.rds.aliyuncs.com',port=65380,password='WXwk2018',db=0)
    keys = r.keys('member:grade:rightbags:1022:*')
    print(type(keys))
    print(keys)
    for k in keys:
        print(k)

    # v   = r.slowlog_get(8192)
    # print('v=',type(v))
    # c = 0
    # for key in v:
    #     # if i['duration'] > 50000:
    #     #     c += 1
    #     key['start_time'] = timestamp_datetime(key['start_time'])
    #     key['duration'] = round(key['duration'] / 1000, 2)
    #     del key['id']
    #     if str(key.get('command'),'UTF-8').count('member:grade:rightbags') ==0:
    #        print('vvvv=',key)
    #
    # print('len=',len(v))
    # print('c=',c)
if __name__ == "__main__":
     main()



