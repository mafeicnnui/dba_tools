#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/26 13:38
# @Author : 马飞
# @File : test_yield.py.py
# @Software: PyCharm

# -*- coding:utf-8 -*-
def consumer():
    r = ''
    while True:
        n = yield r
        if not n:
            return
        print('[CONSUMER] Consuming %s...' % n)
        r = '200 OK'

def produce(c):
    c.send(None) # 等价于next(c)
    n = 0
    while n < 5:
        n = n + 1
        print('[PRODUCER] Producing %s...' % n)
        r = c.send(n)
        print('[PRODUCER] Consumer return: %s' % r)
    c.close()

c = consumer()
produce(c)
