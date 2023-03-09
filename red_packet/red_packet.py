#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/7 18:10
# @Author : ma.fei
# @File : red_packet.py.py
# @Software: PyCharm

import random
from queue import LifoQueue


settings = {
    'money':100,
    'people':60,
    'method':'rand'
}

def redPacket(stack,people, money):
    remain = people
    for i in range(people):
        remain -= 1
        if remain > 0:
            m = random.randint(1, money - remain)
        else:
            m = money
        money -= m
        stack.put(m / 100.0)


if __name__ =='__main__':
    stack = LifoQueue(maxsize = settings['people'])
    redPacket(stack,settings['people'],settings['money'])
    for i in range(settings['people']):
        print(stack.get())

