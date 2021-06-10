#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/18 14:16
# @Author : 马飞
# @File : yield.py.py
# @Software: PyCharm

from collections.abc import Iterable, Iterator, Generator

# 使用列表生成式，注意不是[]，而是()
L = (x * x for x in range(10))
print(isinstance(L, Generator))  # True


# 实现了yield的函数
def mygen(n):
    now = 0
    while now < n:
        yield now
        now += 1

if __name__ == '__main__':
    gen = mygen(4)
    # print(isinstance(gen, Generator))  # True
    # for i in gen:
    #     print(i)
    a=gen.send(None)
    print('a=',a)
    print('a=', next(gen))
    print('a=', gen.send(None))
    print('a=', next(gen))
    print('a=', next(gen))
