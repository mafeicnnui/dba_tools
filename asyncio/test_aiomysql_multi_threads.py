#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/26 10:45
# @Author : 马飞
# @File : test_aiomysql.py
# @Software: PyCharm

# -*- coding:utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from time import ctime, sleep
def loop(nloop, nsec):
    print("start loop", nloop, "at:", ctime())
    sleep(nsec)
    print("loop", nloop, "done at:", ctime())

if __name__=="__main__":
    with ThreadPoolExecutor(max_workers=3) as executor:
        all_task = [executor.submit(loop, i, j) for i, j in zip([1,2],[4,3])]