#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/22 14:48
# @Author : ma.fei
# @File : test.py
# @Software: PyCharm
'''
test.py demo module
'''
# 导入模块
import support

# 现在可以调用模块里包含的函数了
support.print_func("Runoob")

print(__name__)
print(support.__name__)
print(dir(support))
print(type(support.print_func))
for i in dir(support):
    print(i,type(eval('support.{}'.format(i))))

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
for i in dir(support):
    print(i,eval('support.{}'.format(i)))

help(support.print_func)