#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/9 15:08
# @Author : ma.fei
# @File : test.py.py
# @Software: PyCharm

db ={
    'v1':'''class Test:
          def prt():
                print('hello world')''',
    'v2':'''class Test:
          def prt():
                print('How do you do ?')''',
}


class loader(object):
  def __init__(self):
      pass
  def load(self,str):
   try:
      tmp = {}
      exec(str,tmp)
      return tmp
   except:
     import traceback
     print("Load module error: {}".format(traceback.format_exc()))
     return None

if __name__=='__main__':
    load = loader()
    m = load.load(db['v1'])
    c = m['Test']
    c.prt()
