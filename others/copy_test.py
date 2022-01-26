#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/1/24 17:19
# @Author : ma.fei
# @File : copy.py
# @Software: PyCharm
import copy

class Bus:
  def __init__(self, passengers=None):
     if passengers is None:
        self.passengers = []
     else:
        self.passengers = list(passengers)

     def pick(self, name):
        self.passengers.append(name)

     def drop(self, name):
        self.passengers.remove(name)

bus1 = Bus(['Alice', 'Bill', 'Claire', 'David'])
bus2 = copy.copy(bus1)
bus3 = copy.deepcopy(bus1)
id(bus1),id(bus2),id(bus3)
bus1.drop('Bill')
bus2.passengers
id(bus1.passengers), id(bus2.passengers), id(bus3.passengers)
bus3.passengers