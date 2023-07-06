#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/5 14:47
# @Author : ma.fei
# @File : find.py.py
# @Software: PyCharm

import os

def check_file_contents(file):
    with open(file, 'r') as f:
      lines = f.readlines()
    lines = ''.join(lines)
  #   if lines.count("""feign:
  # hystrix:
  #   enabled: true""")>0:
    if lines.count("""hystrix:""") > 0:
        return True
    else:
        return False


file_path='./files'
file_list = []
for root,dir,filename in os.walk(file_path):
 for file in filename:
   if  file == 'bootstrap.yml':
       file_list.append(os.path.join(root,file))

find_list=[]
for f in file_list:
   if check_file_contents(f):
      find_list.append(f)

with open('find.txt', 'w') as f:
    for i in find_list:
       f.write(i+'\n')
