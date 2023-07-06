#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/20 16:52
# @Author : ma.fei
# @File : find_file.py
# @Software: PyCharm

import os

file_path='./output_dir'
file_list = []
cmd = ' enex2notion --token xxxx --done-file  {}/{} {}/{}'
for root,dir,filename in os.walk(file_path):
 for file in filename:
    print(cmd.format(root,file.replace('.enex','.txt'),root,file))
    #os.system(cmd)