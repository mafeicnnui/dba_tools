#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/2 12:50
# @Author : ma.fei
# @File : 修改文件内容.py
# @Software: PyCharm

import json

'''
  功能：通过模板修改nginx特定配置信息
'''
def modify_nginx():
   # 打开模板文件，替换配置
   with open('nginx.conf.tpt', 'r', encoding='utf-8') as f:
      content = f.read().replace('$$SERVER$$','10.2.39.40')\
                        .replace('$$PORT$$','9000')

   # 将配置写入新文件
   with open('nginx.conf', 'w') as f:
      f.write(content)

   # 查询配置
   with open('nginx.conf', 'r') as f:
      print(f.read())

'''
  功能：直接修改json配置文件
'''
def modify_json():
   file = 'config.json'
   with open(file, 'r',encoding='utf-8') as f:
      # 读取文件内容，并转为字典对象
      cfg = json.loads(f.read())

   with open(file, 'w',encoding='utf-8') as f:
      # 修改字典内容
      cfg['db_ip'] = '10.2.39.99'
      # 把字典 cfg 转为 json 写入文件
      f.write(json.dumps(cfg, ensure_ascii=False, indent=4, separators=(',', ':')))

   # 查询配置
   with open('config.json', 'r') as f:
      print(f.read())

'''
  __name__ :python内置变量，表示当前模块名称，如果在本文件中运行，则值为:'__main__',如果从其它文件调用，其值为文件名不含扩展名
'''
if __name__ == '__main__':

    modify_nginx()

    # modify_json()

print('module name :',__name__)