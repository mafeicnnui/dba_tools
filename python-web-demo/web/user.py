#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/9/17 14:13
# @Author : ma.fei
# @File : user.py.py
# @Software: PyCharm

import traceback
from web.db import T_User
from web.utils import BaseController
from playhouse.shortcuts import model_to_dict

class user(BaseController):
     def get(self):
         # 允许跨域访问
         self.set_header("Content-Type", "application/json; charset=UTF-8")
         self.set_header("Access-Control-Allow-Origin", '*')
         self.set_header("Access-Control-Allow-Headers", "x-requested-with")
         self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
         try:
             xm = self.get_argument("xm")
             rs = T_User.select().where(T_User.name.contains(xm))
             res = []
             for r in rs:
                 res.append(model_to_dict(r))
             self.write(self.SuccessJson('用户查询成功',res))
         except:
            self.write(self.ErrorJson('用户查询失败', 500,traceback.format_exc(),None))


     def post(self):
        pass

     def put(self):
        pass

     def delete(self):
        pass