#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/9/17 16:34
# @Author : ma.fei
# @File : utils.py.py
# @Software: PyCharm

import json
import datetime
import tornado.web

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

class BaseController(tornado.web.RequestHandler):

    def SuccessJson(self,name,data):
        res = {
            'Name' : name,
            'Code' : 200,
            'Data' : data
        }
        return json.dumps(res ,cls=DateEncoder)

    def ErrorJson(self,name,code,msg,data):
        res = {
            'Name': name,
            'Code': code,
            'Msg' : msg,
            'Data': data
        }
        return json.dumps(res, cls=DateEncoder)