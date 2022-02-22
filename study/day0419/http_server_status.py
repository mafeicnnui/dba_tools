#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 13:34
# @Author : ma.fei
# @File : http_server.py.py
# @func :  Simple http server
# @Software: PyCharm


import os
import json
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
import warnings
from   study.day0419.utils import get_host_ip
from   study.day0419.db_transfer import read_percent

class percent(tornado.web.RequestHandler):
     def post(self):
         self.set_header("Content-Type", "application/json; charset=UTF-8")
         self.set_header("Access-Control-Allow-Origin", '*')
         self.set_header("Access-Control-Allow-Headers", "x-requested-with")
         self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
         res = read_percent()
         self.write(json.dumps(res))

# http server
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/percent", percent),
        ]
        settings = dict(
            static_path=os.path.join(os.path.dirname(__file__), "static"),  # 配置静态资源 js,css
            template_path=os.path.join(os.path.dirname(__file__), "templates"),  # 前端页面存放位置
            debug=True )
        tornado.web.Application.__init__(self, handlers,**settings)


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(9201)
    print('Web Server running {0} port ...'.format(9200))
    print('Access add user: http://{}:9201/percent'.format(get_host_ip()))
    tornado.ioloop.IOLoop.instance().start()


