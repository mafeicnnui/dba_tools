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
from   study.day0409.db_user import save_user
from   study.day0409.utils import get_host_ip

# 控制器
class user(tornado.web.RequestHandler):
     def get(self):
        self.render('add_user_ajax.html')

     def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        name   = self.get_argument("name")
        age    = self.get_argument("age")
        gender = self.get_argument("gender")
        res    = save_user(name,age,gender)
        print('res=',res)
        self.write(json.dumps(res))



# http server
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/user", user),
        ]
        settings = dict(
            static_path=os.path.join(os.path.dirname(__file__), "static"),  # 配置静态资源 js,css
            template_path=os.path.join(os.path.dirname(__file__), "templates"),  # 前端页面存放位置
            debug=True )
        tornado.web.Application.__init__(self, handlers,**settings)


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(9200)
    print('Web Server running {0} port ...'.format(9200))
    print('Access add  : http://{}:9200/user'.format(get_host_ip()))
    tornado.ioloop.IOLoop.instance().start()


