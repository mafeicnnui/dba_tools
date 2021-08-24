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
from   study.day0412.db_user import save_user,query_user
from   study.day0412.utils import get_host_ip

# 控制器
class user(tornado.web.RequestHandler):
     def get(self):
        self.render('add_user.html')

     def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        name   = self.get_argument("name")
        age = self.get_argument("age")
        gender = self.get_argument("gender")
        res = save_user(name,age,gender)
        self.write(json.dumps(res))

# 控制器
class user_query(tornado.web.RequestHandler):
     def get(self):
        self.render('query_user.html')

     def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        name   = self.get_argument("name")
        res = query_user(name)
        self.write(json.dumps(res))


# 控制器
class user_query2(tornado.web.RequestHandler):
     def get(self):
        self.render('query_user2.html',msg={})

     def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        name   = self.get_argument("name")
        res = query_user(name)
        print(res)
        self.write(json.dumps(res))


# http server
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/user", user),
            (r"/user/query", user_query),
            (r"/user/query/ajax", user_query2),
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
    print('Access add user: http://{}:9200/user'.format(get_host_ip()))
    print('Access query user: http://{}:9200/user/query'.format(get_host_ip()))
    print('Access query user(ajax): http://{}:9200/user/query/ajax'.format(get_host_ip()))

    tornado.ioloop.IOLoop.instance().start()


