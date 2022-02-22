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
from   study.day0419_async.db_transfer_async import get_servers,get_dm_from_dmmx,get_nginx_conf
from   study.day0419.utils import get_host_ip
from   study.day0419_async.transfer_async import transfer_data
from   study.day0419_async.db_transfer_async import read_percent

# 控制器
class transfer(tornado.web.RequestHandler):
     async def get(self):
        self.render('transfer_async.html',
                    servers = await get_servers(),
                    domain_names = await get_dm_from_dmmx('01'),
                    nginx_confs  = await get_nginx_conf()
                    )

     async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        domain_id        =  self.get_argument("domain_id")
        port             =  self.get_argument("port")
        target_dir       =  self.get_argument('target_dir')
        nginx_conf_id    =  self.get_argument("nginx_conf_id")
        servers          =  self.get_argument("servers")
        res              =  await transfer_data(domain_id,port,target_dir,nginx_conf_id,servers)
        self.write(json.dumps(res))

#
class percent(tornado.web.RequestHandler):
     async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        res = await read_percent()
        self.write(json.dumps(res))

# http server
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/transfer", transfer),
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
    http_server.listen(9200)
    print('Sync Web Server running {0} port ...'.format(9200))
    print('Access add user: http://{}:9200/transfer'.format(get_host_ip()))
    tornado.ioloop.IOLoop.instance().start()


