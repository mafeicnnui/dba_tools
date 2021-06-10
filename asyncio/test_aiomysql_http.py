#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/26 10:45
# @Author : 马飞
# @File : test_aiomysql.py
# @Software: PyCharm

import asyncio
import time
from   aiomysql import create_pool
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.httpclient
import pymysql

from tornado.options import define, options
define("port", default=9600, help="run on the given port", type=int)

settings =  {
    "static_path"   : "/static",
    "template_path" : "templates",
    "cookie_secret" : "2379874hsdhf0234990sdhsaiuofyasop977djdj",
    "xsrf_cookies"  : False,
    "debug"         : True,
    "db"   :  {
        "host"     : "10.2.39.17",
        "user"     : "puppet",
        "password" : "Puppet@123",
        "name"     : "test",
        "port"     :  23306
    }
}


def get_db(settings):
    db = pymysql.connect(host    = settings['host'],
                         port    = settings['port'],
                         user    = settings['user'],
                         passwd  = settings['password'],
                         db      = settings['name'],
                         charset = 'utf8')
    return db

class TestHandler(tornado.web.RequestHandler):
    def initialize(self,db):
        self.db = db
        print("db=",db)

    async def get(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        v = ''
        async with create_pool(host=self.db['host'], port=self.db['port'],
                               user=self.db['user'], password=self.db['password'],
                               db=self.db['name'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("select * from emp")
                    v = await cur.fetchone()
        self.render('index.html', v= v,t=time.time())

    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        start = time.time()
        async with create_pool(host=self.db['host'], port=self.db['port'],
                               user=self.db['user'], password=self.db['password'],
                               db=self.db['name'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO async_tab(v,k) SELECT {},SLEEP(10);".format(str(time.time())))
        self.write('inserted complete! Time: {}s'.format(round(time.time()-start),0))


class TestHandler2(tornado.web.RequestHandler):
    def initialize(self,db):
        self.db = db

    def get(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        v = ''
        conn = get_db(self.db)
        cursor = conn.cursor()
        cursor.execute("select v from async_tab")
        rs = cursor.fetchone()
        self.render('message.html', v=rs, t=time.time())

    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        start = time.time()
        conn = get_db(self.db)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO async_tab(v,k) SELECT {},SLEEP(10);".format(str(time.time())))
        self.write('inserted complete! Time: {}s'.format(round(time.time() - start), 0))


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        print('IndexHandler:hello')
        self.write('hello')


urls = [
        (r"/", IndexHandler),
        (r"/t", TestHandler,{"db":settings["db"]}),
        (r"/t2", TestHandler2,{"db":settings["db"]})
]

class Application(tornado.web.Application):
    def __init__(self):
        handlers = urls
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(9700)
    print('http Server running 9600 port ...')
    tornado.ioloop.IOLoop.instance().start()

