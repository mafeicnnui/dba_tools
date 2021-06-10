#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/2 1:57
# @Author : 马飞
# @File : server.py.py
# @Software: PyCharm

from tornado.web import StaticFileHandler, RedirectHandler
from aiomysql import create_pool
import time
import os
from tornado import web
import tornado
from tornado.web import template
from forms  import MessageForm
from models import Message

class MainHandler(web.RequestHandler):
    def initialize(self, db):
        self.db = db

    async def get(self, *args, **kwargs):
        message_from = MessageForm()
        self.render("message.html", message_form=message_from)

    async def post(self, *args, **kwargs):
        message_from = MessageForm(self.request.arguments)
        if message_from.validate():
            name = message_from.name.data
            email = message_from.email.data
            address = message_from.address.data
            message_data = message_from.message.data

            message = Message()
            message.name = name
            message.email = email
            message.address = address
            message.message = message_data

            message.save()

            self.render("message.html", message_form=message_from)
        else:
            self.render("message.html", message_form=message_from)


settings = {
    "static_path"       :'/home/hopson/apps/usr/webserver/dba_tools/wtforms/static',
    "static_url_prefix" : "/static/",
    "template_path"     : "templates",
    "db": {
        "host": "10.2.39.18",
        "user": "puppet",
        "password": "Puppet@123",
        "name": "test",
        "port": 3306
    }
}

if __name__ == "__main__":
    app = web.Application([
        ("/", MainHandler, {"db": settings["db"]}),
        # ("/static/(.*)", StaticFileHandler, {"path": "C:/projects/tornado_overview/blank/static"})
    ], debug=True, **settings)
    app.listen(18888)
    tornado.ioloop.IOLoop.current().start()