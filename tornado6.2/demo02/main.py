#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/15 11:10
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import tornado
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        items = ["Item 1", "Item 22", "Item 33"]
        self.render("template.html", title="My title", items=items)


if __name__ == "__main__":
    settings = dict(static_path="static",
                    template_path="templates",
                    debug=True)
    application = tornado.web.Application([
        (r"/", MainHandler),
      ],
      **settings
    )
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()