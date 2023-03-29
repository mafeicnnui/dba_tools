#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/15 11:10
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

#import tornado
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        if not self.get_cookie("mycookie"):
            self.set_cookie("mycookie", "myvalue")
            self.write("Your cookie was not set yet!")
        else:
            self.write("Your cookie was set!")


if __name__ == "__main__":
    settings = dict(static_path="static",
                    template_path="templates",
                    debug=True,
                    autoreload=True )
    application = tornado.web.Application([
        (r"/", MainHandler),
      ],
      cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
      **settings
    )
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()
