#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/15 11:10
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import asyncio

import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("<h1>Hello, world</h1>")

class StoryHandler(tornado.web.RequestHandler):
    def get(self, story_id):
        self.write("this is story %s" % story_id)

class MyFormHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('<html><body><form action="/myform" method="POST">'
                   '<input type="text" name="message">'
                   '<input type="submit" value="Submit">'
                   '</form></body></html>')

    def post(self):
        self.set_header("Content-Type", "text/plain")
        self.write("You wrote " + self.get_body_argument("message"))

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/story/([0-9]+)", StoryHandler),
        (r"/myform", MyFormHandler),
    ])

async def main():
    app = make_app()
    app.listen(8888)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())