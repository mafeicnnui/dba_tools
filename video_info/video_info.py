#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/16 9:32
# @Author : 马飞
# @File : video_info.py
# @Func : dbops_api Server 提供数据库备份、同步API。
# @Software: PyCharm
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
from   tornado.options  import define, options
import datetime,json
import os,sys
import traceback

class videoadd(tornado.web.RequestHandler):
    def get(self):
        self.render("video_add.html")


class write_config_file(tornado.web.RequestHandler):
    def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            v_config  = '/home/hopson/apps/var/pyhikvision/config/config.ini'
            v_name  = self.get_argument("name")
            v_pass1 = self.get_argument("pass1")
            v_ip    = self.get_argument("ip")
            v_port  = self.get_argument("port")
            result = {}
            result['code'] = 0
            result['msg'] = 'success'
            v_json = json.dumps(result)
            v_file  = '''
[DEFAULT]
SDKPath = /Users/zhubin/python/pyhikvsion/hkws/lib/
User = {}
Password = {}
Port = {}
IP = {}
Plat = 0 （Linux填0，Windows填1）   
'''.format(v_name,v_pass1,v_port,v_ip)
            with open(v_config, 'w') as f:
                f.write(v_file)
            self.write(v_json)
        except:
            result = {}
            result['code'] = -1
            result['msg'] = 'failure'
            v_json = json.dumps(result)
            self.write(v_json)

define("port", default=sys.argv[1], help="run on the given port", type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/videoinfo", videoadd),
            (r"/write_config_file" , write_config_file),
        ]
        settings = dict(
            static_path=os.path.join(os.path.dirname(__file__), "./static"),
            template_path=os.path.join(os.path.dirname(__file__), "./templates"),
            cookie_secret="2379874hsdhf0234990sdhsaiuofyasop977djdj",
            xsrf_cookies=False,
            debug=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(sys.argv[1])
    print('Video info Server running {0} port ...'.format(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()



