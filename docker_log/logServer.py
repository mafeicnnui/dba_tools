#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/11/29 16:48
# @Author : ma.fei
# @File : read_log.py
# @Software: PyCharm

import os
import sys
import pymysql
import datetime
import sys
import json
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
from   tornado.options  import define
import traceback


def format_log(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def current_rq():
    year =str(datetime.datetime.now().year)
    month=str(datetime.datetime.now().month).rjust(2,'0')
    day  =str(datetime.datetime.now().day).rjust(2,'0')
    return year+'-'+month+'-'+day

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           autocommit=True)
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn

def get_cmds():
    db = get_ds_mysql_dict('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr=db.cursor()
    cr.execute('SELECT  * FROM docker_log ORDER BY id')
    rs=cr.fetchall()
    return rs

def get_cmds_pods():
    db = get_ds_mysql_dict('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr=db.cursor()
    cr.execute('SELECT  * FROM docker_log where id=1')
    rs=cr.fetchone()
    return rs

def get_cmds_logrq():
    db = get_ds_mysql_dict('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr=db.cursor()
    cr.execute('SELECT  * FROM docker_log where id=10')
    rs=cr.fetchone()
    return rs


def get_value(cmd):
    try:
      print('\033[0;40;36m'+cmd+'\033[0m')
      r = os.popen(cmd).read()
      print('r=',r)
      return r
    except:
      return None

def exec_cmd(cmd):
    try:
      print('\033[0;40;36m'+cmd+'\033[0m')
      os.system(cmd)
      return True
    except:
      return False

def read_log(local_log_name):
    with open(local_log_name, 'r') as f:
        contents = f.readlines()
    return [v.replace('\n','') for v in contents]

def read_log_from_db(p_begin,p_end,p_string):

    db = get_ds_mysql_dict('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr = db.cursor()
    vv = 'where 1=1 '
    if p_begin !='' :
       vv  = vv + " and substr(msg,1,16) >='{}'".format(p_begin)

    if p_end!= '':
       vv = vv + " and substr(msg,1,16) <='{}'".format(p_end)

    if p_string != '':
       vv = vv + " and instr(msg,'{}')>0".format(p_string)

    st = """select * from docker_log_data {} order by id desc""".format(vv)
    print('st=',st)
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def write_log(logs):
    db = get_ds_mysql_dict('10.2.39.18', '3306', 'puppet', 'puppet', 'Puppet@123')
    cr = db.cursor()
    cr.execute('truncate table docker_log_data')
    for log in logs:
        st = """insert into  docker_log_data(msg) values('{}')""".format(format_log(log))
        cr.execute(st)
def process_log(p_pod,p_logrq,p_begin_rq,p_end_rq,p_string):
    project_name=p_pod
    search_date =p_logrq
    print('p_pod=',p_pod)
    print('search_date=',search_date)
    print('-'.ljust(85,'-'))

    # from db reawd commands
    cfg = {}
    for c in get_cmds():
        if c['id'] == 1:
            cfg[c['msg']] = get_value(c['cmd'].format(p_pod)).split(' ')[0]
            if  cfg[c['msg']]  is None or cfg[c['msg']] =='':
                res = {'code':-1,'data':'获取podname失败'}
                return res
        elif c['id'] == 2:
            cfg[c['msg']] =  [ i for i in get_value(c['cmd'].format(cfg['podname'])).split('\n') if i.count('/') == 7][0]
        elif c['id'] ==3:
            cfg[c['msg']] = exec_cmd(c['cmd'].format(cfg['podname'],cfg['docker_logfile']))
        elif c['id'] == 4:
            cfg[c['msg']] = '{}/{}'.format('/home/hopson/log', cfg['docker_logfile'].split('/')[-1])
        elif c['id'] == 5:
            cfg[c['msg']] = get_value(c['cmd'].format(cfg['podname'],search_date)).replace('\n','')
        elif c['id'] == 6:
            cfg[c['msg']] = exec_cmd(c['cmd'].format(cfg['podname'], cfg['docker_gzfile']))
        elif c['id'] == 7:
            cfg[c['msg']] = '{}/{}'.format('/home/hopson/log', cfg['docker_gzfile'].split('/')[-1])
        elif c['id'] == 8:
            cfg[c['msg']] = exec_cmd(c['cmd'].format(cfg['local_gzfile']))
        elif c['id'] == 9:
            cfg[c['msg']] = '{}/{}'.format('/home/hopson/log', cfg['local_gzfile'].split('/')[-1]).replace('.gz','')

    # print cfg
    print_dict(cfg)

    # write db
    if search_date=='':
        write_log(read_log(cfg['local_logfile']))
    else:
        write_log(read_log(cfg['gzip_local_file']))

    res = read_log_from_db(p_begin_rq,p_end_rq,p_string)
    return {'code':0,'data':res}

def process_pod(p_project):
    project_name=p_project
    print('project_name=',project_name)
    print('-'.ljust(85,'-'))
    res = get_cmds_pods()
    val  = [ i.split(' ')[0] for i in  get_value(res['cmd'].format(project_name)).split('\n')]
    if  val is None or val =='':
        res = {'code':-1,'data':'获取podname失败'}
        return res
    return {'code':0,'data':val}

def process_logrq(p_pod):
    res = get_cmds_logrq()
    print(get_value(res['cmd'].format(p_pod)))
    val  = [ i for i in  get_value(res['cmd'].format(p_pod)).split('\n')]
    rq = []
    for v in get_value(res['cmd'].format(p_pod)).split('\n'):
        if v!='':
           rq.append(v.split('.')[-2])
    print('rq=',rq)
    if  val is None or val =='':
        res = {'code':-1,'data':'获取podname失败'}
        return res
    return {'code':0,'data':rq}


class search(tornado.web.RequestHandler):
    def get(self):
        self.render("search_log.html")


class log(tornado.web.RequestHandler):
    def get(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            pod_name           = self.get_argument("pod_name")
            log_rq             = self.get_argument("log_rq")
            search_date_begin  = self.get_argument("search_date_begin")
            search_date_end    = self.get_argument("search_date_end")
            search_stirng      = self.get_argument("search_stirng")

            print('pod_name=',pod_name)
            print('log_rq=', log_rq)
            print('search_date_begin=',search_date_begin)
            print('search_date_end=', search_date_end)
            print('search_stirng=', search_stirng)

            res  =  process_log(pod_name,log_rq,search_date_begin,search_date_end,search_stirng)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class pod(tornado.web.RequestHandler):
    def get(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            project_name       = self.get_argument("project_name")
            print('project_name=',project_name)
            res  =  process_pod(project_name)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})


class getrq(tornado.web.RequestHandler):
    def get(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            pod_name  = self.get_argument("pod_name")
            print('pod_name=',pod_name)
            res  =  process_logrq(pod_name)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})



define("port", default=sys.argv[1], help="run on the given port", type=int)
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", search),
            (r"/log", log),
            (r"/pod", pod),
            (r"/getRq", getrq),
        ]
        settings = dict(static_path=os.path.join(os.path.dirname(__file__), "static"),
                        template_path=os.path.join(os.path.dirname(__file__), "templates"),
                        debug=True,
                      )
        tornado.web.Application.__init__(self, handlers,**settings)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(sys.argv[1])
    print('LogServer  running {0} port ...'.format(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()