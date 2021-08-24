#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/23 17:01
# @Author : ma.fei
# @File : projects_server_stats.py
# @Software: PyCharm

import os
import pymysql
import threading
import datetime

cfg = {
    'ip': '10.2.39.18',
    'port': 3306,
    'user': 'puppet',
    'password': 'Puppet@123',
    'db': 'puppet',
    'charset': 'utf8'
}

# 建立数据库连接
def get_db():
    db = pymysql.connect(host=cfg['ip'],port=cfg['port'],user=cfg['user'],passwd=cfg['password'],
                         db=cfg['db'],charset='utf8',cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return db

# 更新元数据
def upd_status(p_dx_status,p_lt_status,p_market_id):
    print(p_dx_status,p_lt_status,type(p_dx_status))
    db = get_db()
    cr = db.cursor()
    st = """insert into t_monitor_project_log(market_id,ip_dx_status,ip_lt_status,link_status,update_time)  values('{}','{}','{}','{}',now())""".format(p_market_id,p_dx_status,p_lt_status,p_dx_status or p_lt_status)
    cr.execute(st)
    st = """update t_monitor_project  set ip_dx_status='{}', ip_lt_status='{}', link_status='{}',update_time=now() where market_id='{}'""".format(p_dx_status,p_lt_status,p_dx_status or p_lt_status,p_market_id)
    print(st)
    cr.execute(st)
    db.commit()
    print('upd_status update complete!')
    cr.close()

def get_project(p_market_id):
    db = get_db()
    cr = db.cursor()
    st = "select * from t_monitor_project where market_id='{}'".format(p_market_id)
    print(st)
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs

def get_projects():
    db = get_db()
    cr = db.cursor()
    st = "select * from t_monitor_project  order by id"
    print(st)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    db.commit()
    return rs

def ping_linux(ip):
    ip = 'ping -w 3 {}'.format(ip)
    r = os.popen(ip)
    for i in r:
        if i.count('icmp_seq=3') > 0:
            return True
    return False

def ping_window(ip):
    ip = 'ping -n 3 {}'.format(ip)
    r  = os.popen(ip)
    t  = 0
    for i in r:
        if i.count('请求超时') > 0:
           t = t+1
    if t == 3 :
       return False
    else:
       return True

def ping_server(po):
    ip_dx = po['ip_dx']
    ip_lt = po['ip_lt']
    print('ip_dx=',ip_dx,ip_lt)
    print('Ping Server {}, please wait...'.format(ip_dx))
    if ip_dx !='':
       ip_dx_status = ping_linux(ip_dx)
    else:
       ip_dx_status = False

    print('Ping Server {}, please wait...'.format(ip_lt))
    if ip_lt !='':
       ip_lt_status = ping_linux(ip_lt)
    else:
       ip_lt_status = False
    return ip_dx_status,ip_lt_status


# 获取IP状态
def get_status(p_market_id):
    po = get_project(p_market_id)
    return ping_server(po)

def update_server(p_market_id):
    dx_status, lt_status = get_status(p_market_id)
    upd_status(dx_status, lt_status,p_market_id)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def start_threads():
    threads = []
    projs = get_projects()
    for p in projs:
        thread = threading.Thread(target=update_server, args=(p['market_id'],))
        threads.append(thread)

    for i in range(0, len(projs)):
        print('starting thread:{}'.format(p['market_id']))
        threads[i].start()

    for i in range(0, len(projs)):
        threads[i].join()
    print('all Done at :', get_time())

if __name__ == '__main__':
    start_threads()


