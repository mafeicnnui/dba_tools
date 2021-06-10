#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/12/25 9:01
# @Author : 马飞
# @File : pcsv.py
# @Software: PyCharm

import pymysql
import traceback

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_file_contents(filename):
    lines = []
    f = open(filename,'r')
    line = f.readline()
    lines.append(line)
    while line:
        line = f.readline()
        lines.append(line)
    f.close()
    return lines

def process(p_line):
    try:
        ip = p_line.split(' ')[0]
        mid = p_line.split('mid=')[1].split('&')[0]
        m   = p_line.split('m=')[1].split('&')[0]
        query_str=p_line.split(' ')[1]
        return ip,mid,m,query_str
    except:
        print(traceback.print_exc())
        print(p_line)

def upd_data(p_cr,p_line):
    try:
        ip, mid, m,query_str = process(p_line)
        st = "insert into t2(v1,v2,v3,v4) values('{}','{}','{}','{}')".format(ip,mid,m,query_str)
        #print(st)
        p_cr.execute(st)
    except:
        print(traceback.print_exc())
        print(p_line)


def main():
    lines = get_file_contents('ip.log')
    db    = get_ds_mysql('10.2.39.18', '3306', 'test', 'puppet', 'Puppet@123')
    cr    = db.cursor()
    for i in range(len(lines)-1):
        upd_data(cr,lines[i])
    db.commit()
    db.close()

if __name__ == "__main__":
    main()



