#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/10/12 15:31
# @Author : 马飞
# @File : mysql2binlog2.py.py
# @Software: PyCharm

import os,sys
#python binlog.py binglog-0001 '2013-07-01 00:00:00' '2013-07-02 00:00:00'
#mysqlbinlog --no-defaults --start-datetime='2019-10-11 00:00:00' --stop-datetime='2019-10-11 10:00:00' mysql-bin.000001
def log_w(type,text):
    logfile = "%s.txt" % (type,text)
    #now = time.strftime("%Y-%m-%d %H:%M:%S")
    tt = str(text) + "\n"
    f = open(logfile,'a+')
    f.write(tt)
    f.close()


logname    = sys.argv[1]
start_time = sys.argv[2]
end_time   = sys.argv[3]
comn = "mysqlbinlog --no-defaults --start-datetime='{0}' --stop-datetime='{1}' {2}" .format(start_time,end_time,logname)
aa=os.popen(comn).readlines()
mylist=[]
for a in aa:
    if ('UPDATE' in a):
            update = ' '.join(a.split()[:2])
            mylist.append(update)
    if ('INSERT INTO' in a):
            update = ' '.join(a.split()[:3]).replace("INTO ","")
            mylist.append(update)
    if ('DELETE from' in a):
            update = ' '.join(a.split()[:3]).replace("from ","")
            mylist.append(update)
mylist.sort()
bb = list(set(mylist))
bb.sort()
cc = []
for item in bb:
    cc.append([mylist.count(item),(item)])
cc.sort()
cc.reverse()
for i in cc:
        print(str(i[0])+'\t'+i[1])