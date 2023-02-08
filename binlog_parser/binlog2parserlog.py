#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/8/3 14:00
# @Author : ma.fei
# @File : binlog2db.py
# @Software: PyCharm

import os
import sys
import time
import json
import warnings
import datetime
import argparse
import glob

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def timestamp2string(timeStamp):
    try:
         d = datetime.datetime.fromtimestamp(timeStamp)
         s = d.strftime("%Y-%m-%d %H:%M:%S")
         return s
    except Exception as e:
        print(e)
        return ''

def parse_log(p_binlogfile= None,p_outputdir=None,p_debug = None):
    logfile = os.path.join(p_outputdir,p_binlogfile.split('/')[-1] + '.log')
    if sys.platform == 'win32':
       if os.system('where mysqlbinlog') == 0:
          cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} | find 'SET TIMESTAMP=' > {} ".\
                format(p_binlogfile, logfile)
       else:
          print('mysqlbinlog `{}` not found!'.format(p_binlogfile))
          sys.exit(0)
    else:
       if os.system('which mysqlbinlog &>/dev/null') == 0:
          cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} | grep 'SET TIMESTAMP=' > {}".\
                format(p_binlogfile,logfile)
       else:
          print('mysqlbinlog `{}` not found!'.format(p_binlogfile))
          sys.exit(0)

    print('Parsing binlogfile `{}` ...'.format(p_binlogfile))
    if p_debug == 'Y':
       print(cmd)
    if os.system(cmd) == 0:
       print('Binlogfile `{}` resolve success!'.format(p_binlogfile))
       return logfile
    else:
       print('Resolve binlogfile `{}` failure!'.format(p_binlogfile))
       return

def parsing(binlog_info,p_binlogfile = None,p_outputdir = '.',p_debug = 'N'):
    log = parse_log(p_binlogfile,p_outputdir,p_debug)
    if log is None:
       print('Resolve binlog error!')
       sys.exit(0)
    with open(log,encoding='utf-8',errors='ignore') as file:
        contents = file.readlines()
    begin_timestamp = contents[0].split('SET TIMESTAMP=')[1].split('/*!*/;')[0]
    end_timestamp = contents[-1].split('SET TIMESTAMP=')[1].split('/*!*/;')[0]
    binlog_info.append({
        'binlogfile':p_binlogfile.split('/')[-1],
        'begin_time':timestamp2string(int(begin_timestamp)),
        'end_time':timestamp2string(int(end_timestamp))
    })

def print_info(binlog_info):
    print('-'.ljust(90, '-'))
    k=''
    for key,val in binlog_info[0].items():
        k = k + key+' '.rjust(len(val)-len(key)+3, ' ')
    print(k)
    print('-'.ljust(90, '-'))
    for binlog in sorted(binlog_info,key=lambda i:i['binlogfile']):
        v = ''
        for key in binlog:
           v = v + binlog[key]+' '.rjust(3, ' ')
        print(v)
    print('-'.ljust(90, '-'))

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile timestamp.')
    parser.add_argument('--binlogfiles', help='binlog文件名,支持通配符*，?', required=True)
    parser.add_argument('--outputdir', help='输出文件目录', required=True)
    parser.add_argument('--debug', help='调试模式', default='N')
    args = parser.parse_args()
    return args


'''
   python3 binlog2parserlog.py --binlogfiles=../mysqlbinlog/*mysql-bin.00380* --outputdir=./log --debug=N
'''
if __name__ == '__main__':
    binlog_info = []
    warnings.filterwarnings("ignore")
    args = parse_param()
    for binlogfile in glob.glob(args.binlogfiles):
        parsing(binlog_info, binlogfile, args.outputdir, args.debug)
    print_info(binlog_info)
