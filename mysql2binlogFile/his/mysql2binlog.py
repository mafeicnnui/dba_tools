#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/8/9 9:31
# @Author : 马飞
# @File : mysql2binlogFile.py
# @Software: PyCharm
import re

def read_files(file_name):
    f = open(file_name, 'r')
    lines = f.readlines()
    f.close()
    return  lines

def main():
    lines=read_files('mysqlbinlog.000001')
    v=''
    for line in lines:
       if "SET @@session" not in line \
           and not len(re.findall(r'^/\*!',line,re.M))>0 \
              and not len(re.findall(r'^# Warning',line,re.M))>0 \
                and not len(re.findall(r'^# End of log file', line, re.M)) > 0 \
                   and not len(re.findall(r'^#\d{2,6}',line,re.M))>0 \
                     and 'ROLLBACK' not in line \
                        and 'COMMIT' not in line \
                            and 'BEGIN' not in line \
                               and 'DELIMITER' not in line \
                                  and len(line)>1:
                                   v=v+line.replace('\n','')+'\n'

    #parse row
    row=1

    #最近一次解析内容
    n_pre_pos_begin = None
    v_pre_content   = ''
    v_pre_database  = ''
    v_pre_timestamp = ''


    #当前正在解析内容
    n_cur_pos_begin = None
    v_cur_content   = ''
    v_cur_database  = ''
    v_cur_timestamp = ''

    #解析后将数据写入字典中
    d_binlog        = {}

    # 开始解析
    for line in v.replace('# at ', '').split('\n'):
        print(line)

    #开始解析
    for line in v.replace('# at ','').split('\n'):
        if len(re.findall(r'^\d{1,20}$', line, re.M)) > 0 :
            #output dict
            v_cur_database  = get_db(v_cur_content.replace('/*!*/', ''))
            v_cur_timestamp = get_timestamp(v_cur_content.replace('/*!*/', ''))

            d_binlog[row] = {
                'n_pre_pos_begin': n_pre_pos_begin,
                'v_pre_database' : v_pre_database,
                'v_pre_timestamp': v_pre_timestamp,
                'n_cur_pos_begin': n_cur_pos_begin,
                'v_cur_database' : v_cur_database,
                'v_cur_timestamp': v_cur_timestamp,
                'v_cur_content'  : v_cur_content.replace('/*!*/', '')
            }
            row = row + 1

            #get new val
            n_pre_pos_begin = n_cur_pos_begin
            v_pre_database  = v_cur_database
            v_pre_timestamp = v_cur_timestamp
            n_cur_pos_begin = int(line)
            v_cur_content   = ''

        elif len(re.findall(r'^\d{1,20}$', line, re.M)) == 0 :
            v_cur_content   = v_cur_content+line+'\n'

    #处理字典
    for d in d_binlog:
        if d_binlog[d]['v_cur_database']=='':
           d_binlog[d]['v_cur_database']=d_binlog[d]['v_pre_database']
        if d_binlog[d]['v_cur_timestamp']=='':
           d_binlog[d]['v_cur_timestamp']=d_binlog[d]['v_pre_timestamp']


    # 输出字典
    for d in d_binlog:
        print('d_binlog['+str(d)+']=',d_binlog[d])


def get_db(statement):
    for v in statement.split(';'):
        if len(re.findall(r'^use ', v, re.M)) > 0:
           return v.split(' ')[1]
        elif ('DELETE') in v :
           return v.replace('### ','').split(' ')[2].split('.')[0].replace('`','')
        elif ('UPDATE') in v :
           return v.replace('### ', '').split(' ')[1].split('.')[0].replace('`','')
        elif ('INSERT') in v :
           return v.replace('### ', '').split(' ')[2].split('.')[0].replace('`','')
        else:
           return ''

def get_timestamp(statement):
    for v in statement.split(';'):
        if len(re.findall(r'SET TIMESTAMP=', v.replace('\n',''), re.M)) > 0:
           print(v.split('SET TIMESTAMP=')[1].replace(';',''))
           return v.split('SET TIMESTAMP=')[1].replace(';','')
        else:
           return ''

if __name__ == "__main__":
   main()
