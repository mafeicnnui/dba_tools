#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : ma.fei
# @File : mysql2kafka_sync.py.py
# pip install mysql-replication
# @Software: PyCharm

import traceback
import json,sys
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import *
from pymysqlreplication.row_event import (
     DeleteRowsEvent,
     UpdateRowsEvent,
     WriteRowsEvent,
)

# MYSQL_SETTINGS = {
#     "host"  : "10.2.39.240",
#     "port"  : 3306,
#     "user"  : "canal",
#     "passwd": "canal@Hopson2020",
#     "db"    : "test"
# }

MYSQL_SETTINGS = {
    "host"  : "bj-cynosdbmysql-grp-3k142zlc.sql.tencentcdb.com",
    "port"  : 29333,
    "user"  : "root",
    "passwd": "Dev21@block2022",
    "db"    : "test"
}

class DateEncoder(json.JSONEncoder):
    '''
      自定义类，解决报错：
      TypeError: Object of type 'datetime' is not JSON serializable
    '''
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)

def get_event_name(event):
    if event==2:
       return 'QueryEvent'.ljust(20,' ')+':'
    elif event==30:
       return 'WriteRowsEvent'.ljust(20,' ')+':'
    elif event==31:
       return 'UpdateRowsEvent'.ljust(20,' ')+':'
    elif event==32:
       return 'DeleteRowsEvent'.ljust(20,' ')+':'
    else:
       return ''.ljust(30,' ')

def get_master_pos(file=None,pos=None):
    db = get_db(MYSQL_SETTINGS)
    cr = db.cursor()
    cr.execute('show master status')
    rs=cr.fetchone()
    if file is not None and pos is not None:
        return file,pos
    else:
        return rs[0],rs[1]

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=True)
    return conn

def get_db(config):
    return get_ds_mysql(config['host'],config['port'],config['db'],config['user'],config['passwd'])

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ins_header(event):
    v_ddl = 'insert into {0}.{1} ('.format(MYSQL_SETTINGS['db'],event['table'])
    for key in event['data']:
        v_ddl = v_ddl + '`{0}`'.format(key) + ','
    v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ins_values(event):
    v_tmp=''
    for key in event['data']:
        if event['data'][key]==None:
           v_tmp=v_tmp+"null,"
        else:
           v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    return v_tmp[0:-1]

def get_where(p_where):
    v_where = ' where '
    for key in p_where:
        v_where = v_where+ key+' = \''+str(p_where[key]) + '\' and '
    return v_where[0:-5]

def set_column(p_data):
    v_set = ' set '
    for key in p_data:
        v_set = v_set + key + '=\''+ str(p_data[key]) + '\','
    return v_set[0:-1]

def gen_sql(event):
    if event['action']=='insert':
        sql  = get_ins_header(event)+ ' values ('+get_ins_values(event)+')'
        rsql = "delete from {0}.{1} {2}".format(MYSQL_SETTINGS['db'],event['table'],get_where(event['data']))
    elif event['action']=='update':
        sql  = 'update {0}.{1} {2} {3}'.\
               format(MYSQL_SETTINGS['db'],event['table'],set_column(event['after_values']),get_where(event['before_values']))
        rsql = 'update {0}.{1} {2} {3}'.\
               format(MYSQL_SETTINGS['db'],event['table'],set_column(event['before_values']),get_where(event['after_values']))
    elif event['action']=='delete':
        sql  = 'delete from {0}.{1} {2}'.format(MYSQL_SETTINGS['db'],event['table'],get_where(event['data']))
        rsql = get_ins_header(event)+ ' values ('+get_ins_values(event)+')'
    else:
       pass
    return sql,rsql

def get_binlog(p_file,p_pos,p_db,p_tab):
    try:
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    only_events=[QueryEvent,DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                    # only_tables=[p_db],
                                    # only_schemas=[p_tab],
                                    server_id=9999,
                                    blocking=True,
                                    resume_stream=True,
                                    log_file=p_file,
                                    log_pos=p_pos
                                    )


        for binlogevent in stream:
            if binlogevent is not None:
                # if isinstance(binlogevent, QueryEvent):
                #     if bytes.decode(binlogevent.schema) == "hopson_hft":
                #         # print('binlogevent=', binlogevent.dump())
                #         # print('stream.log_file,stream.log_pos=', stream.log_file, stream.log_pos)
                #         event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                #         print(event)

                if isinstance(binlogevent, DeleteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, WriteRowsEvent):
                    for row in binlogevent.rows:
                        event = {"schema": binlogevent.schema, "table": binlogevent.table}
                        # print('row=',row,binlogevent)
                        # print('event=',event)
                        # print(event['schema'],p_db,event['schema'] == p_db,event['table'] ,p_tab,event['table'] == p_tab)
                        if event['schema'] == p_db  and event['table'] == p_tab :
                            if isinstance(binlogevent, DeleteRowsEvent):
                                event["action"] = "delete"
                                event["data"] = row["values"]
                                sql,rsql = gen_sql(event)
                                print('EVENT:',event)
                                print('DELETE:',sql)


                            elif isinstance(binlogevent, UpdateRowsEvent):
                                event["action"] = "update"
                                event["after_values"] = row["after_values"]
                                event["before_values"] = row["before_values"]
                                sql, rsql = gen_sql(event)
                                print('EVENT:', event)
                                print('UPDATE:', sql)

                            elif isinstance(binlogevent, WriteRowsEvent):
                                event["action"] = "insert"
                                event["data"] = row["values"]
                                sql,rsql = gen_sql(event)
                                print('EVENT:', event)
                                print('INSERT:', sql)


    except Exception as e:
        traceback.print_exc()
    finally:
        stream.close()

def exec_sql(p_db,p_sql):
    db = get_db(MYSQL_SETTINGS)
    cr = db.cursor()

    # 1.获取 file,start_pos
    cr.execute('FLUSH /*!40101 LOCAL */ TABLES')
    cr.execute('FLUSH TABLES WITH READ LOCK')
    cr.execute('show master status')
    rs=cr.fetchone()
    file = rs[0]
    start_pos= rs[1]
    cr.execute('unlock tables')

    # 2.执行SQL语句
    cr.execute('use {}'.format(p_db))
    cr.execute(p_sql)

    # 3.获取 stop_position
    cr.execute('show master status')
    rs = cr.fetchone()
    stop_pos = rs[1]

    return file,start_pos,stop_pos

def main():
    file = 'mysql-bin.001001'
    pos  = 11033
    db='test'
    tab='xs'
    st = get_binlog(file,pos, db,tab)
    print('st=',st)

if __name__ == "__main__":
     main()