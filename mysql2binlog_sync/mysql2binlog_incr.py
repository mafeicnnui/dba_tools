#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : ma.fei
# @File : mysql2kafka_sync.py.py
# pip install mysql-replication
# @Software: PyCharm

import json,datetime,time,sys
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
     DeleteRowsEvent,
     UpdateRowsEvent,
     WriteRowsEvent
)

from pymysqlreplication.event import *

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


MYSQL_SETTINGS = {
    "host"  : "10.2.39.240",
    "port"  : 3306,
    "user"  : "canal",
    "passwd": "canal@Hopson2020",
    "db"    : "test"
}

MYSQL_SYNC_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  : 3306,
    "user"  : "puppet",
    "passwd": "Puppet@123",
    "db"    : "sync"
}

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
    v_ddl = 'insert into {0}.{1} ('.format(MYSQL_SYNC_SETTINGS['db'],event['table'])
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
        sql = get_ins_header(event)+ ' values ('+get_ins_values(event)+')'
    elif event['action']=='update':
        sql = 'update {0}.{1} {2} {3}'.\
             format(MYSQL_SYNC_SETTINGS['db'],event['table'],set_column(event['after_values']),get_where(event['before_values']))
    elif event['action']=='delete':
        sql = 'delete from {0}.{1} {2}'.\
             format(MYSQL_SYNC_SETTINGS['db'],event['table'],get_where(event['data']))
    else:
       pass
    return sql


def get_binlog(p_file,p_db,p_tab,p_start_pos,p_end_pos):
    try:
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    only_schemas=[p_db],
                                    only_tables=[p_tab],
                                    only_events=(QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
                                    server_id=8,
                                    blocking=True,
                                    resume_stream=True,
                                    log_file=p_file,
                                    log_pos=int(p_start_pos)
                                    )

        schema = MYSQL_SETTINGS['db']

        for binlogevent in stream:
            print('stream.log_file,stream.log_pos=', stream.log_file, stream.log_pos)

            if stream.log_pos >= p_end_pos:
                stream.close()
                sys.exit(0)

            if binlogevent.event_type in (2,):

                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}

                if 'create' in event['query'] or 'drop' in event['query'] \
                        or 'alter' in event['query'] or 'truncate' in event['query']:

                    if event['schema'] == schema:
                        print(binlogevent.query.lower())


            if binlogevent.event_type in (30, 31, 32):
                # print('binlogevent=',binlogevent.dump())
                print('binlogevent.packet.log_pos=', binlogevent.packet.log_pos)

                for row in binlogevent.rows:
                    # print('binlogevent.packet.log_pos=', binlogevent.packet.log_pos)
                    # print('stream.log_file,stream.log_pos=', stream.log_file, stream.log_pos)

                    event = {"schema": binlogevent.schema, "table": binlogevent.table}

                    if event['schema'] == schema:

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            print('delete=', gen_sql(event), event)

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            print('update=', gen_sql(event), event)

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            print('insert=', gen_sql(event), event)

                        # print(get_event_name(binlogevent.event_type),json.dumps(event, cls=DateEncoder))
                        # print(json.dumps(event))



    except Exception as e:
        print(str(e))
    finally:
        stream.close()

def exec_sql(p_db,p_tab,p_sql):
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
    print('use {}'.format(p_db))
    cr.execute('use {}'.format(p_db))
    print('lock tables {} write'.format(p_tab))
    cr.execute('lock tables {} write'.format(p_tab))
    print(p_sql)
    cr.execute(p_sql)

    # 3.获取 stop_position
    cr.execute('show master status')
    rs = cr.fetchone()
    stop_pos = rs[1]
    cr.execute('unlock tables')

    return file,start_pos,stop_pos



def main():
    db = 'test'
    tab ='xs'
    sql = """update xs set `name`='liuluoquo2' where xh=2"""
    file,start_pos,stop_pos = exec_sql(db,tab,sql)
    print('parameter:',db,tab,file,start_pos,stop_pos)

    get_binlog(file, db, tab, start_pos, stop_pos)

if __name__ == "__main__":
    main()