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
    "host"  : "10.2.39.40",
    #"host"  : "rm-2ze9y75wip0929gy86o.mysql.rds.aliyuncs.com",
    "port"  : 3306,
    "user"  : "canal2021",
    "passwd": "canal@Hopson2018",
    "db"    : ""
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

def get_binlog(p_file,p_db,p_tab,p_start_pos):
    #rollback_statments = []
    try:
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    only_events=[QueryEvent,DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                    only_tables=["xs"],
                                    only_schemas=["test"],
                                    server_id=100,
                                    blocking=True,
                                    resume_stream=True,
                                   # log_file=p_file,
                                   # log_pos=int(p_start_pos)
                                    )

        #schema = MYSQL_SETTINGS['db']

        for binlogevent in stream:
            #if binlogevent is not None:
                if isinstance(binlogevent, QueryEvent):
                    if bytes.decode(binlogevent.schema) == "test":
                        print('binlogevent=', binlogevent.dump())
                        print('stream.log_file,stream.log_pos=', stream.log_file, stream.log_pos)
                        event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                        print(event)
                else:
                    if binlogevent is not None:
                       print('binlogevent=', binlogevent.dump())
            # print('binlogevent=',binlogevent.dump())
            #print('binlogeven=', binlogevent.event_type,binlogevent.packet.log_pos)
            #
            # if isinstance(binlogevent, QueryEvent):
            #     event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
            #     if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
            #         if event['schema'] == schema:
            #             print(binlogevent.query.lower())
            #
            # # if binlogevent.event_type in (30, 31, 32):
            # if isinstance(binlogevent, DeleteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, WriteRowsEvent):
            #     for row in binlogevent.rows:
            #         event = {"schema": binlogevent.schema, "table": binlogevent.table}
            #
            #         #if event['schema'] == schema:
            #         if isinstance(binlogevent, DeleteRowsEvent):
            #             event["action"] = "delete"
            #             event["data"] = row["values"]
            #             sql,rsql = gen_sql(event)
            #             print('Execute :', sql)
            #             print('Rollback :', rsql)
            #             rollback_statments.append(rsql)
            #
            #         elif isinstance(binlogevent, UpdateRowsEvent):
            #             event["action"] = "update"
            #             event["after_values"] = row["after_values"]
            #             event["before_values"] = row["before_values"]
            #             sql, rsql = gen_sql(event)
            #             print('Execute :', sql)
            #             print('Rollback :', rsql)
            #             rollback_statments.append(rsql)
            #
            #         elif isinstance(binlogevent, WriteRowsEvent):
            #             event["action"] = "insert"
            #             event["data"] = row["values"]
            #             sql,rsql = gen_sql(event)
            #             print('Execute :',sql)
            #             print('Rollback :',rsql)
            #             # print(get_event_name(binlogevent.event_type),json.dumps(event, cls=DateEncoder))
            #             # print(json.dumps(event))
            #             rollback_statments.append(rsql)

            # if  stream.log_pos >= p_end_pos:
            #     print('rollback_statements:', rollback_statments[::-1])
            #     stream.close()
            #     # sys.exit(0)
            #     break
            #
            # if stream.log_pos + 31 == p_end_pos or stream.log_pos >=p_end_pos:
            #     print('rollback_statements:', rollback_statments[::-1])
            #     stream.close()
            #     #sys.exit(0)
            #     break

        # print('xxxxxxxxxxxxx')
        # return rollback_statments[::-1]

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
    file = 'mysql-bin.000168'
    db='test'
    tab='xs',
    start_pos=21894106
    st = get_binlog(file, db,tab, start_pos)
    print('st=',st)

if __name__ == "__main__":
     main()