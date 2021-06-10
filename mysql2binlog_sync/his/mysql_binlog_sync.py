#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/29 9:06
# @Author : 马飞
# @File : mysql2kafka_sync.py.py
# @Software: PyCharm

import json,datetime,time
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
     DeleteRowsEvent,
     UpdateRowsEvent,
     WriteRowsEvent
)

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
    "host"  : "10.2.39.18",
    "port"  : 3307,
    "user"  : "puppet",
    "passwd": "Puppet@123"
}

MYSQL_SYNC_SETTINGS = {
    "host"  : "10.2.39.18",
    "port"  : 3307,
    "user"  : "puppet",
    "passwd": "Puppet@123",
    "db"    : "test_repl"
}

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_db(config):
    return get_ds_mysql(config['host'],config['port'],config['db'],config['user'],config['passwd'])

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ins_header(event):
    v_ddl = 'insert into {0} ('.format(event['table'])
    for key in event['data']:
        v_ddl = v_ddl + '`{0}`'.format(key) + ','
    v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ins_values(event):
    v_tmp=''
    for key in event['data']:
        v_tmp=v_tmp+"'"+format_sql(str(event['data'][key]))+"',"
    return v_tmp[0:-1]

def gen_sql(event):
    if event['action']=='insert':
       sql=get_ins_header(event)+get_ins_values(event)
       print(event['data'])
       print('get_ins_header=',get_ins_header(event))
       print('get_ins_values=',get_ins_values(event))
       print('schema=',sql)

    elif event['action']=='update':
       pass
    elif event['action']=='delete':
       pass
    else:
       pass

def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream #only_schemas = ['test'],
    # only_events = [DeleteRowsEvent,UpdateRowsEvent, WriteRowsEvent]
    stream = BinLogStreamReader( connection_settings=MYSQL_SETTINGS,
                                 server_id   = 8,
                                 blocking    = True,
                                 #only_schemas= ['test'],
                                 #log_file='mysql-bin.000009',
                                 #log_pos='1679'
                                )

    schema='test'
    db=get_db(MYSQL_SYNC_SETTINGS)

    for binlogevent in stream:

        #try:
        #    print('event7=>',bytes.decode(binlogevent.schema))
        #    if bytes.decode(binlogevent.schema) == 'test':
        #print('------------------------------')
        #print('事件类型：', binlogevent.event_type)
        #binlogevent.dump()

        if binlogevent.event_type in (30, 31, 32):
            for row in binlogevent.rows:
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                if event['schema'] == schema:
                    if isinstance(binlogevent, DeleteRowsEvent):
                        event["action"] = "delete"
                        event["data"] = row["values"]
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        event["action"] = "update"
                        # event["data"]   = row["after_values"]
                        event["after_values"] = row["after_values"]
                        event["before_values"] = row["before_values"]
                    elif isinstance(binlogevent, WriteRowsEvent):
                        event["action"] = "insert"
                        event["data"] = row["values"]
                        print('gen_sql=', gen_sql(event))

                    print(get_event_name(binlogevent.event_type),json.dumps(event, cls=DateEncoder))
                    print(json.dumps(event))

        if binlogevent.event_type in (2,):
            event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
            print('event1=>',event)
            if 'create' in event['query'] or 'drop' in event['query'] \
                    or 'alter' in event['query'] or 'truncate' in event['query']:

                if event['schema'] == schema:
                    print('event2=>',get_event_name(binlogevent.event_type), event)
                    cr = db.cursor()
                    try:
                        print('exec:',binlogevent.query.lower())
                        cr.execute(binlogevent.query.lower())
                        db.commit()
                    except Exception as e:
                        print(e)

            # print(get_event_name(binlogevent.event_type), event)

        #except:
        #    pass


        '''
        try:
            for row in binlogevent.rows:

                event = {"schema": binlogevent.schema, "table": binlogevent.table}

                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]

                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event["data"] = row["after_values"]  # 注意这里不是values

                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]

                # print(json.dumps(event, cls=DateEncoder))
                print(json.dumps(event))
        except:
            pass
        '''

        '''
        for row in binlogevent.rows:

            event = {"schema": binlogevent.schema, "table": binlogevent.table}

            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event["data"] = row["values"]

            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event["data"] = row["after_values"]  # 注意这里不是values

            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event["data"] = row["values"]

            #print(json.dumps(event, cls=DateEncoder))
            print(json.dumps(event))
            # sys.stdout.flush()

        '''
    cr.close()
    stream.close()


if __name__ == "__main__":
    main()