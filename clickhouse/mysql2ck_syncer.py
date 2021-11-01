#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/10/22 8:52
# @Author : ma.fei
# @File : mysql2doris_syncer.py.py
# @Software: PyCharm

import pymysql
import re
import traceback
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import *
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)
from clickhouse_driver import Client


META_DS = {
    "db_ip"        : "10.2.39.18",
    "db_port"      : "3306",
    "db_user"      : "puppet",
    "db_pass"      : "Puppet@123",
    "db_service"   : "puppet",
    "db_charset"   : "utf8"
}

META_DB = pymysql.connect(
    host       = META_DS['db_ip'],
    port       = int(META_DS['db_port']),
    user       = META_DS['db_user'],
    passwd     = META_DS['db_pass'],
    db         = META_DS['db_service'],
    charset    = META_DS['db_charset'],
    autocommit = True,
    cursorclass= pymysql.cursors.DictCursor)


class ck_executer:

    def __init__(self,host,port,user,password,database,send_receive_timeout):
        self.client = Client(host=host, port=port, user=user, password=password,database=database, send_receive_timeout=send_receive_timeout)

    def create_table(self,tab):
        pass

    def check_exists(self,tab):
        pass

    def insert_data(self,st):
        print('insert_data=',st)
        self.client.execute(st)

    def close(self):
        self.client.disconnect()


def aes_decrypt(p_password, p_key):
    sql = """select aes_decrypt(unhex('{0}'),'{1}') as pass""".format(p_password, p_key[::-1])
    cur = META_DB.cursor()
    cur.execute(sql)
    rs = cur.fetchone()
    print('aes_decrypt rs=',rs)
    return str(rs['pass'], encoding="utf-8")

def get_ds_by_dsid(p_dsid):
    sql="""select cast(id as char) as dsid,
                  db_type,
                  db_desc,
                  ip,
                  port,
                  service,
                  user,
                  password,
                  status,
                  date_format(creation_date,'%Y-%m-%d %H:%i:%s') as creation_date,
                  creator,
                  date_format(last_update_date,'%Y-%m-%d %H:%i:%s') as last_update_date,
                  updator ,
                  db_env,
                  inst_type,
                  market_id,
                  proxy_status,
                  proxy_server,
                  id_ro
           from t_db_source where id={0}""".format(p_dsid)
    cur = META_DB.cursor()
    cur.execute(sql)
    ds = cur.fetchone()
    print('get_ds_by_dsid=',ds)
    ds['password'] = aes_decrypt(ds['password'],ds['user'])
    return ds

def get_obj_op(p_sql):
    if re.split(r'\s+', p_sql)[0].upper() in('CREATE','DROP') and re.split(r'\s+', p_sql)[1].upper() in('TABLE','INDEX','DATABASE'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('TRUNCATE'):
       return 'TRUNCATE_TABLE'
    if re.split(r'\s+', p_sql)[0].upper()== 'ALTER' and re.split(r'\s+', p_sql)[1].upper()=='TABLE' and  re.split(r'\s+', p_sql)[3].upper() in('ADD','DROP','MODIFY'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()+'_'+re.split(r'\s+', p_sql)[3].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('INSERT','UPDATE','DELETE') :
       return re.split(r'\s+', p_sql)[0].upper()

def get_obj_name(p_sql):
    if p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TABLE") > 0 \
        or p_sql.upper().count("TRUNCATE") > 0 and p_sql.upper().count("TABLE") > 0 \
         or p_sql.upper().count("ALTER") > 0 and p_sql.upper().count("TABLE") > 0 \
           or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("TABLE") > 0 \
             or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("DATABASE") > 0 \
                or  p_sql.upper().count("CREATE")>0 and p_sql.upper().count("VIEW")>0 \
                   or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("FUNCTION") > 0 \
                    or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("PROCEDURE") > 0 \
                      or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 \
                        or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TRIGGER") > 0  \
                           or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("DATABASE") > 0:

       if p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 and p_sql.upper().count("UNIQUE") > 0:
           obj = re.split(r'\s+', p_sql)[3].replace('`', '')
       else:
           obj=re.split(r'\s+', p_sql)[2].replace('`', '')

       if ('(') in obj:
           if obj.find('.')<0:
              return obj.split('(')[0]
           else:
              return obj.split('(')[0].split('.')[1]
       else:
           if obj.find('.') < 0:
              return obj
           else:
              return obj.split('.')[1]

    if get_obj_op(p_sql) in('INSERT','DELETE'):
         if re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`','').find('.')<0:
            return  re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`','')
         else:
            return re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`', '').split('.')[1]

    if get_obj_op(p_sql) in('UPDATE'):
        if re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`','').find('.')<0:
           return re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`','')
        else:
           return re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`', '').split('.')[1]

def get_db(MYSQL_SETTINGS):
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_by_ds(ds):
    conn = pymysql.connect(host=ds['ip'],
                           port=int(ds['port']),
                           user=ds['user'],
                           passwd=ds['password'],
                           db=ds['service'],
                           charset='utf8',autocommit=True)
    return conn

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ins_header(MYSQL_SETTINGS,event):
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

def check_tab_exists_pk(db_conn,db_name,tab_name):
   cr = db_conn.cursor()
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(db_name,tab_name)
   cr.execute(st)
   rs=cr.fetchone()
   cr.close()
   return rs[0]

def get_table_pk_names(db_conn,db_name,tab_name):
    cr = db_conn.cursor()
    v_col=[]
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' and column_key='PRI' order by ordinal_position
          """.format(db_name,tab_name)
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col.append(i[0])
    cr.close()
    return v_col

def get_where(MYSQL_SETTINGS,event,p_where):
    cols = get_table_pk_names( event["db"],MYSQL_SETTINGS['db'], event['table'])
    v_where = ' where '
    for key in p_where:
        if check_tab_exists_pk( event["db"],MYSQL_SETTINGS['db'], event['table']) > 0:
            if key in cols:
                v_where = v_where + key + ' = \'' + str(p_where[key]) + '\' and '
        else:
           v_where = v_where+ key+' = \''+str(p_where[key]) + '\' and '
    return v_where[0:-5]

def set_column(p_data,p_pk):
    v_set = ' set '
    for key in p_data:
        if p_data[key] is None:
           v_set = v_set + key + '=null,'
        else:
           if p_pk.count(key)==0:
              v_set = v_set + key + '=\''+ str(p_data[key]) + '\','
    return v_set[0:-1]

def gen_sql(MYSQL_SETTINGS,event):
    if event['action']=='insert':
        sql  = get_ins_header(MYSQL_SETTINGS,event)+ ' values ('+get_ins_values(event)+');'
    elif event['action']=='update':
        sql  = 'update {0}.{1} {2} {3};'.\
               format(MYSQL_SETTINGS['db'],event['table'],set_column(event['after_values'],event['pk']),get_where(MYSQL_SETTINGS,event,event['before_values']))
    elif event['action']=='delete':
        sql  = 'delete from {0}.{1} {2};'.format(MYSQL_SETTINGS['db'],event['table'],get_where(MYSQL_SETTINGS,event,event['data']))
    else:
       pass
    return sql

def gen_ddl_sql(p_ddl):
    if p_ddl.find('create table')>=0:
       tab = get_obj_name(p_ddl)
       rsql = 'drop table {};'.format(tab)
       return rsql
    return p_ddl

def get_file_and_pos(p_ds):
    db = get_db_by_ds(p_ds)
    cur = db.cursor()
    cur.execute('show master status')
    ds = cur.fetchone()
    print('get_file_and_pos=', ds)
    return ds

def start_syncer(p_ds,p_file,p_db,p_tab,p_start_pos,ck_server):

    MYSQL_SETTINGS = {
        "host": p_ds['ip'],
        "port": int(p_ds['port']),
        "user": "canal2021",
        "passwd": "canal@Hopson2018",
        "db": p_db
    }
    logging.info("MYSQL_SETTINGS=",MYSQL_SETTINGS)

    try:
        stream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            only_events   = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id     = 9999,
            blocking      = True,
            resume_stream = True,
            log_file      = p_file,
            log_pos       = int(p_start_pos))

        schema = MYSQL_SETTINGS['db']
        for binlogevent in stream:
            if binlogevent.event_type in (2,):
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    if event['schema'] == schema:
                        sql = gen_ddl_sql(binlogevent.query.lower()+';')
                        tab = get_obj_name(binlogevent.query.lower() + ';')
                        if  p_tab == tab:
                            print('sync:', sql)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema, "table": binlogevent.table}

                    if event['schema'] == schema and event['table'] == p_tab:
                        event["db"] = get_db(MYSQL_SETTINGS)
                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            sql = gen_sql(MYSQL_SETTINGS,event)
                            ck_server.insert_data(sql)

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            event['pk'] = get_table_pk_names(db, MYSQL_SETTINGS['db'], event['table'])
                            sql = gen_sql(MYSQL_SETTINGS,event)
                            ck_server.insert_data(sql)

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            sql = gen_sql(MYSQL_SETTINGS,event)
                            ck_server.insert_data(sql)


    except Exception as e:
        traceback.print_exc()
    finally:
        stream.close()

if __name__ == "__main__":
    sour_ds  = get_ds_by_dsid(1)
    file,pos = get_file_and_pos(sour_ds)[0:2]
    db       = 'test'
    tab      = 'business'
    ck       = ck_executer('10.2.39.21', 9000, 'default', '6lYaUiFi', 'test', 5)
    start_syncer(sour_ds,file,db,tab,pos,ck)