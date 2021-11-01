#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/10/22 8:52
# @Author : ma.fei
# @File : mysql2doris_syncer.py.py
# @Software: PyCharm

import sys
import time
import pymysql
import re
import traceback
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import *
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)

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

DORIS_TAB_CONFIG = '''ENGINE=OLAP
    UNIQUE KEY($$PK_NAMES$$)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH($$PK_NAMES$$) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
)
'''

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def aes_decrypt(p_password, p_key):
    sql = """select aes_decrypt(unhex('{0}'),'{1}') as pass""".format(p_password, p_key[::-1])
    cur = META_DB.cursor()
    cur.execute(sql)
    rs = cur.fetchone()
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

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_db(MYSQL_SETTINGS):
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_by_ds(p_ds):
    conn = pymysql.connect(host=p_ds['ip'],
                           port=int(p_ds['port']),
                           user=p_ds['user'],
                           passwd=p_ds['password'],
                           charset='utf8',autocommit=True)
    return conn

def get_doris_db(p_ds,p_db):
    conn = pymysql.connect(host=p_ds['ip'],
                           port=int(p_ds['port']),
                           user=p_ds['user'],
                           passwd=p_ds['password'],
                           db=p_db
                          )
    return conn

def get_doris_table_defi(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    st = """SELECT  `column_name`,data_type
              FROM information_schema.columns
              WHERE table_schema='{}'
                AND table_name='{}'  ORDER BY ordinal_position""".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    st= 'create table `{}`.`{}` (\n '.format(event['schema'],event['table'])
    for i in rs:
        if i[1] == 'varchar':
           st =  st + ' `{}`  String,\n'.format(i[0])
        elif i[1] == 'timestamp':
           st = st + ' `{}`  datetime,\n'.format(i[0])
        else:
           st = st + '  `{}`  {},\n'.format(i[0],i[1])
    db.commit()
    cr.close()
    st = st[0:-2]+') \n' + cfg['doris_config']
    return st

def check_doris_tab_exists(cfg,event):
   db=cfg['db_doris']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema='{}' and table_name='{}'""".format(event['schema'],event['table'])
   cr.execute(sql)
   rs=cr.fetchone()
   db.commit()
   cr.close()
   return rs[0]

def check_tab_exists_pk(cfg,event):
   db = cfg['db_mysql']
   cr = db.cursor()
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   cr.close()
   return rs[0]

def get_table_pk_names(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' and column_key='PRI' order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col = v_col + '`{}`,'.format(i[0])
    cr.close()
    return v_col[0:-1]

def create_doris_table(cfg,event):
    db = cfg['db_doris']
    cr = db.cursor()
    if check_tab_exists_pk(cfg,event) >0:
        st = get_doris_table_defi(cfg,event)
        print('doris create table:',st.replace('$$PK_NAMES$$',get_table_pk_names(cfg,event)))
        cr.execute(st.replace('$$PK_NAMES$$',get_table_pk_names(cfg,event)))
        time.sleep(0.1)
        db.commit()
        cr.close()
        print('create doris table `{}` success!'.format(cfg['mysql_tab']))
    else:
        print('Table `{}` have no primary key!'.format(cfg['mysql_tab']))
        sys.exit(0)

def set_column(p_data,p_pk):
    v_set = ' set '
    for key in p_data:
        if p_data[key] is None:
           v_set = v_set + key + '=null,'
        else:
           if p_pk.count(key)==0:
              v_set = v_set + key + '=\''+ str(p_data[key]) + '\','
    return v_set[0:-1]

def get_ins_header(event):
    v_ddl = 'insert into {0}.{1} ('.format(event['schema'],event['table'])
    if event['action'] == 'insert':
        for key in event['data']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    elif event['action'] == 'update':
        for key in event['after_values']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ins_values(event):
    v_tmp=''
    if event['action'] == 'insert':
        for key in event['data']:
            if event['data'][key]==None:
               v_tmp=v_tmp+"null,"
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    elif  event['action'] == 'update':
        for key in event['after_values']:
            if event['after_values'][key]==None:
               v_tmp=v_tmp+"null,"
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['after_values'][key])) + "',"
    return v_tmp[0:-1]

def get_where(cfg,event):
    cols = get_table_pk_names( cfg,event)
    v_where = ' where '
    for key in event['data']:
        if check_tab_exists_pk( cfg,event) > 0:
            if key in cols:
                v_where = v_where + key + ' = \'' + str(event['data'][key]) + '\' and '
        else:
           v_where = v_where+ key+' = \''+str(event['data'][key]) + '\' and '
    return v_where[0:-5]

def gen_sql(cfg,event):
    if event['action'] in ('insert','update'):
        sql  = get_ins_header(event)+ ' values ('+get_ins_values(event)+');'
    elif event['action']=='delete':
        sql  = 'delete from {0}.{1} {2}'.format(event['schema'],event['table'],get_where(cfg,event))
    return sql

def gen_ddl_sql(p_ddl):
    if p_ddl.find('create table')>=0:
       return p_ddl
    else:
       return None

def get_file_and_pos(p_db):
    cur = p_db.cursor()
    cur.execute('show master status')
    ds = cur.fetchone()
    return ds

def process_batch(p_batch):
    pass

def doris_exec(cfg,batch,flag='N'):
    db = cfg ['db_doris']
    cr = db.cursor()
    for tab in batch:
        if flag =='F':
            print('doris_exec patch F:', tab)
            print('doris_exec patch data F:', batch[tab])
            for st in batch[tab]:
                if len(batch[tab]) % cfg['batch_size'] == 0:
                    cr.execute(st['sql'])
                    time.sleep(0.1)

        else:
            print('doris_exec patch N:', tab)
            print('doris_exec patch N data:', batch[tab])
            for st in batch[tab]:
                cr.execute(st['sql'])
                time.sleep(0.1)

def check_sync(cfg,event):
    res = False
    for o in cfg['mysql_tab'].split(','):
        schema,table = o.split('.')
        if event['schema'] == schema  and  event['table'] == table:
            res = True
    return res

def check_batch_exist_data(batch):
    for k in batch:
        if len(batch[k])>0:
           return True
    return False

def check_batch_full_data(batch,cfg):
    for k in batch:
        if len(batch[k]) % cfg['batch_size'] == 0:
           return True
    return False



'''
   检查点：
    1.事件缓存batch[tab]列表长度达到 batch_size 时
    2.非同步表的数据库行事件达到100个
    3.上一次执行后，缓存未满，达到超时时间                        
'''
def start_syncer(cfg):

    MYSQL_SETTINGS = {
        "host"   : cfg['mysql_ds']['ip'],
        "port"   : int(cfg['mysql_ds']['port']),
        "user"   : "canal2021",
        "passwd" : "canal@Hopson2018",
    }

    logging.info("MYSQL_SETTINGS=",MYSQL_SETTINGS)
    batch = {}
    row_event_count = 0

    for o in cfg['mysql_tab'].split(','):
        print('init...batch[`{}`]'.format(o))
        batch[o] = []

    try:
        stream = BinLogStreamReader(
            connection_settings = MYSQL_SETTINGS,
            only_events         = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id           = 9999,
            blocking            = True,
            resume_stream       = True,
            log_file            = cfg['binlogfile'],
            log_pos             = int(cfg['binlogpos']))

        print('Sync Configuration:')
        print('-------------------------------------------------------------')
        print('batch_size=',cfg['batch_size'])
        print('batch_timeout=',cfg['batch_timeout'])
        print('row_event_batch=',cfg['row_event_batch'])

        start_time = datetime.datetime.now()

        for binlogevent in stream:
            row_event_count = row_event_count + 1

            if binlogevent.event_type in (2,):
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    ddl = gen_ddl_sql(event['query'])
                    event['table'] = get_obj_name(event['query']).lower()
                    if check_sync(cfg,event) and ddl is not None:
                       print('ddl:', ddl)
                       if check_doris_tab_exists(cfg,event) == 0:
                          create_doris_table(cfg,event)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):

                for row in binlogevent.rows:

                    event = {"schema": binlogevent.schema.lower(), "table": binlogevent.table.lower()}
                    if check_sync(cfg, event):

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'delete','sql':sql})  # event.schema.event.table

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'insert','sql':sql})

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'insert','sql':sql})


                        if check_batch_full_data(batch,cfg):
                           print('check_batch_full_data...')
                           doris_exec(cfg, batch,'F')
                           for o in cfg['mysql_tab'].split(','):
                               if len(batch[o]) % cfg['batch_size'] == 0:
                                   batch[o] = []
                           start_time = datetime.datetime.now()
                           row_event_count = 0

            if get_seconds(start_time) >= cfg['batch_timeout'] :
                if check_batch_exist_data(batch):
                    print('check_batch_exist_data...timoeout:{},start_time={}'.format(get_seconds(start_time),start_time))
                    doris_exec(cfg, batch)
                    for o in cfg['mysql_tab'].split(','):
                         batch[o] = []
                    start_time = datetime.datetime.now()
                    row_event_count = 0

            if  row_event_count>0 and row_event_count % cfg['row_event_batch'] == 0:
                if check_batch_exist_data(batch):
                    print('check_batch_exist_data...row_event_count={}'.format(row_event_count))
                    doris_exec(cfg, batch)
                    for o in cfg['mysql_tab'].split(','):
                        batch[o] = []
                    start_time = datetime.datetime.now()
                    row_event_count = 0


    except Exception as e:
        traceback.print_exc()
    finally:
        stream.close()



'''
  1.support single db multi table
  2.supprt multi db multi table ,exaple:db1.tab1,db2.tab2
'''

if __name__ == "__main__":
    mysql_ds  =  get_ds_by_dsid(1)
    doris_ds  =  get_ds_by_dsid(185)
    mysql_tab = 'test.xs,test.xs2'.lower()
    doris_db  = 'test'
    db_mysql  =  get_db_by_ds(mysql_ds)
    db_doris  =  get_doris_db(doris_ds,doris_db)
    file,pos  =  get_file_and_pos(db_mysql)[0:2]

    config  = {
        'mysql_ds'        : mysql_ds,
        'doris_ds'        : doris_ds,
        'mysql_tab'       : mysql_tab,
        'doris_db'        : doris_db,
        'db_mysql'        : db_mysql,
        'db_doris'        : db_doris,
        'binlogfile'      : file,
        'binlogpos'       : pos,
        'doris_config'    : DORIS_TAB_CONFIG,
        'batch_size'      : 20,
        'batch_timeout'   : 6,
        'row_event_batch' : 100,
    }

    start_syncer(config)