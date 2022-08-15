#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/8/3 14:00
# @Author : ma.fei
# @File : binlog2db.py
# @Software: PyCharm

import os
import re
import sys
import json
import pymysql
import warnings
import datetime
import argparse

def get_db():
    cfg = read_json('binlog2parser.json')
    print(cfg)
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'],
                           autocommit = True,
                           cursorclass = pymysql.cursors.DictCursor)
    return conn

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_schema(line):
    t = line.split(' ')[-1]
    return t.split('.')[0].replace('`','')

def get_table(line):
    t = line.split(' ')[-1]
    return t.split('.')[1].replace('`', '').replace('\n','')

def get_type(line):
    if line.find(' INSERT INTO `') == 0 :
        return  'insert'
    elif line.find(' UPDATE `') == 0 :
        return 'update'
    elif line.find(' DELETE FROM `') == 0:
        return 'delete'
    else:
        return None

def get_column_mapping(db,schema,table):
    cr=db.cursor()
    st="SELECT ordinal_position,column_name FROM information_schema.columns WHERE table_schema='{}' AND table_name='{}'  ORDER BY ordinal_position"
    cr.execute(st.format(schema,table))
    rs=cr.fetchall()
    return rs

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_rows(p_log):
    c = os.popen('cat {} | wc -l'.format(p_log))
    return int(c.read())

def is_number(v):
    try:
        float(v)
        return True
    except:
        return False

def get_tab_pk_name(db,schema,table):
    cr = db.cursor()
    st = """select column_name 
                from information_schema.columns
                where table_schema='{}'
                  and table_name='{}' and column_key='PRI' order by ordinal_position
            """.format(schema,table)
    cr.execute(st)
    rs = cr.fetchall()
    v = ''
    for i in list(rs):
        v = v + i['column_name'] + ','
    db.commit()
    cr.close()
    return v[0:-1]
    pass

def parse_log(p_start_time = None,
              p_stop_time =None,
              p_start_pos = None,
              p_stop_pos = None,
              p_schema = None,
              p_binlogfile= None,
              p_debug = None):
    vv = ''
    cmd = ''
    if p_start_time is not None:
        vv = vv + ' --start-datetime={}'.format(p_start_time)

    if p_stop_time is not None:
        vv = vv + ' --stop-datetime={}'.format(p_stop_time)

    if p_start_pos is not None:
        vv = vv + ' --start-position={}'.format(p_start_pos)

    if p_stop_pos is not None:
        vv = vv + ' --stop-position={}'.format(p_stop_pos)

    if sys.platform == 'win32':
       if os.system('where mysqlbinlog') == 0:
          if  p_schema is None:
              cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} {} | find '###' | sed  's/###//g' > {} ".\
                    format(vv,p_binlogfile, p_binlogfile + '.log')
          else:
              cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} --database={} {} | find '###' | sed  's/###//g' > {} ". \
                  format(vv, p_schema, p_binlogfile, p_binlogfile + '.log')
       else:
          print('mysqlbinlog not found!')
          sys.exit(0)

    else:
       if os.system('which mysqlbinlog &>/dev/null') == 0:
          if p_schema is None:
              cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} {} | grep '###' | sed  's/###//g' > {}".\
                    format(vv,p_binlogfile,p_binlogfile+'.log')
          else:
              cmd = "mysqlbinlog --no-defaults --skip-gtids --base64-output=decode-rows -vv {} --database={} {} | grep '###' | sed  's/###//g' > {}".\
                    format(vv,p_schema,p_binlogfile,p_binlogfile+'.log')
       else:
          print('mysqlbinlog not found!')
          sys.exit(0)

    print('Parsing binlogfile `{}` ...'.format(p_binlogfile))
    if p_debug == 'Y':
       print(cmd)
    if os.system(cmd) == 0:
       print('Binlogfile `{}` resolve success!'.format(p_binlogfile))
       return p_binlogfile+'.log'
    else:
       print('Resolve binlogfile failure!')
       return None

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'").replace('"','\\"')

def process_value(v):
    if v[0] == "'" and v[-1] == "'":
       return """{}""".format(format_sql(v[1:-1]))
    elif is_number(v):
       try:
           if float(v) == int(float(v)):
              return int(v)
       except:
          return float(v)
    else:
       return v


def parse_col_name_value(event):
    cols = {}
    if event['type'] == 'insert':
       t = event['sql'].split(' SET ')[1][0:-1]
       for i in t.split(' , '):
           cols[i.split('=')[0]] = process_value(i.split('=')[1].strip())
       return cols

    if event['type'] == 'update':
       o = event['sql'].split(' WHERE ')[1].split(' SET ')[0]
       n = event['sql'].split(' WHERE ')[1].split(' SET ')[1]
       cols['old_values_raw'] = o
       cols['new_values_raw'] = n
       cols['old_values'] = {}
       cols['new_values'] = {}
       for i in o.split(' , '):
           cols['old_values'][i.split('=')[0]] = process_value(i.split('=')[1].strip())
       for i in n.split(' , '):
           cols['new_values'][i.split('=')[0]] = process_value(i.split('=')[1].strip())
       return cols

    if event['type'] == 'delete':
        t = event['sql'].split(' WHERE ')[1][0:-1]
        for i in t.split(' AND '):
            cols[i.split('=')[0]] = process_value(i.split('=')[1].strip())
        return cols

def replace_column(p_log):
    for o in p_log['columns']:
        p_log['sql'] = p_log['sql'].replace('@'+str(o['ordinal_position'])+'=',o['column_name']+'=')
    p_log['sql'] = p_log['sql'].replace('\n', '')
    p_log['sql'] = re.sub('\s+', ' ', p_log['sql'])
    col = parse_col_name_value(p_log)
    if p_log['type'] == 'insert':
        cols = ''
        vals = ''
        for k, v in col.items():
            cols = cols + '`{}`,'.format(k)
            if v=='NULL':
                vals = vals + "{},".format(v)
            elif isinstance(v,int) or isinstance(v,float):
                vals = vals + "{},".format(v)
            else:
                vals = vals + "'{}',".format(v)
        p_log['statement'] = 'insert into `{}`.`{}`({}) values ({})'.format(p_log['db'], p_log['table'], cols[0:-1],vals[0:-1])

    if p_log['type'] == 'delete':
       p_log['statement'] =  p_log['sql'].replace(',',' AND ')

    if p_log['type'] == 'update':
        vvv = ''
        val = ''
        if p_log.get('pkn') == '':
            for k, v in col['old_values'].items():
                vvv = vvv + '{} = {} and '.format(k, v)
        else:
            for k, v in col['old_values'].items():
                if p_log.get('pkn').count(k) > 0:
                    vvv = vvv + '{} = {} and '.format(k,  v)

            for k, v in col['new_values'].items():
                if v == 'NULL':
                    val = val + """{}={},""".format(k,v)
                elif isinstance(v, int) or isinstance(v, float):
                    val = val + """{}={},""".format(k,v)
                else:
                    val = val + """{}='{}',""".format(k, v)

        p_log['statement'] = 'update `{}`.`{}` set {} where {}'.\
                             format(p_log['db'],
                                    p_log['table'],
                                    val[0:-1],
                                    vvv[0:-5])

    p_log = gen_rollback(p_log)
    return p_log

def gen_rollback(event):
    col = parse_col_name_value(event)
    if event['type'] == 'insert':
       vvv  = ''
       if event.get('pkn') == '':
           for k, v in col.items():
               if v == 'NULL':
                   vvv = vvv + """{} is {} and """.format(k, v)
               elif isinstance(v, int) or isinstance(v, float):
                   vvv = vvv + """{}={} and """.format(k, v)
               else:
                   vvv = vvv + """{}='{}' and """.format(k, v)
       else:
           for k,v in col.items():
              if event.get('pkn').count(k)>0:
                  if v == 'NULL':
                      vvv = vvv + """{} is {} and """.format(k, v)
                  elif isinstance(v, int) or isinstance(v, float):
                      vvv = vvv + """{}={} and """.format(k, v)
                  else:
                      vvv = vvv + """{} = {} and """.format(k,v)
       event['rollback'] = 'delete from `{}`.`{}` where {}'.format(event['db'],event['table'],vvv[0:-5])
       return event

    if event['type'] == 'update':
        vvv = ''
        if event.get('pkn') == '':
            for k, v in col['new_values'].items():
                if v == 'NULL':
                    vvv = vvv + """{}={} and """.format(k, v)
                elif isinstance(v, int) or isinstance(v, float):
                    vvv = vvv + """{}={} and """.format(k, v)
                else:
                    vvv = vvv + """{}='{}' and """.format(k, v)

        else:
            for k, v in col['new_values'].items():
                if event.get('pkn').count(k) > 0:
                    if v == 'NULL':
                        vvv = vvv + """{} is {} and """.format(k, v)
                    elif isinstance(v, int) or isinstance(v, float):
                        vvv = vvv + """{}={} and """.format(k, v)
                    else:
                        vvv = vvv + """{}='{}' and """.format(k, v)
        vals=''
        for k, v in col['old_values'].items():
            if v == 'NULL':
                vals = vals + """{}={},""".format(k, v)
            elif isinstance(v, int) or isinstance(v, float):
                vals = vals + """{}={},""".format(k, v)
            else:
                vals = vals + """{}='{}',""".format(k, v)


        event['rollback'] = 'update `{}`.`{}` set {} where {}'.\
                             format(event['db'],
                                    event['table'],
                                    vals[0:-1],
                                    vvv[0:-5])
        return event

    if event['type'] == 'delete':
        cols = ''
        vals = ''
        for k, v in col.items():
            cols = cols + '`{}`,'.format(k)
            if v == 'NULL':
                vals = vals + "{},".format(v)
            elif isinstance(v, int) or isinstance(v, float):
                vals = vals + "{},".format(v)
            else:
                vals = vals + "'{}',".format(v)
        event['rollback'] = 'insert into `{}`.`{}`({}) values ({})'.\
                             format(event['db'],
                                    event['table'],
                                    cols[0:-1],format_sql(vals[0:-1]))
        return event
    return None

def parsing(p_start_time = None,
            p_stop_time = None,
            p_start_pos = None,
            p_stop_pos = None,
            p_schema = None,
            p_table = None,
            p_binlogfile = None,
            p_rollback = 'N',
            p_max_rows = None,
            p_debug = 'N'):
    start_time = datetime.datetime.now()
    log = parse_log(p_start_time,p_stop_time,p_start_pos, p_stop_pos, p_schema,p_binlogfile,p_debug)
    if log is None:
       print('Resolve binlog error!')
       sys.exit(0)

    rows= get_rows(log)
    if p_max_rows is not None:
        rows = int(p_max_rows)
    print('logfile {} , total rows :{}'.format(log,rows))
    pattern = re.compile(r'(\/\*.+\*\/)')
    contents=[]
    temp = {}
    row = 0
    with open(log,encoding='utf-8') as file:
       for line in file:
           row = row + 1
           print('\rProcessing sql : {}/{} , progress : {}%'.format(row,rows,round(round(row/rows,4)*100,4)),end='')
           if line.find(' INSERT INTO `') ==0 or line.find(' UPDATE `') == 0 or line.find(' DELETE FROM `') == 0:
               if temp.get('sql') is not None:
                  temp['sql'] = temp['sql'][0:-2]
                  if p_schema is not None and p_table is not None:
                     if temp['db'] == p_schema and temp['table'] == p_table:
                        contents.append(replace_column(temp))
                  elif p_schema is not None and p_table is None:
                     if temp['db'] == p_schema :
                        contents.append(replace_column(temp))
                  elif p_schema is None and p_table is not None:
                     if temp['table'] == p_table :
                        contents.append(replace_column(temp))
                  else:
                      contents.append(replace_column(temp))
               temp = {}
               temp['type']  = get_type(line)
               temp['db']    = get_schema(line)
               temp['table'] = get_table(line)
               temp['sql']   = line
               temp['columns'] = get_column_mapping(get_db(),temp['db'],temp['table'])
               temp['pkn']  = get_tab_pk_name(get_db(),temp['db'],temp['table'])
               continue
           else:
               if line.find(' SET') == 0:
                  temp['sql'] = temp['sql'][0:-2] + line
               elif line.find(' WHERE') == 0:
                  temp['sql'] = temp['sql'] + line
               else:
                  if pattern.findall(line) != []:
                     pre = re.sub(pattern, '', line)
                     temp['sql'] = temp['sql']+pre[0:-1] + ',\n'
                  else:
                     temp['sql'] = temp['sql']+line[0:-1]+',\n'

           if p_max_rows is not None:
               if row>int(p_max_rows):
                  break

    if p_table is not None:
       print('\nTable `{}` total {} rows.'.format(p_table,len(contents)))
    else:
       print('\nTotal {} rows.'.format(len(contents)))

    print('Wrtie sql file :`{}`'.format(log.replace('.log','.sql')))
    with open(log.replace('.log','.sql'), 'w', encoding="utf-8") as f:
       for i in contents:
            f.write(i['statement'].strip()+';\n')

    if p_rollback == 'Y':
       print('Write rollback log file : `{}`...'.format(log.replace('.log', '.rollback.sql')))
       with open(log.replace('.log', '.rollback.sql'), 'w', encoding="utf-8") as f:
           for i in contents[::-1]:
               f.write(i['rollback'].strip() + ';\n')

    print('Elapse time:{}s'.format(get_seconds(start_time)))

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile.')
    parser.add_argument('--start_time', help='开始时间', default=None)
    parser.add_argument('--stop_time', help='停止时间', default=None)
    parser.add_argument('--start_pos', help='开始位置', default=None)
    parser.add_argument('--stop_pos', help='停止位置', default=None)
    parser.add_argument('--schema', help='解析库名', default=None)
    parser.add_argument('--table', help='解析表名', default=None)
    parser.add_argument('--binlogfile', help='binlog文件名', required=True)
    parser.add_argument('--rollback', help='生成回滚语句', default='N')
    parser.add_argument('--max_rows', help='最大解析日志行数', default=None)
    parser.add_argument('--debug', help='调试模式', default='N')
    args = parser.parse_args()
    return args

def demo():
    '''
      1.one primary
      2.more primary
      3.dml
      4 ddl
      # hft
        parser(None,
               None,
               None,
               None,
               'hft_settle_center',
               'trade_order_task',
               'mysql_172_17_41_177_3306_mysql-bin.000993',
               'Y',
               50000)
       #hst
        parsing(None,
               None,
               None,
               None,
               'hopsonone_park',
               'orders',
               'mysql_172.17.219.137_3306_mysql-bin.000844',
               'Y',
               None)
        print(json.dumps(i, ensure_ascii=False, indent=4, separators=(',', ':')))

        db = get_db()
        cr = db.cursor()
        cr.execute("SELECT ordinal_position,column_name FROM information_schema.columns WHERE table_schema='bigdata_platform' AND table_name='coupons_member_relations'  ORDER BY ordinal_position")
        rs = cr.fetchall()
        print('rs=',rs)
        for i in rs:
            print(i)
    '''
    pass

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    args = parse_param()
    parsing(args.start_time,
           args.stop_time,
           args.start_pos,
           args.stop_pos,
           args.schema,
           args.table,
           args.binlogfile,
           args.rollback,
           args.max_rows,
           args.debug)