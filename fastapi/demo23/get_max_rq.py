#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/21 12:22
# @Author : ma.fei
# @File : get_max_rq.py
# @Software: PyCharm

import json
import pymysql

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())

    cfg['db_mysql_dict'] =pymysql.connect(host=cfg['db_ip'],
                                     port=int(cfg['db_port']),
                                     user=cfg['db_user'],
                                     passwd=cfg['db_pass'],
                                     db=cfg['db_service'],
                                     charset=cfg['db_charset'],
                                     autocommit=False,
                                     cursorclass = pymysql.cursors.DictCursor)
    cfg['db_mysql'] = pymysql.connect(host=cfg['db_ip'],
                                      port=int(cfg['db_port']),
                                      user=cfg['db_user'],
                                      passwd=cfg['db_pass'],
                                      db=cfg['db_service'],
                                      charset=cfg['db_charset'],
                                      autocommit=False)
    return cfg

cfg = read_json('./config.json')

def get_table_part_col(cr,event):
    tj = "select max({}),count(0) from {}.{}"

    st ="""SELECT COUNT(0)
                FROM information_schema.columns
                WHERE table_schema='{}'
                AND table_name='{}' 
                AND data_type IN('date','timestamp','datetime')
                -- AND is_nullable='NO'
                AND column_name  = '{}'
          """

    st2 = """SELECT COUNT(0)
                    FROM information_schema.columns
                    WHERE table_schema='{}'
                    AND table_name='{}' 
                    AND data_type IN('date','timestamp','datetime')
                    AND is_nullable='YES'
                    AND column_name  not in ('create_time','create_dt','update_time','update_dt')
              """.format(event['schema'], event['table'])

    st3 = """SELECT column_name
                       FROM information_schema.columns
                       WHERE table_schema='{}'
                       AND table_name='{}' 
                       AND data_type IN('date','timestamp','datetime')
                       AND is_nullable='YES'
                       AND column_name  not in ('create_time','create_dt','update_time','update_dt') limit 1
                 """.format(event['schema'], event['table'])

    cr.execute(st.format(event['schema'], event['table'], 'update_time'))
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(tj.format('update_time', event['schema'], event['table']))
        rs = cr.fetchone()
        if rs[0] is None:
            return {'column_name': 'update_time', 'max_time': '均为空值!', 'rows': str(rs[1])}
        if rs[1] > 0:
            return {'column_name': 'update_time', 'max_time': str(rs[0]), 'rows': str(rs[1])}
        return {'column_name': 'update_time', 'max_time': '', 'rows': str(rs[1])}

    cr.execute(st.format(event['schema'], event['table'], 'update_dt'))
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(tj.format('update_dt', event['schema'], event['table']))
        rs = cr.fetchone()
        if rs[0] is None:
            return {'column_name': 'update_dt', 'max_time': '均为空值!', 'rows': str(rs[1])}
        if rs[1] > 0:
            return {'column_name': 'update_dt', 'max_time': str(rs[0]), 'rows': str(rs[1])}
        return {'column_name': 'update_dt', 'max_time': '', 'rows': str(rs[1])}


    cr.execute(st.format(event['schema'],event['table'],'create_time'))
    rs = cr.fetchone()
    if rs[0] >0 :
       cr.execute(tj.format('create_time',event['schema'],event['table']))
       rs=cr.fetchone()
       #print(tj.format('create_time',event['schema'],event['table']),'rs=',rs)
       if rs[0] is None:
          return {'column_name': 'create_time', 'max_time': '均为空值!', 'rows': str(rs[1])}
       if rs[1] >0:
          return {'column_name':'create_time','max_time':str(rs[0]),'rows':str(rs[1])}
       return {'column_name':'create_time','max_time':'','rows':str(rs[1])}

    cr.execute(st.format(event['schema'],event['table'],'create_dt'))
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(tj.format('create_dt', event['schema'], event['table']))
        rs = cr.fetchone()
        if rs[0] is None:
            return {'column_name': 'create_dt', 'max_time': '均为空值!', 'rows': str(rs[1])}
        if rs[1] > 0:
           return {'column_name':'create_dt','max_time':str(rs[0]),'rows':str(rs[1])}
        return {'column_name': 'create_dt', 'max_time': '','rows':str(rs[1])}

    cr.execute(st2)
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(st3)
        rs = cr.fetchone()
        cr.execute(tj.format(rs[0], event['schema'], event['table']))
        rs2 = cr.fetchone()
        if rs2[0] is None:
            return {'column_name': rs[0], 'max_time': '均为空值!', 'rows': str(rs2[1])}
        if rs2[1] > 0:
           return {'column_name': rs[0],'max_time':str(rs2[0]),'rows':str(rs2[1])}
        return {'column_name': rs[0],'max_time': '','rows':str(rs2[1])}

    tj = "select count(0) from {}.{}"
    cr.execute(tj.format(event['schema'], event['table']))
    return {'column_name':'无时间列!','max_time':'','rows':str(rs[0] )}

def main():
    dbd = cfg['db_mysql_dict']
    dbt = cfg['db_mysql']
    crd = dbd.cursor()
    crt = dbt.cursor()
    crd.execute("SELECT table_schema,table_name FROM information_schema.TABLES WHERE table_schema='hopsonone_business'")
    print('表名                                         时间列                最大时间           行数')
    print('-'.ljust(100,'-'))
    for i in crd.fetchall():
        ev = {'schema':i['table_schema'],'table':i['table_name']}
        #print(i)
        i.update(get_table_part_col(crt,ev))
        print(i['table_name'].ljust(40,' '),i['column_name'].ljust(20,' '),i['max_time'].ljust(30,' '),i['rows'].ljust(30,' '))

if __name__ == '__main__':
    main()