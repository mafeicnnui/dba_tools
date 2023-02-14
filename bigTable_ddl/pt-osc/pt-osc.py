#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/10 14:18
# @Author : ma.fei
# @File : pt-osc.py.py
# @Software: PyCharm

import time
import pymysql
import warnings
import argparse

cfg = {}

def add_share_lock(cfg,args):
    cr = cfg['cr']
    st = "lock table `{}`.`{}` read".format(args.database,args.table)
    if args.debug.upper().upper() == 'Y':
       print(st)
    cr.execute(st)
    print('add table `{}`.`{}` add share lock success'.format(args.database,args.table))

def unlock_table(cfg,args):
    cr = cfg['cr']
    st = 'unlock tables'
    cr.execute(st)
    print('unlock tables `{}`.`{}` success'.format(args.database, args.table))

def get_db(args):
    conn = pymysql.connect(host= args.host,
                           port= int(args.port,),
                           user = args.user,
                           passwd = args.password,
                           db  = args.database,
                           charset = 'utf8mb4',
                           autocommit = False,
                           cursorclass = pymysql.cursors.DictCursor,
                           read_timeout = 60,
                           write_timeout = 60)
    return conn

def create_temp_table(cfg,args):
    cr = cfg['cr']
    st = 'show create table `{}`.`{}`'.format(args.database, args.table)
    cr.execute(st)
    rs=cr.fetchone()
    st=rs['Create Table'].replace('`{}`'.format(args.table),'`{}`.`{}_tmp`'.format(args.database,args.table))
    if args.debug.upper() == 'Y':
        print(st.replace('\n',''))
    cr.execute(st)
    print('Create temporary table `{}`.`{}_tmp` success!'.format(args.database, args.table))
    st = "alter table `{}`.`{}_tmp` {}".format(args.database, args.table, args.alter)
    if args.debug.upper() == 'Y':
       print(st)
    cr.execute(st)
    print('Temporary table `{}`.`{}_tmp` apply `{}` success!'.format(args.database, args.table,args.alter))

def create_log_table(cfg,args):
    cr = cfg['cr']
    st = 'create table `{}`.`{}_log`(id bigint auto_increment primary key,op varchar(1),pk varchar(100),log text,ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)'.format(args.database,args.table)
    if args.debug.upper() == 'Y':
        print(st)
    cr.execute(st)
    print('Create log table `{}`.`{}_log` success!'.format(args.database,args.table))

def drop_temp_table(cfg,args):
    cr = cfg['cr']
    st = 'drop table if exists `{}`.`{}_tmp`'.format(args.database, args.table)
    if args.debug.upper() == 'Y':
       print(st)
    cr.execute(st)
    print('Temporary table `{}`.`{}_tmp` drop success!'.format(args.database, args.table, args.alter))

def drop_log_table(cfg,args):
    cr = cfg['cr']
    st = 'drop table if exists `{}`.`{}_log`'.format(args.database,args.table)
    if args.debug.upper() == 'Y':
       print(st)
    cr.execute(st)
    print('Log table `t_log` drop success!'.format(args.database, args.table, args.alter))

def drop_old_table(cfg,args):
    cr = cfg['cr']
    st = 'drop table if exists `{}`.`{}_old`'.format(args.database, args.table)
    if args.debug.upper() == 'Y':
        print(st)
    cr.execute(st)
    print('Table `{}`.`{}_old` drop success!'.format(args.database, args.table, args.alter))

def check_data(cfg,args):
    cr = cfg['cr']
    st1='select count(0) as rec from `{}`.`{}`'.format(args.database, args.table)
    cr.execute(st1)
    rs1=cr.fetchone()
    st2='select count(0) as rec from `{}`.`{}_tmp`'.format(args.database, args.table)
    cr.execute(st2)
    rs2 = cr.fetchone()
    if rs1['rec'] == rs2['rec']:
       return True
    else:
       return False

def get_tab_pk_name(cfg,args):
    cr = cfg['cr']
    v_col = ''
    v_sql = """select column_name 
                 from information_schema.columns
                 where table_schema='{}'
                   and table_name='{}' and column_key='PRI' order by ordinal_position
             """.format(args.database, args.table)
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in rs:
        v_col = v_col + '`{}`,'.format(i['column_name'])
    return v_col[0:-1]

# def get_tab_columns(cfg,args):
#     cr = cfg['cr']
#     st = """SELECT group_concat('`',column_name,'`') as column_name,
#                    group_concat(concat('new.',column_name)) as ins_col_name
#             FROM information_schema.columns
#              WHERE table_schema='{}'
#               AND table_name='{}'
#               order by ordinal_position""".format(args.database, args.table)
#     cr.execute(st)
#     rs=cr.fetchone()
#     t=''
#     u=''
#     for i in rs['ins_col_name'].split(','):
#         t=t+'''"'",{},"',",'''.format(i)
#         u=u+'''{}='",{},"','''.format(i.split('.')[1],i)
#     rs['ins_col_name'] =t[0:-3]+')"'
#     rs['upd_col_name'] = u[0:-1]
#     rs['pk_name'] = get_tab_pk_name(cfg,args)
#     return rs

def get_tab_columns(cfg,args):
    cr = cfg['cr']
    st = """SELECT group_concat('`',column_name,'`') as column_name,
                   group_concat(concat('new.',column_name)) as ins_col_name
            FROM information_schema.columns
             WHERE table_schema='{}'  
              AND table_name='{}' 
              order by ordinal_position""".format(args.database, args.table)
    cr.execute(st)
    rs=cr.fetchone()
    t=''
    u=''
    for i in rs['ins_col_name'].split(','):
        t=t+'''"'",IFNULL({},'NULL'),"',",'''.format(i)
        u=u+'''{}='",IFNULL({},'NULL'),"','''.format(i.split('.')[1],i)

    rs['ins_col_name'] =t[0:-3]+')"'
    rs['upd_col_name'] = u[0:-1]
    rs['pk_name'] = get_tab_pk_name(cfg,args)
    return rs

def get_statement(cfg,args,type):
    cols = get_tab_columns(cfg,args)
    if type=='I':
       return  """concat('insert into `{}`.`{}_tmp`({}) values(',{})"""\
           .format(args.database, args.table,cols['column_name'],cols['ins_col_name'])
    elif type=='D':
        return """concat("delete from `{}`.`{}_tmp` where {}=","'",old.{},"'")""" \
           .format(args.database, args.table, cols['pk_name'], cols['pk_name'])
    elif type=='U':
        return """concat("update `{}`.`{}_tmp` set ","{} where {}=","'",old.{},"'")""" \
            .format(args.database, args.table, cols['upd_col_name'],cols['pk_name'], cols['pk_name'])

def get_tri_templates(p_type):
   if p_type=='I':
      return '''CREATE TRIGGER `{}`.`tri_{}_ins` BEFORE INSERT ON `{}` 
FOR EACH ROW BEGIN
    INSERT INTO `{}`.`{}_log`(`op`,`pk`,`log`) VALUES('I',new.{},{});
END;'''
   elif p_type=='D':
      return '''CREATE TRIGGER `{}`.`tri_{}_del` BEFORE DELETE ON `{}` 
FOR EACH ROW BEGIN
   INSERT INTO `{}`.`{}_log`(`op`,`pk`,`log`) VALUES('D',old.{},{});
END;'''
   elif p_type=='U':
      return '''CREATE TRIGGER `{}`.`tri_{}_upd` BEFORE UPDATE ON `{}` 
FOR EACH ROW BEGIN
  INSERT INTO `{}`.`{}_log`(`op`,`pk`,`log`) VALUES('U',new.{},{});
END;'''
   else:
       pass

def create_trigger(cfg,args):
    cr = cfg['cr']
    st = get_statement(cfg,args,'I')
    pk = get_tab_pk_name(cfg,args)
    st = get_tri_templates('I').format(args.database, args.table,args.table,args.database,args.table,pk,st)
    cr.execute(st)
    print('Table `{}`.`{}` insert trigger created!'.format(args.database, args.table))
    st = get_statement(cfg, args, 'D')
    st = get_tri_templates('D').format(args.database, args.table, args.table, args.database, args.table,pk, st)
    cr.execute(st)
    print('Table `{}`.`{}` delete trigger created!'.format(args.database, args.table))
    st = get_statement(cfg, args, 'U')
    st = get_tri_templates('U').format(args.database, args.table, args.table, args.database, args.table,pk, st)
    cr.execute(st)
    print('Table `{}`.`{}` update trigger created!'.format(args.database, args.table))

def get_tab_rows(cfg,args):
    cr = cfg['cr']
    cr.execute('select count(0) as rec from `{}`.`{}`'.format(args.database, args.table))
    rs=cr.fetchone()
    return rs['rec']

def copy_table(cfg,args):
    cr = cfg['cr']
    col = get_tab_columns(cfg,args)['column_name']
    row = get_tab_rows(cfg,args)
    cr.execute('SET autocommit=0')
    cr.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cr.execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    n_pos = 0
    print('coping table from `{}`.`{}` to `{}`.`{}_tmp`...'.format(args.database, args.table,args.database, args.table))
    for i in range(row//int(args.batch)+1):
        st = 'insert into `{}`.`{}_tmp`({}) select {} from  `{}`.`{}` limit {},{}' \
            .format(args.database, args.table, col, col, args.database, args.table, n_pos,args.batch)
        n_pos = n_pos+int(args.batch)
        if args.debug.upper() == 'Y':
            print(st)
        cr.execute(st)
        time.sleep(3)
    cfg['db'].commit()

def apply_log(cfg,args):
    cr = cfg['cr']
    st = 'select * from  `{}`.`{}_log` order by ts'.format(args.database, args.table)
    cr.execute(st)
    rs=cr.fetchall()
    print('apply table `{}`.`{}` log...'.format(args.database, args.table))
    pk = get_tab_pk_name(cfg, args)
    for i in rs:
      if args.debug.upper() == 'Y':
         print(i['log'].replace("'NULL'",'NULL'))
      if  i['op'] == 'I' :
          st = "select count(0) as rec from `{}`.`{}_tmp` where {}='{}'".format(args.database, args.table, pk, i['pk'])
          cr.execute(st)
          rs = cr.fetchone()
          if rs['rec'] == 1 :
             print('Table `{}`.`{}_tmp` already exists data,update data({}={})!'.format(args.database, args.table,pk,i['pk']))
             st="delete from  `{}`.`{}_tmp` where {}='{}'".format(args.database, args.table,pk,i['pk'])
             if args.debug.upper() == 'Y':
                print(st)
             cr.execute(st)
             cr.execute(i['log'].replace("'NULL'",'NULL'))
      else:
          cr.execute(i['log'].replace("'NULL'",'NULL'))
    cfg['db'].commit()

def rename_table(cfg,args):
    cr = cfg['cr']
    st ='rename table `{}`.`{}` to `{}`.`{}_old`,`{}`.`{}_tmp` to `{}`.`{}`'\
        .format(args.database, args.table,args.database, args.table,
                args.database, args.table,args.database, args.table)
    print('rename table,please wait...')
    if args.debug.upper() == 'Y':
       print(st)
    cr.execute(st)

def parse_param():
    parser = argparse.ArgumentParser(description='Resolve mysql binlogfile timestamp.')
    parser.add_argument('--host', help='地址', required=True)
    parser.add_argument('--port', help='端口', required=True)
    parser.add_argument('--user', help='用户名', required=True)
    parser.add_argument('--password', help='密码', required=True)
    parser.add_argument('--database', help='数据库名称', required=True)
    parser.add_argument('--table', help='表名称', required=True)
    parser.add_argument('--alter', help='操作', required=True)
    parser.add_argument('--batch', help='复制表批大小', required=True,default=2)
    parser.add_argument('--debug', help='调试模式', required=True, default='Y')
    args = parser.parse_args()
    return args

def main():
    warnings.filterwarnings("ignore")
    args = parse_param()
    cfg['db'] = get_db(args)
    cfg['cr'] = cfg['db'].cursor()
    cfg['cr'].execute('SET GLOBAL group_concat_max_len = 102400')
    create_temp_table(cfg, args)
    create_log_table(cfg, args)
    create_trigger(cfg,args)
    copy_table(cfg, args)
    apply_log(cfg, args)
    if check_data(cfg, args):
       rename_table(cfg, args)
       print('sleep 30s...')
       time.sleep(20)
       drop_log_table(cfg,args)
       drop_temp_table(cfg,args)
       drop_old_table(cfg,args)
    cfg['cr'].close()
    cfg['db'].close()

'''
  功能：通过python实现pt-osc功能，表必须有主键
  步骤：1.获取表级只读锁，超时失败，成功则记录
       2.建立临时表，执行DDL(add column,drop column,modify column)
       3.在原表上建立三个触发器（insert,update,delete)，用于记录原表变化并应用至临时表(建立一张日志表，用记记录变化)
       4.释放表级只读锁(这几步锁定时间非常短）
       5.复制源表数据至临时表(对于大表这个过程比较长)，期间用户可以正常操作源表
       6.应用日志表至临时表(第一次)，并记录最大p_max_logid
       7.锁定原表，再次应用p_max_logid之后产生的日志（因为前面应用过一次日志，所以后面的日志量不会很大，应用非常快）
       8.对比原表和临时表记录数，一致说明数据没问题(
       9.切换表，将临时表名改为原表，原表删除,释放锁
       10.切换完成
  准备:
    SET GLOBAL group_concat_max_len = 102400;
    SET SESSION group_concat_max_len = 102400;

    create table xs(xh int primary key,xm varchar(20),nl int);
    insert into xs(xh,xm,nl) values(1,'zhang.san',23),(2,'li.shi',43),(3,'wang.wu',37);
    alter table `test`.`xs` add column `csrq` date NULL after `nl`;     
    pt-osc.py --host=10.2.39.40 --port=3306 --user=puppet --password=Puppet@123 --database=test --table=xs --batch=2 --debug=y --alter="add column `csrq` date null after `nl`"
    pt-osc.py --host=10.2.39.40 --port=3306 --user=puppet --password=Puppet@123 --database=test --table=sales_report_day --batch=1000 --debug=y --alter="add column `csrq` date null"

'''

if __name__ == '__main__':
     main()