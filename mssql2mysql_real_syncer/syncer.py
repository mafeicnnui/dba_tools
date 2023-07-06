#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/30 10:26
# @Author : ma.fei
# @File : syncer.py
# @Func : sqlserver cdc syncer
# @Software: PyCharm

'''
  0.为test增加cdc文件组及数据文件
  1.开启sqlserver db cdc
  2.开戾sqlserver table cdc
  3.待解决难点
  4.检测项目
    库是否存在、表是否存在、表是否存在主键

'''
import json
import time
import datetime
import logging
import warnings
import pymssql
import traceback

logging.basicConfig(filename='syncer.log'.format(datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

def get_time():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def init_cdc(cfg):
    if cfg['initialize']:
        unset_table_cdc(cfg)
        unset_db_cdc(cfg)
        set_db_cdc(cfg)
        set_table_cdc(cfg)

def check_cdc(cfg):
    if check_tab_exist_pk(cfg) == 0:
       logging.error('`{table_schema}`.`{table_name}` have not primary key!')

def print_cfg(config):
    logging.info('-'.ljust(85,'-'))
    logging.info(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    logging.info('-'.ljust(85,'-'))
    for key in config:
      logging.info(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    logging.info('-'.ljust(85,'-'))

def get_ds_sqlserver(ip, port, db, user, password):
    conn = pymssql.connect(host=ip,
                           port=int(port),
                           user=user,
                           password=password,
                           database=db,
                           charset='utf8',
                           autocommit=True)
    return conn

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_cfg():
    cfg = read_json('./config.json')
    cfg['db'] = get_ds_sqlserver(
                        ip=cfg['db_ip'],
                        port=int(cfg['db_port']),
                        db=cfg['table_schema'],
                        user=cfg['db_user'],
                        password=cfg['db_pass'],
                        )
    cfg['cur'] = cfg['db'].cursor()
    cfg['dic_cur'] = cfg['db'].cursor(as_dict=True)
    return cfg

def get_capture_instance(cfg):
    cr = cfg['dic_cur']
    st = """select capture_instance 
              from [cdc].[change_tables] 
               where source_object_id=object_id('{table_schema}.dbo.{table_name}')
                 order by create_date desc""".format(**cfg)
    cr.execute(st)
    rs = cr.fetchall()
    if len(rs) == 2:
       cfg['capture_instance_new'] = rs[0]['capture_instance']
       cfg['capture_instance_old'] = rs[1]['capture_instance']
    elif len(rs) == 1:
       cfg['capture_instance_new'] = rs[0]['capture_instance']
       cfg['capture_instance_old'] = ''
    elif len(rs) == 0:
       cfg['capture_instance_new'] = ''
       cfg['capture_instance_old'] = ''
    else:
       cfg['capture_instance_new'] = ''
       cfg['capture_instance_old'] = ''
    return rs

def get_ckpt():
    try:
        cfg = read_json('./checkpoint.json')
        return cfg
    except:
        return {}

def set_ckpt(log,cfg):
    get_capture_instance(cfg)
    with open('./checkpoint.json', 'w') as f:
        f.write(json.dumps({
            'lsn':str(log['__$start_lsn']),
            'capture_instance_new':cfg['capture_instance_new'],
            'capture_instance_old':cfg['capture_instance_old']
        },ensure_ascii=False, indent=4, separators=(',', ':')))
        logging.info('set checkpoint:{}'.format(log['__$start_lsn']))

def upd_ckpt(cfg):
    get_capture_instance(cfg)
    ckpt = get_ckpt()
    with open('./checkpoint.json', 'w') as f:
        f.write(json.dumps({
            'lsn':ckpt['lsn'],
            'capture_instance_new':cfg['capture_instance_new'],
            'capture_instance_old':cfg['capture_instance_old']
        },ensure_ascii=False, indent=4, separators=(',', ':')))
        logging.info('update checkpoint:{}'.format(ckpt['lsn']))

def set_db_cdc(cfg):
    try:
        st="""USE {table_schema};
IF EXISTS(select 1 from sys.databases where name='{table_schema}' and is_cdc_enabled=0)
BEGIN
  exec sys.sp_cdc_enable_db
END""".format(**cfg)
        if cfg['debug']:
            logging.info("set_db_cdc:\n" + st)
        cfg['cur'].execute(st)
        logging.info('set_db_cdc `{table_schema}` success!'.format(**cfg))
    except:
        logging.error('set_db_cdc `{table_schema}` failure!'.format(**cfg))
        traceback.print_exc()

def set_table_cdc(cfg):
    try:
        xh = get_time()
        cfg['capture_instance_old'] = cfg.get('capture_instance_new','')
        cfg['capture_instance_new'] = cfg['table_name']+'_'+xh
        st = """USE {table_schema};
IF EXISTS(SELECT 1 FROM sys.tables WHERE name='{table_name}' AND is_tracked_by_cdc =0)
BEGIN
EXECUTE sys.sp_cdc_enable_table
 @source_schema = 'dbo', 
        @source_name = '{table_name}', 
        @capture_instance = '{capture_instance_new}',
        @supports_net_changes = 1, 
        @role_name = NULL,
        @index_name = NULL,
        @captured_column_list = NULL, 
        @filegroup_name = 'CDC'
END""".format(**cfg)
        if cfg['debug']:
           logging.warning("set_table_cdc:\n"+st)
        cfg['cur'].execute(st)
        logging.info('set_table_cdc `{table_schema}`.`{table_name}` success!'.format(**cfg))
    except:
        logging.error('set_table_cdc `{table_schema}`.`{table_name}` failure!'.format(**cfg))
        traceback.print_exc()

def set_table_cdc_2(cfg):
    try:
        xh = get_time()
        cfg['capture_instance_old'] = cfg.get('capture_instance_new','')
        cfg['capture_instance_new'] = cfg['table_name']+'_'+xh
        st = """USE {table_schema};
IF EXISTS(SELECT 1 FROM sys.tables WHERE name='{table_name}' AND is_tracked_by_cdc =1)
BEGIN
EXECUTE sys.sp_cdc_enable_table
 @source_schema = 'dbo', 
        @source_name = '{table_name}', 
        @capture_instance = '{capture_instance_new}',
        @supports_net_changes = 1, 
        @role_name = NULL,
        @index_name = NULL,
        @captured_column_list = NULL, 
        @filegroup_name = 'CDC'
END""".format(**cfg)
        if cfg['debug']:
           logging.warning("set_table_cdc_2:\n"+st)
        cfg['cur'].execute(st)
        logging.info('set_table_cdc_2 `{table_schema}`.`{table_name}` success!'.format(**cfg))
    except:
        logging.error('set_table_cdc_2 `{table_schema}`.`{table_name}` failure!'.format(**cfg))
        traceback.print_exc()

def unset_db_cdc(cfg):
    try:
        st="""USE {table_schema};
IF EXISTS(select 1 from sys.databases where name='{table_schema}' and is_cdc_enabled=1)
BEGIN
  exec sys.sp_cdc_disable_db
END""".format(**cfg)
        if cfg['debug']:
            logging.info("unset_db_cdc:\n" + st)
        cfg['cur'].execute(st)
        logging.info('unset_db_cdc `{table_schema}` success!'.format(**cfg))
    except:
        logging.error('unset_db_cdc `{table_schema}` failure!'.format(**cfg))
        traceback.print_exc()

def unset_table_cdc(cfg):
    try:
      for c in get_capture_instance(cfg):
        st = """USE {table_schema};
IF EXISTS(SELECT 1 FROM sys.tables WHERE name='{table_name}' AND is_tracked_by_cdc =1)
BEGIN
EXECUTE sys.sp_cdc_disable_table
       @source_schema = 'dbo', 
       @source_name = '{table_name}', 
       @capture_instance = '{capture_instance}';
END""".format(**cfg,**c)
        if cfg['debug']:
           logging.warning("unset_table_cdc:\n"+st)
        cfg['cur'].execute(st)
        logging.info('unset_table_cdc `{table_schema}`.`{table_name}` capture_instance:`{capture_instance}` success!'.format(**cfg,**c))
    except:
        logging.error('unset_table_cdc `{table_schema}`.`{table_name}` capture_instance:`{capture_instance}` failure!'.format(**cfg,**c))
        traceback.print_exc()

def unset_table_cdc_2(cfg):
    try:
      for c in get_capture_instance(cfg):
        if cfg.get('capture_instance_old') == c['capture_instance'] :
            st = """USE {table_schema};
    IF EXISTS(SELECT 1 FROM sys.tables WHERE name='{table_name}' AND is_tracked_by_cdc =1)
    BEGIN
    EXECUTE sys.sp_cdc_disable_table
           @source_schema = 'dbo', 
           @source_name = '{table_name}', 
           @capture_instance = '{capture_instance_old}';
    END""".format(**cfg,**c)
            if cfg['debug']:
               logging.warning("unset_table_cdc_2:\n"+st)
            cfg['cur'].execute(st)
            logging.info('unset_table_cdc_2 `{table_schema}`.`{table_name}` capture_instance:`{capture_instance_old}` success!'.format(**cfg,**c))
    except:
        logging.error('unset_table_cdc_2 `{table_schema}`.`{table_name}` capture_instance:`{capture_instance_old}` failure!'.format(**cfg,**c))
        traceback.print_exc()

def check_tab_exist_pk(cfg):
    cr = cfg['dic_cur']
    st = """select
 count(0) as rec
from syscolumns col, sysobjects obj
where col.id=obj.id and obj.id=object_id('{table_name}')
and  (select  1
      from  dbo.sysindexes si
          inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
          inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
          inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
      where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(**cfg)
    cr.execute(st)
    rs = cr.fetchone()
    return rs['rec']

def get_tab_pk_name(cfg):
    cr = cfg['dic_cur']
    st = """select
   col.name
from syscolumns col, sysobjects obj
where col.id=obj.id and obj.id=object_id('{table_name}')
and  (select  1
      from  dbo.sysindexes si
          inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
          inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
          inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
      where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(**cfg)
    cr.execute(st)
    rs = cr.fetchone()
    return rs['name']

def get_tab_columns(cfg):
    cr = cfg['dic_cur']
    st = """select col.name
from syscolumns col, sysobjects obj
where col.id=obj.id 
 and obj.id=object_id('{table_name}')
order by isnull((SELECT  'Y'
                FROM  dbo.sysindexes si
                INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
""".format(**cfg)
    cr.execute(st)
    rs = cr.fetchall()
    cl = []
    for r in rs:
        cl.append(r['name'].lower())
    return cl

def get_ct_tab_columns(cfg):
    cr = cfg['dic_cur']
    st = """select col.name
from syscolumns col, sysobjects obj
where col.id=obj.id 
 and obj.id=object_id('[cdc].[{capture_instance_new}_CT]')
 and col.name not in('__$start_lsn','__$end_lsn','__$seqval','__$operation','__$update_mask')
order by isnull((SELECT  'Y'
                FROM  dbo.sysindexes si
                INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
""".format(**cfg)
    cr.execute(st)
    rs = cr.fetchall()
    cl = []
    for r in rs:
        cl.append(r['name'].lower())
    return cl

def get_data_from_tab(cfg,res):
    cr = cfg['dic_cur']
    if isinstance(res['pk_val'], int) or isinstance(res['pk_val'], float):
       st = """select * from [{table_schema}].dbo.[{table_name}] where {pk_name}= {pk_val}""".format(**res)
    else:
       st = """select * from [{table_schema}].dbo.[{table_name}] where {pk_name}= '{pk_val}'""".format(**res)
    cr.execute(st)
    rs = cr.fetchone()
    res.update(rs)
    return res

def gen_range_sql(cfg,log):
    if cfg['cdc_log']:
       logging.info('cdc range log='+json.dumps(log))
    res = {
        'table_schema':cfg['table_schema'],
        'table_name':cfg['table_name']
    }
    if log['__$operation'] =='2':
       res['type'] ='I'
       res['pk_name'] = get_tab_pk_name(cfg)
       res['pk_val'] = log[res['pk_name']]
       res['columns'] = ','.join(["'{}'".format(i)  for i in get_tab_columns(cfg)])
       val=''
       logging.info('\033[33mfrom source table `{table_schema}.dbo.{table_name}` get data!\033[0m'.format(**cfg))
       res = get_data_from_tab(cfg,res)
       logging.info('get_data_from_tab log=' + json.dumps(res))
       for c in get_tab_columns(cfg):
           if res[c] is None:
              val = val + "null,"
           else:
              val = val +"'{}',".format(res[c])

       res['values']=val[0:-1]
       if cfg['common_log']:
           logging.info('Common log : ' + json.dumps(res))

       if cfg['mysql_adapter']:
           mysql_adapter(res)

    if log['__$operation'] == '4':
        res['type'] = 'U'
        res['pk_name'] = get_tab_pk_name(cfg)
        res['pk_val'] = log[res['pk_name']]
        res['columns'] = get_tab_columns(cfg)
        val = []
        logging.info('\033[33mfrom source table `{}.dbo.{}` get data!\033[0m'.format(**cfg))
        res = get_data_from_tab(cfg, res)
        logging.info('get_data_from_tab log=' + json.dumps(res))

        for c in get_tab_columns(cfg):
            res[c] = 'null' if res[c] is None else res[c]
            val.append('null' if res[c] is None else res[c])

        res['values'] =val
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))

        if cfg['mysql_adapter']:
            mysql_adapter(res)

    if log['__$operation'] == '1':
        res['type'] = 'D'
        res['pk_name'] = get_tab_pk_name(cfg)
        res['pk_val'] = log[res['pk_name']]
        res['columns'] = get_tab_columns(cfg)
        val = []
        logging.info('\033[33mfrom source table `{}.dbo.{}` get data!\033[0m'.format(**cfg))
        res = get_data_from_tab(cfg, res)
        logging.info('get_data_from_tab log=' + json.dumps(res))

        for c in get_tab_columns(cfg):
            res[c] = 'null' if res[c] is None else res[c]
            val.append('null' if res[c] is None else res[c])

        res['values'] =val
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))

        if cfg['mysql_adapter']:
            mysql_adapter(res)

    if log['__$operation'] == '-1':
        res['type'] = 'DDL'
        res['ddl'] =  log['ddl']
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))

        if cfg['mysql_adapter']:
            mysql_adapter(res)

def gen_sql(cfg,log):
    if cfg['cdc_log']:
       logging.info('cdc log='+json.dumps(log))
    res = {
        'table_schema':cfg['table_schema'],
        'table_name':cfg['table_name']
    }
    if log['__$operation'] =='2':
       res['type'] ='I'
       res['pk_name'] = get_tab_pk_name(cfg)
       res['pk_val'] =  log[res['pk_name']]
       res['columns'] = ','.join(["'{}'".format(i)  for i in get_ct_tab_columns(cfg)])
       val=''
       for c in get_ct_tab_columns(cfg):
           if log[c] is None:
              val = val + "null,"
           else:
              val = val +"'{}',".format(log[c])
       res['values']=val[0:-1]
       if cfg['common_log']:
          logging.info('Common log : '+json.dumps(res))
       if cfg['mysql_adapter']:
          mysql_adapter(res)

    if log['__$operation'] == '4':
        res['type'] = 'U'
        res['pk_name'] = get_tab_pk_name(cfg)
        res['pk_val'] = log[res['pk_name']]
        res['columns'] = get_ct_tab_columns(cfg)
        val = []
        for c in get_ct_tab_columns(cfg):
            res[c] = 'null' if log[c] is None else log[c]
            val.append('null' if log[c] is None else log[c])
        res['values'] =val
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))
        if cfg['mysql_adapter']:
            mysql_adapter(res)

    if log['__$operation'] == '1':
        res['type'] = 'D'
        res['pk_name'] = get_tab_pk_name(cfg)
        res['pk_val'] = log[res['pk_name']]
        res['columns'] = get_ct_tab_columns(cfg)
        val = []
        for c in get_ct_tab_columns(cfg):
            res[c] = 'null' if log[c] is None else log[c]
            val.append('null' if log[c] is None else log[c])
        res['values'] =val
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))
        if cfg['mysql_adapter']:
            mysql_adapter(res)

    if log['__$operation'] == '-1':
        res['type'] = 'DDL'
        res['ddl'] =  log['ddl']
        if cfg['common_log']:
            logging.info('Common log : ' + json.dumps(res))
        if cfg['mysql_adapter']:
            mysql_adapter(res)

def mysql_adapter(res):
    if res['type'] == 'I':
       st = 'insert into `{table_schema}`.`{table_name} ({columns}) values({values})'.format(**res)
       logging.info('\033[33mmysql_adapter : {}\033[0m'.format(st))

    if res['type'] == 'U':
       cols=''
       for c,v in zip(res['columns'],res['values']):
           if c !=res['pk_name']:
               if isinstance(v, int) or isinstance(v, float):
                  if v == 'null':
                      cols = cols + "`{}` = null,".format(c, v)
                  else:
                      cols = cols + "`{}` = {},".format(c, v)
               else:
                  if v == 'null':
                      cols = cols+"`{}` = null,".format(c,v)
                  else:
                      cols = cols + "`{}` = '{}',".format(c, v)
       res['columns'] = cols[0:-1]
       if isinstance(res['pk_val'], int) or isinstance(res['pk_val'], float):
          st = """update `{table_schema}`.`{table_name}` set {columns} where {pk_name} = {pk_val}""".format(**res)
       else:
          st = """update `{table_schema}`.`{table_name}` set {columns} where {pk_name} = '{pk_val}'""".format(**res)
       logging.info('\033[33mmysql_adapter : {}\033[0m'.format(st))

    if res['type'] == 'D':
        if isinstance(res['pk_val'], int) or isinstance(res['pk_val'], float):
           st = """delete from `{table_schema}`.`{table_name}` where  {pk_name} = {pk_val}""".format(**res)
        else:
           st = """delete from `{table_schema}`.`{table_name}` where  {pk_name} = '{pk_val}'""".format(**res)
        logging.info('\033[33mmysql_adapter : {}\033[0m'.format(st))

    if res['type'] == 'DDL':
        logging.info('\033[34mmysql_adapter : {}\033[0m'.format(res['ddl']))

def get_query_column(cfg):
    return {
        'dml_columns' : ','.join(['[{}]'.format(c) for c in get_ct_tab_columns(cfg)]),
        'ddl_columns' : ','.join(["''" for c in get_ct_tab_columns(cfg)])
    }

def get_cdc_range_logs(cfg):
    cfg.update(get_ckpt())
    get_capture_instance(cfg)
    st = """select 
        convert(VARCHAR(1000), [__$start_lsn], 2) as [__$start_lsn] ,
        convert(VARCHAR(1000), [__$end_lsn], 2) as [__$end_lsn] ,
        convert(VARCHAR(1000), [__$seqval], 2) as [__$seqval] ,
        convert(VARCHAR(1000), [__$operation], 2) as [__$operation] ,
        convert(VARCHAR(1000), [__$update_mask], 2) as [__$update_mask],      
        {dml_columns}
    from [{table_schema}].[cdc].[{capture_instance_old}_CT] 
    where [__$operation] in(1,2,4) and [__$start_lsn]>0x{lsn} 
      and [__$start_lsn] not in(select [__$start_lsn] from [{table_schema}].[cdc].[{capture_instance_new}_CT])
     order by [__$start_lsn]""".format(**cfg)
    if cfg['debug']:
       logging.info('get_cdc_range_logs:'+st)
    cfg['dic_cur'].execute(st)
    logs = cfg['dic_cur'].fetchall()
    return logs

def ddl_process(cfg):
    '''
      1.创建另一个捕获进程,更新capture_instance_new,capture_instance_old配置
      2.从老表中获取创建捕获进程期间产生的日志
      3.从区间中获取操作类型和主键ID，返查原表按规则生成该区间产生的日志
      4.将日志写入队列中，每应有一条，写一次检查点
      5.退出循环，进行下一次计调度
    '''
    logging.info('ddl_process please wait....')
    time.sleep(cfg['sleep'])
    unset_table_cdc_2(cfg)
    set_table_cdc_2(cfg)
    logs = get_cdc_range_logs(cfg)
    for log in logs:
        gen_range_sql(cfg,log)
        set_ckpt(log,cfg)
    unset_table_cdc_2(cfg)

def get_cdc_logs(cfg):
    cfg.update(get_ckpt())
    cfg.update(get_query_column(cfg))
    if cfg['debug']:
        print_cfg(cfg)

    if cfg['debug']:
       logging.info('checkpoint : '+cfg['lsn'])

    if cfg['lsn'] == '':
       st="""select * from (
select 
    convert(VARCHAR(1000), [__$start_lsn], 2) as [__$start_lsn] ,
    convert(VARCHAR(1000), [__$end_lsn], 2) as [__$end_lsn] ,
    convert(VARCHAR(1000), [__$seqval], 2) as [__$seqval] ,
    convert(VARCHAR(1000), [__$operation], 2) as [__$operation] ,
    convert(VARCHAR(1000), [__$update_mask], 2) as [__$update_mask] ,
    null as ddl,
    {dml_columns}
from [{table_schema}].[cdc].[{capture_instance_new}_CT] 
where [__$operation] in(1,2,4) 
union
select 
   convert(VARCHAR(1000), [ddl_lsn], 2) as [__$start_lsn] , 
   null as [__$end_lsn] ,
   null as [__$seqval] ,
   '-1' as [__$operation] ,
   null as [__$update_mask] ,
   ddl_command as ddl,
   {ddl_columns}
from [cdc].[ddl_history]
) t order by t.[__$start_lsn]""".format(**cfg)
    else:
       st="""select * from (
select 
    convert(VARCHAR(1000), [__$start_lsn], 2) as [__$start_lsn] ,
    convert(VARCHAR(1000), [__$end_lsn], 2) as [__$end_lsn] ,
    convert(VARCHAR(1000), [__$seqval], 2) as [__$seqval] ,
    convert(VARCHAR(1000), [__$operation], 2) as [__$operation] ,
    convert(VARCHAR(1000), [__$update_mask], 2) as [__$update_mask] ,
    null as ddl,
    {dml_columns}
from [{table_schema}].[cdc].[{capture_instance_new}_CT] 
where [__$operation] in(1,2,4) and [__$start_lsn]>0x{lsn} 
union 
select 
   convert(VARCHAR(1000), [ddl_lsn], 2) as [__$start_lsn] , 
   null as [__$end_lsn] ,
   null as [__$seqval] ,
   '-1' as [__$operation] ,
   null as [__$update_mask] ,
   ddl_command as ddl,
   {ddl_columns}
from [cdc].[ddl_history] where [ddl_lsn]>0x{lsn} 
) t order by t.[__$start_lsn]""".format(**cfg)
    if cfg['debug']:
       logging.info('get_cdc_logs:'+st)

    cfg['dic_cur'].execute(st)
    logs = cfg['dic_cur'].fetchall()
    for log in logs:
        gen_sql(cfg,log)
        set_ckpt(log,cfg)
        if log['__$operation'] == '-1':
            logging.info('find ddl processing,please wait...')
            ddl_process(cfg)
            logging.info('ddl process complete!')
            break


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug']:
        print_cfg(cfg)

    init_cdc(cfg)

    check_cdc(cfg)

    upd_ckpt(cfg)

    while True:
        get_cdc_logs(cfg)
        time.sleep(cfg['sleep'])

