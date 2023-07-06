#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/30 10:26
# @Author : ma.fei
# @File : syncer.py
# @Func : sqlserver cdc syncer
# @Software: PyCharm

import json
import time
import pymssql
import datetime
import logging
import warnings
import traceback
import xlwt

logging.basicConfig(filename='dict.log'.format(datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')


def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_ds_sqlserver(ip, port,user, password,db):
    conn = pymssql.connect(host=ip,
                           port=int(port),
                           user=user,
                           password=password,
                           database=db,
                           charset='utf8',
                           autocommit=True)
    return conn


def get_cfg():
    cfg = read_json('./dict.json')
    cfg['db'] = get_ds_sqlserver(
                        ip=cfg['db_ip'],
                        port=int(cfg['db_port']),
                        user=cfg['db_user'],
                        password=cfg['db_pass'],
                        db=cfg['table_schema'],
                        )
    cfg['cur'] = cfg['db'].cursor()
    cfg['dic_cur'] = cfg['db'].cursor(as_dict=True)
    return cfg

def print_cfg(config):
    logging.info('-'.ljust(85,'-'))
    logging.info(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    logging.info('-'.ljust(85,'-'))
    for key in config:
      logging.info(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    logging.info('-'.ljust(85,'-'))

def get_table_schema(cfg):
    st="""SELECT name as schema_name
FROM sys.databases
 where name not  in('test','DWConfiguration','DWDiagnostics','DWQueue','master','model','msdb','tempdb')"""
    cfg['dic_cur'].execute(st)
    rs=cfg['dic_cur'].fetchall()
    return rs

def get_no_comments_tables(cfg,table_schema):
    st = """SELECT a.name as table_name
    FROM [{}].sys.tables a
     LEFT JOIN [{}].sys.extended_properties d  on  d.major_id = A.object_id  and d.minor_id=0
    WHERE  EXISTS(
      SELECT 1 FROM [{}].sys.columns b 
        LEFT JOIN [{}].sys.extended_properties c ON c.major_id = b.object_id AND c.minor_id = b.column_id
      WHERE a.object_id = b.object_id
         AND b.name NOT IN('id','create_time','update_time','create_by','update_by', 'createtime','createby','updatetime','updateby') 
    	 AND c.value is null)
      or d.value is null
    order by a.name""".format(table_schema['schema_name'], table_schema['schema_name'],
                              table_schema['schema_name'], table_schema['schema_name'])
    cfg['dic_cur'].execute(st)
    rs=cfg['dic_cur'].fetchall()
    return rs

def get_table_info(cfg,schema,table):
    st="""SELECT
'{}' as table_schema,
A.name+'('+cast(isnull(D.value,'') as varchar)+')' AS table_name,
B.name AS column_name,
T.name as column_type,
cast(C.value as varchar) AS column_comment,
CASE WHEN  b.name NOT IN('id','create_time','update_time','create_by','update_by') 
        AND (c.value='' OR c.value IS NULL) 
        and (d.value is NULL OR d.value ='')  THEN
          '增加表注释、列注释'
       WHEN  b.name  NOT IN('id','create_time','update_time','create_by','update_by') 
        AND (c.value='' OR c.value IS NULL) 
        AND d.value is NOT NULL AND d.value<>'' THEN
          '增加列注释'  
  ELSE
     '' END AS flag
FROM [{}].sys.tables A
INNER JOIN [{}].sys.columns B ON B.object_id = A.object_id
INNER JOIN [{}].sys.types T on t.user_type_id=b.user_type_id
LEFT JOIN [{}].sys.extended_properties C ON C.major_id = B.object_id AND C.minor_id = B.column_id
LEFT JOIN [{}].sys.extended_properties D ON D.major_id = A.object_id AND D.minor_id = 0
WHERE A.name = '{}' order by b.column_id""".\
        format(schema['schema_name'],schema['schema_name'],
               schema['schema_name'],schema['schema_name'],
               schema['schema_name'],schema['schema_name'],table['table_name'])
    cfg['dic_cur'].execute(st)
    rs = cfg['dic_cur'].fetchall()
    for r in rs:
        print(r)
    return rs

def set_header_styles(p_fontsize,p_color):
    header_borders = xlwt.Borders()
    header_styles  = xlwt.XFStyle()
    # add table header style
    header_borders.left   = xlwt.Borders.THIN
    header_borders.right  = xlwt.Borders.THIN
    header_borders.top    = xlwt.Borders.THIN
    header_borders.bottom = xlwt.Borders.THIN
    header_styles.borders = header_borders
    header_pattern = xlwt.Pattern()
    header_pattern.pattern = xlwt.Pattern.SOLID_PATTERN
    header_pattern.pattern_fore_colour = p_color
    # add font
    font = xlwt.Font()
    font.name = u'微软雅黑'
    font.bold = True
    font.size = p_fontsize
    header_styles.font = font
    #add alignment
    header_alignment = xlwt.Alignment()
    header_alignment.horz = xlwt.Alignment.HORZ_CENTER
    header_alignment.vert = xlwt.Alignment.VERT_CENTER
    header_styles.alignment = header_alignment
    header_styles.borders = header_borders
    header_styles.pattern = header_pattern
    return header_styles

def set_row_styles(p_fontsize,p_color):
    cell_borders   = xlwt.Borders()
    cell_styles    = xlwt.XFStyle()

    # add font
    font = xlwt.Font()
    font.name = u'微软雅黑'
    font.bold = True
    font.size = p_fontsize
    cell_styles.font = font

    #add col style
    cell_borders.left     = xlwt.Borders.THIN
    cell_borders.right    = xlwt.Borders.THIN
    cell_borders.top      = xlwt.Borders.THIN
    cell_borders.bottom   = xlwt.Borders.THIN

    row_pattern           = xlwt.Pattern()
    row_pattern.pattern   = xlwt.Pattern.SOLID_PATTERN
    row_pattern.pattern_fore_colour = p_color

    # add alignment
    cell_alignment        = xlwt.Alignment()
    cell_alignment.horz   = xlwt.Alignment.HORZ_LEFT
    cell_alignment.vert   = xlwt.Alignment.VERT_CENTER

    cell_styles.alignment = cell_alignment
    cell_styles.borders   = cell_borders
    cell_styles.pattern   = row_pattern
    cell_styles.font      = font
    return cell_styles

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    cfg = get_cfg()
    if cfg['debug']:
        print_cfg(cfg)

    r  = 0
    workbook  = xlwt.Workbook(encoding='utf8')
    worksheet = workbook.add_sheet('dict')
    header_styles = set_header_styles(45,1)
    file_name   = 'dict.xls'
    header = ['库名', '表名', '列名','类型', '注释','备注']
    for k in range(len(header)):
        worksheet.write(r, k, header[k], header_styles)

    for schema in get_table_schema(cfg):
        print(schema)
        for table in get_no_comments_tables(cfg,schema):
            print(table)
            for row in get_table_info(cfg,schema,table):
                print('row=',row)
                r = r + 1
                c = 0
                for col in row:
                    if row[col] is None:
                        worksheet.write(r, c, '')
                    else:
                        worksheet.write(r, c, str(row[col]))
                    c = c + 1

    workbook.save(file_name)
    print("{0} export complete!".format(file_name))

