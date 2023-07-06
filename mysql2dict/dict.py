#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/30 10:26
# @Author : ma.fei
# @File : syncer.py
# @Func : sqlserver cdc syncer
# @Software: PyCharm

import json
import time
import pymysql
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

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_cfg():
    cfg = read_json('./dict.json')
    cfg['db'] = get_ds_mysql(
                        ip=cfg['db_ip'],
                        port=int(cfg['db_port']),
                        service=cfg['table_schema'],
                        user=cfg['db_user'],
                        password=cfg['db_pass'],

                        )
    cfg['cur'] = cfg['db'].cursor()
    cfg['dic_cur'] = cfg['db'].cursor()
    return cfg

def print_cfg(config):
    logging.info('-'.ljust(85,'-'))
    logging.info(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    logging.info('-'.ljust(85,'-'))
    for key in config:
      logging.info(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    logging.info('-'.ljust(85,'-'))

def get_table_schema(cfg):
    st=""" SELECT schema_name FROM information_schema.`SCHEMATA` 
WHERE schema_name NOT IN('information_schema','mysql','performance_schema','sys','test')"""
    cfg['dic_cur'].execute(st)
    rs=cfg['dic_cur'].fetchall()
    return rs

def get_no_comments_tables(cfg,table_schema):
    st="""SELECT table_name 
FROM information_schema.`TABLES` t
WHERE t.table_schema='{}'
 AND EXISTS(
  SELECT 1 FROM information_schema.`columns` c 
    WHERE c.table_schema=t.table_schema
      AND c.table_name=t.`TABLE_NAME`
       AND c.column_name NOT IN('id','create_time','update_time','create_by','update_by') 
        AND (column_comment='' OR column_comment IS NULL)
 )""".format(table_schema['schema_name'])
    cfg['dic_cur'].execute(st)
    rs=cfg['dic_cur'].fetchall()
    return rs

def get_table_info(cfg,schema,table):
    st="""SELECT 
  c.table_schema,         
  CONCAT(c.table_name,'(',t.table_comment, ')') AS table_name,
  c.column_name,
  c.column_type,
  c.column_comment,
  CASE WHEN  c.column_name NOT IN('id','create_time','update_time','create_by','update_by') 
        AND (c.column_comment='' OR c.column_comment IS NULL) 
        and (t.table_comment is  NULL OR t.table_comment ='')  THEN
          '增加表注释、列注释'
       WHEN  c.column_name NOT IN('id','create_time','update_time','create_by','update_by') 
        AND (c.column_comment='' OR c.column_comment IS NULL) 
        AND t.table_comment is NOT NULL AND t.table_comment<>'' THEN
          '增加列注释'  
  ELSE
     '' END AS flag
FROM information_schema.columns c ,information_schema.tables t
WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name
  AND c.table_schema='{}' AND c.table_name='{}'
 ORDER BY c.table_name,c.ordinal_position""".format(schema['schema_name'],table['table_name'])
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

