#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/29 9:28
# @Author : ma.fei
# @File : gen_reporter.py
# @Software: PyCharm

'''
  功能：按不同项目生成各各指标，定时发送邮件
    SELECT * FROM `kpi_po`  ORDER BY market_id+0;
    SELECT * FROM `kpi_item` ORDER BY CODE;
    SELECT * FROM kpi_item_sql WHERE if_stat='Y' ORDER BY item_code
    SELECT * FROM `kpi_po_mx`
    SELECT a.* FROM kpi_po_mx a,kpi_po b WHERE a.market_id=b.market_id ORDER BY b.sxh,item_code+0
    SELECT a.* FROM kpi_po_hz a,kpi_po b WHERE a.market_id=b.market_id ORDER BY b.sxh,item_code+0

'''

import json
import pymysql
import datetime
import smtplib
import traceback
from email.mime.text import MIMEText

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

# 读取配置文件
cfg  = read_json('./config.json')

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_templete():
    return '''<html>
                    <head>
                       <style type="text/css">
                           .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                           .xwtable thead td {font-size: 12px;color: #333333;
                                              text-align: center;background: url(table_top.jpg) repeat-x top center;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable thead th {font-size: 12px;color: #333333;
                                              text-align: center;background-color: #C0C0C0;
                                              border: 1px solid #ccc; font-weight:bold;}
                           .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                           .xwtable tbody tr.alt-row {background: #f2f7fc;}
                           .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                       </style>
                    </head>
                    <body>
                       <h3 align="center"><b>商管BI-KPI统计情况表</b></h3>
                       <p></p>
                       <br>统计日期：<br>
                           &nbsp;&nbsp;<span>$$TJRQ$$</span>
                       
                       <br>统计范围：<br>
                           &nbsp;&nbsp;本月：<span>$$TJRQQ$$～$$TJRQZ$$</span><br>
                           &nbsp;&nbsp;累计：<span>$$LJTJRQQ$$～$$TJRQZ$$</span>
                       <p>
                       $$TABLE$$
                    </body>
                  </html>
               '''

def get_month():
    return cfg['month']

def get_first_day():
    return cfg['begin_date']

def get_last_day():
    return cfg['end_date']

def get_bbrq():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def get_db_dict():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'],
                           cursorclass = pymysql.cursors.DictCursor,
                           autocommit = True)
    return conn

def get_db_from_ds(p_ds):
    conn = pymysql.connect(host    = p_ds['ip'],
                           port    = int(p_ds['port']),
                           user    = p_ds['user'],
                           passwd  = p_ds['password'],
                           db      = p_ds['service'],
                           charset ='utf8')
    return conn

def aes_decrypt(p_db,p_password, p_key):
    st = """select aes_decrypt(unhex('{0}'),'{1}') as pass""".format(p_password, p_key[::-1])
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return str(rs['pass'], encoding="utf-8")


def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")


def get_ds_by_dsid(p_db,p_dsid):
    st ="""select cast(id as char) as dsid,
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
                  proxy_server
           from t_db_source where id={0}""".format(p_dsid)
    cr=p_db.cursor()
    cr.execute(st)
    rs=cr.fetchone()
    rs['password'] =  aes_decrypt(p_db,rs['password'],rs['user'])
    rs['url'] = 'MySQL://{0}:{1}/{2}'.format(rs['ip'], rs['port'], rs['service'])
    cr.close()
    return rs

def get_bbtj_sql(p_db,p_stat_id):
    st ="""select * from t_kpi_item_sql where stat_sql_id='{}'  order by xh""".format(p_stat_id)
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs


def get_value(p_dbd,p_dsid,p_sql):
    try:
        ds = get_ds_by_dsid(p_dbd,p_dsid)
        db = get_db_from_ds(ds)
        cr = db.cursor()
        cr.execute(p_sql)
        rs = cr.fetchone()
        cr.close()
        val = '' if rs[0] is None  else str(rs[0])
        return val
    except:
        traceback.print_exc()
        return ''

def set_item_value(dbd,stat_sql_id,xh,val):
    cr = dbd.cursor()
    st = """update puppet.t_kpi_item_sql set value='{}' where stat_sql_id={} and xh={}""".format(val,stat_sql_id,xh)
    cr.execute(st)

def check_item_log_exists(dbd,i,s):
    cr = dbd.cursor()
    st = """select  count(0)  as rec from `t_kpi_item_log` 
                where item_type='{}' and item_name= '{}' 
                  and market_id='{}' and market_name='{}'
                    and stat_sql_id={} and xh={}
         """.format(i['item_type'],i['item_name'],i['market_id'],i['market_name'],s['stat_sql_id'],s['xh'])
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    return rs['rec']


def write_item_log(dbd,i,s,q,v):
    cr = dbd.cursor()
    if check_item_log_exists(dbd,i,s) >0:
        st = """delete from t_kpi_item_log 
                    where item_type='{}'  and  item_name='{}' 
                     and  market_id='{}'  and  market_name='{}'
                     and  stat_sql_id={}  and  xh ={}
             """.format(i['item_type'],i['item_name'],i['market_id'],i['market_name'],s['stat_sql_id'],s['xh'])
        cr.execute(st)
        print('deltete t_kpi_item_log {}=>{}(sql_stat_id:{}/xh={})'.format(i['market_name'],i['item_name'],s['stat_sql_id'],s['xh']))

    st = """INSERT INTO t_kpi_item_log(item_type,item_name,market_id,market_name,stat_sql_id,xh,statement,item_value,create_time)
                VALUES('{}','{}','{}','{}','{}','{}','{}','{}',now())
         """.format(i['item_type'],i['item_name'],i['market_id'],i['market_name'],s['stat_sql_id'],s['xh'],format_sql(q),v)
    cr.execute(st)
    print('insert t_kpi_item_log {}=>{}(sql_stat_id:{}/xh={},val={})'.format(i['market_name'],i['item_name'],s['stat_sql_id'],s['xh'],v))


def get_markets(dblog,flag):
    st = "select  * from `t_kpi_item` where is_stat='Y'  order by id".format(flag)
    cr = dblog.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_label_id(dbd,market_id):
    cr = dbd.cursor()
    st = """SELECT label_id FROM t_kpi_label WHERE market_id='{}'""".format(market_id)
    cr.execute(st)
    rs = cr.fetchone()
    if rs is not None:
       cr.close()
       return rs['label_id']
    else:
      return ''


if __name__ == '__main__':
    dbd    = get_db_dict()
    bbrqq  = get_first_day()
    bbrqz  = get_last_day()

    print('统计范围:{}~{}'.format(bbrqq,bbrqz))
    print('-------------------------------------------')
    for i in get_markets(dbd,'N'):
        for s in  get_bbtj_sql(dbd,i['stat_sql_id']):
            if __name__ == '__main__':
                q = s['statement'].\
                    replace('$$BBRQQ$$',bbrqq).\
                    replace('$$BBRQZ$$',bbrqz).\
                    replace('$$MARKET_ID$$',i['market_id']).\
                    replace('$$LABEL_ID$$',get_label_id(dbd,i['market_id']))
            v = get_value(dbd,s['dsid'],q)
            set_item_value(dbd,s['stat_sql_id'],s['xh'],v)
            write_item_log(dbd,i,s,q,v)

    dbd.close()

