#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/29 9:28
# @Author : ma.fei
# @File : gen_reporter.py
# @Software: PyCharm

'''
 接收KPI邮件名单：
    彭伟萍，546564@hopson.com.cn
    廖醒文，820506@cre-hopson.com
    李治冶，820618@cre-hopson.com
    林丽双，850646@cre-hopson.com
    任春宇，820987@cre-hopson.com
    谢芳，190634@lifeat.cn
    蔡世强，821498@cre-hopson.com
    "receiver"     : "190343@lifeat.cn,546564@hopson.com.cn,820506@cre-hopson.com,820618@cre-hopson.com,850646@cre-hopson.com,820987@cre-hopson.com,190634@lifeat.cn,821498@cre-hopson.com"

'''

import sys
import json
import pymysql
import datetime
import smtplib
import traceback
from email.mime.text import MIMEText
from email.header import Header

errors_sql = []

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

def send_mail25_win(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = Header(p_title, 'utf-8')
        msg["From"]    = Header(p_from_user, 'utf-8')
        msg["To"]      = Header(",".join(to_user), 'utf-8')
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
                       <br>统计日期：<span>$$TJRQ$$</span>
                       <br>统计范围：<span>$$TJRQQ$$～$$TJRQZ$$</span>                          
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
        errors_sql.append(p_sql)
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
        print('delete t_kpi_item_log {}=>{}(sql_stat_id:{}/xh={})'.format(i['market_name'],i['item_name'],s['stat_sql_id'],s['xh']))

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
    st = """SELECT GROUP_CONCAT(label_id) as label_id FROM t_kpi_label WHERE   instr('{}',market_id)>0""".format(market_id)
    try:
        cr.execute(st)
        rs = cr.fetchone()
        if rs is not None:
           cr.close()
           return rs['label_id']
        else:
          return ''
    except:
       return ''

def write_bbtj(dbd):
    cr = dbd.cursor()
    st = """INSERT INTO t_kpi_bbtj(bbrq,item_type,item_name,market_id,market_name,item_value,item_month,item_rate,create_time,sn)
SELECT
  DATE_FORMAT( NOW(),'%Y-%m-%d'),
  item_type,
  item_name,
  market_id,
  market_name,
  item_value,
  (SELECT item_value FROM t_kpi_item_value b 
        WHERE b.market_name=a.`market_name` 
          AND b.item_month=DATE_FORMAT(NOW(),'%Y-%m') 
          AND b.item_name=a.item_name) AS item_month,
  '' AS item_rate,
  NOW(),
  (SELECT sn FROM t_kpi_bbtj_sn b WHERE b.item_type=a.item_type AND b.market_name=a.market_name) 
FROM t_kpi_item_log a
WHERE (a.market_name,a.item_name,a.stat_sql_id,a.xh) IN(
SELECT market_name,item_name,stat_sql_id,MAX(xh)
 FROM t_kpi_item_log GROUP BY market_name,item_name,stat_sql_id)
 ORDER BY 
   CASE WHEN a.item_type='项目' THEN 1 
        WHEN a.item_type='区域' THEN 2 
        ELSE 3 END,
   CASE WHEN a.market_name='北京朝阳合生汇' THEN 1
        WHEN a.market_name='北京合生麒麟新天地' THEN 2
        WHEN a.market_name='北京木樨园合生广场' THEN 3
        WHEN a.market_name='成都温江合生汇' THEN 4
        WHEN a.market_name='广州海珠合生广场(南)' THEN 5
        WHEN a.market_name='广州海珠合生新天地' THEN 6
        WHEN a.market_name='广州增城合生汇' THEN 7
        WHEN a.market_name='上海青浦合生新天地' THEN 8
        WHEN a.market_name='上海五角场合生汇' THEN 9 
        WHEN a.market_name='直管区域' THEN 10
        WHEN a.market_name='上海区域' THEN 11
        WHEN a.market_name='广州区域' THEN 12
        WHEN a.market_name='商管总部及合生通' THEN 13
        ELSE 14 END"""
    cr.execute("delete from t_kpi_bbtj where bbrq=DATE_FORMAT( NOW(),'%Y-%m-%d')")
    cr.execute(st)
    cr.execute("""
    UPDATE t_kpi_bbtj 
 SET item_rate=
	CASE WHEN item_value IS NULL OR item_value='' THEN 
	 '0' 
	ELSE
	 CONCAT(ROUND(REPLACE(item_value,'%','')/REPLACE(item_month,'%','')*100,2),'%') 
	END 
    """)

def get_hz_log(dbd):
   cr=dbd.cursor()
   st="""SELECT bbrq,
market_id,
market_name,
item_name,
item_month_value,
item_finish_value,
CASE WHEN item_month_value = 0 THEN
    ''
ELSE 
    CONCAT(ROUND(REPLACE(item_finish_value,'%','')/REPLACE(item_month_value,'%','')*100,2),'%')
END  percent,
create_time
FROM `v_kpi_bbtj` 
WHERE bbrq = '{}'  
  AND (item_name,market_name) IN (SELECT item_name,market_name FROM t_kpi_item WHERE is_mail='Y') 
ORDER BY id""".format(get_time()[0:11])
   cr.execute(st)
   rs=cr.fetchall()
   return rs


def get_html_contents(p_dbd):
    v_html = get_templete()
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '''<thead>
                           <tr>
                               <th>报表日期</th>
                               <th>项目编码</th>
                               <th>项目名称</th>
                               <th>指标名称</th>
                               <th>月度指标</th>
                               <th>指标完成</th>
                               <th>完成率</th>
                               <th>生成时间</th>
                           </tr>  
                    </thead>'''
    v_tab_tbody   = '<tbody>'
    for log in list(get_hz_log(p_dbd)):
        v_tab_tbody = v_tab_tbody + \
            '''<tr>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
                   <td>{}</td>
             </tr>'''.format(
                             log.get('bbrq'),
                             log.get('market_id'),
                             log.get('market_name'),
                             log.get('item_name'),
                             log.get('item_month_value'),
                             log.get('item_finish_value'),
                             log.get('percent'),
                             log.get('create_time'))

    v_tab_tbody = v_tab_tbody + '</tbody>'
    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html = v_html.replace('$$TABLE$$',v_table)
    v_html = v_html.replace('$$TJRQ$$',get_time())
    v_html = v_html.replace('$$TJRQQ$$', get_first_day())
    v_html = v_html.replace('$$TJRQZ$$', get_last_day())
    return v_html

if __name__ == '__main__':
    dbd    = get_db_dict()
    bbrqq  = get_first_day()
    bbrqz  = get_last_day()

    if cfg['only_bbtj'] == 'N':
        print('统计范围:{}~{}'.format(bbrqq,bbrqz))
        print('-------------------------------------------')
        for i in get_markets(dbd,'N'):
            for s in  get_bbtj_sql(dbd,i['stat_sql_id']):
                q = s['statement'].\
                    replace('$$BBRQQ$$',bbrqq).\
                    replace('$$BBRQZ$$',bbrqz).\
                    replace('$$MARKET_ID$$',i['market_id']).\
                    replace('$$LABEL_ID$$',str(get_label_id(dbd,i['market_id'])))
                v = get_value(dbd,s['dsid'],q)
                set_item_value(dbd,s['stat_sql_id'],s['xh'],v)
                write_item_log(dbd,i,s,q,v)

        print('write t_kpi_bbtj...')
        write_bbtj(dbd)
        print('write t_kpi_bbtj ok!')

        if len(errors_sql)>0:
            print('---------------------SQL语句执行出错-------------------------')
            for sql in errors_sql:
                print(sql+'\n')
            print('------------------------------------------------------------')
    else:
        print('write t_kpi_bbtj...')
        write_bbtj(dbd)
        print('write t_kpi_bbtj ok!')

    # 发送邮件
    if cfg['is_send_mail'] == 'Y':
        v_title = '商管BI-KPI统计情况表-{}'.format(get_bbrq())
        v_content = get_html_contents(dbd)

        if sys.platform == 'win32':
           print('>>>>>>>>>>>>')
           send_mail25_win(cfg['sender'], cfg['sender_pass'], cfg['receiver'], v_title, v_content)
        else:
           send_mail25(cfg['sender'], cfg['sender_pass'], cfg['receiver'], v_title, v_content)

    dbd.close()

