#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/29 9:28
# @Author : ma.fei
# @File : gen_reporter.py
# @Software: PyCharm

'''
  功能：按不同项目生成各各指标，定时发送邮件
'''

import sys
import json
import pymysql
import smtplib
import datetime
from email.mime.text import MIMEText

def get_first_day():
    now = datetime.date.today()
    return datetime.datetime(now.year, now.month, 1).strftime("%Y-%m-%d 0:0:0")

def get_last_day():
    now = datetime.date.today()
    if datetime.date.today().day==1:
       return datetime.datetime(now.year, now.month, 1).strftime("%Y-%m-%d 23:59:59")
    else:
       return (datetime.date.today() + datetime.timedelta(days = -0)).strftime("%Y-%m-%d 23:59:59")

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_db():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'])
    return conn

def get_db_dict():
    cfg  = read_json('./config.json')
    conn = pymysql.connect(host    = cfg['db_ip'],
                           port    = int(cfg['db_port']),
                           user    = cfg['db_user'],
                           passwd  = cfg['db_pass'],
                           db      = cfg['db_service'],
                           charset = cfg['db_charset'],
                           cursorclass = pymysql.cursors.DictCursor)
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

def get_bbtj_sql(p_db):
    st ="""select * from t_bbtj_sql where bbdm='bb01' order by xh"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_value(p_dbd,p_dsid,p_st):
    try:
        ds = get_ds_by_dsid(p_dbd,p_dsid)
        db = get_db_from_ds(ds)
        cr = db.cursor()
        cr.execute(p_st)
        rs = cr.fetchone()
        cr.close()
        if rs[0] is None:
           return 0
        else:
           #print('get_value=', p_st,str(rs[0]))
           return str(rs[0])
    except:
        return 0

def get_markets():
    return '108,110,132,164,213,218,237,159,234,278'.split(',')

def get_bb_values(bb):
    v = {}
    for b in bb:
        v['market'] = b.get('market')
        v[b.get('item')] = b.get('value')
    return v

def write_log(db,bb):
    bb = get_bb_values(bb)
    st = """delete from t_bbtj_log where market_id='{}' and bbdm='bb01' and tjrq=DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 0 DAY),'%Y-%m-%d')""".format(bb['market'])
    cr= db.cursor()
    cr.execute(st)
    st = """insert into t_bbtj_log(market_id,bbdm,tjrq,v1,v2,v3,v4,v5,create_time) 
              values('{}','{}',DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 0 DAY),'%Y-%m-%d'),'{}','{}','{}','{}','{}',now())
            """.format(bb['market'],'bb01',bb['v1'],bb['v2'],bb['v3'],bb['v4'],str(round(float(bb['v5'])+float(bb['v6']),2)))
    cr.execute(st)
    db.commit()

def get_log(dbd):
   cr=dbd.cursor()
   st="""select market_id,
                b.dmmc as market_name, 
                a.tjrq,
                a.v1,a.v2,a.v3,a.v4,a.v5,                
                date_format(a.create_time,'%Y-%m-%d %H:%i:%s') as create_time
        from t_bbtj_log a ,t_dmmx b
         where a.market_id=b.dmm and b.dm='05' and a.tjrq=DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 0 DAY),'%Y-%m-%d') order by market_id"""
   cr.execute(st)
   rs=cr.fetchall()
   return rs

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
                       <h3 align="center"><b>合生通项目KPI统计情况表</b></h3>
                       <p></p>
                       <br>统计日期：<span>$$TJRQ$$</span>
                       <br>统计范围：<span>$$TJRQQ$$～$$TJRQZ$$</span>
                       <p>
                       $$TABLE$$
                    </body>
                  </html>
               '''

def get_html_contents(p_dbd):
    v_html = get_templete()
    v_tab_header  = '<table class="xwtable">'
    v_tab_thead   = '''<thead>
                           <tr>
                               <th>项目编码</th>
                               <th>项目名称</th>
                               <th>报表日期</th>
                               <th>商品上传spu</th>
                               <th>会员拉新(万人)</th>
                               <th>支付即积分覆盖率</th>
                               <th>保底积分率</th>
                               <th>总GMV(万元)</th>
                               <th>生成时间</th>
                           </tr>  
                    </thead>'''
    v_tab_tbody   = '<tbody>'
    for log in list(get_log(p_dbd)):
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
                   <td>{}</td>
             </tr>'''.format(log.get('market_id'),
                             log.get('market_name'),
                             log.get('tjrq'),
                             log.get('v1'),log.get('v2'),log.get('v3'),log.get('v4'),log.get('v5'),log.get('create_time'))

    v_tab_tbody = v_tab_tbody + '</tbody>'
    v_table=v_tab_header+v_tab_thead+v_tab_tbody+'</table>'
    v_html = v_html.replace('$$TABLE$$',v_table)
    v_html = v_html.replace('$$TJRQ$$',get_time())
    v_html = v_html.replace('$$TJRQQ$$', get_first_day())
    v_html = v_html.replace('$$TJRQZ$$', get_last_day())
    return v_html


if __name__ == '__main__':
    # 初始化
    db    = get_db()
    dbd   = get_db_dict()
    bbrqq = get_first_day()
    bbrqz = get_last_day()

    # # 生成数据
    for market in get_markets():
        bb = get_bbtj_sql(dbd)
        for b in bb:
            b['statement'] = b['statement'].replace('$$BBRQQ$$',bbrqq).replace('$$BBRQZ$$',bbrqz).replace('$$MARKET_ID$$',market)
            b['value']     = get_value(dbd,b['ds_id'],b['statement'])
            b['market']    = market
        # 写日志
        write_log(db,bb)

    # 生成HTML
    v_title = '合生通项目KPI统计情况表-{}'.format(get_last_day()[0:10])
    v_content = get_html_contents(dbd)

    # 发送邮件
    # send_mail25('190343@lifeat.cn', 'R86hyfjobMBYR76h', '190343@lifeat.cn', v_title, v_content)
    send_mail25('190343@lifeat.cn', 'R86hyfjobMBYR76h','190634@lifeat.cn,807216@lifeat.cn,546564@hopson.com.cn,807319@cre-hopson.com,608520@hopson.com.cn,609479@hopson.com.cn,gonghaipeng@hopson.com.cn,820618@cre-hopson.com,820636@cre-hopson.com,808080@cre-hopson.com,190343@lifeat.cn', v_title, v_content)
    # 释放连接
    db.close()
    dbd.close()
