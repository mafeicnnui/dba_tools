#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/26 14:27
# @Author : ma.fei
# @File : projects_server_monitor.py
# @Software: PyCharm

import pymysql
import smtplib
import datetime
from email.mime.text import MIMEText

cfg = {
    'ip': '10.2.39.18',
    'port': 3306,
    'user': 'puppet',
    'password': 'Puppet@123',
    'db': 'puppet',
    'charset': 'utf8'
}

def get_db():
    db = pymysql.connect(host=cfg['ip'],port=cfg['port'],user=cfg['user'],passwd=cfg['password'],
                         db=cfg['db'],charset='utf8',cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return db

def get_recent_logs(p_market_id):
    db = get_db()
    cr = db.cursor()
    st = """SELECT  cast((@i:=@i+1) as SIGNED ) xh,
                    a.market_id,
                    a.link_status,
                    DATE_FORMAT(a.update_time,'%Y-%m-%d %H:%i:%s') AS  update_time
              FROM t_monitor_project_log a,(SELECT @i:=0) AS b
                WHERE a.market_id='{}'  AND a.update_time>=DATE_SUB(NOW(),INTERVAL 5 MINUTE) 
                  ORDER BY a.update_time""".format(p_market_id)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    db.commit()
    return rs

def get_projects():
    db = get_db()
    cr = db.cursor()
    st = "select * from t_monitor_project order by id"
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    db.commit()
    return rs

def get_market_name(p_market_id):
    db = get_db()
    cr = db.cursor()
    st = "select market_name from t_monitor_project where market_id='{}'".format(p_market_id)
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs['market_name']

def get_warnings():
    db = get_db()
    cr = db.cursor()
    st = "select market_id,link_status,DATE_FORMAT(update_time,'%Y-%m-%d %H:%i:%s') AS  update_time from t_monitor_project_err order by id"
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    db.commit()
    return rs

def check_valid(p_logs):
      flag = False
      for log in p_logs:
          if log['xh']>=2 :
              if log['link_status'] == 'False':
                 flag = True
      return flag

def check_error(p_market_id):
    db = get_db()
    cr = db.cursor()
    st = """select count(0) as sl from t_monitor_project_err where market_id='{}'""".format(p_market_id)
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs['sl']

def write_errlog(p_market_id):
    db = get_db()
    cr = db.cursor()
    if check_error(p_market_id)==0:
        st = """insert into t_monitor_project_err(market_id,link_status,update_time) values('{}','{}',now())""".format(p_market_id,'fault')
    else:
        st = """update  t_monitor_project_err set update_time = now() where market_id='{}'""".format(p_market_id)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    db.commit()
    return rs

def delete_errlog(p_market_id):
    db = get_db()
    cr = db.cursor()
    if check_error(p_market_id)>0:
        st = """delete from t_monitor_project_err where market_id='{}'""".format(p_market_id)
        cr.execute(st)
        cr.close()
        db.commit()


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

def get_html_contents(thead,tbody):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_html='''<html>
		<head>
		   <style type="text/css">
			   .xwtable {width:400px;border-collapse: collapse;border: 1px solid #ccc;}
			   .xwtable thead td {font-size: 12px;color: #333333;
					          text-align: center;background: url(table_top.jpg) repeat-x top center;
				              border: 1px solid #ccc; font-weight:bold;}
			   .xwtable thead th {font-size: 12px;color: #333333;
				              text-align: center;background: url(table_top.jpg) repeat-x top center;
					      border: 1px solid #ccc; font-weight:bold;}
			   .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
			   .xwtable tbody tr.alt-row {background: #f2f7fc;}
			   .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
		   </style>
		</head>
		<body>
		  <h4>发送时间：'''+nowTime+'''</h4>
		  <table class="xwtable">
			<thead>\n'''+thead+'\n</thead>\n'+'<tbody>\n'+tbody+'''\n</tbody>
		  </table>
		</body>
	    </html>
           '''.format(thead,tbody)
    return v_html

if __name__ == '__main__':

    # 1.故障写入日志表
    for p in get_projects():
        logs = get_recent_logs(p['market_id'])
        if  check_valid(logs):
            title = p['market_name']
            content = '网络线路故障!'
            write_errlog(p['market_id'])

    # 2.生成故障信息
    title = '合生通项目网络链路故障情况表'
    thead = '<tr><th>项目编码</th><th>项目名称</th><th>线路状态</th><th>更新时间</th></tr>'
    tbody = ''
    for p in get_warnings():
       tbody = tbody +"""<tr>
                             <td>$$MARKET_ID$$</td>
                             <td>$$MARKET_NAME$$</td>
                             <td>$$LINK_STATUS$$</td>
                             <td>$$UPDATE_TIME$$</td>
                        </tr>""".replace('$$MARKET_ID$$',p['market_id']).\
                                 replace('$$MARKET_NAME$$',get_market_name(p['market_id'])).\
                                 replace('$$LINK_STATUS$$','链路故障!').\
                                 replace('$$UPDATE_TIME$$',p['update_time'])

    content = get_html_contents(thead,tbody)
    send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn,190205@lifeat.cn', title, content)

    # 2.恢复通知
    for p in get_warnings():
        logs = get_recent_logs(p['market_id'])
        if  not check_valid(logs):
            title = get_market_name(p['market_id'])
            content = '网络线路恢复!'
            send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn,190205@lifeat.cn', title, content)
            delete_errlog(p['market_id'])

            # 错误次数大于3以次以上，触发报警
