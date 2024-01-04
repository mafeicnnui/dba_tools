#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/1/3 15:33
# @Author : ma.fei
# @File : performance_report.py
# @Software: PyCharm

import os
import re
import pymysql
import pymongo
import pymysql as py
import pandas as pd
import datetime as dt
import openpyxl
import zipfile
import pathlib
import smtplib
from datetime import datetime,date,timedelta
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class SendMail(object):
    '''
    https://cloud.tencent.com/developer/article/1598257
    '''
    def __init__(self,sender,Cc,title,content):
        self.sender = sender.split(',')  #发送地址
        self.Cc = Cc.split(',')  # 抄送地址
        self.title = title  # 标题
        self.content = content  # 发送内容
        self.sys_sender = '190343@lifeat.cn'  # 系统账户
        self.sys_pwd = 'R86hyfjobMBYR76h'  # 系统账户密码

    def send(self,file_list):
        """
        发送邮件
        :param file_list: 附件文件列表
        :return: bool
        """
        try:
            # 创建一个带附件的实例
            msg = MIMEMultipart()
            # 发件人格式
            msg['From'] = formataddr(["技术服务部", self.sys_sender])
            # 收件人格式
            print('a=',[formataddr(["", u]) for u in self.sender])
            msg['To'] = sender

            # 抄送人
            msg['Cc'] = Cc

            # 邮件主题
            msg['Subject'] = self.title

            # 邮件正文内容
            msg.attach(MIMEText(self.content, 'plain', 'utf-8'))

            # 多个附件
            for file_name in file_list:
                print("file_name",file_name)
                # 构造附件
                xlsxpart = MIMEApplication(open(file_name, 'rb').read())
                # filename表示邮件中显示的附件名
                xlsxpart.add_header('Content-Disposition','attachment',filename = '%s'%file_name)
                msg.attach(xlsxpart)

            # SMTP服务器
            server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465,timeout=10)
            # 登录账户
            server.login(self.sys_sender, self.sys_pwd)
            # 发送邮件
            #server.sendmail(self.sys_sender, [self.sender, ], msg.as_string())
            server.sendmail(self.sys_sender,
                            [formataddr(["", u]) for u in self.sender]+
                            [formataddr(["", u]) for u in self.Cc],
                            msg.as_string())
            # 退出账户
            server.quit()
            return True
        except Exception as e:
            print(e)
            return False

def file_to_zip(path_original, path_zip):
    '''
     作用：压缩文件到指定压缩包里
     参数一：压缩文件的位置
     参数二：压缩后的压缩包
    '''
    # 提前读取，避免把压缩包自己加上去
    # 这里用list()做一个克隆提前执行下，不然会在后面循环时才执行这一引用，如果压缩包在这个路径下，会将它读取进来。
    f_list = list(pathlib.Path(path_original).glob("**/*"))
    z = zipfile.ZipFile(path_zip, 'w')
    #print('len(path_original)=',len(path_original))
    for f in f_list:
        #print('f=',f,str(f),str(f)[len(path_original):])
        z.write(f, str(f)[len(path_original):])
    z.close()

def get_last_month26_to_this_month25(strftime_str="%Y-%m-%d"):
    today = date.today()
    day25_of_this_month = date(today.year, today.month, 25)
    day25_of_this_month = day25_of_this_month.strftime(strftime_str)
    last_day_of_last_month = date(today.year, today.month, 1) - timedelta(1)
    day26_of_last_month = date(last_day_of_last_month.year, last_day_of_last_month.month, 26)
    day26_of_last_month = day26_of_last_month.strftime(strftime_str)
    return day26_of_last_month, day25_of_this_month

def write_perf_report(p_rq_start,p_rq_end,p_week_rq):
    开始日期, 结束日期, 周报日期 = p_rq_start, p_rq_end, p_week_rq
    # 连接数据库，执行SQL语句
    连接对象 = py.connect(host='rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', user='hopsonone_do',passwd='8Loxk2IJxaenJkE3', database='hopsonone_do')
    项目文件夹 = output_dir + '业绩上报数据-' + 周报日期
    os.mkdir(项目文件夹)
    # 查询业绩上报给数据
    cr = 连接对象.cursor()
    cr.execute('drop table if exists business_report')
    cre_temp_tab = """CREATE TABLE business_report AS 
SELECT
a.`场馆名称`,
a.`月份`,
a.`日期`,
b.`楼层`,
a.business_id AS '店铺Id',
b.`店铺名称`,
b.`铺位号`,
a.`上报时间`,
a.`锁定时间`,
a.`上报判断`,
a.`锁定判断`,
IF(a.`上报判断` != '上报正常', a.`上报判断`,
IF(a.`锁定判断` != '锁定正常', a.`锁定判断`,
'均正常')) AS '状态',
IF(a.`上报判断` = '未上报', '1、未上报',
IF(a.`上报判断` = '上报延期', '2、上报延期',
IF(a.`锁定判断` = '锁定延期','3、锁定延期',
IF(a.`锁定判断` = '未锁定', '4、未锁定',
'均正常')))) AS '问题归类',
c.`负责人`,
"" AS '是否统计'
FROM
(SELECT
	s.market_id,
	s.business_id,
	m.market_name AS '场馆名称',
	MONTH(s.trade_date) AS '月份',
	s.trade_date AS '日期',
	s.report_time AS '上报时间',
	s.lock_time AS '锁定时间',
	IF(s.report_time IS NULL,"未上报",IF(HOUR(TIMEDIFF(s.report_time, STR_TO_DATE(s.trade_date, '%Y-%m-%d %H')))<= 35,"上报正常","上报延期")) AS "上报判断",
	#销售日当天到第二天上午12点前上报，为上报正常
	IF(s.lock_time IS NULL,"未锁定",IF(HOUR(TIMEDIFF(s.audit_time, STR_TO_DATE(s.trade_date, '%Y-%m-%d %H')))<= 35,"锁定正常","锁定延期")) AS "锁定判断"
	#销售日当天到第二天上午12点前锁定，为锁定正常
FROM shop_side_operation_real_time.sales_report_day s,
merchant_entity.market m
WHERE s.market_id IN (108,110,132,164,213,218,237,234,278,287,306,327)
AND s.business_id NOT IN (5403,5441,16871,21945,21956,2202248604,50166,51934,56486,77814,79369,93327,93328,93330)
AND DATE(s.trade_date)>='{}'
AND DATE(s.trade_date)<='{}'
AND s.market_id=m.id) a
LEFT JOIN
(SELECT
b1.market_id,
d.dic_desc AS '楼层',
b1.store_id AS '店铺Id',
b1.store_name AS '店铺名称',
b1.store_berth AS '铺位号',
b1.apply_status
FROM
(SELECT b.market_id,b.store_id,b.store_name,b.store_berth,b.floor_type,b.apply_status
FROM merchant_entity.entity_store b
WHERE b.market_id IN (108,110,132,164,213,218,237,234,278,287,306,327)) b1
LEFT JOIN
(SELECT s1.market_id,s1.dic_desc,s1.dic_value
FROM hopsonone_cms.sys_dic s1
WHERE s1.type_name = 'floortype'
AND s1.market_id IN (108,110,132,164,213,218,237,234,278,287,306,327)) d
ON b1.market_id=d.market_id AND d.dic_value=b1.floor_type) b
ON a.market_id=b.market_id AND a.business_id=b.`店铺Id`
LEFT JOIN
(SELECT u.business_id,
GROUP_CONCAT(u.user_name) AS '负责人'
FROM merchant_entity.business_hs_user u
WHERE u.business_id!=0
AND LENGTH(u.user_name)>1
GROUP BY u.business_id
HAVING LENGTH(GROUP_CONCAT(u.user_name))>1) c
ON a.business_id=c.business_id
WHERE b.apply_status=1
ORDER BY a.`场馆名称`,b.`店铺Id`,a.`日期` DESC""".format(开始日期,结束日期)
    cr.execute(cre_temp_tab)
    业绩上报sql = """select * from business_report a ORDER BY a.`场馆名称`,a.`店铺Id`,a.`日期` DESC"""
    res = pd.read_sql(业绩上报sql, con=连接对象)
    with pd.ExcelWriter(项目文件夹 + '/' + '业绩上报数据-' + 周报日期 + '.xlsx') as 表:
        res.to_excel(表, sheet_name='业绩上报', index=False)

if __name__ == '__main__':
    # add by ma.fei
    output_dir = './out/业绩上报数据/'
    os.system('rm -rf ./out/业绩上报数据/*')
    start_rq, end_rq = get_last_month26_to_this_month25()
    week_rq = start_rq[5:7] + start_rq[8:] + '~' + end_rq[5:7] + end_rq[8:]

    print('合生通项目组-业绩上报数据:')
    print('-----------------------------')
    print('开始日期：', start_rq,type(start_rq))
    print('结束日期：', end_rq,type(end_rq))
    print('周报日期：', week_rq)
    print('-----------------------------')

    write_perf_report(start_rq,end_rq,week_rq)

    # 文件打包
    path_original = 'out/业绩上报数据'
    path_zip = './out/业绩上报数据/业绩上报数据-{}.zip'.format(week_rq)
    zip_name = '业绩上报数据-{}.zip'.format(week_rq)

    file_to_zip(path_original, path_zip)
    print('文件打包完成!')

    # 发送邮件及附件
    sender = '190634@lifeat.cn,101540@cre-hopson.com,808080@cre-hopson.com,852673@cre-hopson.com'
    Cc = '190343@lifeat.cn,820987@cre-hopson.com'

    title = '合生通业绩上报数据-{}'.format(week_rq)
    content = '''各位领导：
          {}
          详见附件,请查收。
          '''.format(title)
    # 附件列表
    os.chdir('./out/业绩上报数据')
    file_list = [zip_name]
    ret = SendMail(sender, Cc,title, content).send(file_list)
    print(ret, type(ret))
    print('邮件邮件完成')