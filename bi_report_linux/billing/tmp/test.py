#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/5/27 16:37
# @Author : ma.fei
# @File : test.py.py
# @Software: PyCharm
import pymysql
import pandas as pd
import datetime as dt
import os
import openpyxl

# 连接数据库，执行SQL语句
连接对象 = pymysql.connect(host='rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', user='hopsonone_do',
                  passwd='8Loxk2IJxaenJkE3', database='hopsonone_do')
# 会员注册量SQL语句
当月会员注册量sql = """
select ma.market_name as '商场名称',count(a.m_id) as '会员量'
from hopsonone_members.members a,
merchant_entity.market ma
where a.market_id in (110,108,218)
and a.create_time>=%(date_op)s
and a.create_time<=concat(%(date_ed)s,' 23:59:59')
and a.market_id=ma.id
GROUP BY a.market_id
"""
# 计时
当月会员注册量 = pd.read_sql(当月会员注册量sql, con=连接对象, params={'date_op': '2024-01-01', 'date_ed': '2024-01-31'}, index_col='商场名称')
print(当月会员注册量)
print('-------------------------')
print(当月会员注册量.loc['成都温江合生汇']['会员量'])

conn = pymysql.connect(host='rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com', user='hopsonone_do', passwd='8Loxk2IJxaenJkE3',
                  database='shop_bill')

billing_sql = """SELECT o.market_id as '商场ID',
	ROUND(SUM(CASE WHEN o.tran_type='0002' THEN o.total_fee ELSE 0 END)/100,2) AS 'B2B缴费金额',
	SUM(CASE WHEN o.tran_type='0002' THEN 1 ELSE 0 END) AS 'B2B缴费笔数',
	ROUND(SUM(CASE WHEN o.tran_type='0001' THEN o.total_fee ELSE 0 END)/100,2) AS 'B2C缴费金额',
	SUM(CASE WHEN o.tran_type='0001' THEN ROUND(o.total_fee*0.002/100,2) ELSE 0 END) AS 'B2C缴费手续费',
	ROUND(SUM(CASE WHEN o.tran_type='0009' THEN o.total_fee ELSE 0 END)/100,2) AS '扫码付缴费金额',
	SUM(CASE WHEN o.tran_type='0009' THEN o.handling_fee/100 ELSE 0 END) AS '扫码付缴费手续费'
FROM hft_business_pay.bill_order o
WHERE o.complete_date>="2023-10-01"
	AND o.complete_date<=CONCAT("2023-12-31",' 23:59:59')
	AND o.state=3	
GROUP BY o.market_id"""

billing_data = pd.read_sql(billing_sql, con=conn, index_col='商场ID')
print('---------------------')
print(billing_data)
print(billing_data.loc[108]['B2B缴费金额'])