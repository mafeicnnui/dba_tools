#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/24 11:09
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

'''
1.获取有效商户
2.获取某个商户金额 totalAmount、refundAmount、netIncomeAmount、relationOrderCount
'''
import datetime
import pymysql

settings = {
    "host"         : "39.107.70.55",
    "port"         : "3306",
    "user"         : "root",
    "passwd"       : "xxxx",
    "db"           : "test",
    "db_charset"   : "utf8"
}

def get_db():
    conn = pymysql.connect(
       host = settings['host'],
       port=int(settings['port']),
       user=settings['user'],
       passwd=settings['passwd'],
       db=settings['db'],
       charset='utf8',
       cursorclass=pymysql.cursors.DictCursor,
       autocommit=True)
    return conn

def get_wallet_account_info(p_db):
    '''
       p_manage_mode : 经验模式 0=电商，1=门店，2= 写字楼
    '''
    st = """SELECT 
               wai.out_biz_no AS business_id,
               wai.user_name,
               wai.manage_mode,
               wai.account_no
            FROM test.wallet_account_info wai 
            WHERE wai.user_type=3 
              AND wai.status=1 
              AND wai.market_id <>90000
              AND EXISTS(
                SELECT 1 FROM test.`wallet_charge_cash`
                WHERE `status` = 1 
                 AND trade_type = 9
                 AND account_no = wai.account_no
                 AND trade_time >= '2021-01-01 00:00:00'
                 AND trade_time <= '2022-12-30 23:59:59')"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_wallet_charge_cash_info(p_db,p_account_no):
    st = """SELECT SUM(temp.totalAmount) as totalAmount,
                   SUM(relationOrderCount) as relationOrderCount
            FROM (SELECT account_no                   AS accountNo,
                  DATE_FORMAT(trade_time, '%Y-%m-%d') AS tradeDate,
                  SUM(service_amount)                 AS totalAmount,
                  COUNT(1)                            AS relationOrderCount
            FROM test.`wallet_charge_cash`
            WHERE `status` = 1
             AND trade_type = 9
             AND account_no ='{}'     
             AND trade_time >= '2021-01-01 00:00:00'
             AND trade_time <= '2022-12-30 23:59:59'
            GROUP BY account_no, trade_time) temp
   """.format(p_account_no)
    #print(st)
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs

def get_statistics_commission_acctinfo(p_db,p_account_no):
    st = "SELECT * FROM `statistics_commission_acctinfo` where account_no='{}'".format(p_account_no)
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs[0]

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_sql(v_sql):
    return v_sql.replace("'","\\'")

ACCOUNT_CHARGE ='897418001459761152'   # 门店账户
INSERT_TEMPLETE='''insert into  statis_commission_day(`bill_month`,`payee_biz_id`,`payee_acct_no`,`payer_biz_id`,`payer_acct_no`,`payer_biz_name`,`source_type`,`manage_mode`,`income_item`,`total_amount`,`refund_amount`,`statis_date`,`relation_order_count`,`statis_type`,`create_time`,`update_time`) values('{}','{}','{}','{}','{}','{}',{},{},{},{},{},'{}',{},{},'{}','{}');'''

if __name__ == '__main__':
    db = get_db()
    with open('charge.sql', 'w') as f:
        for account_info in get_wallet_account_info(db):
            wallet_charge_cash_info = get_wallet_charge_cash_info(db,account_info['account_no'])
            statistics_commission_acctinfo = get_statistics_commission_acctinfo(db,ACCOUNT_CHARGE)
            now =  get_time()
            tpl = INSERT_TEMPLETE.format(
                         #1
                         '2022',
                         statistics_commission_acctinfo['user_no'],
                         statistics_commission_acctinfo['account_no'],
                         #2
                         account_info['business_id'],
                         account_info['account_no'],
                         format_sql(account_info['user_name']),
                         #3
                         '10',
                         account_info['manage_mode'],
                         '3',
                         #4
                         wallet_charge_cash_info['totalAmount'],
                         0,
                         '2022-12-30',
                         #5
                         wallet_charge_cash_info['relationOrderCount'],
                         '1',
                         now,
                         now
                     )
            print(tpl)
            f.write(tpl + '\n')