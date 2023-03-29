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

def get_wallet_account_info(p_db,p_manage_mode):
    '''
       p_manage_mode : 经验模式 0=电商，1=门店，2= 写字楼
    '''
    st = """SELECT wai.out_biz_no AS business_id,
                   MAX(wai.user_name) AS user_name,
                   MAX(wai.manage_mode) AS manage_mode,
                   MAX(wai.account_no) AS account_no
            FROM test.wallet_account_info wai 
            WHERE manage_mode={}
              AND wai.user_type=3 
              AND STATUS=1 
              AND wai.market_id <>90000 
              AND EXISTS(
                    SELECT	1
                    FROM test.order_combine oc 
                        LEFT JOIN test.settle_order so ON oc.sub_order_no=so.sub_order_no
                    WHERE oc.payee_account_no='917695185528901632' 
                      AND oc.`business_id`= wai.out_biz_no
                      AND oc.status IN (2,3) 
                      AND oc.exec_status=1
                      AND ( (oc.unfrozen_time >='2021-01-01 00:00:00' AND oc.unfrozen_time <='2022-12-30 23:59:59' )
                            OR (oc.arrival_mode=2 
                                  AND oc.unfrozen_time IS NULL 
                                AND oc.combine_time >='2021-01-01 00:00:00' AND oc.combine_time <='2022-12-30 23:59:59'             
                                 )
                        ) 
                      ) 
                      GROUP BY wai.out_biz_no""".format(p_manage_mode)
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_wallet_flow_details_info(p_db,p_account_no,p_business_id):
    st = """SELECT
       IFNULL(SUM(CASE direction WHEN  2 THEN confirm_amount ELSE 0 END),0) AS totalAmount,
       IFNULL(SUM(CASE direction WHEN  1 THEN confirm_amount ELSE 0 END),0) AS refundAmount,
       (IFNULL(SUM(CASE direction WHEN  2 THEN confirm_amount ELSE 0 END),0) - IFNULL(SUM(CASE direction WHEN  1 THEN confirm_amount ELSE 0 END),0)) AS netIncomeAmount,
       COUNT(1) AS relationOrderCount,
       MAX(source_type) AS source_type
FROM test.`wallet_flow_details` WFD
WHERE WFD.account_no='{}' AND WFD.sub_trade_no IN ( 
    SELECT oc.sub_order_no
    FROM test.order_combine oc
        LEFT JOIN test.settle_order so ON oc.sub_order_no=so.sub_order_no
    WHERE oc.business_id ={}
      AND payee_account_no='{}' 
      AND oc.status IN (2,3) AND oc.exec_status=1
      AND ( (oc.unfrozen_time >='2021-01-01 00:00:00' AND oc.unfrozen_time <='2022-12-30 23:59:59' )
                OR (oc.arrival_mode=2 
                      AND oc.unfrozen_time IS NULL 
                    AND oc.combine_time >='2021-01-01 00:00:00' AND oc.combine_time <='2022-12-30 23:59:59'             
                     )
            ) 
    )   
AND source_type IN (11,23)
AND trade_time >= '2021-01-01 00:00:00' AND trade_time <='2022-12-30 23:59:59'
AND function_flag IN (2,3)
AND `status` = 1""".format(p_account_no,p_business_id,p_account_no)
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

MANAGE_MODE_BUSINESS='0'
ACCOUNT_NO_SHOP ='917695185528901632'   # 门店账户
INSERT_TEMPLETE='''insert into  statis_commission_day(`bill_month`,`payee_biz_id`,`payee_acct_no`,`payer_biz_id`,`payer_acct_no`,`payer_biz_name`,`source_type`,`manage_mode`,`income_item`,`total_amount`,`refund_amount`,`statis_date`,`relation_order_count`,`statis_type`,`create_time`,`update_time`) values('{}','{}','{}','{}','{}','{}',{},{},{},{},{},'{}',{},{},'{}','{}');'''

if __name__ == '__main__':
    db = get_db()
    with open('business.sql', 'w') as f:
        for account_info in get_wallet_account_info(db,MANAGE_MODE_BUSINESS):
            wallet_flow_details_info = get_wallet_flow_details_info(db,ACCOUNT_NO_SHOP,account_info['business_id'])
            statistics_commission_acctinfo = get_statistics_commission_acctinfo(db,ACCOUNT_NO_SHOP)

            if wallet_flow_details_info['totalAmount'] != 0:
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
                             wallet_flow_details_info['source_type'],
                             account_info['manage_mode'],
                             '1',
                             #4
                             wallet_flow_details_info['totalAmount'],
                             wallet_flow_details_info['refundAmount'],
                             '2022-12-30',
                             #5
                             wallet_flow_details_info['relationOrderCount'],
                             '1',
                             now,
                             now
                         )
               print(tpl)
               f.write(tpl + '\n')