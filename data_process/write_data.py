#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/28 10:01
# @Author : ma.fei
# @File : write_data.py
# @Software: PyCharm

import warnings
import pymysql

settings = {
    "host"         : "39.107.70.55",
    "port"         : "3306",
    "user"         : "root",
    "passwd"       : "root321HOPSON",
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

def write_detail(p_db):
    # 电商明细
    print('-- 电商明细...')
    st = """SELECT 
               '1640175934112731138' AS `income_no`,
                concat('16401759341127311',(@rownum := @rownum+1)) AS `sub_income_no`,
                '2023-03' as `month`,
                b.business_id,
                b.business_name,
                c.subject_no as `payer_subject_no`,
                c.member_name as `payer_subject_name`,
                c.certificate_number as `payer_social_number`,
                '0501044' as `payee_subject_no`,
                '北京合商云汇科技有限公司' as `payee_subject_name`,
                 b.je as `amount`,
                 a.relation_order_count as `order_num`,
                 '4' as `invoice_status`,
                 NOW() as `create_time`,
                 NOW() as `update_time`
            FROM statis_commission_day a,bzd b ,wallet_account_bank_info c,(SELECT @rownum :=39) d
            WHERE a.payer_biz_id = b.`business_id`
              AND a.income_item!=3 AND b.status='1' 
              AND b.business_id=c.business_id
              AND c.bind_status='3'
              AND a.income_item ='1'"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    st="""insert into business_finance.bfi_system_collection_income_order_detail(`income_no`,`sub_income_no`,`month`,`business_id`,`business_name`,`payer_subject_no`,`payer_subject_name`,`payer_social_number`,`payee_subject_no`,`payee_subject_name`,`amount`,`order_num`,`invoice_status`,`create_time`,`update_time`) values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');"""
    for r in rs:
        t = st.format(r['income_no'],str(r['sub_income_no']),r['month'],
                  r['business_id'], r['business_name'], r['payer_subject_no'],
                  r['payer_subject_name'], r['payer_social_number'], r['payee_subject_no'],
                  r['payee_subject_name'], r['amount'], r['order_num'],
                  r['invoice_status'], r['create_time'], r['update_time'])
        print(t)

    # 门店明细
    print('-- 门店明细...')
    st = """SELECT 
              '1640175934112731139' AS `income_no`,
               concat('16401759341127311',(@rownum := @rownum+1)) AS `sub_income_no`,
               '2023-03' as `month`,
               b.business_id,
               b.business_name,
               c.subject_no as `payer_subject_no`,
               c.member_name as `payer_subject_name`,
               c.certificate_number as `payer_social_number`,
               '0501044' as `payee_subject_no`,
               '北京合商云汇科技有限公司' as `payee_subject_name`,
                b.je as `amount`,
                a.relation_order_count as `order_num`,
                '4' as `invoice_status`,
                NOW() as `create_time`,
                NOW() as `update_time`
           FROM statis_commission_day a,bzd b ,wallet_account_bank_info c,(SELECT @rownum :=43) d
           WHERE a.payer_biz_id = b.`business_id`
             AND a.income_item!=3 AND b.status='1' 
             AND b.business_id=c.business_id
             AND c.bind_status='3'
             AND a.income_item ='2'"""
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    st = """insert into business_finance.bfi_system_collection_income_order_detail(`income_no`,`sub_income_no`,`month`,`business_id`,`business_name`,`payer_subject_no`,`payer_subject_name`,`payer_social_number`,`payee_subject_no`,`payee_subject_name`,`amount`,`order_num`,`invoice_status`,`create_time`,`update_time`) values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');"""
    for r in rs:
        t = st.format(r['income_no'], str(r['sub_income_no']), r['month'],
                      r['business_id'], r['business_name'], r['payer_subject_no'],
                      r['payer_subject_name'], r['payer_social_number'], r['payee_subject_no'],
                      r['payee_subject_name'], r['amount'], r['order_num'],
                      r['invoice_status'], r['create_time'], r['update_time'])
        print(t)


def write_order(p_db):
    st = "SELECT * FROM `statistics_commission_acctinfo` where account_no='{}'"
    cr = p_db.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs[0]

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    db = get_db()
    write_detail(db)