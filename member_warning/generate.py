#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/13 8:58
# @Author : ma.fei
# @File : generate.py.py
# @Software: PyCharm
'''
SELECT * FROM t_rule_id ORDER BY market_id
213	3
132	80
164	82
110	1004
218	312
'''
import time
import pymysql
import pymongo

mysql_settings = {
    "host"         : "rm-2zer0v9g25bgu4rx43o.mysql.rds.aliyuncs.com",
    "port"         : "3306",
    "user"         : "puppet",
    "passwd"       : "Puppet@123",
    "db"           : "test",
    "db_charset"   : "utf8"
}

mongo_settings = {
    "host"         : "dds-2ze39d52c51a85f42163-pub.mongodb.rds.aliyuncs.com",
    "port"         : "3717",
    "user"         : "hopsonone_ro",
    "passwd"       :"lvOD4ljLBkREapZd",
    "auth_db"      : "admin",
    "conn_db"      : "hopsonone_rule"
}

mongo_insert_template = '''db.member_list.insert({{
    "m_id" : {},
    "list_type" : 1,
    "identity_type" : 0,
    "t_info_id" : 6,
    "market_id" : {},
    "warn_date" : {},
    "warn_num" : 1,
    "create_time" : {},
    "update_time" : {},
    "remark" : "系统处理",
    "operation_id" : 0,
    "operation_name" : "系统",
    "scene" : 1,
    "into_channel" : 3,
    "order_sort" : 1,
    "rule_content" : "{}",
    "range" : 1,
    "business_id" : "{}",
    "_class" : "cn.com.hopson.hopsonone.rule.domian.MemberList"
}});
'''

mongo_insert_status_template = '''db.member_status_record.insert({{
	"m_id" : {},
	"list_type" : 1,
	"dispose_type" : 1,
	"scene" : 1,
	"remark" : "系统处理",
	"market_id" : {},
	"operation_id" : 0,
	"operation_name" : "系统",
	"into_channel" : 3,
	"create_time" : {},
	"update_time" : {},
	"rule_id" : "{}",
	"rule_name" : "{}",
	"strategy_id" : "{}",
	"rule_content" : "{}",
	"range" : 1,
	"_class" : "cn.com.hopson.hopsonone.rule.domian.MemberStatusRecord"
}});
'''

mongo_remove_template = '''db.member_list.remove({{"m_id" :{}}});\n'''

mongo_template = {
    'insert' : mongo_insert_template,
    'insert_status': mongo_insert_status_template,
    'remove' : mongo_remove_template
}

def get_ds_mongo():
    ip            = mongo_settings['host']
    port          = mongo_settings['port']
    service       = mongo_settings['auth_db']
    user          = mongo_settings['user']
    password      = mongo_settings['passwd']
    conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
    db            = conn[service]
    db.authenticate(user, password, mechanism='SCRAM-SHA-1')
    return conn[mongo_settings['conn_db']]

def get_ds_mysql():
    conn = pymysql.connect(host=mysql_settings['host'],
                           port=int(mysql_settings['port']),
                           user=mysql_settings['user'],
                           passwd=mysql_settings['passwd'],
                           db=mysql_settings['db'],
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    return conn

def check_mongo_mid_exists(db_mongo,p_mid):
    cr_mongo = db_mongo['member_list']
    rs_mongo = cr_mongo.find({'m_id': int(p_mid)}).count()
    if rs_mongo >0 :
       return True
    else:
       return False

def get_market_alert_info(db_mysql,p_market_id):
    st="""SELECT * FROM t_rule_res_all_alert 
WHERE id IN( SELECT MAX(id) FROM t_rule_res_all_alert GROUP BY market_id,m_id)  
 AND market_id={}""".format(p_market_id)
    cr = db_mysql.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_timestamp():
    return int(round(time.time()*1000))

def get_rule_id_strategy_id(p_member,):
    st = """SELECT rule_id,strategy_id
            FROM t_rule_id  
            WHERE market_id={} and description='{}'""".format(p_member['market_id'],p_member['description'])
    cr = db_mysql.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs

if __name__ == '__main__':
    #market_id_list =[213,110,218,164,132]
    market_id_list = [110,218,164,132]
    db_mongo = get_ds_mongo()
    db_mysql = get_ds_mysql()
    for market_id in market_id_list:
        member_info = get_market_alert_info(db_mysql,market_id)
        i_write_rows = 0
        print('Reading member alert info for market:{},rows:{}'.format(market_id,len(member_info)))
        filename = './sql/member_rule_{}.sql'.format(market_id)
        with open(filename, 'w') as f:
            i_counter = 0
            for m in member_info:
                i_counter += 1
                print('\rProcess:{}/{},{}%(remove)...'.format(i_counter, len(member_info),round(i_counter/len(member_info),4)*100), end='')
                if not check_mongo_mid_exists(db_mongo,m['m_id']):
                    i_write_rows += 1
                    d = mongo_template['remove'].format(m['m_id'])
                    f.write(d)

            i_counter = 0
            for m in member_info:
                i_counter += 1
                print('\rProcess:{}/{},{}%(insert)...'.format(i_counter, len(member_info),round(i_counter/len(member_info),4)*100), end='')
                if not check_mongo_mid_exists(db_mongo,m['m_id']):
                    i= mongo_template['insert'].\
                        format(m['m_id'],
                               m['market_id'],
                               get_timestamp(),
                               get_timestamp(),
                               get_timestamp(),
                               m['description'],
                               m['business_id'] if m['business_id'] != 'None' else '')
                    f.write(i)
                    r = get_rule_id_strategy_id(m)
                    i = mongo_template['insert_status'].\
                        format(m['m_id'],
                               m['market_id'],
                               get_timestamp(),
                               get_timestamp(),
                               r['rule_id'],
                               m['description'],
                               r['strategy_id'],
                               m['description'])
                    f.write(i)

            print('\nWriting complete for market `{}` to file `{}`, rows :{}\n'.format(market_id,filename,i_write_rows))
