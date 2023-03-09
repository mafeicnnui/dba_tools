#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/3/6 13:37
# @Author : ma.fei
# @File : alert.py.py
# @Software: PyCharm
import traceback
import datetime
import pymysql

settings = {
    "host"         : "xxxxxxxxxxxx",
    "port"         : "3306",
    "user"         : "puppet",
    "passwd"       : "xxxxxxxxxxxxxxxx",
    "db"           : "test",
    "db_charset"   : "utf8"
}

def get_db():
    conn = pymysql.connect(host=settings['host'],
                           port=int(settings['port']),
                           user=settings['user'],
                           passwd=settings['passwd'],
                           db=settings['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_dict():
    conn = pymysql.connect(host=settings['host'],
                           port=int(settings['port']),
                           user=settings['user'],
                           passwd=settings['passwd'],
                           db=settings['db'],
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    return conn

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_mid(settings):
    cr = settings['dbd'].cursor()
    st = '''SELECT DISTINCT m_id as mid
FROM test.members_points_detail_20230306
WHERE behavior IN (5, 8, 20, 26, 27)
  AND state = 0
  AND market_id ={} 
  AND create_time >= '2022-07-01 00:00:00'
    '''.format(settings['market_id'])
    cr.execute(st)
    rs=cr.fetchall()
    cr.close()
    return rs

def get_mid_max_rq(settings,m):
    cr = settings['dbd'].cursor()
    st = '''SELECT DATE_FORMAT(MAX(create_time),'%Y-%m-%d %H:%i:%s') as rq
FROM test.members_points_detail_20230306
WHERE behavior IN (5, 8, 20, 26, 27)
  AND state = 0
  AND market_id = {} 
  AND create_time >= '2022-07-01 00:00:00'
  AND m_id={}
        '''.format(settings['market_id'],m['mid'])
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs[0]

def get_mid_vals_business(settings,m):
    rq = get_mid_max_rq(settings,m)
    cr = settings['dbd'].cursor()
    st = '''SELECT business_id,
                   SUM(actual_point) as actual_point_sum,
                   COUNT(actual_point)  as actual_point_count,
                   '{}' as rq
FROM test.members_points_detail_20230306
WHERE behavior IN (5, 8, 20, 26, 27)
  AND state = 0
  AND market_id = {}
  AND create_time >= '2022-07-01 00:00:00'
  AND create_time BETWEEN CONCAT(DATE(DATE_SUB('{}', INTERVAL 7 DAY)),'  0:0:0') AND '{}'
  AND m_id={} group by business_id'''.format(rq['rq'],settings['market_id'],rq['rq'],rq['rq'],m['mid'])
    if settings['debug']:
       print('\nfunc get_mid_vals_business:',st)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_mid_vals_market(settings,m):
    rq = get_mid_max_rq(settings,m)
    cr = settings['dbd'].cursor()
    st = '''SELECT SUM(actual_point) as actual_point_sum,
                   COUNT(actual_point)  as actual_point_count,
                   '{}' as rq
FROM test.members_points_detail_20230306
WHERE behavior IN (5, 8, 20, 26, 27)
  AND state = 0
  AND market_id = {}
  AND create_time >= '2022-07-01 00:00:00'
  AND create_time BETWEEN CONCAT(DATE(DATE_SUB('{}', INTERVAL 7 DAY)),'  0:0:0') AND '{}'
  AND m_id={}'''.format(rq['rq'],settings['market_id'],rq['rq'],rq['rq'],m['mid'])
    if settings['debug']:
       print('\nfunc get_mid_vals_market:',st)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_rule(settings,p_type,p_point_type):
    cr = settings['dbd'].cursor()
    st = '''SELECT * FROM t_rule WHERE market_id={} and rule_type='{}' and point_type='{}' ORDER BY id'''.\
        format(settings['market_id'],p_type,p_point_type)
    cr.execute(st)
    rs = cr.fetchall()
    cr.close()
    return rs

def get_expr(p_settings,p_expr,p_vals):
    s = p_expr\
        .replace('$count',str(p_vals['actual_point_count']))\
        .replace('$sum',str(p_vals['actual_point_sum']))
    if p_settings['debug']:
       print('\nfunc get_expr:',s)
    return s

def write_log(e):
    cr = settings['dbd'].cursor()
    st = '''INSERT INTO t_rule_res(m_id,business_id,actual_point_count,actual_point_sum,max_rq,rule_type,point_type,description)
 VALUES({},'{}',{},{},'{}','{}','{}','{}')'''.\
        format(e['mid'],
               e['business_id'],
               int(e['actual_point_count']),
               int(e['actual_point_sum']),
               e['max_date'],
               e['rule_type'],
               e['point_type'],
               e['description']
               )
    if settings['debug']:
        print('\nfunc write_log:', st)
    cr.execute(st)
    cr.close()

def init(settings):
    cr = settings['dbd'].cursor()
    st = 'truncate table t_rule_res'
    if settings['debug']:
        print('\nfunc init:', st)
    cr.execute(st)
    cr.close()


if __name__ == '__main__':
    settings.update({
        'db': get_db(),
        'dbd': get_db_dict(),
        'debug':False,
        'market_id':'132'
    })
    init(settings)
    print('get_mid for market {}...'.format(settings['market_id']))
    mids = get_mid(settings)
    rec = 1
    start_time = datetime.datetime.now()
    for m in mids:
        print('\rProcess:{}/{},elaspse:{}s...'.format(rec,len(mids),str(get_seconds(start_time))),end='')
        rec = rec +1
        try:
            # exception->business
            for r in get_mid_vals_business(settings,m):
                for e in get_rule(settings,'exception','store'):
                    if eval(get_expr(settings,e['rule_expr'],r)):
                        e.update({
                            'mid': m['mid'],
                            'business_id': r['business_id'],
                            'actual_point_count': r['actual_point_count'],
                            'actual_point_sum': r['actual_point_sum'],
                            'max_date': r['rq']
                        })
                        write_log(e)

            # exception->mareket
            for r in get_mid_vals_market(settings, m):
                for e in get_rule(settings,'exception','market'):
                    if eval(get_expr(settings, e['rule_expr'], r)):
                        e.update({
                            'mid': m['mid'],
                            'business_id': '',
                            'actual_point_count': r['actual_point_count'],
                            'actual_point_sum': r['actual_point_sum'],
                            'max_date': r['rq']
                        })
                        write_log(e)

            # alert->business
            for r in get_mid_vals_business(settings, m):
                for e in get_rule(settings,'alert', 'store'):
                    if eval(get_expr(settings, e['rule_expr'], r)):
                        e.update({
                            'mid': m['mid'],
                            'business_id': r['business_id'],
                            'actual_point_count': r['actual_point_count'],
                            'actual_point_sum': r['actual_point_sum'],
                            'max_date': r['rq']
                        })
                        write_log(e)

            # alert->mareket
            for r in get_mid_vals_market(settings, m):
                for e in get_rule(settings,'alert', 'market'):
                    if eval(get_expr(settings, e['rule_expr'], r)):
                        e.update({
                            'mid': m['mid'],
                            'business_id': '',
                            'actual_point_count': r['actual_point_count'],
                            'actual_point_sum': r['actual_point_sum'],
                            'max_date': r['rq']
                        })
                        write_log(e)
        except:
          traceback.print_exc()
          print('except=>mid=',m)

