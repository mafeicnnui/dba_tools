#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/23 16:48
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

'''
CREATE TABLE test.t_business_log(
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  msg TEXT,
  create_time DATETIME DEFAULT  CURRENT_TIMESTAMP
);
SHOW GRANTS FOR 'puppet'@'%';

SELECT * FROM test.t_business_log

SELECT COUNT(0) FROM merchant_entity.mallcoo_unusual;  -- 39152
SELECT COUNT(0) FROM hopsonone_business.mallcoo_unusual; -- 43929

SELECT * FROM merchant_entity.mallcoo_unusual WHERE id=13071;   -- 6944b5adcd624cd6
SELECT * FROM hopsonone_business.mallcoo_unusual  WHERE id=13071;   -- 8642e6191aba4334
SELECT table_schema,table_name,COUNT(0) FROM test.t_business_log GROUP BY table_schema,table_name
'''
import time
import pymysql

def get_db():
    conn = pymysql.connect(host= 'rm-2ze9y75wip0929gy86o.mysql.rds.aliyuncs.com',
                           port= 3306,
                           user = 'puppet',
                           passwd = 'Puppet@123',
                           db  = 'information_schema',
                           charset = 'utf8mb4',
                           autocommit = True,
                           cursorclass = pymysql.cursors.DictCursor,
                           read_timeout = 60,
                           write_timeout = 60)
    return conn

def write_log(db,v1,v2,v3,v4,v5):
    cr = db.cursor()
    st = """insert into test.t_business_log(table_schema,table_name,column_name,table_id,value1,value2) 
             values('{}','{}','{}','{}','{}','{}')""".\
        format('hopsonone_business',v1,v2,v3,v4,v5)
    cr.execute(st)

def main():
    check_tab_list  = ['shop_relavance_v2','pos_relavance','device_detail','unrelavance_business_abnormal','unrelavance_hopson_abnormal','unrelavance_mallcoo_abnormal']
    db = get_db()
    for tab in check_tab_list:
        cr = db.cursor()
        cr.execute('select * from hopsonone_business.{}  order by id'.format(tab))
        rs= cr.fetchall()
        i_counter = 0
        i_error = 0
        for s in rs:
            i_counter = i_counter +1
            time.sleep(0.01)
            print('\rComparing table data for `{}`,{}/{} - {}%'.format(tab,i_counter,len(rs),100*round(i_counter/len(rs),4)),end='')
            cr.execute('select * from merchant_entity.{} where id={}'.format(tab,s['id']))
            t = cr.fetchone()
            if t is not None:
                for k,v in s.items():
                    if str(v) != str(t[k]) :
                       i_error = i_error + 1
                       msg = 'table:{},id:{},column:{},error:{},v1:{},v2:{}'.format(tab,s['id'],k,i_error,v,t[k])
                       write_log(db,tab,k,s['id'],v,t[k])

    print('\n')


if __name__ == '__main__':
     main()