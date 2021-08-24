#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 13:44
# @Author : ma.fei
# @File : db_user.py.py
# @Software: PyCharm

import pymysql
import traceback
from .settings import cfg


''' 
CREATE TABLE xs(
     id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
     NAME VARCHAR(40),
     age INT,
     gender VARCHAR(3)
 )
'''

#返回字典
db = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     cursorclass = pymysql.cursors.DictCursor,
                     autocommit=True)

#返回元组
db2 = pymysql.connect(host=cfg['ip'],
                     port=cfg['port'],
                     user=cfg['user'],
                     passwd=cfg['password'],
                     db=cfg['db'],
                     charset='utf8',
                     autocommit=True)

def save_user(name,age,gender):
    try:
        st = "insert into xs(name,age,gender) values('{}','{}','{}')".format(name,age,gender)
        cr = db.cursor()
        print('Execute sql:{}'.format(st))
        cr.execute(st)
        return {'code':0,'msg':'保存成功!'}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':'保存失败!({})'.format(str(e))}


def query_user(name,gender):
    try:
        if gender!='':
           st = "select name,age,gender from xs where name like '%{}%' and gender='{}'".format(name,gender)
        else:
           st = "select name,age,gender from xs where name like '%{}%'".format(name)
        cr = db2.cursor()
        print('Execute sql:{}'.format(st))
        cr.execute(st)
        rs = cr.fetchall()
        return {'code':0,'msg':rs}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':'查询失败!({})'.format(str(e))}


'''
    1、代码大类表
    DROP TABLE IF EXISTS `t_dmlx`;
    CREATE TABLE `t_dmlx` (
      `dm` VARCHAR(10) NOT NULL COMMENT '大类代码',
      `mc` VARCHAR(100) NOT NULL COMMENT '大类名称',
      PRIMARY KEY (`dm`),
      KEY `idx_t_dmlx` (`dm`)
    ) ENGINE=INNODB  CHARSET=utf8;
    
    2、代码小类表
    DROP TABLE IF EXISTS `t_dmmx`;
    CREATE TABLE `t_dmmx` (
      `dm` VARCHAR(10) NOT NULL  COMMENT '代码大类',
      `dmm` VARCHAR(200) NOT NULL COMMENT '代码小类',
      `dmmc` VARCHAR(100) NOT NULL COMMENT '小类名称',
      PRIMARY KEY (`dm`,`dmm`),
      KEY `idx_t_dmmx` (`dm`,`dmm`)
    ) ENGINE=INNODB  CHARSET=utf8;
    
    3、初始化性别代码
    INSERT INTO t_dmlx(dm,mc) VALUES('01','性别');
    INSERT INTO t_dmmx (dm,dmm,dmmc) VALUES('01','1','男');
    INSERT INTO t_dmmx (dm,dmm,dmmc) VALUES('01','2','女');

'''

def get_genders():
    st = "select dmm,dmmc from t_dmmx where dm='01'"
    cr = db2.cursor()
    print('Execute sql:{}'.format(st))
    cr.execute(st)
    rs = cr.fetchall()
    return rs
