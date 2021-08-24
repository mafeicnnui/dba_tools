#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 13:44
# @Author : ma.fei
# @File : db_user.py.py
# @Software: PyCharm

import pymysql
from .settings import cfg

'''
CREATE TABLE `t_dmlx` (
  `dm` VARCHAR(10) NOT NULL COMMENT '大类代码',
  `mc` VARCHAR(100) NOT NULL COMMENT '大类名称',
  PRIMARY KEY (`dm`),
  KEY `idx_t_dmlx` (`dm`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;


CREATE TABLE `t_dmmx` (
  `dm` VARCHAR(10) NOT NULL COMMENT '代码大类',
  `dmm` VARCHAR(200) NOT NULL COMMENT '代码小类',
  `dmmc` VARCHAR(100) NOT NULL COMMENT '小类名称',
  PRIMARY KEY (`dm`,`dmm`),
  KEY `idx_t_dmmx` (`dm`,`dmm`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `t_server` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `desc` varchar(50) DEFAULT NULL COMMENT '描述',
  `cfg` text NOT NULL COMMENT '配置',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb4

CREATE TABLE `t_settings` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(100) DEFAULT NULL,
  `value` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4

CREATE TABLE `t_templete` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `description` varchar(40) DEFAULT NULL,
  `contents` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4

CREATE TABLE `t_progress` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `percent` varchar(20) DEFAULT NULL,
  `log` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4




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


def get_servers():
    st = "select id,`desc` from t_server order by id"
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_server_by_id(p_server_id):
    st = "select * from t_server t  where id='{}'".format(p_server_id)
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs

def get_settings(p_key):
    st = "select `value` from t_settings  where `key`='{}'".format(p_key)
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]


def get_dm_from_dmmx(p_lx):
    st = "select dmm,dmmc from t_dmmx where dm='{}' order by dm,dmm".format(p_lx)
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_nginx_conf():
    st = "select id,description from t_templete  order by id"
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def write_percent(p_percent,p_log):
    st = "update  t_progress set percent='{}',log='{}' where id=3".format(p_percent,p_log)
    cr = db2.cursor()
    cr.execute(st)

def read_percent():
    st = "select  percent,log from t_progress  where id=3"
    cr = db.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs

def get_domain_name_by_id(p_id):
    st = "select dmmc from t_dmmx where dm='01' and dmm='{}'".format(p_id)
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]

def get_nginx_conf_by_id(p_nginx_id):
    st = "select contents from t_templete where id='{}' ".format(p_nginx_id)
    cr = db2.cursor()
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]
