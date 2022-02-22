#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/19 10:20
# @Author : ma.fei
# @File : transfer.py.py
# @Software: PyCharm

import json
import time
from   study.day0419.db_transfer import get_domain_name_by_id,get_nginx_conf_by_id,get_server_by_id,get_settings,write_percent
from   study.day0419.utils import write_file,ftp_helper

def transfer_data(p_domain_id,p_port,p_target_dir,p_nginx_conf_id,p_servers):
    # 初始化进度条
    write_percent(0, '')

    # 通过 p_domain_id 获取 域名称
    domain_name =  get_domain_name_by_id(p_domain_id)

    # 通过nginx_id 获取 nginx配置文件
    nginx_conf  =  get_nginx_conf_by_id(p_nginx_conf_id)

    # 根据域名修改nginx配置
    nginx_conf = nginx_conf.replace('$$SERVER$$',domain_name).replace('$$PORT$$',p_port)

    # 将配置写入./temp/nginx.conf
    write_file('nginx.conf',nginx_conf)

    # 将 server key 写入 ./tmp/keys 文件
    write_file('keys',get_settings('server_key'))

    # 循环传输nginx.conf文件至目标服务器
    n_total = len(p_servers.split(','))
    n_cur = 0
    for id in p_servers.split(','):
        svr =  get_server_by_id(id)
        cfg = json.loads(svr.get('cfg'))
        ftp = ftp_helper(cfg)
        v_log = 'Transfering nginx.conf to {}...'.format(cfg['ip'])
        ftp.transfer('./temp/nginx.conf','{}/nginx.conf'.format(p_target_dir))
        n_cur = n_cur +1
        n_percent = str(round(n_cur/n_total,4)*100)
        print('n_percent=',n_percent)
        write_percent(n_percent,v_log)
        ftp.close()