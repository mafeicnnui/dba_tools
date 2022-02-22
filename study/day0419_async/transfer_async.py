#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/19 10:20
# @Author : ma.fei
# @File : transfer.py.py
# @Software: PyCharm

import json
import time
import traceback
from   study.day0419_async.db_transfer_async import get_domain_name_by_id,get_nginx_conf_by_id,get_server_by_id,get_settings,write_percent
from   study.day0419.utils import write_file,ftp_helper

async def transfer_data(p_domain_id,p_port,p_target_dir,p_nginx_conf_id,p_servers):
    try:

        # 初始化进度条
        await write_percent(0,'')

        # 通过 p_domain_id 获取 域名称
        domain_name = await get_domain_name_by_id(p_domain_id)

        # 通过nginx_id 获取 nginx配置文件
        nginx_conf  = await get_nginx_conf_by_id(p_nginx_conf_id)

        # 根据域名修改nginx配置
        nginx_conf = nginx_conf.replace('$$SERVER$$',domain_name).replace('$$PORT$$',p_port)

        # 将配置写入./temp/nginx.conf
        write_file('nginx.conf',nginx_conf)

        # 将 server key 写入 ./tmp/keys 文件
        write_file('keys',await get_settings('server_key'))

        #总传输文件个数
        n_total = len(p_servers.split(','))

        #当前传输个数
        n_cur = 0

        # 循环传输nginx.conf文件至目标服务器
        for id in p_servers.split(','):
            svr = await get_server_by_id(id)
            cfg = json.loads(svr.get('cfg'))
            ftp = ftp_helper(cfg)
            v_log = 'Transfering nginx.conf to {}...'.format(cfg['ip'])
            ftp.transfer('./temp/nginx.conf','{}/nginx.conf'.format(p_target_dir))
            n_cur = n_cur +1
            n_percent = str(round(n_cur/n_total,4)*100)
            await write_percent(n_percent,v_log)
            ftp.close()
            time.sleep(1)
        return {'code':200,'msg':'success'}
    except Exception as e:
        traceback.print_exc()
        return {'code':500,'msg':str(e)}

