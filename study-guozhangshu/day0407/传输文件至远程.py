#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/7 13:36
# @Author : ma.fei
# @File : 传输文件至远程服务器.py
# @Software: PyCharm

import os
import paramiko
import traceback
import warnings

# 输出字典
def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    print('-'.ljust(85,'-'))

# ftp 客户端类
class ftp_helper:
    def __init__(self, cfg, timeout=6):
        self.server_ip   = cfg['server_ip']
        self.server_port = int(cfg['server_port'])
        self.username    = cfg['server_user']
        self.password    = cfg['server_pass']
        self.timeout     = timeout
        self.transport   = paramiko.Transport((self.server_ip, self.server_port))
        self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def transfer(self,local,remote):
        try:
            self.sftp.put(localpath=local, remotepath=remote)
            print('Script:{0} send to {1} ok.'.format(local, remote))
            return True
        except:
            traceback.print_exc()
            return False

    def close(self):
        self.transport.close()

# 远程服务器配置
cfg = {
    'server_ip':'10.2.39.17',
    'server_port':'65508',
    'server_user':'hopson',
    'server_pass':'Tong2@01!8*',
    'f_local'    :'{}/nginx.conf'.format(os.getcwd()),
    'f_remote'   :'/home/hopson/apps/usr/webserver/dba/nginx/nginx.conf'
}

# 不显示告警信息
warnings.filterwarnings("ignore")

# 创建ftp传输客户端
ftp = ftp_helper(cfg)

# 打印配置信息
print_dict(cfg)

# 传输nginx.conf文件
if not ftp.transfer(cfg['f_local'], cfg['f_remote']):
    print('输入失败!')
else:
    print('传输成功!')

# 关闭ftp连接
ftp.close()
