#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/7 13:36
# @Author : ma.fei
# @File : 传输文件至远程服务器.py
# @Software: PyCharm

import paramiko
import warnings

# 输出字典
def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    print('-'.ljust(85,'-'))

# ssh 客户端类
class ssh_helper:
    def __init__(self,cfg,timeout=6):
        self.server_ip   = cfg['server_ip']
        self.server_port = int(cfg['server_port'])
        self.username    = cfg['server_user']
        self.password    = cfg['server_pass']
        self.ssh         = paramiko.SSHClient()
        self.timeout     = timeout
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=self.server_ip,port=self.server_port,username=self.username,password=self.password)

    def exec(self,cmd):
        stdout_lines = []
        stderr_lines = []
        cmd_exec_status = True
        try:
            stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout=self.timeout)
            stdout_lines = stdout.readlines()
            stderr_lines = stderr.readlines()
            if stdout.channel.recv_exit_status() != 0:
               raise paramiko.SSHException()
            print('Execute remote cmd: {}'.format(cmd))
        except paramiko.SSHException as e:
            print("Failed to execute the command on '{}': {}".format(self.server_ip, str(e)))
            if len(stderr_lines) > 0:
                print("Error reported by {}: {}".format(self.server_ip, "\n".join(stderr_lines)))
            cmd_exec_status = False
        return {'status': cmd_exec_status, 'stdout': stdout_lines}

    def close(self):
        self.ssh.close()

# 远程服务器配置
cfg = {
    'server_ip':'10.2.39.17',
    'server_port':'65508',
    'server_user':'hopson',
    'server_pass':'Tong2@01!8*',
    'run_cmd'    :'crontab -l'
}

# 不显示告警信息
warnings.filterwarnings("ignore")

# 创建ssh客户端
ssh = ssh_helper(cfg)

# 打印配置信息
print_dict(cfg)

# 执行远程命令
res = ssh.exec(cfg['run_cmd'])
if res['status']:
    res = {'code': 200, 'msg': res['stdout']}
else:
    res = {'code': -1, 'msg': 'failure!'}

# 输出命令结果
if res['code']==200:
  for task in res['msg']:
      print(task[:-1])
else:
   print('运行失败!')