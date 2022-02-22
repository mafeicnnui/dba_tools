#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 14:52
# @Author : ma.fei
# @File : utils.py.py
# @Software: PyCharm

import socket
import paramiko
import traceback
from   study.day0419.db_transfer import get_settings


def get_host_ip():
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',80))
        ip=s.getsockname()[0]
    finally:
        s.close()

    return ip

def write_file(p_file,p_content):
   with open('./temp/{}'.format(p_file), 'w') as f:
      f.write(p_content)

# ssh 客户端类
class ssh_helper:
    def __init__(self,cfg):
        self.server_ip   = cfg['ip']
        self.server_port = int(cfg['port'])
        self.username    = cfg['user']
        self.ssh         = paramiko.SSHClient()
        self.timeout     = int(get_settings('ssh_timeout'))
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key_file('./temp/keys', cfg['user'])
        self.ssh.connect(hostname=self.server_ip,port=self.server_port,username=self.username,pkey=key)

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


# ftp 客户端类
class ftp_helper:
    def __init__(self, cfg):
        self.server_ip   = cfg['ip']
        self.server_port = int(cfg['port'])
        self.username    = cfg['user']
        self.timeout     = int(get_settings('ftp_timeout'))
        private_key      = paramiko.RSAKey.from_private_key_file('./temp/keys')
        self.transport   = paramiko.Transport((self.server_ip, self.server_port))
        self.transport.connect(username=self.username, pkey=private_key)
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