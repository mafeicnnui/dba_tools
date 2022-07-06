#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/3/2 14:22
# @Author : ma.fei
# @File : test.py.py
# @Software: PyCharm

import win32crypt
import binascii
import os

def genRdp(ip, username, passwd, rdpFileName):
    pwdHash = win32crypt.CryptProtectData(passwd, u'psw', None, None, None, 0)
    pwdHash_ok = binascii.hexlify(pwdHash)
    rdpFileStr = u'''screen mode id:i:1
desktopwidth:i:1920
desktopheight:i:1080
session bpp:i:24
winposstr:s:2,3,188,8,1062,721
full address:s:{ip}
compression:i:1
keyboardhook:i:2
audiomode:i:0
redirectdrives:i:0
redirectprinters:i:0
redirectcomports:i:0
redirectsmartcards:i:0
displayconnectionbar:i:1
autoreconnection enabled:i:1
username:s:{username}
domain:s:MyDomain
alternate shell:s:
shell working directory:s:
password 51:{pwdHash_ok}
disable wallpaper:i:1
disable full window drag:i:1
disable menu anims:i:1
disable themes:i:0
disable cursor setting:i:0
bitmapcachepersistenable:i:1
    '''.format(ip=ip, username=username, pwdHash_ok=pwdHash_ok)
    # print(rdpFileStr)
    with open(rdpFileName, 'w', encoding='utf-8') as f:
        f.write(rdpFileStr)


if __name__ == '__main__':
    ip = u'172.26.29.65:3389'  # ip地址加端口
    username = u'administrator'  # 用户名
    passwd = b'123BI@HOPSON'  # 密码
    rdpFileName = 'aaa.rdp'  # 保存文件名
    genRdp(ip, username, passwd, rdpFileName)
    os.system("mstsc ./aaa.rdp")  # 调用CMD命令运行远程桌面程序