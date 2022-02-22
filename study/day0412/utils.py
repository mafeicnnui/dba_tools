#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/9 14:52
# @Author : ma.fei
# @File : utils.py.py
# @Software: PyCharm

import socket

def get_host_ip():
    """
    查询本机ip地址
    :return:
    """
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',80))
        ip=s.getsockname()[0]
    finally:
        s.close()

    return ip