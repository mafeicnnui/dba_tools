#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/9/17 14:12
# @Author : ma.fei
# @File : urls.py.py
# @Software: PyCharm

from web.user import user

urls =  [
        (r"/user", user),
]