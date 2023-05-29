#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/5/18 10:15
# @Author : ma.fei
# @File : string.py
# @Software: PyCharm

import string
import secrets
alphabet = string.ascii_letters + string.digits + string.punctuation
password = ''.join(secrets.choice(alphabet) for i in range(18))
print(password)