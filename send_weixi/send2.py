#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/10/8 15:07
# @Author : ma.fei
# @File : send2.py
# @Software: PyCharm

import itchat
# 登录网页版微信
itchat.auto_login()

# 向指定好友发送消息
itchat.send('hello world',toUserName='一切随缘')