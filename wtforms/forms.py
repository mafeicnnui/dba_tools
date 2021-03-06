#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/2 2:00
# @Author : 马飞
# @File : forms.py.py
# @Software: PyCharm
# pip3 install  wtforms==2.2.1 -i https://pypi.douban.com/simple

from wtforms.fields import StringField, TextAreaField
from wtforms_tornado import Form
from wtforms.validators import DataRequired, Length, Email

class MessageForm(Form):
    name = StringField("姓名", validators=[DataRequired(message="请输入姓名"), Length(min=2,max=5, message="长度为2-5")])
    email = StringField("邮箱", validators=[Email(message="邮箱不合法")])
    address = StringField("地址", validators=[DataRequired(message="请填写地址")])
    message = TextAreaField("留言", validators=[DataRequired(message="请填写留言")])