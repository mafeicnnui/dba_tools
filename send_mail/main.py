#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/7/23 10:02
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

import smtplib
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(subject, body, to_email):
    # 发件人和收件人邮箱
    from_email = "190343@lifeat.cn"
    from_password = "R86hyfjobMBYR76h"

    # 创建邮件对象
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # 邮件正文
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP_SSL(host='smtp.exmail.qq.com',port=465,timeout=60)
        #server.starttls()  # 启用 TLS
        server.login(from_email, from_password)  # 登录
        server.sendmail(from_email, to_email, msg.as_string())  # 发送邮件
        server.quit()  # 断开连接
        print("Email sent successfully!")
    except Exception as e:
        traceback.print_exc()
        print(f"Failed to send email: {e}")

# 调用函数发送邮件
send_email("Test Subject2", "This is a test email body.", "zhdn_791005@163.com")
