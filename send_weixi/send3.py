#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/10/8 15:12
# @Author : ma.fei
# @File : qrcode.py
# @Software: PyCharm

import qrcode
# qr = qrcode.QRCode(
#     version=1,
#     error_correction=qrcode.constants.ERROR_CORRECT_L,
#     box_size=10,
#     border=4,
# )
# qr.add_data('http://lizhiyu.iteye.com/')
# qr.make(fit=True)
# img = qr.make_image()
# img.save('123.png')

img = qrcode.make('http://lizhiyu.iteye.com/blog/2331662')
img.save('test.png')