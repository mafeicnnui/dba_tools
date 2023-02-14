#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/9 16:25
# @Author : ma.fei
# @File : main.py.py
# @Software: PyCharm

from flask import Flask

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000,debug=True)