#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/22 14:58
# @Author : ma.fei
# @File : urls.py.py
# @Software: PyCharm

from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
]