#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/20 15:54
# @Author : ma.fei
# @File : main2.py.py
# @Software: PyCharm  secret
import uvicorn

from fastapi import FastAPI, Depends
# 导入OAuth2PasswordBearer
from fastapi.security import OAuth2PasswordBearer

app = FastAPI()
# 实例化oauth2, tokenUrl暂时随便给一个值, 后面会讲解其用法
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='xxx')

# 定义一个API, 这个API需要登录后才可以进行访问
# 那么就需要设置一个参数, 这个参数依赖于上面的oauth2_scheme
@app.get('/')
async def test(s: str = Depends(oauth2_scheme)):
    return {'hello': s}

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,host="10.2.39.21",port=8000,debug=True)