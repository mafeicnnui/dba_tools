#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/20 15:54
# @Author : ma.fei
# @File : main2.py.py
# @Software: PyCharm  secret
import uvicorn

from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, Body
from typing import Optional
from jose import JWTError, jwt

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "ed970259a19edfedf1010199c7002d183bd15bcaec612481b29bac1cb83d8137"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()

def get_user_id(user_name: str, password: str):
    return 123

def create_jwt_token(data: dict, expire_delta: Optional[timedelta] = None):
    # 如果传入了过期时间, 那么就是用该时间, 否则使用默认的时间
    expire = datetime.utcnow() + expire_delta if expire_delta else datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    # 需要加密的数据data必须为一个字典类型, 在数据中添加过期时间键值对, 键exp的名称是固定写法
    data.update({'exp': expire})
    # 进行jwt加密
    token = jwt.encode(claims=data, key=SECRET_KEY, algorithm=ALGORITHM)
    return token

@app.post('/login/')
async def login(user_name: str = Body(...), password: str = Body(...)):
    # 校验用户密码逻辑暂时省略, 这里我们不校验, 认为都是用户密码都是对的, 返回一个固定user_id
    user_id = get_user_id(user_name, password)
    # 使用user_id生成jwt token
    data = {'user_id': user_id}
    token = create_jwt_token(data)
    return {'token': token}

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,host="10.2.39.21",port=8000,debug=True)