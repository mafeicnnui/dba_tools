#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/20 15:54
# @Author : ma.fei
# @File : main2.py.py
# @Software: PyCharm  secret
import uvicorn

from fastapi import FastAPI, Depends, Body, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from fastapi import FastAPI, Depends, Body,Request
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional

from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "ed970259a19edfedf1010199c7002d183bd15bcaec612481b29bac1cb83d8137"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()

class login(BaseModel):
    user_name: str
    password: str


templates = Jinja2Templates(directory="./templates")
app.mount("/static", StaticFiles(directory="./static"), name="static")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='xxx')

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

# @app.post('/login/')
# async def login(user_name: str = Body(...), password: str = Body(...)):
#     print('user_name=',user_name,'password=',password)
#     # 校验用户密码逻辑暂时省略, 这里我们不校验, 认为都是用户密码都是对的, 返回一个固定user_id
#     user_id = get_user_id(user_name, password)
#     # 使用user_id生成jwt token
#     data = {'user_id': user_id}
#     token = create_jwt_token(data)
#     return {'token': token}

@app.post('/login/')
async def login(item: login = Body(embed=True)):
    # 校验用户密码逻辑暂时省略, 这里我们不校验, 认为都是用户密码都是对的, 返回一个固定user_id
    user_id = get_user_id(item.user_name, item.password)
    # 使用user_id生成jwt token
    data = {'user_id': user_id}
    token = create_jwt_token(data)
    return {'token': token}

@app.get('/')
async def test(token: str = Depends(oauth2_scheme)):
    # 定义一个验证异常的返回
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="认证失败",
        # 根据OAuth2规范, 认证失败需要在响应头中添加如下键值对
        headers={'WWW-Authenticate': "Bearer"}
    )
    # 验证token
    try:
        # 解密token, 返回被加密的字典
        payload = jwt.decode(token=token, key=SECRET_KEY, algorithms=[ALGORITHM])
        print(f'payload: {payload}')
        # 从字典中获取user_id数据
        user_id = payload.get('user_id')
        print(f'user_id: {user_id}')
        # 若没有user_id, 则返回认证异常
        if not user_id:
            raise credentials_exception
    except JWTError as e:
        print(f'认证异常: {e}')
        # 如果解密过程出现异常, 则返回认证异常
        raise credentials_exception
    # 解密成功, 返回token中包含的user_id
    return {'userid': user_id}

@app.get("/user")
def home(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request
        }
    )


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,host="10.2.39.21",port=8000,debug=True)