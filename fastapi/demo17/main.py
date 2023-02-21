#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/20 15:54
# @Author : ma.fei
# @File : main2.py.py
# @Software: PyCharm
import uvicorn

from typing import Union

from fastapi import Depends, FastAPI
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None


def fake_decode_token(token):
    return User(
        username=token + "fakedecoded", email="john@example.com", full_name="John Doe"
    )


async def get_current_user(token: str = Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    return user


@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,host="10.2.39.21",port=8000,debug=True)