#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/20 15:54
# @Author : ma.fei
# @File : main2.py.py
# @Software: PyCharm
import uvicorn

from fastapi import Depends, FastAPI
from fastapi.security import OAuth2PasswordBearer

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.get("/items/")
async def read_items(token: str = Depends(oauth2_scheme)):
    return {"token": token}


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,host="10.2.39.21",port=8000,debug=True)