#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/2 16:51
# @Author : ma.fei
# @File : enumTest.pyy.py
# @Software: PyCharm

from enum import Enum
from fastapi import FastAPI
from typing import Union
import uvicorn

app = FastAPI()
fake_items_db = [{"item_name": "Foo"}, {"item_name": "Bar"}, {"item_name": "Baz"}]

@app.get("/items/")
async def read_item(skip: int = 0, limit: int = 10):
    return fake_items_db[skip : skip + limit]

# @app.get("/items/{item_id}")
# async def read_item(item_id: str, q: Union[str, None] = None):
#     if q:
#         return {"item_id": item_id, "q": q}
#     return {"item_id": item_id}

@app.get("/items/{item_id}")
async def read_item(item_id: str, q: Union[str, None] = None, short: bool = False):
    item = {"item_id": item_id}
    if q:
        item.update({"q": q})
    if not short:
        item.update(
            {"description": "This is an amazing item that has a long description"}
        )
    return item

@app.get("/users/{user_id}/items/{item_id}")
async def read_user_item(
    user_id: int, item_id: str, q: Union[str, None] = None, short: bool = False
):
    item = {"item_id": item_id, "owner_id": user_id}
    if q:
        item.update({"q": q})
    if not short:
        item.update(
            {"description": "This is an amazing item that has a long description"}
        )
    return item

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)

