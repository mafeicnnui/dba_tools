#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/2/2 16:51
# @Author : ma.fei
# @File : enumTest.pyy.py
# @Software: PyCharm

from enum import Enum
from fastapi import FastAPI
import uvicorn
app = FastAPI()

class ModelName(str, Enum):
    alexnet = "alexnet"
    resnet = "resnet"
    lenet = "lenet"


@app.get("/models/{model_name}")
async def get_model(model_name: ModelName):
    if model_name is ModelName.alexnet:
        return {"model_name": model_name, "message": "Deep Learning FTW!"}

    if model_name.value == "lenet":
        return {"model_name": model_name, "message": "LeCNN all the images"}

    return {"model_name": model_name, "message": "Have some residuals"}


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)