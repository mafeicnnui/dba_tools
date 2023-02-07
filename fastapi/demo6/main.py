'''
FastApi:https://fastapi.tiangolo.com/zh/tutorial/path-params/
https://fastapi.tiangolo.com/zh/tutorial/body-multiple-params/

'''
import uvicorn
from typing import Union
from fastapi import FastAPI, Body
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    name: str
    description: Union[str, None] = None
    price: float
    tax: Union[float, None] = None


@app.put("/items/{item_id}")
async def update_item(item_id: int, item: Item = Body(embed=True)):
    results = {"item_id": item_id, "item": item}
    return results


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)