'''
FastApi:https://fastapi.tiangolo.com/zh/tutorial/path-params/
https://fastapi.tiangolo.com/zh/tutorial/body-multiple-params/
https://fastapi.tiangolo.com/zh/tutorial/body-nested-models/

'''
import uvicorn
from typing import List,Set, Union
from fastapi import Body, FastAPI
from pydantic import BaseModel, HttpUrl
app = FastAPI()

class Image(BaseModel):
    url: HttpUrl
    name: str


class Item(BaseModel):
    name: str
    description: Union[str, None] = None
    price: float
    tax: Union[float, None] = None
    tags: Set[str] = set()
    images: Union[List[Image], None] = None


@app.put(" /items/{item_id}")
async def update_item(item_id: int, item: Item):
    results = {"item_id": item_id, "item": item}
    return results

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)