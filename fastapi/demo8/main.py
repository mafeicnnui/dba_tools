'''
FastApi:https://fastapi.tiangolo.com/zh/tutorial/path-params/
https://fastapi.tiangolo.com/zh/tutorial/body-multiple-params/
https://fastapi.tiangolo.com/zh/tutorial/body-nested-models/

'''
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI,Cookie, HTTPException,Response
from typing import Union

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

items = {"foo": "The Foo Wrestlers"}
cookie_info: str = Cookie(None)

# @app.get("/items/{item_id}")
# async def read_item(item_id: str):
#     if item_id not in items:
#         raise HTTPException(status_code=404, detail="Item not found")
#     return {"item": items[item_id]}

@app.get("/items/")
async def read_items(ads_id: Union[str, None] = Cookie(default=None)):
    return {"ads_id": ads_id}


@app.get("/get_cookie")
async def read_cookie_info(*, session_info: str = Cookie(None),
                              username: str = Cookie(None),
                              message: str = Cookie(None)):
    return {"session_info": session_info,"username":username,"message":message}


@app.get("/set_cookie/")
def read_cookie_info(response: Response):
    response.set_cookie(key="session_info", value="hello world!")
    response.set_cookie(key="username", value="admin")
    response.set_cookie(key="message", value="How do you do ?")
    return {"message": "add cookie"}

@app.get("/get_cookie2/{name}")
async def read_cookie_info(*, name: str = Cookie(None)):
    return {name: name}

'''
  blog:https://zhuanlan.zhihu.com/p/355244229

'''
if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)