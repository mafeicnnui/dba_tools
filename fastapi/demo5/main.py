'''
FastApi:https://fastapi.tiangolo.com/zh/tutorial/path-params/
https://fastapi.tiangolo.com/zh/tutorial/body-multiple-params/

'''
import uvicorn
from typing import Union
from fastapi import FastAPI, Path, Query

app = FastAPI()


# @app.get("/items/{item_id}")
# async def read_items(
#     item_id: int = Path(title="The ID of the item to get"),
#     q: Union[str, None] = Query(default=None, alias="item-query"),
# ):
#     results = {"item_id": item_id}
#     if q:
#         results.update({"q": q})
#     return results

# @app.get("/items/{item_id}")
# async def read_items(q: str, item_id: int = Path(title="The ID of the item to get")):
#     results = {"item_id": item_id}
#     if q:
#         results.update({"q": q})
#     return results

# @app.get("/items/{item_id}")
# async def read_items(*, item_id: int = Path(title="The ID of the item to get"), q: str):
#     results = {"item_id": item_id}
#     if q:
#         results.update({"q": q})
#     return results

# @app.get("/items/{item_id}")
# async def read_items(
#     *, item_id: int = Path(title="The ID of the item to get", gt=1), q: str
# ):
#     results = {"item_id": item_id}
#     if q:
#         results.update({"q": q})
#     return results

@app.get("/items/{item_id}")
async def read_items(
    *,
    item_id: int = Path(title="The ID of the item to get", ge=0, le=1000),
    q: str,
    size: float = Query(gt=0, lt=10.5)
):
    results = {"item_id": item_id}
    if q:
        results.update({"q": q})
    return results


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)