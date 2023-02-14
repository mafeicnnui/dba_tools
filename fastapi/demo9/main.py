import uvicorn
from datetime import datetime, time, timedelta
from typing import Union
from uuid import UUID

from fastapi import Body, FastAPI

app = FastAPI()

@app.post("/items/{item_id}")
async def read_items(
    item_id: UUID,
    start_datetime: Union[datetime, None] = Body(default=None),
    end_datetime: Union[datetime, None] = Body(default=None),
    repeat_at: Union[time, None] = Body(default=None),
    process_after: Union[timedelta, None] = Body(default=None),
):
    start_process = start_datetime + process_after
    duration = end_datetime - start_process
    return {
        "item_id": item_id,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "repeat_at": repeat_at,
        "process_after": process_after,
        "start_process": start_process,
        "duration": duration,
    }

'''
    import uuid
    uuid = uuid.uuid1()
    uuid = f77236e4-a791-11ed-9e29-005056c00008
    {
      "start_datetime": "2023-02-08T09:25:52.129Z",
      "end_datetime": "2023-02-08T09:26:52.129Z",
      "repeat_at": "09:25:52.129Z",
      "process_after": 10
    }
'''

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)