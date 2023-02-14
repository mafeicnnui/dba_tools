import uvicorn

from fastapi import Body, FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI, Request

app = FastAPI()

templates = Jinja2Templates(directory="./templates")
app.mount("/static", StaticFiles(directory="./static"), name="static")


@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(
        "FastAPI.html",
        {
            "request": request
        }
    )

if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)