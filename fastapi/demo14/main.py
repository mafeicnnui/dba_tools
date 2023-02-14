import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse

some_file_path = "large-video-file.mp4"
#some_file_path = "https://hst-db-bak.oss-cn-beijing.aliyuncs.com/test/large-video-file.mp4?Expires=1675930935&OSSAccessKeyId=TMP.3KhMoFtitJd1JFWSvgg1jQgg4LbTsSpTUdAuc4mUZqAaVWMyYKRRCniY77CKh4UAHaLVukywMPvVVQhFuxMz4JaA7jgH4Z&Signature=WIswBXz9O0Jf%2FOOqxFi0ol3uBok%3D"

app = FastAPI()


@app.get("/")
async def main():
    return FileResponse(some_file_path)


if __name__ == '__main__':
   uvicorn.run('main:app',reload=True,port=8000,debug=True)