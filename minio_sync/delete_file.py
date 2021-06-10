#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/4/23 10:06
# @Author : 马飞
# @File : file_upload.py
# @Software: PyCharm
# @func:.项目图片文件同步至miniO服务器

import os
import datetime
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)
minioClient = Minio('10.2.39.50:30004',access_key='minio',secret_key='minio123',secure=False)

def delete_file():
    try:
        objects = minioClient.list_objects_v2('194', prefix='*',recursive=True)
        print(objects)
        for obj in objects:
            print('xx')
            print(obj.bucket_name, obj.object_name.encode('utf-8'), obj.last_modified,
                  obj.etag, obj.size, obj.content_type)
            minioClient.remove_object(obj.bucket_name, obj.object_name)


    except ResponseError as err:
        print(err)

if __name__ == "__main__":
    delete_file()