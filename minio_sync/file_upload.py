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

#上海五角场图片路径：/home/hopson/apps/var/webapps/JS_images

config= {
    # 'file_path':'/home/hopson/apps/usr/webserver/dba/Upload',
    # 'file_path':'/tmp/10.5.3.147:811/Upload/Images/',
    'file_path':'/home/hopson/apps/usr/webserver/dba/downloads/Upload/Images/',
    'upload_path':'/home/hopson/apps/usr/webserver/dba/downloads/Upload/Images/2020/1/1/',  #适用于增量
    'bucket_name':'235'
}

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def check_exists(obj):
    try:
        minioClient.stat_object(config['bucket_name'],obj)
        return True
    except:
        return False

def upload_file():
    '''
       1.建立bucket ，遍历file_path路径下所有子目录
       2.pip install minio
    '''
    try:
        minioClient.make_bucket(config['bucket_name'], location="cn-north-1")
        print('Bucket {0} created!'.format(config['bucket_name']))
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        pass
    except ResponseError as err:
        pass

    start_time = datetime.datetime.now()
    for root, dirs, files in os.walk(config['upload_path']):

        if len(files)>0:
            for file in files:
                try:
                    full_name = root + '/'+ file
                    obj_name  = full_name.replace(config['file_path'],'')

                    # print('fill_name=',full_name)
                    # print('root=',root)
                    # print('file_name=',file)
                    # print('object=',obj_name)

                    if not check_exists(obj_name):
                       print('Uploading obj: {0} into bucket {1},Elaspse Time:{2}s'.
                              format(obj_name, config['bucket_name'], get_seconds(start_time)), end='\n')
                       minioClient.fput_object(config['bucket_name'],
                                               obj_name,
                                               full_name,
                                               'image/jpeg')
                    else:
                        print('Uploading obj: {0} already exists bucket {1},skipping..'.
                              format(obj_name, config['bucket_name']), end='\n')

                except ResponseError as err:
                    print(err)


if __name__ == "__main__":
     upload_file()

