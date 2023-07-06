#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2023/6/8 12:24
# @Author : ma.fei
# @File : xxl-job.py.py
# @Software: PyCharm

import time
import requests
import traceback

cfg = {
    'host':'172.17.194.37',
    "port":'8088',
    "token":'3931623531333561366137356662363362616639336539326638613565343930'
}

run_task_ids = [
    226,
    67,
    111,
    93,
    109,
    127,
    126,
    125,
    124,
    122,
    138,
    137,
    136,
    135,
    134,
    20,
    89,
    21,
    176,
    101,
    225,
    224,
    6,
    239,
    166,
    8,
    64,
    203,
    202,
    219,
    218,
    217,
    216,
    62,
    59,
    58,
    56,
    55,
    54,
    160,
    52,
    51,
    115
]
resume_task_ids = [
    244, 243, 176, 175, 181, 169, 168, 49, 184, 183, 182, 132, 102, 101, 100, 99, 198, 213, 104, 128, 151, 226, 225,
    224, 249, 131, 208, 130, 129, 15, 14, 117, 19, 33, 32, 18, 97, 96, 95, 94, 154, 153, 152, 1, 5, 2, 70, 4, 67, 65, 6,
    3, 111, 110, 93, 109, 241, 88, 240, 87, 86, 239, 85, 84, 222, 220, 48, 47, 46, 45, 44, 73, 43, 72, 42, 71, 35, 81,
    80, 75, 74, 41, 40, 39, 38, 37, 149, 148, 147, 146, 145, 144, 143, 25, 142, 141, 140, 139, 28, 27, 82, 69, 68, 250,
    177, 79, 78, 77, 121, 112, 63, 92, 34, 91, 166, 165, 210, 209, 83, 179, 251, 178, 120, 119, 8, 64, 108, 205, 204,
    203, 202, 66, 219, 218, 217, 216, 62, 215, 61, 214, 60, 59, 58, 57, 56, 55, 54, 53, 201, 105, 167, 164, 163, 162,
    161, 160, 159, 157, 156, 155, 127, 126, 125, 124, 123, 122, 223, 138, 137, 136, 135, 134, 133, 116, 115, 114, 113,
    188, 187, 186, 185, 206, 212, 211, 228, 227, 248, 247, 246, 245, 150, 170, 207, 20, 7, 11, 90, 89, 30, 24, 21, 252,
    10

]
pause_task_ids = [
    244, 243, 176, 175, 181, 169, 168, 49, 184, 183, 182, 132, 102, 101, 100, 99, 198, 213, 104, 128, 151, 226, 225,
    224, 249, 131, 208, 130, 129, 15, 14, 117, 19, 33, 32, 18, 97, 96, 95, 94, 154, 153, 152, 1, 5, 2, 70, 4, 67, 65, 6,
    3, 111, 110, 93, 109, 241, 88, 240, 87, 86, 239, 85, 84, 222, 220, 48, 47, 46, 45, 44, 73, 43, 72, 42, 71, 35, 81,
    80, 75, 74, 41, 40, 39, 38, 37, 149, 148, 147, 146, 145, 144, 143, 25, 142, 141, 140, 139, 28, 27, 82, 69, 68, 250,
    177, 79, 78, 77, 121, 112, 63, 92, 34, 91, 166, 165, 210, 209, 83, 179, 251, 178, 120, 119, 8, 64, 108, 205, 204,
    203, 202, 66, 219, 218, 217, 216, 62, 215, 61, 214, 60, 59, 58, 57, 56, 55, 54, 53, 201, 105, 167, 164, 163, 162,
    161, 160, 159, 157, 156, 155, 127, 126, 125, 124, 123, 122, 223, 138, 137, 136, 135, 134, 133, 116, 115, 114, 113,
    188, 187, 186, 185, 206, 212, 211, 228, 227, 248, 247, 246, 245, 150, 170, 207, 20, 7, 11, 90, 89, 30, 24, 21, 252,
    10]

def pause():
    for task_id in pause_task_ids:
        print('pause task:',task_id)
        payload = {
            'id':str(task_id),
            'executorParam=':''
         }
        headers = {
            'Cookie': 'XXL_JOB_LOGIN_IDENTITY={}'.format(cfg['token']),
            'User-Agent': 'Apifox/1.0.0 (https://www.apifox.cn)',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Host': '{}:{}'.format(cfg['host'], str(cfg['port'])),
            'Connection': 'keep-alive'
        }
        try:
            url = 'http://{}:{}//xxl-job-admin/jobinfo/pause'.format(cfg['host'],cfg['port'])
            res = requests.post(url, data=payload, headers=headers).json()
            print('`{}` {}'.format(task_id,res))
        except:
            traceback.print_exc()

def resume():
    for task_id in resume_task_ids:
        print('resume task:', task_id)
        payload = {
            'id': str(task_id),
            'executorParam=': ''
        }
        headers = {
            'Cookie': 'XXL_JOB_LOGIN_IDENTITY={}'.format(cfg['token']),
            'User-Agent': 'Apifox/1.0.0 (https://www.apifox.cn)',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Host': '{}:{}'.format(cfg['host'], str(cfg['port'])),
            'Connection': 'keep-alive'
        }
        try:
            url = 'http://{}:{}/xxl-job-admin/jobinfo/resume'.format(cfg['host'], cfg['port'])
            res = requests.post(url, data=payload, headers=headers).json()
            print('`{}` {}'.format(task_id,res))
        except:
            traceback.print_exc()

def run():
    for task_id in run_task_ids:
        print('run task:', task_id)
        payload = {
            'id': str(task_id),
            'executorParam=': ''
        }
        headers = {
            'Cookie': 'XXL_JOB_LOGIN_IDENTITY={}'.format(cfg['token']),
            'User-Agent': 'Apifox/1.0.0 (https://www.apifox.cn)',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Host': '{}:{}'.format(cfg['host'], str(cfg['port'])),
            'Connection': 'keep-alive'
        }
        try:
            url = 'http://{}:{}/xxl-job-admin/jobinfo/trigger'.format(cfg['host'],cfg['port'])
            res = requests.post(url, data=payload, headers=headers).json()
            print('`{}` {}'.format(task_id,res))
        except:
            traceback.print_exc()

if __name__ == '__main__':
     #pause()
     #resume()
     run()