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
    182,
    170,
    24
]
resume_task_ids = [
    182,
    170,
    24
]
pause_task_ids = [
    182,
    170,
    24]

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
     #暂停
     pause()
     #恢复
     #resume()
     #运行
     #run()