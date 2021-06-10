#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/9 9:33
# @Author : 马飞
# @File : runTask.py.py
# @Software: PyCharm

import time
import datetime
import os

cfg = {
    'first': True
}

tasks = [
    {
        'run_time': '23:00',
        'run_script': 'd:\dba\script\db_backup.bat',
        'task_type': 'timing'
    },
    {
        'run_time': '300',
        'run_script': 'd:\dba\script\gather_agent.bat',
        'task_type': 'period'
    }
]


def get_time():
    return datetime.datetime.now().strftime("%H:%M")


def get_seconds(b):
    a = datetime.datetime.now()
    return int((a - b).total_seconds())


def check_valid(p_cfg, p_task):
    if p_task['task_type'] == 'timing':
        if p_task['run_time'] == get_time():
            return True
        else:
            return False

    elif p_task['task_type'] == 'period':

        if p_cfg['first'] == True:
            p_cfg['start'] = datetime.datetime.now()
            p_cfg['first'] = False

        if get_seconds(p_cfg['start']) >= int(p_task['run_time']):
            p_cfg['start'] = datetime.datetime.now()
            return True
        else:
            return False


def schedule():
    while True:
        for task in tasks:
            if check_valid(cfg, task):
                if task['task_type'] == 'timing':
                    print('\r', task['run_script'])
                    os.system(task['run_script'])

                if task['task_type'] == 'period':
                    print('\r', task['run_script'])
                    os.system(task['run_script'])
            else:
                if task['task_type'] == 'timing':
                    print('\rSleeping 60s...', end='')
                    time.sleep(60)

                if task['task_type'] == 'period':
                    print('\rWaiting {}s'.format(get_seconds(cfg['start']), end=''))

        time.sleep(1)
        print('\rSleeping...', end='')


if __name__ == "__main__":
    schedule()