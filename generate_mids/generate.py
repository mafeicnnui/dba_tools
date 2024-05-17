#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/4/15 17:15
# @Author : ma.fei
# @File : generate.py.py
# @Software: PyCharm

import json

def get_local_config(lname):
    try:
        with open(lname, 'r') as f:
             cfg = json.loads(f.read())
        return cfg
    except:
        return None

def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    print('-'.ljust(85, '-'))

if __name__ == '__main__' :
    cfg = get_local_config('mids0424.json')
    print_dict(cfg)
    values = ''
    # for i in cfg['data']:
    #     #values +='({}),'.format(i)
    #     print('({}),'.format(i))
    #print('values=',values[0:-1])

    with open('output.txt', 'w') as file:
        for i in cfg['data']:            # values +='({}),'.format(i)
            #print('({}),'.format(i))
            file.write('({}),'.format(i))