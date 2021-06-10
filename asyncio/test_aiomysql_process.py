#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/26 10:45
# @Author : 马飞
# @File : test_aiomysql.py
# @Software: PyCharm

'''
CREATE TABLE async_tab(i INT NOT NULL AUTO_INCREMENT PRIMARY KEY,v VARCHAR(10),k INT);
INSERT INTO async_tab(v,k) SELECT 'a',SLEEP(3) ;
TRUNCATE TABLE async_tab;
SELECT * FROM async_tab;
'''

import asyncio
import aiomysql
import datetime

loop = asyncio.get_event_loop()

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


async def test_example(s):
    pool = await aiomysql.create_pool(host='10.2.39.17', port=23306,
                                      user='puppet', password='Puppet@123',
                                      db='test', loop=loop,autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            print("INSERT INTO async_tab(v,k) SELECT 'a',SLEEP({});".format(s))
            await cur.execute("INSERT INTO async_tab(v,k) SELECT char({}),SLEEP({});".format(97+s,s))
    pool.close()
    await pool.wait_closed()
    return s*s


async def main():
    s = [3,1,2,6,4]
    for r in s:
        task = asyncio.Task(test_example(r))
        done, pending = await asyncio.wait({task})
        print(done,pending)

async def main2():
    s = [3,1,2,6,4]
    tasks = []
    for r in s:
        task = asyncio.Task(test_example(r))
        tasks.append(task)
    done, pending = await asyncio.wait(tasks)
    print(done,pending)

async def main3():
    s = [3,1,2,6,4,1,1,1,2,2,2,3,3,4,4,4]
    tasks = []
    for r in s:
        tasks.append(test_example(r))
    done, pending = await asyncio.wait(tasks)
    print(done,pending)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    loop.run_until_complete(main3())
    print('run time:{}s'.format(get_seconds(start_time)))


