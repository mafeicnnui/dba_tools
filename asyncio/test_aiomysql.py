#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/1/26 10:45
# @Author : 马飞
# @File : test_aiomysql.py
# @Software: PyCharm

import asyncio
import aiomysql
import datetime

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

async def test_example():
    pool = await aiomysql.create_pool(host='10.2.39.17', port=23306,
                                      user='puppet', password='Puppet@123',
                                      db='test',autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            #await cur.execute("INSERT INTO async_tab(v,k) SELECT 'a',SLEEP(10);")
            await cur.execute("select * from async_tab")
            value = await cur.fetchone()
            print('value=', value)
    # pool.close()
    # await pool.wait_closed()


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_example())
    print('run time:{}s'.format(get_seconds(start_time)))