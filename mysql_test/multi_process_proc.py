import time
import pymysql
import datetime
import requests
import threading
import traceback
import json

cfg = {
    "host": "10.2.39.40",
    "port": "3306",
    "user": "root",
    "passwd": "root123HOPSON",
    "schema": "hft_bbtj",
    "process_num" : 100
}

RES = []
error = 0


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn


# 每个会话独立连接压测
def test(cfg,counter,thread_num,):
    try:
        db = get_ds_mysql(cfg['host'], cfg['port'], cfg['schema'], cfg['user'], cfg['passwd'])
        start_time =datetime.datetime.now()
        print('exec sql for thread:{}...'.format(counter))
        cr = db.cursor()
        cr.callproc(procname='proc_bbtj',args = ('bb01','market_id:218#bbrqq:2022-01-01 0:0:0#bbrqz:2022-03-31 23:59:59',))
        cr.execute('SELECT * FROM t_intel_order2')
        rs = cr.fetchall()
        end_time = datetime.datetime.now()
        RES.append(
          {
                "thread_num": counter,
                "start_time": start_time.strftime( "%Y-%m-%d %H:%M:%S.%f"),
                "end_time"  : end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "rows":len(rs),
                "elaspse_time": get_seconds(start_time),
                "error": False
            }
        )
        print('exec call for thread:{} complete!'.format(counter))
    except:
        traceback.print_exc()
        RES.append(
            {
                "thread_num": counter,
                "start_time": '',
                "end_time": '',
                "rows": 0,
                "elaspse_time": 0,
                "error":True,
                "message":traceback.format_exc()
            }
        )
        print('exec call for thread:{} failure!'.format(counter))


def main():
    print('threading number:',cfg['process_num'])

    threads = []
    i_counter = 1
    for thread_num in range(cfg['process_num']):
        print('start threading for {}...'.format(i_counter))
        thread = threading.Thread(target=test, args=(cfg, i_counter,thread_num,))
        threads.append(thread)
        i_counter += 1

    for i in range(0, len(threads)):
        threads[i].start()
        time.sleep(0.5)

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    print('\nThread Exec info:')
    print('-----------------------------------------------------------------')
    for i in RES:
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1
      print(i)

    print('\nTotal info: thread num:',cfg['process_num'])
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message'].split('pymysql.err.ProgrammingError:')))


if __name__=="__main__":
     main()
