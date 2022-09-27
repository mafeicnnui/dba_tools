import time
import pymysql
import datetime
import requests
import threading
import traceback
import json

cfg = {
    "host": "10.2.39.144",
    "port": "9030",
    "user": "root",
    "passwd": "",
    "schema": "hopson_hft_dev",
    "process_num" : 500,
    "api" : "http://10.2.39.74:40012/bff-app/trade/statistical/trans/summary?businessId={}&terminalNo={}&startDt={}&endDt={}"
}

RES = []
error = 0


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_ds_doris(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

doris_db = get_ds_doris(cfg['host'], cfg['port'], cfg['schema'], cfg['user'], cfg['passwd'])

def get_thream_num():
    db = get_ds_doris(cfg['host'],
                      cfg['port'],
                      cfg['schema'],
                      cfg['user'],
                      cfg['passwd'])
    cr = db.cursor()
    cr.execute("""SELECT business_id,
                         terminal_no,
                         date_format(MIN(payment_time),'%Y-%m-%d') AS payment_time,
                         date_format(DATE_ADD(MIN(payment_time),INTERVAL 90 DAY),'%Y-%m-%d') AS payment_time2,count(0) as rec
                 FROM `intel_order` 
                  WHERE payment_time >='2022-01-01'
                   AND STATUS=3 
                  group by business_id,terminal_no 
                  HAVING COUNT(0)>100 limit {}""".format(cfg['process_num']))
    rs=cr.fetchall()
    cr.close()
    return rs

# 每个会话独立连接压测
def test(cfg,counter,thread_num,):
    start_time =datetime.datetime.now()
    print('exec sql for thread:{}...'.format(counter))
    print('api:',cfg['api'].format(thread_num['business_id'],thread_num['terminal_no'],thread_num['payment_time'],thread_num['payment_time2']))
    api = cfg['api'].format(thread_num['business_id'],thread_num['terminal_no'],thread_num['payment_time'],thread_num['payment_time2'])
    res = requests.get(api)
    msg = json.loads(res.text)
    end_time = datetime.datetime.now()
    if msg.get('code') == 200:
        RES.append(
          {
                "thread_num": counter,
                "start_time": start_time.strftime( "%Y-%m-%d %H:%M:%S.%f"),
                "end_time"  : end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "elaspse_time": get_seconds(start_time),
                "rows":thread_num['rec'],
                "data"  : msg['data'],
                "error": False
            }
        )
        print('exec sql for thread:{} complete!'.format(counter))
    else:
        RES.append(
            {
                "thread_num": counter,
                "start_time": '',
                "end_time": '',
                "elaspse_time": 0,
                "rows": 0,
                "data": msg,
                "error":True,
                "message":traceback.format_exc()
            }
        )
        print('exec sql for thread:{} failure!'.format(counter))


def main():
    cfg['process_num'] = len(get_thream_num())
    print('threading number:',cfg['process_num'])

    threads = []
    i_counter = 1
    for thread_num in get_thream_num():
        print('start threading for {}...'.format(i_counter))
        thread = threading.Thread(target=test, args=(cfg, i_counter,thread_num,))
        threads.append(thread)
        i_counter += 1

    for i in range(0, len(threads)):
        threads[i].start()
        #time.sleep(0.01)

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
