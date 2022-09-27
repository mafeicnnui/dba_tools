import time
import datetime
import threading
import traceback
from clickhouse_driver import Client

cfg = {
    "host": "172.17.158.0",
    "port": "9000",
    "user": "default",
    "passwd": "6lYaUiFi",
    "schema": "hft_hopson_hft",
     "process_num" : 30,
     "sql":"""select io.business_name    AS businessName,
                   iop.party_business_id AS businessId,
                   iop.party_terminal_no AS terminalNo,
                   iop.trans_type        AS transType,
                   iop.trans_way         AS transWay,
                   iop.scan_channel      AS scanChannel,
                   io.total_amount       AS totalAmount,
                   io.party_free_amount  AS partyFreeAmount,
                   io.received_amount    AS receivedAmount
            from intel_order io left join intel_order_payment iop on io.order_id = iop.order_id
            where io.business_id = '{}'
              and io.terminal_no = '{}'
              and iop.sub_code = 1
              and iop.trans_channel in (1, 2, 4)
              and io.payment_time between '2022-01-01' and '2022-03-31'
              and io.flow_status !=12"""
}

RES = []
error = 0

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_ds_ch(ip,port,service ,user,password):
    return  Client(host=ip,
                   port=port,
                   user=user,
                   password=password,
                   database=service,
                   send_receive_timeout=600000)


def get_thream_num():
    db = get_ds_ch(cfg['host'],
                   cfg['port'],
                   cfg['schema'],
                   cfg['user'],
                   cfg['passwd'])
    st = """SELECT business_id,
                   terminal_no
            FROM `intel_order` 
            WHERE payment_time >='2022-01-01' AND status=3 
             GROUP BY business_id,terminal_no 
              HAVING COUNT(0)>100  limit {}""".format(cfg['process_num'])
    rs = db.execute(st)
    return rs

def test(cfg,thread_num,counter):
    try:
        db = get_ds_ch(cfg['host'],
                          cfg['port'],
                          cfg['schema'],
                          cfg['user'],
                          cfg['passwd'])

        start_time =datetime.datetime.now()
        print('exec sql for thread:{}...'.format(thread_num))
        #print(cfg['sql'].format(thread_num[0],thread_num[1]))
        rs = db.execute(cfg['sql'].format(thread_num[0],thread_num[1]))
        end_time =datetime.datetime.now()
        RES.append(
          {
                "thread_num": counter,
                "start_time": start_time.strftime( "%Y-%m-%d %H:%M:%S.%f"),
                "end_time"  : end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "elaspse_time": get_seconds(start_time),
                "rows" :len(rs),
                "error": False
            }
        )
        print('exec sql for thread:{} complete,rows = {}!'.format(counter,len(rs)))
    except:
        RES.append(
            {
                "thread_num": counter,
                "start_time": '',
                "end_time": '',
                "elaspse_time": 0,
                "rows": 0,
                "error":True,
                "message":traceback.format_exc()
            }
        )

def main():
    cfg['process_num'] = len(get_thream_num())
    print('threading number:',cfg['process_num'])

    threads = []
    counter = 1
    for thread_num in get_thream_num():
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test, args=(cfg, thread_num,counter,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        time.sleep(0.2)

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1
          del i['message']
      print(i)

    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message']))


if __name__=="__main__":
     main()
