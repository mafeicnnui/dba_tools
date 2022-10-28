import time
import datetime
import threading
import traceback
import random
from clickhouse_driver import Client

cfg = {
    "host": "172.17.158.0",
    "port": "9000",
    "user": "default",
    "passwd": "6lYaUiFi",
    "schema": "hft_hopson_hft",
     "process_num" : 1000,
     "sql":"""select io.business_name    AS businessName,
                   iop.party_business_id AS businessId,
                   iop.party_terminal_no AS terminalNo,
                   iop.trans_type        AS transType,
                   iop.trans_way         AS transWay,
                   iop.scan_channel      AS scanChannel,
                   io.total_amount       AS totalAmount,
                   io.party_free_amount  AS partyFreeAmount,
                   io.received_amount    AS receivedAmount
            from intel_order2 io join intel_order_payment iop on io.order_id = iop.order_id
            where  io.business_id = '{}'
              and io.terminal_no = '{}'
              and iop.sub_code = 1
              and iop.trans_channel in (1, 2, 4)
              and io.create_time between '2022-01-01' and '2022-01-07'
              and io.flow_status !=12 limit 10""",
     "sql_hz":"""SELECT
                    toDate(o.`create_time`) as paymentTime ,
                    o.`business_id` as businessId,
                    o.`business_name` as businessName,
                    o.`market_id` as marketId,
                    SUM(o.`payable_amount`) as payableAmount,
                    SUM(o.`discount_amount`) as discountAmount,
                    SUM(o.`integral_amount`) as integralAmount,
                    SUM(o.`foodstamps_amount`) as foodstampsAmount,
                    SUM(o.`payable_amount` - o.`discount_amount` - o.`integral_amount` - o.foodstamps_amount - o.party_free_amount) as amount, 
                    COUNT(CASE WHEN o.status = 3 AND o.direction_status = 1 THEN 1 END) orderCount,
                    SUM(CASE WHEN o.status = 8 AND o.direction_status = 2 THEN (o.total_amount - o.party_free_amount) ELSE 0 END)
                    refundAmount,
                    COUNT(CASE WHEN o.status = 8 AND o.direction_status = 2 THEN 1 END) refundCount,
                    SUM(o.`payable_amount` - o.`discount_amount` - o.`integral_amount` - o.`foodstamps_amount` -
                    o.party_free_amount) - SUM(CASE WHEN
                    o.status = 8 AND o.direction_status = 2 THEN (o.total_amount - o.party_free_amount) ELSE 0 END) incomeAmount
            FROM `intel_order2` o
             WHERE o.`create_time`  >=  '2022-01-01' AND o.`create_time`  <=  '2022-9-30'            
                AND o.market_id = 218
                AND o.status IN(3,8) AND flow_status != 12 
            GROUP BY toDate(o.`create_time`),o.`business_id`, o.`business_name`,  o.`market_id`
            ORDER BY toDate(o.`create_time`) DESC LIMIT 10""",
     "sql_mx":"""select 
                     io.business_name    AS businessName,
                     iop.party_business_id AS businessId,
                     iop.party_terminal_no AS terminalNo,
                     iop.trans_type        AS transType,
                     iop.trans_way         AS transWay,
                     iop.scan_channel      AS scanChannel,
                     io.total_amount       AS totalAmount,
                     io.party_free_amount  AS partyFreeAmount,
                     io.received_amount    AS receivedAmount
                from (select 
                           io.order_id,
                           io.total_amount, 
                           io.party_free_amount, 
                           io.received_amount,
                           io.business_name
                     from intel_order2 as io 
                       prewhere io.business_id = '{}'
                         and io.terminal_no = '{}'
                         and io.create_time between '2022-01-01' and '2022-01-03'
                         and io.flow_status !=12) as io
                 join
                     (select   
                         iop.order_id,
                              iop.party_business_id ,
                         iop.party_terminal_no,
                         iop.trans_type,
                         iop.trans_way,
                         iop.scan_channel
                    from intel_order_payment as iop
                     prewhere iop.create_time between '2022-01-01' and '2022-01-03'
                                 and iop.sub_code = 1
                                 and iop.trans_channel in (1, 2, 4)) as iop
                    on io.order_id = iop.order_id
                  limit 10,10""",
     "sql_mx2": """select 
                   io.business_name    AS businessName,
                   iop.party_business_id AS businessId,
                   iop.party_terminal_no AS terminalNo,
                   iop.trans_type        AS transType,
                   iop.trans_way         AS transWay,
                   iop.scan_channel      AS scanChannel,
                   io.total_amount       AS totalAmount,
                   io.party_free_amount  AS partyFreeAmount,
                   io.received_amount    AS receivedAmount
              from (select 
                         io.order_id,
                         io.total_amount, 
                         io.party_free_amount, 
                         io.received_amount,
                         io.business_name
                   from intel_order3 as io 
                     prewhere io.business_id = '{}'
                       and io.terminal_no = '{}'
                       and io.create_time between '2022-01-01' and '2022-01-30'
                       and io.flow_status !=12) as io
               join
                   (select   
                       iop.order_id,
                       iop.party_business_id ,
                       iop.party_terminal_no,
                       iop.trans_type,
                       iop.trans_way,
                       iop.scan_channel
                  from intel_order_payment as iop
                   prewhere iop.create_time between '2022-01-01' and '2022-01-30'
                       and iop.sub_code = 1
                       and iop.trans_channel in (1, 2, 4)) as iop
                  on io.order_id = iop.order_id
                limit 10,10"""
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
            FROM `intel_order2` 
            WHERE create_time >='2022-01-01' and payment_time <='2022-06-31' AND status=3 
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

def test_mx(cfg,thread_num,counter):
    try:
        db = get_ds_ch(cfg['host'],
                       cfg['port'],
                       cfg['schema'],
                       cfg['user'],
                       cfg['passwd'])

        start_time =datetime.datetime.now()
        print('exec sql for thread:{}...'.format(thread_num))
        rs = db.execute(cfg['sql_mx'].format(thread_num[0],thread_num[1]))
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

def test_mx2(cfg,thread_num,counter):
    try:
        db = get_ds_ch(cfg['host'],
                       cfg['port'],
                       cfg['schema'],
                       cfg['user'],
                       cfg['passwd'])

        start_time =datetime.datetime.now()
        print('exec sql for thread:{}...'.format(thread_num))
        rs = db.execute(cfg['sql_mx2'].format(thread_num[0],thread_num[1]))
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

def test_hz(cfg,counter):
    try:
        db = get_ds_ch(cfg['host'],
                       cfg['port'],
                       cfg['schema'],
                       cfg['user'],
                       cfg['passwd'])

        start_time =datetime.datetime.now()
        print('exec hz sql for thread:{}...'.format(counter))
        rs = db.execute(cfg['sql_hz'])
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
        print('exec hz sql for thread:{} complete,rows = {}!'.format(counter,len(rs)))
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
        counter += 1
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test, args=(cfg, thread_num,counter,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        time.sleep(random.uniform(0,3))

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      r = i
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1


    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message']))
           break

def main_mx():
    print('threading number:',cfg['process_num'])
    time.sleep(3)

    threads = []
    counter = 1
    for thread_num in get_thream_num():
        counter += 1
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test_mx, args=(cfg, thread_num,counter,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        time.sleep(random.uniform(0,1))

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      r = i
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1


    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message']))
           break

def main_mx2():
    print('threading number:',cfg['process_num'])
    time.sleep(3)

    threads = []
    counter = 1
    for thread_num in get_thream_num():
        counter += 1
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test_mx2, args=(cfg, thread_num,counter,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        #time.sleep(random.uniform(0,3))

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      r = i
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1


    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message']))
           break

def main_hz():
    print('threading number:',cfg['process_num'])
    threads = []
    counter = 1
    for thread_num in range(cfg['process_num']):
        counter += 1
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test_hz, args=(cfg, counter,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        #time.sleep(random.uniform(0,3))

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      r = i
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1


    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message']))
           break

if __name__=="__main__":
     #main()
     #main_hz()
     main_mx()
     #main_mx2()
