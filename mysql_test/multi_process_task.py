import time
import pymysql
import datetime
import threading
import traceback

cfg = {
    "host": "10.2.39.40",
    "port": "3306",
    "user": "root",
    "passwd": "root123HOPSON",
    "schema": "hopson_hft_doris",
     "process_num" : 500,
      "sql":"""select io.business_name   AS businessName,
                   iop.party_business_id AS businessId,
                   iop.party_terminal_no AS terminalNo,
                   iop.trans_type        AS transType,
                   iop.trans_way         AS transWay,
                   iop.scan_channel      AS scanChannel,
                   io.total_amount       AS totalAmount,
                   io.party_free_amount  AS partyFreeAmount,
                   io.received_amount    AS receivedAmount
            from intel_order io join intel_order_payment iop on io.order_id = iop.order_id
            where io.business_id = '{}'
              and io.terminal_no = '{}'
              and iop.sub_code = 1
              and iop.trans_channel in (1, 2, 4)
              and io.payment_time between '{}' and '{}'
              and io.flow_status !=12"""
}

RES = []
error = 0

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_ds_doris(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

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
                         date_format(DATE_ADD(MIN(payment_time),INTERVAL 30 DAY),'%Y-%m-%d') AS payment_time2,count(0) as rec
                 FROM `intel_order` 
                  WHERE payment_time >='2022-01-01' AND payment_time <= '2022-03-01'
                   AND STATUS=3 
                  group by business_id,terminal_no 
                  HAVING COUNT(0)>100 limit {}""".format(cfg['process_num']))
    rs=cr.fetchall()
    cr.close()
    return rs

def test(cfg,thread_num):
    try:
        db = get_ds_doris(cfg['host'],
                          cfg['port'],
                          cfg['schema'],
                          cfg['user'],
                          cfg['passwd'])
        cr = db.cursor()
        start_time =datetime.datetime.now()
        print('exec sql for thread:{}...'.format(thread_num))
        #print(cfg['sql'].format(thread_num[0],thread_num[1],thread_num[2],thread_num[3]))
        cr.execute(cfg['sql'].format(thread_num[0],thread_num[1],thread_num[2],thread_num[3]))
        rs = cr.fetchall()
        end_time =datetime.datetime.now()
        RES.append(
          {
                "thread_num": thread_num,
                "start_time": start_time.strftime( "%Y-%m-%d %H:%M:%S.%f"),
                "end_time"  : end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "elaspse_time": get_seconds(start_time),
                "rows" :len(rs),
                "error": False
            }
        )
        print('exec sql for thread:{} complete,rows = {}!'.format(thread_num,len(rs)))
        #db.close()
    except:
        RES.append(
            {
                "thread_num": thread_num,
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
    for thread_num in get_thream_num():
        print('start threading for {}...'.format(thread_num))
        thread = threading.Thread(target=test, args=(cfg, thread_num,))
        threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()
        #time.sleep(0.1)

    for i in range(0, len(threads)):
        threads[i].join()

    s = 0
    e = 0
    for i in RES:
      s = s + i['elaspse_time']
      if i['error']:
          e = e + 1
      print(i)

    print('\ntotal info:')
    print('-----------------------------------------------------------------')
    print('avg time:{}'.format(s/cfg['process_num']))
    print('error threads:{}'.format(str(e)))
    print('-----------------------------------------------------------------')
    for i in RES:
        if i['error']:
           print('thread:{},error:{}'.format(i['thread_num'],i['message'].split('pymysql.err.ProgrammingError:')[1]))

if __name__=="__main__":
     main()
