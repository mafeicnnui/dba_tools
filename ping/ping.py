import os
import sys
import time
import datetime

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
  ip = sys.argv[1]
  while True:
     res = os.popen('ping '+ip+' -c 1 -w 1').read()
     flag= res.split('\n')[4].split(',')[2].split(' ')[1]
     if flag=='0%':
        print('time:{},ping {} success!'.format(get_time(),ip))
     else:
        print('time:{},ping {} failure!'.format(get_time(),ip))
     time.sleep(0.2)

if __name__ == '__main__':
    main()