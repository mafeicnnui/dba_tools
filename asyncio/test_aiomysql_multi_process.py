# -*- coding:utf-8 -*-
from concurrent.futures import ProcessPoolExecutor
from time import ctime, sleep

def loop(nloop, nsec):
    print("start loop", nloop, "at:", ctime())
    sleep(nsec)
    print("loop", nloop, "done at:", ctime())

if __name__=="__main__":
    with ProcessPoolExecutor(max_workers=3) as executor:
        #all_task = [executor.submit(loop, i, j) for i, j in zip([1,2],[4,3])]
        executor.submit(loop, 1, 4)
        executor.submit(loop, 2, 3)