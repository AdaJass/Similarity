from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support 
from mongoconnect import *
import random,time,queue

task_queue = queue.Queue()
result_queue = queue.Queue()

class QueueManager(BaseManager):
    pass
def return_task_queue():
    global task_queue
    return task_queue
def return_result_queue():
    global result_queue
    return result_queue

def build_table_s(indb, outdb):
    QueueManager.register('get_task_queue',callable=return_task_queue)
    QueueManager.register('get_result_queue',callable=return_result_queue)
    manager = QueueManager(address=('node0',5002),authkey=b'abc')
    manager.start()
    print('start server master')
    task = manager.get_task_queue()
    result = manager.get_result_queue()
    cat_list = list(indb.find({}))
    time.sleep(150)
    for ind in range(0,len(cat_list),50):    
        for j in range(50): 
            if ind+j >= len(cat_list):
                break 
            task.put(cat_list[(ind+j):])
            print('input task by index of: ',ind+j)
        
        print('try get results...')
        for j in range(50):
            if ind+j >= len(cat_list):
                break 
            r = result.get(timeout=10000000000)
            # print('result:%s' % r)
            print('receive %s result.'%j)
            outdb.insert(r)


    manager.shutdown()
    print('master exit')

if __name__ == '__main__':
    freeze_support()
    build_table_s(eur_m15, eur_m15_dist)