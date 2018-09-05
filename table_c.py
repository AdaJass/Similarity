import time,sys,queue
from multiprocessing.managers import BaseManager
from factor import *
class QueueManager(BaseManager):
    pass

QueueManager.register('get_task_queue')
QueueManager.register('get_result_queue')

server_addr = 'node0'
print('connect to server %s...'% server_addr)
m = QueueManager(address=(server_addr,5002),authkey=b'abc')
m.connect()
task = m.get_task_queue()
result = m.get_result_queue()
while True:
    try:
        cat_list = task.get(timeout=1000000000)
        print('server data is: ',cat_list[0])
        ca = cat_list[0]
        key1 = ca['id']
        outlist=[]
        # for it in cat_list[1:]:
        #     key2 = it['id']
        #     c1 = init_factor_set(ca['data'][0])
        #     c2 = init_factor_set(it['data'][0])
        #     re = compare_factor_set(c1, c2, ['M15','H4','D1'])
        #     outlist.append({'key1':key1,'key2':key2,'result':re})
        # print('current compare of ',key,', result is: ',re)
        # r = '%d * %d = %d' % (n,n,n*n)   
        outlist.append({'ss':38,'skj':88})     
        result.put(outlist)
    except queue.Empty:
        print('task queue is empty')
print('worker exit')