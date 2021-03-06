from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support  #server启动报错，提示需要引用此包
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

def test():
    QueueManager.register('get_task_queue',callable=return_task_queue)
    QueueManager.register('get_result_queue',callable=return_result_queue)
    manager = QueueManager(address=('127.0.0.1',5000),authkey=b'abc')
    manager.start()
    print('start server master')
    task = manager.get_task_queue()
    result = manager.get_result_queue()
    for i in range(10):
        n = random.randint(0,10000)
        print('put task %d...' % n)
        task.put({'hh'+str(n):n})
    print('try get results...')
    for i in range(10):
        r = result.get(timeout=100)
        
        print('result:%s' % r)

    #关闭
    manager.shutdown()
    print('master exit')

if __name__ == '__main__':
    freeze_support()
    test()