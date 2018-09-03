#task_master.py
#coding=utf-8

#多进程分布式例子
#服务器端

from multiprocessing.managers import BaseManager
from multiprocessing import freeze_support  #server启动报错，提示需要引用此包
import random,time,queue

#发送任务的队列
task_queue = queue.Queue()
#接收结果的队列
result_queue = queue.Queue()

#从BaseManager继承的QueueManager
class QueueManager(BaseManager):
    pass
#win7 64 貌似不支持callable下调用匿名函数lambda，这里封装一下
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
    #启动Queue
    manager.start()
    #server = manager.get_server()
    #server.serve_forever()
    print('start server master')
    #获得通过网络访问的Queue对象
    task = manager.get_task_queue()
    result = manager.get_result_queue()
    #放几个任务进去
    for i in range(10):
        n = random.randint(0,10000)
        print('put task %d...' % n)
        task.put({'hh'+str(n):n})
    #从result队列读取结果
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