#######################
coroutine amqp consumer
#######################

模型
====

1. 分为两个组件, connection和worker_pool

2. 两个组件互不影响, 一个负责io, 一个负责worker的管理, 通过queue来通信

3. connection把收到的msg通过queue发送给worker_pool就完事了, worker_pool拿到msg

   之后, 负责spawn协程worker

4. ack的发送是worker发送到ack_queue的, connection还负责监听ack_queue

5. 所以, 整个应用有至少有3个协程, 分别是:
   
   a. connection启动的监听socket, 监听ack_queue

   b. worker_pool启动的监听amqp_queue

.. code-block:: python

    '''

    一般消息:
    
    worker1
    
    worker2   <--spawn---  worker_pool <---amqp_queue <-----  connection <---- socket
    
    worker3


    ack消息:

    worker1   --->
    
    worker2   ---> ack_queue ---->  connection ----> socket

    worker3   --->
    
    '''


功能
====


1. qos可配置

2. cold/warm shutdown, shutdown能保证尽量ack

TODO
====


看情况

1. 掉线重连

2. 高低水位可配置

3. batch ack

benchmark
=========

.. code-block:: 

    pip install -r bench_requirements.txt
    
    python3.6 bench.py --help

