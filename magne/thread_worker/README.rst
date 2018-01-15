magne thread worker
=====================

1. 想法是主线程watch socket, 然后recv, 获取msg, 然后传给线程池这样, 不然如果是一个线程watch一个channel的话怎么做?

**做不到, send/recv都是针对socket的, 多个channel也都是通过一个socket交互.**

2. channel的msg都是通过socket传进来的, 然后每个线程都watch同一个socket么?如果此时收到的msg不是当前线程的channel的话, 怎么办?

**一般做法都是开一个io线程, 其他子线程都是把io交给io线程**

3. amqp的best practice都说一个线程一个channel, 是说一个线程负责消费指定channel的msg?

如果是这样的话, 何必多个channel?一个channel, 然后多个线程(线程池)来消费msg这样不好么?

参考了 `rabbit和dramatiq的运行模式 <https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst>`_ 之后决定不要遵循什么一个thread一个channel的best practice

一个io线程, 一个thread pool, 一个connection, 一个channel, 这样就够了, 和process worker中的一样, 只不过process换成thread而已

关于线程的关闭
----------------

cpython中, 线程并没有标准的主动停止的接口, 除非你调用C接口, 或者底层的_thread.exit

可以参考 `dramatiq <https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst>`_ 通过设置异常来停止worker


连接数
---------

1. 不同于celery, 参考dramatiq, 一个worker进程一个连接， 主进程不负责io, 由worker来io, 主进程只负责开启和关闭worker.

2. 和celery一样, 主进程负责io, worker进程开启线程来执行任务, 主进程把task分发给worker.

这里直接是一个worker进程一个连接吧.


模型
---------

`threading.Thread的C实现 <https://github.com/allenling/LingsKeep/blob/master/python_thread.rst>`_

`python thread的同步对象实现(C) <https://github.com/allenling/LingsKeep/blob/master/python_thread_sync_primitive.rst>`_


TODO
------

很多amqp功能, 看情况吧


benchmark
------------

pip install -r bench_requirements.txt

python3.6 bench.py --workers=8



