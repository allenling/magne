magne thread worker
=====================

想法是主线程watch socket, 然后recv, 获取msg, 然后传给线程池这样, 不然如果是一个线程watch一个channel的话怎么做?

**做不到, send/recv都是针对socket的, 多个channel也都是通过一个socket交互.**

channel的msg都是通过socket传进来的, 然后每个线程都watch同一个socket么?如果此时收到的msg不是当前线程的channel的话, 怎么办?

**一般做法都是开一个io线程, 其他子线程都是把io交给io线程**

amqp的best practice都说一个线程一个channel, 是说一个线程负责消费指定channel的msg?

如果是这样的话, 何必多个channel?一个channel, 然后多个线程(线程池)来消费msg这样不好么?

**rabbitpy和dramatiq如何运行: https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst**

看了rabbit和dramatiq的运行模式之后, 决定不要遵循什么一个thread一个channel的best practice, 一个io线程, 一个thread pool, 一个connection, 一个channel

就够了, 和process_worker中的一样, 只不过process换成curio thread而已

模型
---------


TODO
------

很多amqp功能, 看情况吧


benchmark
------------



