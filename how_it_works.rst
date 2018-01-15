########
模型思路
########

简化为三个组件: master, connection, worker_pool

用curio重写跟网络有关的部分, 包括组件之间的数据交互


amqp相关
==========

connection和channel如何分配?
----------------------------

一般best practice告诉我们, 一个线程一个channel, 但是这样比较麻烦, 一个channel一个线程, 线程有问题的话, 必须关闭掉channel, 否则无法关闭connection,

所以要检测线程有问题的情况. 然后就算一个线程一个channel, 那么读写channel还是只能通过指定的socket来和rabbitmq交互, 无法做到一个线程watch不同的channel.

如果多个线程都watch一个socket, 显然recv会有混乱的情况, 这样真不如另开一个线程, 称为io线程, 去专门send/recv, 那这样的话, 一个connection, 一个channel就够了,
没看到多个channel的必要.

所以我觉得没必要. 一个connection一个channel就好了, worker跟connection或者channel都无关, worker只负责处理task

master
=========

协调角色, 保证正确的开启connection和worker pool, 已经正确的关闭

开启和关闭都需要有顺序才能保证流程正确.


connection
==============

打开amqp连接, 然后spawn出一个协程序来一直接收amqp消息, 并且发送ack.

关闭连接的是发送关闭channel和关闭connection数据.



worker_pool
==============


worker pool的功能创建出worker, spawn出等待接收task的协程. 接收task的协程收到task之后, 分配task到各个worker, 由worker来执行task, 并且worker pool监控各个worker是否超时

进程, 线程, 协程模式的区别就是worker pool中的worker, 进程模式中worker是进程来执行task, 线程模式中worker是线程, 协程模式中worker是协程.


启动/关闭顺序
===============


先启动connection, join保证connection一定启动完成, 然后join一下worker pool, 保证worker pool启动完成.

关闭的时候应该保证已完成的task能ack掉, 应该:

1. connection取消掉接收rabbitmq数据的协程, 这里保证了不再接收task.

2. worker pool取消掉接收task的协程, 保证不再接收task, 如果是warm shutdown, 则等待一个时间(timeout), 让还在运行的worker能尽可能的运行完毕, 最后, 取消掉所有的worker.

3. connection判断如果ack队列依然有数据, 则发送所有的ack


send/rev
===========

进程
--------

master和worker进程就通过socket来通信


协程
-------

协程之间可以使用curio中的queue来通信, 然后这个send/recv是遵循一个尽可能send, 尽可能recv的模式.


比如a, b, c三个协程把数据发送到q, 然后协程d调用q.get, 然后有调用顺序是可能a.send, b.send, c.send, 然后如果q一次get一个的话, 效率不太高, 所以q.get一旦有数据, 则"排干"q

.. code-block:: python

    async def d(q):
        while True:
            data = await q.get()
            if q.empty() is False:
                # 一次拿完queue中的数据
                drain_queue(q)
            for future, fdata in data:
                # 这一步是如果a, b, c需要等待返回的话才需要, 记录下future对象, 用来发送返回值
                # 先put再调用send保证了命令返回和future对象是对应的
                await future_queue.put(future)
                # 调用真正的send
                await send(fdata)

比如上面的q拿到的data是redis的命令, 那么for循环就发送命令, 然后由同一个recv来获取redis返回. 如果是a, b, c调用send之后一起等待recv的话, 会遇到

比如拿到的返回不是自己命令的返回, 调用recv(size)的时候, 因为size比起单条命令的大小来说非常大, 所以有可能拿到的返回是多个命令的返回等等问题, 所以用一个recv协程来统一recv.

而a, b, c则是通过监听future对象来等待数据返回.


.. code-block:: python

    # a
    async def a(q):
        # 发送future和data到发送队列
        await q.send((future, data))
        result = await future.wait
   
   # recv
   async def recv_coro():
       while True:
           data = await recv(1024)
           for d in data:
               # 从future对象列表中拿到对应的future对象
               fu = fu_queue.get()
               # 设置future对象的结果
               await fu.set(d)


线程
--------

worker线程和io线程都是通过queue来传输数据.

多个worker在一个worker_queue中调用get, io线程拿到amqp数据之后, 发送task到worker queue中, 一旦worker完成task, 那么把ack信息发送到ack_queue,

io线程从ack_queue中拿到ack信息, 发送ack.


监视超时
============

进程
-------

spawn一个协程去监听发送结果的socket, 超时的话就简单地杀死worker进程.

celery也是杀死worker进程.

线程
-------

杀死线程
~~~~~~~~~~~

线程超时就杀死线程, 线程worker和进程worker不太一样的一点是, daemon线程如果在解释器被退出之后又被调度的话, 又可能导致一些资源未被释放~~

并且 **py中并不能杀死线程(C接口好像也没有)**

设置异常
~~~~~~~~~~~~

通过线程的c api函数: PyThreadState_SetAsyncExc, 可以给线程一个超时异常, 这样程序里面可以控制超时的时候做一些clean up, 线程worker的超时异常可以参考 `dramatiq <https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst#dramatiq例子>`_

系统调用
~~~~~~~~~

但是有个问题, 就算调用PyThreadState_SetAsyncExc, 也不会取消掉系统调用. 

比如time.sleep, 或者socket.recv, 就算你添加了异常exc, 但是由于线程已经处于等待中断状态(放在os的休眠队列中)

那么未被中断唤醒之前线程是不会被调度的, 那么这个exc在python代码也不会被raise, 所以就出现了为线程添加了exc异常, 但是由于阻塞在系统调用, 在系统调用返回之前是catch不到这样异常的,

也就是说你超时10s, 然后你函数执行time.sleep(30), 那么这个异常依然是在30s的时候才会被catch到, 因为此时time.sleep才结束, 线程才会被os调度, 然后解释器发现有异常, 才会raise异常

**所以定义的task应该最好自己去处理timeout, 比如使用select这种去设置一个自己的timeout等等**

协程
--------

直接设置定时器, 如果coro超时没有返回, 那么调用coro.throw, 终止协程就好, curio会取消掉对应的系统调用的回调的.


协程高低水位
=================

因为协程只是一个py对象, 那么理论上是可以无限生成的, 但是coroutine越多, 那么调度也越频繁, cpu消耗也更好, 同样的, coroutine对象越多, 内存占用就的越多

鉴于python内存返回给os并不那么"频繁", 那么内存也会变大.

所以需要根据实际情况, 设定一个task数量的阀值, 称为高水位, 一旦超过这个阀值, 就不会再spawn协程. 

超过高水位之后, 如果需要等到没有worker协程在运行才继续spawn的话, 又丧失了协程spawn很便宜的特性, 所以需要设置一个阀值, 一旦task数量超过了高水位,

那么等到worker协程数量低于这个阀值之后, 就可以重新spawn了, 这个阀值称为低水位.

