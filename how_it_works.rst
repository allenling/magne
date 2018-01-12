########
模型思路
########

简化为三个角色: master, connection, worker_pool

用curio重写跟网络有关的部分, 包括组件之间的数据交互


amqp相关
==========

* connection和channel如何分配?

一般best practice告诉我们, 一个线程一个channel, 但是这样比较麻烦, 一个channel一个线程, 线程有问题的话, 必须关闭掉channel, 否则无法关闭connection,

所以要检测线程有问题的情况. 然后就算一个线程一个channel, 那么读写channel还是只能通过指定的socket来和rabbitmq交互, 无法做到一个线程watch不同的channel.

如果多个线程都watch一个socket, 显然recv会有混乱的情况, 这样真不如另开一个线程, 称为io线程, 去专门send/recv, 那这样的话, 一个connection, 一个channel就够了.

没看到多个channel的必要.

所以我觉得没必要. 一个connection一个channel就好了, worker跟connection或者channel都无关, worker只负责处理task.

master
=========

协调角色, 保证正确的开启connection和worker pool, 已经正确的关闭

开启和关闭都需要有顺序才能保证流程正确.


connection
==============




worker_pool
==============



建立amqp连接
===============


send/rev
===========

协程
-------

线程
--------


监视超时
============

进程
-------

线程
-------

超时就杀死worker, 特别是线程worker和进程worker的时候不太好, 因为杀死worker的话一些资源就不会被正确释放~~~

可以给worker一个超时异常, 这样程序里面可以控制超时的时候做一些clean up, 线程worker的超时异常可以参考 `dramatiq <https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst>`_

协程
--------


协程高低水位
=================



