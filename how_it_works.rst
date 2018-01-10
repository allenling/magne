组件
=========

分为三个组件: master, connection, worker_pool

master
=========

启动
--------


关闭
--------

connection
==============


建立连接
-----------


send/rev
-----------


关闭
---------


worker_pool
==============


启动
--------


监视超时
------------

超时就杀死worker, 特别是线程worker和进程worker的时候不太好, 因为杀死worker的话一些资源就不会被正确释放~~~

可以给worker一个超时异常, 这样程序里面可以控制超时的时候做一些clean up, 线程worker的超时异常可以参考 `dramatiq <https://github.com/allenling/magne/tree/master/magne/thread_worker/how_rabbitpy_dramatiq_works.rst>`_


关闭
--------




