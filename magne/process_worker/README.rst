magne process worker
======================

模型
----------


功能
---------

1. qos可配置

2. warm/cold shutdown, shutdown能保证已经完成的任务能都ack掉

3. 掉线重连

4. 重启掉线的worker

TODO
---------

很多amqp的功能, 看情况吧

benchmark
------------

.. code-block:: 

    pip install -r bench_requirements.txt
    
    pythone3.6 bench.py


