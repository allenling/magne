magne
=======

Curio, RabbitMQ, Distributed Task Queue(celery), MIT LICENSE.

Python >= 3.6, curio >= 0.8, pika >= 0.11.2

usage
------

clone and cd magne, and run magne/run.py:

.. code-block::

    python3.6 magne/run.py --help


how it works
--------------

bechmark
-----------

ubuntu16.04 Intel(R) Core(TM) i5-4250U(4 cores)

benchmark reference: dramatiq https://github.com/Bogdanp/dramatiq/blob/master/benchmarks/bench.py

benchmark function: latency_bench

1. process worker, no thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

APPROXIMATELY EQUAL TO celery, much slower than dramatiq

100 tasks, celery takes 40s, dramatiq takes 6s.

2. process worker with threads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

3. coroutine consumer
~~~~~~~~~~~~~~~~~~~~~~~

coroutine takes more cpu, memory, io usage, monitored by dstat.

qos=0, and coroutine with one process compares with dramatiq with 8 processes

(the library of drawing table: https://github.com/allenling/draw-docs-table)

+-------+-----------+----------+
|       +           +          +
| tasks + coroutine + dramatiq +
|       +           +          +
+-------+-----------+----------+
|       +           +          +
| 100   + 5.33s     + 6.52s    +
|       +           +          +
+-------+-----------+----------+
|       +           +          +
| 1000  + 10.46s    + 39.57s   +
|       +           +          +
+-------+-----------+----------+

and when there are 1200+ ready tasks in curio(>1500 tasks in rabbitmq), one process coroutine would takes almost 100% cpu usage and hang.

3.1 one process, but config lower water and height water
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

when the amount of ready task reach height water, we will wait until amount of ready task down to low water

low water default 400 tasks, height water default is 1000 tasks

+-------+-----------+----------+
|       +           +          +
| tasks + coroutine + dramatiq +
|       +           +          +
+-------+-----------+----------+
|       +           +          +
| 1000  + 10.72s    + 39.57s   +
|       +           +          +
+-------+-----------+----------+
|       +           +          +
| 5000  + 28.95s    + 204.64s  +
|       +           +          +
+-------+-----------+----------+
|       +           +          +
| 10000 + 49.47s    + 408.10s  +
|       +           +          +
+-------+-----------+----------+


3.2 multiprocess, but no lower water and height water
++++++++++++++++++++++++++++++++++++++++++++++++++++++++



