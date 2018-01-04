magne
=======

Curio, RabbitMQ, Distributed Task Queue

Python >= 3.6, curio >= 0.8, pika >= 0.11.2

usage
------

1.git clone or download and
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: 

    pip install -r requirements.txt
    cd magne/magne


2. run process worker
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    python run.py process --help

3. run coroutine worker
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    python run.py coroutine

how it works
--------------

bechmark
-----------

ubuntu16.04 Intel(R) Core(TM) i5-4250U(4 cores)

benchmark reference: **dramatiq** https://github.com/Bogdanp/dramatiq/blob/master/benchmarks/bench.py

benchmark function: latency_bench

drawing library: https://github.com/allenling/draw-docs-table

1. process worker, no thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------+--------------+----------+
|       +              +          +
| tasks + celery/magne + dramatiq +
|       +              +          +
+-------+--------------+----------+
|       +              +          +
| 100   + 45.12s       + 6.52s    +
|       +              +          +
+-------+--------------+----------+

2. process worker with threads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

3. coroutine consumer
~~~~~~~~~~~~~~~~~~~~~~~

qos = 0

single process coroutine

dramatiq runs 8 processes

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

and when there are 1200+ ready tasks in curio(>1500 tasks in rabbitmq), coroutine process would takes almost 100% cpu and hang.

3.1 lower water and height water
++++++++++++++++++++++++++++++++++

when the amount of ready task reach height water, we will wait until amount of ready task down to low water

set lower water to 400, height water to 1000

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


3.2 multiprocess
++++++++++++++++++

one child process, on amqp channel

