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

benchmark reference: dramatiq https://github.com/Bogdanp/dramatiq/blob/master/benchmarks/bench.py

benchmark function: latency_bench

process worker, no thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

APPROXIMATELY EQUAL TO celery, much slower than dramatiq

100 tasks, celery takes 40s, dramatiq takes 6s.

process process with threads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

coroutine consumer
~~~~~~~~~~~~~~~~~~~~~~~

magne.coro_consumer

qos=0, and coroutine with one process, dramatiq with 8 processes

(draw table library: https://github.com/allenling/draw-docs-table)

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

