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

consumption speed Compares to celery and dramatiq

process worker, no thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

APPROXIMATELY EQUAL TO celery, much slower than dramatiq

process process with threads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

coroutine consumer
~~~~~~~~~~~~~~~~~~~~~~~

100 tasks

1 process coroutine consumer APPROXIMATELY EQUAL TO dramatiq with 8 processes.


