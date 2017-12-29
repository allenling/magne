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

Compare to celery, dramatiq


TODO:
------
1. restart, reload
2. more process control
3. more test
4. monitor, message timestamp, message unique id,...
5. amqp heartbeat?
6. more threads to consume?

