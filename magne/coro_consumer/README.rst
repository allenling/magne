coroutine amqp consumer
=========================

1. qos config

2. warm/cold shutdown

   shutdown makes sure ack accomplishｍent.

TODO: 
~~~~~~~~~~

1. reconnect

feature:
~~~~~~~~~~

1. batch ack, maybe

benchmark
-------------
pip install -r requirements.txt

python3.6 bench.py

