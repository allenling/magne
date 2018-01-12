#######################
coroutine amqp consumer
#######################

模型
====



功能
====


1. qos可配置

2. cold/warm shutdown, shutdown能保证尽量ack

TODO
====


看情况

1. 掉线重连

2. 高低水位可配置

3. batch ack

benchmark
=========

.. code-block:: 

    pip install -r bench_requirements.txt
    
    python3.6 bench.py --help

