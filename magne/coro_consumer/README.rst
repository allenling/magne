coroutine amqp consumer
=========================

模型
----------

spawn/wrap spawn
~~~~~~~~~~~~~~~~~~~~~

curio有个小细节, 就是你spawn的话, 不会执行spawn里面的协程序, 如果你想spawn一个协程的时候运行直到它挂起(yield)

那么可以加一层协程, 然后join, 例如:

一直spawn
^^^^^^^^^^^^

.. code-block:: python

    async def mycoro():
        await curio.sleep(10)
        return
    
    async main():
        for _ in range(10):
            task = await curio.spawn(mycoro)
        return

这样的话会一直spawn, 直到for结束, spawn的时候会把task放入到ready队列, 并不会执行mycoro, 如果你要执行的话, 只能调用

task.join, 那么这样的话for就是阻塞在tasjk.join上了

并且这样有个问题, 如果task超过一定数量的话, 那么cpu会吃满

wrap spawn
^^^^^^^^^^^^

wrap spawn的思路是在coro前面加一层coro, 称为wrap_coro，然后wrap.join一下, 这样我们的spawn的时候能够执行到coro第一个await

启动

.. code-block:: python

    async def mycoro():
        await curio.sleep(10)
        return

    async def wrap_spawn():
        c = await curio.spawn(mycoro)
        return

    async main():
        for _ in range(10):
            task = await curio.spawn(wrap_spawn)
            task.join()
        return

我们join那个wrap_spawn任务的话会执行到mycoro的第一个await, 然后退出, 这样就达到了我们一边spawn, 并且能同时执行mycoro, 并且不会阻塞在join上

整个项目的spawn都是这个思路, 然后其实就算wrap spawn之后, 回还是会出现cpu吃满的时候, 但是task数量的限制比起直接spawn会多得多得多~~所以高低水位还是需要的

**coro_consumer中, 之前是直接join, 然后达到1200 ready任务的话, cpu吃满, 缓存wrap spwn之后, 每一个处理amqp消息的task都不是ready的, 而是running的, 这个时候

就算有1w个amqp任务进来, 都能达到3000+的running任务同时再执行~~~!!!!**

关于ready任务太多的导致一些未定义行为, 比如有可能cpu挂起, 有一些task会莫名其妙的timeout等等, 原因还需调查


curio.spawn
---------------

spawn是调用到kernel.py中的_trap_spawn

.. code-block:: python

    # Add a new task to the kernel
    def _trap_spawn(coro, daemon):
        # 这里调用下_new_task
        task = _new_task(coro, daemon)
        task.parentid = current.id
        current.next_value = task


    def _new_task(coro, daemon=False):
        nonlocal njobs
        # 创建一个Task
        task = Task(coro, daemon)
        tasks[task.id] = task
        if not daemon:
            njobs += 1
        # 然后加入到ready队列
        _reschedule_task(task)
        return task

    def _reschedule_task(task, value=None, exc=None):
        # 这里只是把任务加入到ready队列而已
        ready_append(task)
        task.next_value = value
        task.next_exc = exc
        task.state = 'READY'
        task.cancel_func = None

Task.join
---------------




功能
--------

1. qos可配置

2. cold/warm shutdown, shutdown能保证尽量ack

TODO: 
----------

看情况

1. 重新连接

2. 高低水位可配置

3. batch ack

benchmark
-------------

.. code-block:: 

    pip install -r requirements.txt
    
    python3.6 bench.py --help

