magne thread worker
=====================

最早的想法是主线程watch socket, 然后recv, 获取msg, 然后传给线程池这样, 不然如果是一个线程watch一个channel的话怎么做?

*做不到, send/recv都是针对socket的, 多个channel也都是通过一个socket交互.*

channel的msg都是通过socket传进来的, 然后每个线程都watch同一个socket么?如果此时收到的msg不是当前线程的channel的话, 怎么办?

*一般做法都是开一个io线程, 其他子线程都是把io交给io线程*

amqp的best practice都说一个线程一个channel, 是说一个线程负责消费指定channel的msg?

如果是这样的话, 何必多个channel?一个channel, 然后多个线程(线程池)来消费msg这样不好么?

rabbitpy的例子
------------------

rabbitpy主线程会开启一个io线程, io线程去send, recv, recv的时候看是哪个channel, 然后把msg发给对应的子线程~~~所以线程数量=主线程+io线程+N个子线程

实例代码: https://rabbitpy.readthedocs.io/en/latest/threads.html


.. code-block:: python

    def consumer(connection):
        received = 0
        with connection.channel() as channel:
            for message in rabbitpy.Queue(channel, QUEUE).consume_messages():
                print(message.body)
                message.ack()
                received += 1
                print('received: %s' % received)
                time.sleep(10)
                if received == MESSAGE_COUNT:
                    break

    def main():
        这里当实例化Connection的时候, 已经开启一个io线程去建立连接了
        with rabbitpy.Connection() as connection:
	    kwargs = {'connection': connection}
        # 开启子线程
        consumer_thread = threading.Thread(target=consumer, kwargs=kwargs)
	consumer_thread.start()

实例化Connection
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    class Connection(base.StatefulObject):
        def __init__(self, url=None):
            self._connect()
        def _connect(self):
            self._set_state(self.OPENING)
    	    # 开启一个io线程
            self._io = self._create_io_thread()
            self._io.daemon = True
            self._io.start()

所以当在子线程里面调用with connection的时候, 已经是建立好了的connection

开启channel
~~~~~~~~~~~~


.. code-block:: python

    class Connection(base.StatefulObject):

        def channel(self, blocking_read=False):

            with self._channel_lock:
	        # 获取channel的id
                channel_id = self._get_next_channel_id()
		# 这个channel_frames就是channel的_read_queue
		# channel实例化的时候第五个参数就是了
                channel_frames = queue.Queue()
		# 这里创建channel
                self._channels[channel_id] = channel.Channel(channel_id, self.capabilities,
                                                             self._events,
                                                             self._exceptions,
                                                             channel_frames,
                                                             self._write_queue,
                                                             self._max_frame_size,
                                                             self._io.write_trigger,
                                                             self,
                                                             blocking_read)
	        # 这里的_add_channel_to_io就是IO._channels[int(channel)] = channel, write_queue
		# 这里的write_queue是io的写queue, 对应来说就是channel的_read_queue, 也就是channel_frame
		# channel被保存到io线程内的字典而已

                self._add_channel_to_io(self._channels[channel_id], channel_frames)

		# 这里的open就是构建channel.open的frame, 然后通过write_trigger
		# 来让io线程去发送frame, write_trigger就是一个socket, 对这个socket发送一个字符, io线程收到提醒就发送
		# 缓存区(self._write_queue)里面的数据了

                self._channels[channel_id].open()
                return self._channels[channel_id]

所以, 子线程中开启channel也是通过io线程来完成

send/rev
~~~~~~~~~

接下来是接收frame和分配到子线程的过程

整个send/rev都是在io线程完成的


.. code-block:: python

    # rabbitpy.io.IO

    class IO(threading.Thread, base.StatefulObject):

        def run(self):
            self._connect()# io线程启动的时候先去连接, 然后开启io loop
            self._loop = _IOLoop(
                self._socket, self.on_error, self.on_read, self.on_write,
                self._write_queue, self._events, self._write_listener,
                self._exceptions)
            # 启动loop, 这个loop就是epoll的poll了, 这里注册了self.on_read作为读取到数据时候的回调
            self._loop.run() 

        def on_read(self, data):
            # on_read就是读取到数据的时候的回调
            self._buffer += data 

            while self._buffer:
                # value是已经解包好的数据, value[0]是channel的id, value[1]是数据, 这里把数据发送给对应的channel线程
                self._add_frame_to_read_queue(value[0], value[1]) 

        def _add_frame_to_read_queue(self, channel_id, frame_value):
            self._channels[channel_id][1].put(frame_value) # channel初始化的是会把自己和自己的write_queu注册

然后呢, channel如何拿到frame? 

.. code-block:: python

    # 这一句呢, 最后会回到rabbitpy.base.AMQPChannel._wait_on_frame中了
    for message in rabbitpy.Queue(channel, QUEUE).consume_messages():
        pass

channel等待frame的到来

.. code-block:: python

    # rabbitpy.base.AMQPChannel._wait_on_frame
    def _wait_on_frame(self, frame_type=None):
        start_state = self.state
        self._waiting = True
        while (start_state == self.state and
                not self.closed and
                not self._connection.closed):
            value = self._read_from_queue() # 这一句就是等待之前io线程的write_queue有数据了

ack的过程
~~~~~~~~~~~~

ack呢也是把数据发送给io线程, 让它去发送的了

.. code-block:: python

    # rabbitpy.message.Message.ack
    def ack(self, all_previous=False):
        # 这里就是把ack通过channel来发送, 流程和开启channel的时候一样, write_trigger
        self.channel.write_frame(basic_ack) 

小结
~~~~~~

所以所谓的一个线程一个channel就是每一个线程负责消费对应channel的数据, 然后所有的send/recv都由io线程来执行, recv的时候通过

queue来唤醒对应的线程.

**那么, 为什么一个channel还不够呢?多个channel的话感觉就很麻烦呀~~~**

既然都是靠一个单独的io线程来分配msg, 那么多个channel的意义呢? 感觉只有每一个channel都能单独send/recv才有单独出来的意义呀

不然多个线程的收发的瓶颈还是在io线程上, 分离channel并不能提高收发, 不如一个channel一个connection, 然后

产生thread pool, 把msg扔到thread pool去执行~~~~

benchmark
------------



