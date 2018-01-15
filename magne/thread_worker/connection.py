import curio
import pika
import struct


from magne.helper import BaseAsyncAmqpConnection

CLIENT_INFO = {'platform': 'Python 3.6.3', 'product': 'coro thread worker', 'version': '0.1.0'}


class Connection(BaseAsyncAmqpConnection):
    logger_name = 'MagneThread-Connection'
    client_info = CLIENT_INFO
    qos_global = False

    def __init__(self, ack_queue, amqp_queue, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self.ack_queue = ack_queue
        self.amqp_queue = amqp_queue
        self.fragment_frame = []
        self.ack_done = curio.Event()
        return

    async def run(self):
        await self.connect()
        await self.start_consume()
        self.fetch_task = await curio.spawn(self.fetch_from_amqp)
        self.wait_ack_task = await curio.spawn(self.wait_ack)
        return

    async def wait_ack(self):
        while True:
            data = await self.ack_queue.get()
            ack_msg = [data]
            if self.ack_queue.empty() is False:
                # fetch all data
                for d in self.ack_queue._queue:
                    ack_msg.append(d)
                self.ack_queue._task_count = 0
                self.ack_queue._queue.clear()
            self.ack_done.clear()
            for c_number, d_tag in ack_msg:
                await self.ack(c_number, d_tag)
            await self.ack_done.set()
            self.logger.debug('ack %s done, set ack_done: %s' % (len(ack_msg), self.ack_done.is_set()))
        return

    async def start_consume(self):
        # create amqp consumers
        for tag, queue_name in enumerate(self.queues):
            start_comsume = pika.spec.Basic.Consume(queue=queue_name, consumer_tag=str(tag))
            self.logger.debug('send basic.Consume %s %s' % (queue_name, str(tag)))
            frame_value = pika.frame.Method(self.channel_obj.channel_number, start_comsume)
            await self.sock.sendall(frame_value.marshal())
            data = await self.sock.recv(self.MAX_DATA_SIZE)
            count, frame_obj = pika.frame.decode_frame(data)
            if isinstance(frame_obj.method, pika.spec.Basic.ConsumeOk) is False:
                if isinstance(frame_obj.method, pika.spec.Basic.Deliver):
                    count = 0
                else:
                    raise Exception('got basic.ConsumeOk error, frame_obj %s' % frame_obj)
            self.logger.debug('get basic.ConsumeOk')
            # message data after ConsumeOk
            if len(data) > count:
                await self.parse_and_spawn(data[count:])
        self.logger.debug('start consume done!')
        return

    async def fetch_from_amqp(self):
        self.logger.info('staring fetch_from_amqp')
        try:
            while True:
                try:
                    data = await self.sock.recv(self.MAX_DATA_SIZE)
                except ConnectionResetError:
                    self.logger.error('fetch_from_amqp ConnectionResetError, wait for reconnect...')
                except curio.CancelledError:
                    self.logger.info('fetch_from_amqp cancel')
                    break
                except Exception as e:
                    self.logger.error('fetch_from_amqp error: %s' % e, exc_info=True)
                else:
                    await self.parse_and_spawn(data)
        except curio.CancelledError:
            self.logger.info('fetch_from_amqp canceled')
        return

    def fragment_frame_size(self, data_in):
        try:
            (frame_type, channel_number,
             frame_size) = struct.unpack('>BHL', data_in[0:7])
        except struct.error:
            return 0, None

        # Get the frame data
        frame_end = pika.spec.FRAME_HEADER_SIZE + frame_size + pika.spec.FRAME_END_SIZE
        return frame_end

    async def parse_and_spawn(self, data):
        # [Basic.Deliver, frame.Header, frame.Body, ...]
        count = 0
        last_body = {}
        if self.fragment_frame:
            last_body, frag_data = self.fragment_frame
            data = frag_data + data
            self.fragment_frame = []
        while data:
            # 不完整的frame只可能在第一个或者最后一个
            # 第一个的话, 意味着上一次的最后一个frame也是不完整的
            # 那么我们把这两个不完整的拼接起来
            try:
                count, frame_obj = pika.frame.decode_frame(data)
            except Exception as e:
                self.logger.error('decode_frame error: %s, %s' % (data, e), exc_info=True)
                self.fragment_frame.extend([last_body, data])
                break
            else:
                if frame_obj is None:
                    self.logger.error('fragment fragment frame: %s' % data)
                    self.fragment_frame.extend([last_body, data])
                    break
            data = data[count:]
            if getattr(frame_obj, 'method', None) and isinstance(frame_obj.method, pika.spec.Basic.Deliver):
                last_body = {'channel': frame_obj.channel_number,
                             'delivery_tag': frame_obj.method.delivery_tag,
                             'consumer_tag': frame_obj.method.consumer_tag,
                             'exchange': frame_obj.method.exchange,
                             'routing_key': frame_obj.method.routing_key,
                             }
            elif isinstance(frame_obj, pika.frame.Body):
                last_body['data'] = frame_obj.fragment.decode("utf-8")
                await self.amqp_queue.put(last_body)
                count += 1
                last_body = {}
        return

    async def close(self):
        await self.fetch_task.cancel()
        self.logger.debug('ack queue empty: %s, ack_done.is_set: %s' % (self.ack_queue.empty(), self.ack_done.is_set()))
        if self.ack_queue.empty() is False and self.ack_done.is_set() is False:
            self.ack_done.clear()
            try:
                self.logger.info('wait 600(s) for ack done')
                await curio.timeout_after(600, self.ack_done.wait)
            except curio.TaskTimeout:
                self.logger.warning('wait ack timeout')
        await self.wait_ack_task.cancel()
        await self.send_close_connection()
        return
