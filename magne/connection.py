'''
should not hide close logic, close correctly rely on master coordination

body = {'channel_number': channel_number,
        'delivery_tag': delivery_tag,
        'consumer_tag': consumer_tag,
        'exchange': exchange,
        'routing_key': routing_key,
        'data': {'func_name': func_name, 'args': []},
        }
'''
import json
from collections import deque

import curio
import pika
from curio import Queue, Event

CLIENT_INFO = {'platform': 'Python 3.6.3', 'product': 'curio amqp worker', 'version': '0.1.0'}


class ConnectionStatus:
    INITAL = 0
    ERROR = 1
    RUNNING = 2
    CLOSED = 3
    PRECLOSE = 4


class Exchange:
    def __init__(self, name):
        self.name = name


class Queue:
    def __init__(self, name):
        self.name = name


class Channel:
    def __init__(self, channel_number):
        self.channel_number = channel_number


class MagneConnection:

    # rabbitmq frame max size is 131072
    MAX_DATA_SIZE = 131072

    def __init__(self, queues, logger, getter_queue, putter_queue, qos=None, amqp_url='amqp://guest:guest@localhost:5672//'):
        # queues = ['q1', 'q2', 'q3', ...]
        self.logger = logger
        self.queues = set([i.upper() for i in queues])
        self.amqp_url = amqp_url
        self.getter_queue, self.putter_queue = getter_queue, putter_queue
        self.parse_amqp_url()
        self.status = ConnectionStatus.INITAL
        self.reconnect_done_event = Event()
        self.qos = qos if qos is not None else len(self.queues)
        self.logger.debug('connection initial~~~~')
        return

    def parse_amqp_url(self):
        protocol, address = self.amqp_url.split('://')
        assert protocol == 'amqp'
        name_pwd, ip_address = address.split('@')
        self.username, self.pwd = name_pwd.split(':')
        self.host, port = ip_address.split(':')
        if '//' in port:
            self.port, self.vhost = port.split('//')[0], '/'
        else:
            self.port, self.vhost = port.split('/')
        return

    async def assert_recv_method(self, method_class):
        data = await self.sock.recv(self.MAX_DATA_SIZE)
        frame_obj = pika.frame.decode_frame(data)[1]
        try:
            assert isinstance(frame_obj.method, method_class)
        except Exception as e:
            self.logger.error('assert_recv_method : %s, %s, %s' % (method_class, frame_obj, e), exc_info=True)
            raise e
        return frame_obj

    async def send_amqp_procotol_header(self):
        amqp_header_frame = pika.frame.ProtocolHeader()
        await self.sock.sendall(amqp_header_frame.marshal())
        return

    async def send_start_ok(self):
        start_ok_response = b'\0' + pika.compat.as_bytes(self.username) + b'\0' + pika.compat.as_bytes(self.pwd)
        start_ok_obj = pika.spec.Connection.StartOk(client_properties=CLIENT_INFO, response=start_ok_response)
        frame_value = pika.frame.Method(0, start_ok_obj)
        await self.sock.sendall(frame_value.marshal())
        return

    async def send_tune_ok(self):
        # TODO: for now, do not want a heartbeat
        tunk = pika.spec.Connection.TuneOk(frame_max=self.MAX_DATA_SIZE)
        frame_value = pika.frame.Method(0, tunk)
        await self.sock.sendall(frame_value.marshal())
        return

    async def send_connection_open(self):
        connection_open = pika.spec.Connection.Open(insist=True)
        frame_value = pika.frame.Method(0, connection_open)
        await self.sock.sendall(frame_value.marshal())
        # got openok
        await self.assert_recv_method(pika.spec.Connection.OpenOk)
        return

    async def connect(self):
        self.sock = await curio.open_connection(self.host, self.port)
        self.logger.debug('open amqp connection')
        # send amqp header frame
        await self.send_amqp_procotol_header()
        self.logger.debug('send amqp header')
        # got start
        await self.assert_recv_method(pika.spec.Connection.Start)
        self.logger.debug('get amqp connection.Start')
        # send start ok back
        await self.send_start_ok()
        self.logger.debug('send amqp connection.StartOk')
        # got tune
        await self.assert_recv_method(pika.spec.Connection.Tune)
        self.logger.debug('get amqp connection.Tune')
        # send tune ok
        await self.send_tune_ok()
        self.logger.debug('send amqp connection.TuneOk')
        # and we send open
        await self.send_connection_open()
        self.logger.debug('send amqp connection.Open and get connection.OpenOk')
        # open channel
        await self.open_channel()
        return

    async def open_channel(self):
        # send Channel.Open
        channel_open = pika.spec.Channel.Open()
        self.logger.debug('send channel.Open')
        frame_value = pika.frame.Method(1, channel_open)
        await self.sock.sendall(frame_value.marshal())
        # got Channel.Open-Ok
        frame_obj = await self.assert_recv_method(pika.spec.Channel.OpenOk)
        self.logger.debug('get channel.OpenOk')
        self.channel_number = channel_number = frame_obj.channel_number
        assert frame_obj.channel_number == 1
        self.channel_obj = Channel(channel_number=channel_number)
        # create exchange, queue, and update QOS
        for queue_name in self.queues:
            exchange_name = queue_name
            await self.declare_exchange(channel_number, exchange_name)
            self.logger.debug('declare exchange %s' % exchange_name)
            await self.declare_queue(channel_number, queue_name)
            self.logger.debug('declare queue %s' % queue_name)
            await self.bind_queue_exchange(channel_number, exchange_name, queue_name, routing_key=queue_name)
            self.logger.debug('bind exchange and queue')
        await self.update_qos(channel_number, self.qos)
        self.logger.debug('update qos %s' % self.qos)
        return

    async def declare_exchange(self, channel_number, name):
        exchange_declare = pika.spec.Exchange.Declare(exchange=name)
        frame_value = pika.frame.Method(channel_number, exchange_declare)
        await self.sock.sendall(frame_value.marshal())
        await self.assert_recv_method(pika.spec.Exchange.DeclareOk)
        return Exchange(name=name)

    async def declare_queue(self, channel_number, name):
        queue_declare = pika.spec.Queue.Declare(queue=name)
        frame_value = pika.frame.Method(channel_number, queue_declare)
        await self.sock.sendall(frame_value.marshal())
        await self.assert_recv_method(pika.spec.Queue.DeclareOk)
        return Queue(name=name)

    async def bind_queue_exchange(self, channel_number, exchange, queue, routing_key):
        queue_bind = pika.spec.Queue.Bind(queue=queue, exchange=exchange, routing_key=routing_key)
        frame_value = pika.frame.Method(channel_number, queue_bind)
        await self.sock.sendall(frame_value.marshal())
        await self.assert_recv_method(pika.spec.Queue.BindOk)
        return

    async def update_qos(self, channel_number, qos, global_=True):
        qos_obj = pika.spec.Basic.Qos(prefetch_count=qos, global_=global_)
        frame_value = pika.frame.Method(channel_number, qos_obj)
        await self.sock.sendall(frame_value.marshal())
        await self.assert_recv_method(pika.spec.Basic.QosOk)
        return

    async def ack(self, channel_number, delivery_tag):
        self.logger.info('ack: %s, %s' % (channel_number, delivery_tag))
        ack = pika.spec.Basic.Ack(delivery_tag=delivery_tag)
        frame_value = pika.frame.Method(channel_number, ack)
        await self.sock.sendall(frame_value.marshal())
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
            assert isinstance(frame_obj.method, pika.spec.Basic.ConsumeOk)
            self.logger.debug('get basic.ConsumeOk')
            # message data after ConsumeOk
            if len(data) > count:
                await self.send_msg(data[count:])
        self.logger.debug('start consume done!')
        self.status = ConnectionStatus.RUNNING
        return

    async def run(self):
        # send start consume
        await self.start_consume()
        # spawn wait_queue task
        self.fetch_amqp_task = await curio.spawn(self.fetch_from_amqp)
        self.logger.debug('spawn fetch_from_amqp')
        self.wait_ack_queue_task = await curio.spawn(self.wait_ack_queue)
        self.logger.debug('spawn wait_ack_queue')
        return

    async def reconnect(self):
        self.logger.info('starting reconnect~~~')
        self.reconnect_done_event.clear()
        reconnect_count = 1
        sleep_time = 0
        while True:
            try:
                await self.connect()
                await self.start_consume()
            except ConnectionRefusedError:
                sleep_time += 2 * reconnect_count
                self.logger.info('reconnect %s fail! sleep: %s seconds' % (reconnect_count, sleep_time))
                await curio.sleep(sleep_time)
                reconnect_count += 1
                continue
            except Exception as e:
                self.logger.error('got unexcept exception: %s', e, exc_info=True)
                continue
            self.logger.info('reconnect success!')
            break
        await self.reconnect_done_event.set()
        return

    async def handle_connection_error(self):
        reconnect_task = None
        try:
            if self.status != ConnectionStatus.ERROR:
                self.status = ConnectionStatus.ERROR
                # should perform reconnect
                # spawn and join!
                self.logger.info('spawn reconnect task')
                reconnect_task = await curio.spawn(self.reconnect)
                await reconnect_task.join()
            else:
                # wait for reconnect done
                await self.reconnect_done_event.wait()
        except curio.CancelledError:
            # need manually cancel reconnect_task
            if reconnect_task is not None:
                self.logger.info('reconnect_task cancel')
                await reconnect_task.cancel()
        return

    async def fetch_from_amqp(self):
        self.logger.info('staring fetch_from_amqp')
        try:
            while True:
                try:
                    data = await self.sock.recv(self.MAX_DATA_SIZE)
                    await self.send_msg(data)
                except ConnectionResetError:
                    self.logger.error('fetch_from_amqp ConnectionResetError, wait for reconnect...')
                    await self.handle_connection_error()
                    self.logger.debug('go on fetch_from_amqp')
        except curio.CancelledError:
            # what about reconnect
            self.logger.info('fetch_from_amqp cancel')
        return

    async def send_msg(self, data):
        # [Basic.Deliver, frame.Header, frame.Body, ...]
        bodys = []
        while data:
            count, frame_obj = pika.frame.decode_frame(data)
            data = data[count:]
            if isinstance(frame_obj.method, pika.spec.Basic.Deliver):
                body = {'channel_number': frame_obj.channel_number,
                        'delivery_tag': frame_obj.method.delivery_tag,
                        'consumer_tag': frame_obj.method.consumer_tag,
                        'exchange': frame_obj.method.exchange,
                        'routing_key': frame_obj.method.routing_key,
                        }
                count, frame_obj = pika.frame.decode_frame(data)
                if isinstance(frame_obj, pika.frame.Header):
                    data = data[count:]
                    count, frame_obj = pika.frame.decode_frame(data)
                    if isinstance(frame_obj, pika.frame.Body):
                        data = data[count:]
                        body['data'] = frame_obj.fragment.decode("utf-8")
                        bodys.append(json.dumps(body))
        await self.send_queue(bodys)
        return

    async def send_queue(self, datas):
        for data in datas:
            await self.putter_queue.put(data)
        return

    async def wait_ack_queue(self):
        # TODO: cancel while await ack, what should we do?
        self.logger.info('staring wait_ack_queue')
        try:
            while True:
                ack_delivery_tag = await self.getter_queue.get()
                try:
                    await self.ack(self.channel_number, ack_delivery_tag)
                except ConnectionResetError:
                    self.logger.error('wait_queue ConnectionResetError, wait for reconnect...')
                    await self.handle_connection_error()
                    self.logger.debug('go on wait_ack_queue')
                    # reinsert failed ack_delivery_tag into queue
                    self.getter_queue._queue.appendleft(ack_delivery_tag)
                    self.getter_queue._task_count += 1
        except curio.CancelledError:
            self.logger.info('wait_ack_queue cancel')
        return

    async def send_close_connection(self):
        try:
            # 302: An operator intervened to close the connection for some reason. The client may retry at some later date.
            # close channel first
            close_channel_frame = pika.spec.Channel.Close(reply_code=302, reply_text='close connection',
                                                          class_id=0, method_id=0)
            close_channel_frame_value = pika.frame.Method(self.channel_number, close_channel_frame)
            await self.sock.sendall(close_channel_frame_value.marshal())
            await curio.timeout_after(1, self.assert_recv_method, pika.spec.Channel.CloseOk)
            self.channel_number = 0
            self.logger.info('closed channel')

            close_connection_frame = pika.spec.Connection.Close(reply_code=302, reply_text='close connection',
                                                                class_id=0, method_id=0)
            frame_value = pika.frame.Method(self.channel_number, close_connection_frame)
            await self.sock.sendall(frame_value.marshal())
            await curio.timeout_after(1, self.assert_recv_method, pika.spec.Connection.CloseOk)
            self.logger.info('closed connection')
        except curio.TaskTimeout:
            self.logger.error('send close connection frame got CloseOk TaskTimeout')
        except ConnectionResetError:
            self.logger.error('send close connection frame ConnectionResetError')
        except Exception as e:
            self.logger.error('send close connection frame exception: %s' % e, exc_info=True)
        self.logger.info('closed amqp connection')
        return

    async def close_amqp_connection(self):
        # close connection if necessarily
        if self.status & ConnectionStatus.RUNNING:
            try:
                # last ack
                self.logger.info('last ack...')
                last_ack_delivery_tags = []
                while self.getter_queue.empty() is False:
                    delivery_tag = await self.getter_queue.get()
                    last_ack_delivery_tags.append(delivery_tag)
                for d in last_ack_delivery_tags:
                    await self.ack(self.channel_number, d)
            except ConnectionResetError:
                self.logger.error('last ack occur ConnectionResetError')
            except Exception as e:
                self.logger.error('last ack occur exception: %s' % e, exc_info=True)
            else:
                self.logger.info('closing amqp connection')
                await self.send_close_connection()
        # self.status = ConnectionStatus.CLOSE means we would not accept any ack msg more
        self.status = ConnectionStatus.CLOSED
        return

    async def pre_close(self):
        # would not put any msg into queue more
        self.logger.debug('preclosing...')
        self.logger.debug('empty putter_queue')
        self.putter_queue._queue = deque
        # would not fetch any amqp msg more
        self.logger.debug('cancel fetch_amqp_task...')
        await self.fetch_amqp_task.cancel()
        self.status = self.status | ConnectionStatus.PRECLOSE
        self.logger.debug('status %s, preclose done' % self.status)
        return

    async def close(self):
        '''
        connection should not be closed independently, it should be coordinated by master
        so, before connection close, worker pool have closed already
        '''
        self.logger.debug('closing connection')
        if not (self.status & ConnectionStatus.PRECLOSE):
            self.logger.warning('should pre close connection!!')
        self.logger.debug('cancel wait_ack_queue_task')
        await self.wait_ack_queue_task.cancel()
        self.logger.debug('cancel close_amqp_connection')
        await self.close_amqp_connection()
        self.logger.debug('close connection done')
        return


async def test_connection():
    # for test
    import logging
    import os
    logger = logging.getLogger('test_connection')
    log_handler = logging.StreamHandler()
    log_format = logging.Formatter('%(levelname)s %(asctime)s %(pathname)s %(lineno)d %(message)s')
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    getter_queue = curio.Queue()
    putter_queue = curio.Queue()
    c = MagneConnection(['tc1', 'tc2'], logger, getter_queue, putter_queue)
    logger.info('connection pid: %s' % os.getpid())
    await c.connect()
    await c.run()
    await curio.sleep(10)
    await c.close()
    return


def main():
    curio.run(test_connection, with_monitor=True)
    return


if __name__ == '__main__':
    main()
