import pika
import logging
import curio

from magne.logger import get_component_log


def parse_amqp_url(amqp_url):
    protocol, address = amqp_url.split('://')
    assert protocol == 'amqp'
    name_pwd, ip_address = address.split('@')
    username, pwd = name_pwd.split(':')
    host, port = ip_address.split(':')
    if '//' in port:
        port, vhost = port.split('//')[0], '/'
    else:
        port, vhost = port.split('/')
    return username, pwd, host, port, vhost


def register(func):
    '''
    register as a task
    '''
    tasks[func.__qualname__.upper()] = func
    return func


tasks = {}


CLIENT_INFO = {'platform': 'Python 3.6.3', 'product': 'async amqp connection', 'version': '0.1.0'}


class Exchange:
    def __init__(self, name):
        self.name = name


class Queue:
    def __init__(self, name):
        self.name = name


class Channel:
    def __init__(self, channel_number):
        self.channel_number = channel_number


class BaseAsyncAmqpConnection:
    MAX_DATA_SIZE = 131072
    logger_name = 'Magne-AsyncConnection'
    client_info = CLIENT_INFO

    def __init__(self, queues, amqp_url, qos, log_level=logging.DEBUG):
        self.queues = queues
        self.qos = qos
        self.log_level = log_level
        self.logger = self.get_logger()
        self.username, self.pwd, self.host, self.port, self.vhost = parse_amqp_url(amqp_url)
        self.start_bodys = []
        return

    def get_logger(self):
        return get_component_log(self.logger_name, self.log_level)

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
        start_ok_obj = pika.spec.Connection.StartOk(client_properties=self.client_info, response=start_ok_response)
        frame_value = pika.frame.Method(0, start_ok_obj)
        await self.sock.sendall(frame_value.marshal())
        return

    async def send_tune_ok(self):
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
            self.logger.debug('bind exchange %s and queue %s' % (exchange_name, queue_name))
        await self.update_qos(channel_number, self.qos)
        self.logger.info('update qos %s' % self.qos)
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
                self.start_bodys.extend(self.parse_amqp_body(data[count:]))
        self.logger.debug('start consume done!')
        return

    async def ack(self, channel_number, delivery_tag):
        self.logger.debug('ack: %s, %s' % (channel_number, delivery_tag))
        ack = pika.spec.Basic.Ack(delivery_tag=delivery_tag)
        frame_value = pika.frame.Method(channel_number, ack)
        await self.sock.sendall(frame_value.marshal())
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

    def parse_amqp_body(self, data):
        # [Basic.Deliver, frame.Header, frame.Body, ...]
        bodys = []
        while data:
            count, frame_obj = pika.frame.decode_frame(data)
            data = data[count:]
            if isinstance(frame_obj.method, pika.spec.Basic.Deliver):
                body = {'channel': frame_obj.channel_number,
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
                        bodys.append(body)
        return bodys
