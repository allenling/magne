'''
TODO: detect connection list lost
'''
import logging
import json
import importlib
import os
import signal

import curio
from curio import SignalQueue

from magne.logger import get_component_log
from magne.helper import BaseAsyncAmqpConnection, tasks as helper_tasks

CLIENT_INFO = {'platform': 'Python 3.6.3', 'product': 'coro amqp consumer', 'version': '0.1.0'}


class LarvaePool:

    def __init__(self, ack, timeout, task_module, log_level=logging.DEBUG):
        # TODO: detect connection lost, and wait for reconnect(event)
        # ack is the async method/function
        self.ack = ack
        self.timeout = timeout
        self.task_module = task_module
        self.watching = {}
        self.logger = get_component_log('Magne-LarvaePool', log_level)
        return

    async def spawning(self, bodys):
        # spawn consumers as many as we can
        for b in bodys:
            self.logger.debug('got body: %s' % b)
            ack_immediately = False
            try:
                channel, devlivery_tag, data = b['channel'], b['delivery_tag'], json.loads(b['data'])
                task_name, args = data['func'], data['args']
                task = getattr(self.task_module, task_name)
                assert task is not None
            except Exception:
                ack_immediately = True
                self.logger.error('invalid body frame: %s' % b, exc_info=True)
            # spawn daemon, for ignoring `never joined`, and when closing, we will cancel all!
            broodling_task = await curio.spawn(self.broodling, channel, devlivery_tag, task, args, ack_immediately, daemon=True)
            self.logger.debug('spawn task %s(%s)' % (task_name, args))
            self.watching['%s_%s' % (channel, devlivery_tag)] = broodling_task
        return

    async def broodling(self, channel, devlivery_tag, task, args, ack_immediately=False):
        try:
            msg = ''
            if ack_immediately is False:
                try:
                    # timeout will cancel coro
                    res = await curio.timeout_after(self.timeout, task, *args)
                except curio.TaskTimeout:
                    msg = 'task %s(%s) timeout' % (task, args)
                except Exception:
                    msg = 'await timeout task %s(%s) exception' % (task, args)
                else:
                    msg = 'task %s(%s) done, res: %s' % (task, args, res)
            await self.ack(channel, devlivery_tag)
            del self.watching['%s_%s' % (channel, devlivery_tag)]
            self.logger.info(msg)
        except curio.CancelledError:
            self.logger.info('broodling %s canceled' % devlivery_tag)
        return

    async def close(self, warm=True):
        # close all watch tasks
        if warm is True:
            self.logger.info('waiting for watch tasks join, timeout: %s' % self.timeout)
            try:
                async with curio.timeout_after(self.timeout):
                    async with curio.TaskGroup(self.watching.values()) as wtg:
                        await wtg.join()
            except curio.TaskTimeout:
                # all task would be canceled if task group join timeout!!!
                self.logger.info('watch task group join timeout...')
        else:
            self.logger.info('cold shutdown, cancel all watching tasks')
            for t in self.watching:
                await t.cancel()
        # delete ack
        self.ack = None
        return


class SpellsConnection(BaseAsyncAmqpConnection):
    logger_name = 'Magne-Connection'
    client_info = CLIENT_INFO

    async def run(self, spawn_method):
        await self.connect()
        await self.start_consume()
        self.fetch_task = await curio.spawn(self.fetch_from_amqp, spawn_method)
        return

    async def fetch_from_amqp(self, spawn_method):
        self.logger.info('staring fetch_from_amqp')
        if self.start_bodys:
            sbodys = self.start_bodys
            self.start_bodys = []
            await spawn_method(sbodys)
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
                    bodys = self.parse_amqp_body(data)
                    self.logger.debug('bodys: %s' % bodys)
                    await spawn_method(bodys)
        except curio.CancelledError:
            self.logger.info('fetch_from_amqp canceled')
        return

    async def preclose(self):
        await self.fetch_task.cancel()
        return

    async def close(self):
        await self.send_close_connection()
        return


class Queen:
    name = 'Magne-Queue'

    def __init__(self, timeout, task_module, qos, amqp_url='amqp://guest:guest@localhost:5672//', log_level=logging.DEBUG):
        self.task_modue = importlib.import_module(task_module)
        self.timeout = timeout
        self.qos = qos
        self.log_level = log_level
        self.logger = get_component_log(self.name, log_level)
        self.amqp_url = amqp_url
        self.queues = list(helper_tasks.keys())
        return

    async def watch_signal(self):
        while True:
            # term for warm shutdown
            # int  for cold shutdown
            # hup  for reload
            with SignalQueue(signal.SIGTERM, signal.SIGINT, signal.SIGHUP) as sq:
                signo = await sq.get()
                self.logger.info('get signal: %s' % signo)
                if signo == signal.SIGHUP:
                    self.logger.info('reloading...')
                    # TODO: reload, restart?
                    continue
                if signo == signal.SIGTERM:
                    self.logger.info('kill myself...warm shutdown')
                    await self.shutdown()
                else:
                    self.logger.info('kill myself...cold shutdown')
                    await self.shutdown(warm=False)
                break
        return

    async def start(self):
        self.logger.info('Queue pid: %s' % os.getpid())
        self.con = SpellsConnection(self.queues, self.amqp_url, self.qos, log_level=self.log_level)

        self.spawning_pool = LarvaePool(self.con.ack, self.timeout, self.task_modue, log_level=self.log_level)
        con_run_task = await curio.spawn(self.con.run, self.spawning_pool.spawning)
        await con_run_task.join()
        signal_task = await curio.spawn(self.watch_signal)
        await signal_task.join()
        return

    async def shutdown(self, warm=True):
        await self.con.preclose()
        await self.spawning_pool.close(warm)
        await self.con.close()
        return


def main():
    import sys
    log_level = logging.DEBUG
    if len(sys.argv) == 2 and '--log-level' in sys.argv[1]:
        log_level_name = sys.argv[1].split('=')[1]
        if log_level_name == 'INFO':
            log_level = logging.INFO
    queen = Queen(30, 'magne.coro_consumer.demo_task', 0, log_level=log_level)
    curio.run(queen.start, with_monitor=True)
    return


if __name__ == '__main__':
    main()
