'''
master start worker pool and connection, wait signal, coordinate close process

start process:

  1. start a amqp connection(join mode), if can not start a amqp connection, raise exception and fail

  2. start worker pool

  3. wait signal

close process:

  1. connection : preclose, empty putter queue, cancel fetch amqp task

  2. worker pool: empty getter_queue, [wait timeout if warm shutdown and] kill all worker processes, try to send all ack msgs into ack queue

  3. connection : try send all ack msgs to rabbitmq, and close connection


task1 --- exchange1 --- queue1 --- consumer1 ----+                                    +---> worker1
                                                 |  qos                               |
task2 --- exchange2 --- queue2 --- consumer2 ----+ ------- channel --- connection --- +---> worker2
                                                 |                                    |
taskN --- exchangeN --- queueN --- consumerN-----+                                    +---> workerN

'''
import os
import logging
import signal
import importlib

import curio
from curio import Queue
from curio import SignalQueue

import magne
from magne.connection import MagneConnection
from magne.worker_pool import MagneWorkerPool
from magne.helper import tasks as _tasks
from magne.logger import get_component_log


class MagneMaster:

    def __init__(self, worker_nums, worker_timeout, task_module, amqp_url, qos, logger_level=None):
        self.worker_nums = worker_nums
        self.worker_timeout = worker_timeout
        self.task_module = task_module
        self.amqp_url = amqp_url
        self.qos = qos
        self.logger_level = logger_level or logging.INFO
        self.logger = get_component_log('Magne-Master', self.logger_level)
        return

    async def watch_signal(self):
        while True:
            # term for warm shutdown
            # int  for cold shutdown
            # hup  for reload
            with SignalQueue(signal.SIGTERM, signal.SIGINT, signal.SIGCHLD, signal.SIGHUP) as sq:
                signo = await sq.get()
                self.logger.info('get signal: %s' % signo)
                if signo != signal.SIGCHLD:
                    if signo == signal.SIGTERM:
                        self.logger.info('kill myself...warm shutdown')
                        await self.shutdown()
                    elif signo == signal.SIGINT:
                        self.logger.info('kill myself...cold shutdown')
                        await self.shutdown(warm=False)
                    elif signo == signal.SIGHUP:
                        self.logger.info('reloading...')
                        # TODO: reload, restart?
                        pass
                    break
                else:
                    self.worker_pool.reap_workers()
                    continue
        return

    async def start(self):
        self.logger.info('Magne v%s starting..., pid: %s' % (magne.__version__, os.getpid()))

        self._task_module = importlib.import_module(self.task_module)
        self.logger.info('import task_module %s' % self.task_module)
        self.tasks = _tasks
        assert len(self.tasks) != 0

        # communication queue between worker pool and connection
        con_put_worker_pool = Queue()
        worker_pool_put_con = Queue()

        self.logger.debug('create new connection instance')
        self.con = MagneConnection(queues=list(self.tasks.keys()),
                                   logger=get_component_log('Magne-Connection', self.logger_level),
                                   putter_queue=con_put_worker_pool, getter_queue=worker_pool_put_con,
                                   qos=self.qos, amqp_url=self.amqp_url,
                                   )

        self.logger.debug('create new worker pool instance')
        self.worker_pool = MagneWorkerPool(worker_nums=self.worker_nums, logger=get_component_log('Magne-WorkerPool', self.logger_level),
                                           worker_timeout=self.worker_timeout, putter_queue=worker_pool_put_con,
                                           getter_queue=con_put_worker_pool, task_module_path=self.task_module,
                                           )

        self.logger.info('connection.connect')
        # must wait for connect success!
        connect_task = await curio.spawn(self.con.connect)
        # join make sure that subtask  has been spawned
        await connect_task.join()

        self.logger.info('connection.run')
        con_run_task = await curio.spawn(self.con.run)
        # join make sure that subtask  has been spawned
        await con_run_task.join()

        self.logger.info('run worker_pool.start')
        # wait for worker pool start
        wp_start_task = await curio.spawn(self.worker_pool.start)
        # join make sure that subtask  has been spawned
        await wp_start_task.join()

        self.logger.info('watching signal')
        await self.watch_signal()

        self.logger.info('Magne gone...')
        return

    async def close_worker_pool(self, warm):
        await self.worker_pool.close(warm)
        return

    async def close_connection(self):
        await self.con.close()
        return

    async def shutdown(self, warm=True):
        # master coordinate close process
        await self.con.pre_close()
        # must close worker pool first
        await self.close_worker_pool(warm=warm)
        # and close connection
        await self.close_connection()
        return


def main(worker_nums, worker_timeout, task_module, amqp_url, qos, logger_level):
    # for test
    m = MagneMaster(worker_nums, worker_timeout, task_module, amqp_url, qos, logger_level)
    try:
        curio.run(m.start, with_monitor=True)
    except Exception:
        print('run magne master exception!')
    return


if __name__ == '__main__':
    # for test
    ws = 2
    main(ws, 30, 'magne.demo_task', 'amqp://guest:guest@localhost:5672//', ws, logger_level=logging.DEBUG)
