'''
master start worker pool and connection, wait signal, coordinate close process

start process:

  1. start a amqp connection(join mode), if can not start a amqp connection, raise exception and fail

  2. start worker pool

  3. wait signal

close process:

  1. worker pool: wait and close(kill) all worker processes, do not get amqp msg, try send all ack msgs into ack queue

  2. connection : try send all ack msgs to rabbitmq, close connection


task1 ---> exchange1 ---> queue1 ---> consumer1 ----                                       ---> worker 1
                                                    |  qos                                |
                                                     -------> channel ---> connection ---> ---> worker 2
                                                    |                                     |
task2 ---> exchange2 ---> queue2 ---> consumer2-----                                       ---> worker n

'''
import os
import logging
import signal
import importlib

import curio
from curio import Queue
from curio import SignalQueue

from magne.connection import MagneConnection
from magne.worker_pool import MagneWorkerPool
from magne.helper import tasks as _tasks
from magne.logger import get_component_log


class MagneMaster:

    def __init__(self, worker_nums, worker_timeout, task_module, amqp_url, shutdown_wait=30, logger_level=None):
        self.worker_nums = worker_nums
        self.worker_timeout = worker_timeout
        self.shutdown_wait = shutdown_wait
        self.task_module = task_module
        self.amqp_url = amqp_url
        self.logger_level = logger_level or logging.INFO
        self.logger = get_component_log('Master', self.logger_level)
        self.logger.debug('ready for start!!!!!!!!!!!!!')
        return

    async def watch_signal(self):
        # TODO: reload, restart, blablabla...
        while True:
            with SignalQueue(signal.SIGTERM, signal.SIGINT, signal.SIGCHLD, signal.SIGHUP) as sq:
                signo = await sq.get()
                self.logger.info('get signal: %s' % signo)
                if signo != signal.SIGCHLD:
                    self.logger.info('kill myself...')
                    # TODO: kill myself
                    break
                else:
                    self.worker_pool.reap_workers()
                    continue
        return

    async def start(self):
        self.logger.info('starting...')

        self.logger.info('import task_module')
        self._task_module = importlib.import_module(self.task_module)
        self.tasks = _tasks
        assert len(self.tasks) != 0

        # communication queue between worker pool and connection
        con_put_worker_pool = Queue()
        worker_pool_put_con = Queue()

        self.logger.info('create new connection instance')
        self.con = MagneConnection(queues=list(self.tasks.keys()), logger=get_component_log('Connection', self.logger_level),
                                   putter_queue=con_put_worker_pool, getter_queue=worker_pool_put_con,
                                   qos=self.worker_nums, amqp_url=self.amqp_url,
                                   )

        self.logger.info('create new worker pool instance')
        self.worker_pool = MagneWorkerPool(worker_nums=self.worker_nums, worker_timeout=self.worker_timeout,
                                           putter_queue=worker_pool_put_con, getter_queue=con_put_worker_pool,
                                           task_module_path=self.task_module,
                                           logger=get_component_log('WorkerPool', self.logger_level),
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

        self.logger.info('watch signal')
        self.sig_task = await curio.spawn(self.watch_signal)

        self.logger.info('started %s' % os.getpid())
        return

    async def close_worker_pool(self):
        self.worker_pool.close()
        return

    async def close_connection(self):
        self.con.close()
        return

    async def shutdown(self):
        # cancel task
        await self.sig_task.cancel()
        # master coordinate close process
        # must close worker pool first
        await self.close_worker_pool()
        # and close connection
        await self.close_connection()
        return


def main(worker_nums, worker_timeout, task_module, amqp_url, shutdown_wait, logger_level):
    # for test
    m = MagneMaster(worker_nums, worker_timeout, task_module, amqp_url, shutdown_wait, logger_level)
    try:
        curio.run(m.start, with_monitor=True)
    except Exception:
        print('run magne master exception!')
    return


if __name__ == '__main__':
    # for test
    main(os.cpu_count(), 30, 'magne.demo_task', 'amqp://guest:guest@localhost:5672//', 30, logger_level=logging.DEBUG)
