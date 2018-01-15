import logging
import signal
import os
import importlib
import threading

import curio

from magne.thread_worker.worker_pool import ThreadWorkerPool
from magne.thread_worker.connection import Connection
from magne.helper import tasks
from magne.logger import get_component_log


class ThreadMaster:
    name = 'Magne-ThreadMaster'

    def __init__(self, worker_timeout, task_module_path, thread_nums, qos, amqp_url, log_level=logging.DEBUG):
        self.amqp_url = amqp_url
        self.qos = qos
        self.log_level = log_level
        self.thread_nums = thread_nums
        self.worker_timeout = worker_timeout
        self.task_module_path = task_module_path
        self.task_module = importlib.import_module(task_module_path)
        self._tasks = tasks
        self.logger = get_component_log(self.name, log_level)
        return

    async def watch_signal(self):
        while True:
            # term for warm shutdown
            # int  for cold shutdown
            # hup  for reload
            ss = [signal.SIGTERM, signal.SIGINT, signal.SIGCHLD, signal.SIGHUP]
            sname = {i.value: i.name for i in ss}
            with curio.SignalQueue(*ss) as sq:
                signo = await sq.get()
                self.logger.info('get signal: %s' % sname[signo])
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
        self.logger.info('starting in pid: %s' % (os.getpid()))
        amqp_task_queue = curio.Queue()
        ack_queue = curio.Queue()
        self.con = Connection(ack_queue, amqp_task_queue, list(self._tasks.keys()), self.amqp_url, self.qos, self.log_level)
        con_run_task = await curio.spawn(self.con.run)
        await con_run_task.join()

        self.worker_pool = ThreadWorkerPool(self.thread_nums, self.worker_timeout, self.task_module,
                                            ack_queue, amqp_task_queue, self.log_level
                                            )
        pool_task = await curio.spawn(self.worker_pool.run)
        await pool_task.join()

        signal_task = await curio.spawn(self.watch_signal)
        await signal_task.join()

        return

    async def shutdown(self, warm=True):
        self.logger.info('shutdown~~~~~~~~~~~~~~~~')
        self.worker_pool.close()
        await self.con.close()
        return


def main(worker_timeout, task_module_path, thread_nums, qos, amqp_url='amqp://guest:guest@localhost:5672//', log_level=logging.DEBUG, curio_debug=False):
    thread_master = ThreadMaster(worker_timeout, task_module_path, thread_nums,
                                 qos, amqp_url, log_level=log_level
                                 )
    curio.run(thread_master.start, with_monitor=curio_debug)
    return


if __name__ == '__main__':
    import sys
    log_level = logging.DEBUG
    if len(sys.argv) == 2 and '--log-level' in sys.argv[1]:
        log_level_name = sys.argv[1].split('=')[1]
        if log_level_name == 'INFO':
            log_level = logging.INFO
    main(30, 'magne.thread_worker.demo_tasks', 4, 16, log_level=log_level, curio_debug=True)
