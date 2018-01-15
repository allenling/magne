
import json
import ctypes

import curio

from magne.logger import get_component_log


class TimeoutException(Exception):
    pass


class WorkerExitException(Exception):
    pass


class ThreadWorker:

    def __init__(self, age, task_module, timeout, task_queue, ack_queue, log_level):
        self.alive = True
        # 发送task
        self.task_queue = task_queue
        # 发送task的结果
        self.task_module = task_module
        self.result_queue = curio.Queue()
        self.ack_queue = ack_queue
        self.timeout = timeout
        self.done_ev = curio.Event()
        self.age = age
        self.name = 'Magne-ThreadWorker_%s' % age
        self.logger = get_component_log(self.name, log_level)
        return

    def process(self, task_queue, ack_queue, task_module):
        while self.alive:
            task_result = None
            timeout = False
            self.done_ev.clear()
            tinfo = None
            try:
                try:
                    msg = curio.thread.AWAIT(curio.timeout_after(self.timeout, task_queue.get))
                except curio.TaskTimeout:
                    continue
                if self.alive is False:
                    self.logger.info('%s alive is False' % self.thread_ident)
                    break
                try:
                    channel, delivery_tag = msg['channel'], msg['delivery_tag']
                    j_data = json.loads(msg['data'])
                    task, args = j_data['func'], j_data['args']
                except Exception as e:
                    self.logger.error('got error msg %s' % msg)
                else:
                    func = getattr(task_module, task)
                    tinfo = 'task: %s, args: %s' % (task, args)
                    self.logger.info('got %s' % tinfo)
                    # spawn超时
                    wrap_watch_task = curio.thread.AWAIT(curio.spawn(self.wrap_watch, daemon=True))
                    curio.thread.AWAIT(wrap_watch_task.join)
                    # 执行
                    task_result = func(*args)
            except TimeoutException:
                timeout = True
            except WorkerExitException:
                self.logger.info('%s got WorkerExitException, break' % self.thread_ident)
                break
            except Exception as e:
                self.logger.error('%s catch exception %s' % (tinfo, e))
            else:
                self.logger.info('%s got result: %s' % (tinfo, task_result))
            curio.thread.AWAIT((ack_queue.put((channel, delivery_tag))))
            if timeout is False:
                curio.thread.AWAIT(self.done_ev.set())
        self.logger.info('worker %s return' % self.thread_ident)
        return

    async def start(self):
        self.async_thread = curio.thread.AsyncThread(target=self.process, args=[self.task_queue,
                                                                                self.ack_queue,
                                                                                self.task_module,
                                                                                ],
                                                     daemon=True
                                                     )
        await self.async_thread.start()
        self.thread_ident = self.async_thread._thread.ident
        self.logger.info('stared, ident: %s' % self.thread_ident)
        return

    async def wrap_watch(self):
        watch_task = await curio.spawn(self.watch_done, daemon=True)
        return watch_task

    async def watch_done(self):
        try:
            await curio.timeout_after(self.timeout, self.done_ev.wait)
        except curio.TaskTimeout:
            ident = self.thread_ident
            self.logger.error('%s timeout' % ident)
            if self.alive:
                return
            self._send_exc(TimeoutException)
        return

    def _send_exc(self, exc):
        thread_id = ctypes.c_long(self.thread_ident)
        exception = ctypes.py_object(exc)
        count = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exception)
        if count == 0:
            self.logger.error("Failed to set exception %s in worker thread." % exc)
        elif count > 1:  # pragma: no cover
            self.logger.error("Exception %s was set in multiple threads.  Undoing..." % exc)
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.c_long(0))
        return

    def close(self):
        self.alive = False
        self._send_exc(WorkerExitException)
        return


class ThreadWorkerPool:

    def __init__(self, thread_nums, worker_timeout, task_module, ack_queue, amqp_queue, log_level):
        self.thread_nums = thread_nums
        self.worker_timeout = worker_timeout
        self.task_module = task_module
        self.ack_queue = ack_queue
        self.amqp_queue = amqp_queue
        self.log_level = log_level
        self.workers = {}
        return

    async def run(self):
        for age in range(self.thread_nums):
            thread_obj = ThreadWorker(age, self.task_module, self.worker_timeout, self.amqp_queue, self.ack_queue,
                                      self.log_level,
                                      )
            start_task = await curio.spawn(thread_obj.start)
            await start_task.join()
            self.workers[thread_obj.thread_ident] = thread_obj
        return

    def close(self):
        for w in self.workers.values():
            w.close()
        return
