'''
inherit from curio.worker.ProcessWorker

spawn worker processes, manage workers

1. how to watch a busy worker?, spawn timeout_after watch task, set daemon=True

   if daemon=False, annoying nerver joined warning

2. what about edge case? timeout 30s, and worker success in 31s?

body = {'channel_number': channel_number,
        'delivery_tag': delivery_tag,
        'consumer_tag': consumer_tag,
        'exchange': exchange,
        'routing_key': routing_key,
        'data': {'func': func_name, 'args': []},
        }
'''
import os
import errno
import json
import multiprocessing
import signal
import importlib
from collections import deque


import curio
from curio.workers import ProcessWorker, ExceptionWithTraceback
from curio.channel import Connection
from curio import Event


class MagneWorker(ProcessWorker):

    def __init__(self, task_module_path):
        self.delivery_tag = None
        self.func, self.args = None, None
        self.process = None
        self.client_ch = None
        self.terminated = False
        self.ident = None
        self.task_module_path = task_module_path
        self._launch()
        return

    def _launch(self):
        client_ch, server_ch = multiprocessing.Pipe()
        self.process = multiprocessing.Process(target=self.run_server, args=(server_ch, self.task_module_path))
        self.process.start()
        server_ch.close()
        self.client_ch = Connection.from_Connection(client_ch)
        self.ident = self.process.ident
        return

    def shutdown(self):
        self.terminated = True
        if self.process:
            self.process.terminate()
            self.process = None
            self.nrequests = 0
        return

    def run_server(self, ch, task_module_path,):
        # TODO: capture signal?
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        task_module = importlib.import_module(task_module_path)
        while True:
            func_name, args = ch.recv()
            func = getattr(task_module, func_name, None)
            if func is None:
                ch.send((False, 'invalid func name'))
                continue
            try:
                result = func(*args)
                ch.send((True, result))
            except Exception as e:
                e = ExceptionWithTraceback(e, e.__traceback__)
                ch.send((False, e))
            del func, args
        return

    async def apply(self, func, args, delivery_tag):
        # send and return, do not wait for recv
        msg = (func, args)
        self.func, self.args, self.delivery_tag = func, args, delivery_tag
        try:
            await self.client_ch.send(msg)
        except Exception as e:
            self.shutdown()
            raise e
        return

    async def recv(self):
        success, result = await self.client_ch.recv()
        return success, result


class MagneWorkerPool:

    def __init__(self, worker_nums, worker_timeout, getter_queue, putter_queue, task_module_path, logger):
        self.worker_nums = worker_nums
        self.worker_timeout = worker_timeout
        self.getter_queue, self.putter_queue = getter_queue, putter_queue
        self.task_module_path = task_module_path
        self.workers = {}
        self.idle_workers = []
        self.busy_workers = {}
        self.watch_tasks = {}
        self.idle_available = Event()
        self.wait_for_idle = False
        self.logger = logger
        self.alive = True
        self.logger.debug('+++new worker pool instance++++')
        return

    def __str__(self):
        return 'WorkerPool<ws:%s, iws:%s>' % (self.workers, self.idle_workers)

    def manage_worker(self):
        nc = len(self.workers) - self.worker_nums
        if nc < 0:
            # spawn extra workers
            while -nc:
                w = MagneWorker(self.task_module_path)
                self.workers[w.ident] = w
                self.logger.info('create new worker: %s' % w.ident)
                self.idle_workers.append(w.ident)
                nc += 1
        elif nc > 0:
            # kill idle worker
            while self.idle_workers and nc:
                w = self.idle_workers.pop()
                wobj = self.workers.pop(w)
                wobj.shutdown()
                nc -= 1
        return

    async def start(self):
        try:
            self.manage_worker()
        except Exception as e:
            self.logger.error('worker pool start exception, %s' % e, exc_info=True)
            self.kill_all_workers()
            raise e
        self.wait_amqp_msg_task = await curio.spawn(self.wait_amqp_msg)
        self.logger.debug('spawn wait_amqp_msg')
        self.logger.info('worker pool started!')
        return

    async def apply(self, func_name, args, delivery_tag):
        # if we do not spawn watch task, and await worker recv,
        # we would block in recv, and can not recv next amqp msg!
        while not self.idle_workers:
            self.wait_for_idle = True
            # there is no idle worker for ready, just wait
            self.logger.debug('waiting for any idle worker...')
            await self.idle_available.wait()
            self.logger.debug('a idle worker avaliable...')
            self.idle_available.clear()
        self.wait_for_idle = False
        w = self.idle_workers.pop(0)
        wobj = self.workers[w]
        # apply msg to worker process
        try:
            await wobj.apply(func_name, args, delivery_tag)
        except Exception as e:
            self.logger.error('apply worker %s exception!: %s' % (wobj.ident, e), exc_info=True)
            self.idle_workers.append(w)
            await self.send_ack_queue(delivery_tag)
            return
        self.logger.debug('worker pool apply worker %s: %s %s(%s)' % (w, delivery_tag, func_name, args))
        self.busy_workers[w] = wobj
        # watching task be set to daemon, it is a good idea?
        watch_worker_task = await curio.spawn(self.watch_worker, wobj, daemon=True)
        # save watch task for closing
        self.watch_tasks[wobj.ident] = watch_worker_task
        return

    async def wait_amqp_msg(self):
        self.logger.info('staring wait_amqp_msg')
        delivery_tag = None
        try:
            while True:
                msg = await self.getter_queue.get()
                self.logger.debug('wait_amqp_msg got msg %s' % msg)
                try:
                    msg_dict = json.loads(msg)
                    self.logger.debug('msg_dict %s' % msg_dict)
                    # msg_dict must contains delivery_tag!
                    delivery_tag = msg_dict['delivery_tag']
                    data = json.loads(msg_dict['data'])
                    func_name, args = data['func'], data['args']
                except Exception as e:
                    self.logger.error('invalid msg, %s, %s' % (msg, e), exc_info=True)
                    # delivery_tag must had been set!!!!
                    await self.send_ack_queue(delivery_tag)
                else:
                    self.logger.info('got a task %s, %s(%s)' % (delivery_tag, func_name, args))
                    await self.apply(func_name, args, delivery_tag)
        except curio.CancelledError:
            # cancel while await getter_queue.get, it`s fine, just discarding all msgs
            # cancel while await self.apply?
            # if had not apply to worker process yet, discarding msg is fine
            # if had apply to worker process, we will wait a least one worker timeout while close
            self.logger.info('wait_amqp_msg cancel')
        except Exception as e:
            self.logger.error('wait_amqp_msg error: %s' % e, exc_info=True)
            raise e
        return

    async def send_ack_queue(self, delivery_tag):
        self.logger.debug('send ack %s' % delivery_tag)
        try:
            await self.putter_queue.put(delivery_tag)
        except Exception as e:
            self.logger.error('worker pool send_ack_queue error, %s' % e, exc_info=True)
            raise e
        return

    async def watch_worker(self, wobj):
        func_name, args = wobj.func, wobj.args
        self.logger.debug('watching worker %s for %s(%s)' % (wobj.ident, func_name, args))
        success, res = False, None
        canceled = False
        try:
            # timeout will cancel coro
            success, res = await curio.timeout_after(self.worker_timeout, wobj.recv)
        except curio.TaskTimeout:
            # got timeout
            self.logger.error('worker %s run %s(%s) timeout!' % (wobj.ident, func_name, args))
            self.kill_worker(wobj)
            self.logger.info('shutdown worker %s...' % wobj.ident)
            if self.alive is True:
                # do not create new worker process while closing worker pool
                self.manage_worker()
        except curio.CancelledError:
            self.logger.info('watch %s cancel' % wobj.ident)
            canceled = True
        else:
            self.logger.info('worker %s run %s(%s) return %s, %s' % (wobj.ident, func_name, args, success, res))
            del self.busy_workers[wobj.ident]
            self.idle_workers.append(wobj.ident)
        del self.watch_tasks[wobj.ident]
        # cancel would not send ack!!!!
        if canceled is False:
            await self.send_ack_queue(wobj.delivery_tag)
            if self.wait_for_idle is True:
                await self.idle_available.set()
        return

    def kill_all_workers(self):
        # unkindly kill every single worker!
        for _, wobj in list(self.workers.items()):
            self.kill_worker(wobj)
        return

    def kill_worker(self, wobj):
        self.logger.info('killing worker %s' % wobj.ident)
        wobj.shutdown()
        if wobj.ident in self.busy_workers:
            del self.busy_workers[wobj.ident]
        else:
            self.idle_workers.remove(wobj.ident)
        del self.workers[wobj.ident]
        return

    def reap_workers(self):
        try:
            while True:
                self.logger.info('reaping workers')
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    self.logger.info('there is not any worker wait for reap')
                    break
                self.logger.info('reap worker %s, exit with %s' % (wpid, status))
                if wpid not in self.workers:
                    self.logger.error('worker pool do not contains reapd worker %' % wpid)
                    # TODO: how?
                else:
                    self.kill_worker(self.workers[wpid])
                self.manage_worker()
        except OSError as e:
            if e.errno != errno.ECHILD:
                self.logger.error('reap worker error signal %s' % e.errno, exc_info=True)
        return

    async def close(self, warm=True):
        # do not get amqp msg
        self.alive = False
        self.getter_queue._queue = deque()
        await self.wait_amqp_msg_task.cancel()
        # wait for worker done
        if warm is True:
            try:
                self.logger.info('watching tasks join, wait %ss' % self.worker_timeout)
                async with curio.timeout_after(self.worker_timeout):
                    async with curio.TaskGroup(self.watch_tasks.values()) as wtg:
                        await wtg.join()
            except curio.TaskTimeout:
                # task_group will cancel all remaining tasks while catch TaskTimeout(CancelError), yes, that is true
                # so, we do not have to cancel all remaining tasks by ourself
                self.logger.info('watch_tasks join timeout...')
        else:
            # cold close, just cancel all watch tasks
            for watch_task_obj in list(self.watch_tasks.values()):
                await watch_task_obj.cancel()
        self.kill_all_workers()
        return


async def test_worker():
    import logging
    logger = logging.getLogger('test_connection')
    log_handler = logging.StreamHandler()
    log_format = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    getter_queue = curio.Queue()
    putter_queue = curio.Queue()
    wp = MagneWorkerPool(worker_nums=2, worker_timeout=15,
                         getter_queue=getter_queue, putter_queue=putter_queue,
                         task_module_path='magne.process_worker.demo_task', logger=logger,
                         )
    stask = await curio.spawn(wp.start)
    await stask.join()
    await curio.sleep(5)
    body = {'channel_number': 1,
            'delivery_tag': 1,
            'consumer_tag': 1,
            'exchange': 1,
            'routing_key': 1,
            'data': json.dumps({'func': 'sleep', 'args': [10]}),
            }
    await wp.getter_queue.put(json.dumps(body))
    await curio.sleep(5)
    print(wp.idle_workers, wp.workers, wp.busy_workers)
    await curio.sleep(6)
    print(wp.putter_queue)
    print(wp.idle_workers, wp.workers, wp.busy_workers)
    os.kill(wp.idle_workers[0], signal.SIGKILL)
    async with curio.SignalQueue(signal.SIGCHLD) as p:
        await p.get()
        wp.reap_workers()
    print('reap done')
    print(wp.idle_workers, wp.workers, wp.busy_workers)
    print('range send msg...')
    for i in range(3):
        new_body = {'delivery_tag': i + 2, 'data': json.dumps({'func': 'sleep', 'args': [15 * (i + 1) + 1]})}
        await wp.getter_queue.put(json.dumps(new_body))
    await curio.sleep(3)
    print(wp.idle_workers, wp.workers, wp.busy_workers, wp.getter_queue)
    print('closing...')
    await wp.close(warm=False)
    print(wp.putter_queue)
    return


def main():
    # for test
    curio.run(test_worker, with_monitor=True)
    return


if __name__ == '__main__':
    main()
