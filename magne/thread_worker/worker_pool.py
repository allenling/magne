from curio.thread import AsyncThread


class ConnectionThread:
    return


class WorkerThread:

    def __init__(self):
        self.async_thread = AsyncThread()
        return

    async def start(self):
        return


class WorkerPool:

    def __init__(self, worker_nums, worker_timeout, task_module_path, logger):
        self.worker_nums = worker_nums
        self.worker_timeout = worker_timeout
        self.task_module_path = task_module_path
        self.logger = logger
        self.workers = {}
        return

    async def start(self):
        for age in range(self.worker_nums):
            wt = WorkerThread()
            self.workers[age] = wt
        return
