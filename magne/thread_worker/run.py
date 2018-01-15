import os
import logging

import argparse

from magne.thread_worker.master import main as magne_thread_main


def get_parser():
    cpu_count = os.cpu_count()
    parser = argparse.ArgumentParser(prog='magne thread', description='magne thread worker')
    task_default = 'magne.thread_worker.demo_tasks'
    parser.add_argument('--task', type=str, help='task module path, default: %s' % task_default, default=task_default)
    parser.add_argument('--amqp-url', type=str, help='amqp address, default: amqp://guest:guest@localhost:5672//',
                        default='amqp://guest:guest@localhost:5672//',
                        )
    parser.add_argument('--workers', type=int, help='worker count, default: cpu count(invalid now!)',
                        default=cpu_count,
                        )
    parser.add_argument('--worker-timeout', type=int, help='worker timeout, default 60s',
                        default=60,
                        )
    parser.add_argument('--qos', type=int, help='prefetch count, default qos=16',
                        default=16,
                        )
    parser.add_argument('--log-level', type=str, help='default: INFO',
                        default='INFO',
                        )
    parser.add_argument('--threads', type=int, help='default: threads per worker process',
                        default=8,
                        )
    parser.add_argument('--curio-debug', type=int, help='curio monitor, default: 0, any greater than 0 will start debug',
                        default=0,
                        )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    worker_nums = args.workers
    worker_timeout = args.worker_timeout
    task_module = args.task
    qos = args.qos if args.qos >= 0 else worker_nums
    amqp_url = args.amqp_url
    threads = args.threads
    logger_level = args.log_level.upper()
    if logger_level not in logging._nameToLevel:
        raise Exception('invalid log level')
    logger_level = logging._nameToLevel[logger_level]
    cdebug = args.curio_debug
    # worker进程数暂时没用
    magne_thread_main(worker_timeout, task_module, threads, qos, amqp_url, logger_level, cdebug)
    return


if __name__ == '__main__':
    main()
