import os
import logging

import argparse

from magne.master import main as magne_main


def main():
    cpu_count = os.cpu_count()
    parser = argparse.ArgumentParser(prog='magne', description='run magne queue')
    parser.add_argument('--task', type=str, help='task module path, default: magne.demo_task', default='magne.demo_task')
    parser.add_argument('--amqp-url', type=str, help='amqp address, default: amqp://guest:guest@localhost:5672//',
                        default='amqp://guest:guest@localhost:5672//',
                        )
    parser.add_argument('--workers', type=int, help='worker count, default: cpu count',
                        default=cpu_count,
                        )
    parser.add_argument('--worker-timeout', type=int, help='worker timeout, default 60s',
                        default=60,
                        )
    parser.add_argument('--qos', type=int, help='prefetch count, default cpu count',
                        default=cpu_count,
                        )
    parser.add_argument('--log-level', type=str, help='any level in logging, default: INFO',
                        default='INFO',
                        )
    args = parser.parse_args()
    worker_nums = args.workers
    worker_timeout = args.worker_timeout
    task_module = args.task
    qos = args.qos
    amqp_url = args.amqp_url
    logger_level = args.log_level.upper()
    if logger_level not in logging._nameToLevel:
        raise Exception('invalid log level')
    logger_level = logging._nameToLevel[logger_level]
    magne_main(worker_nums, worker_timeout, task_module, amqp_url, qos, logger_level)
    return


if __name__ == '__main__':
    main()
