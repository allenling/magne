import logging

import argparse

from magne.coro_consumer.coro_consumer import main as coro_main


def main():
    parser = argparse.ArgumentParser(prog='magne coroutine', description='magne coroutine worker')
    parser.add_argument('--task', type=str, help='task module path, default: magne.coro_consumer.demo_task',
                        default='magne.coro_consumer.demo_task')
    parser.add_argument('--amqp-url', type=str, help='amqp address, default: amqp://guest:guest@localhost:5672//',
                        default='amqp://guest:guest@localhost:5672//',
                        )
    parser.add_argument('--timeout', type=int, help='worker timeout, default 60s',
                        default=60,
                        )
    parser.add_argument('--qos', type=int, help='prefetch count, default 0',
                        default=0,
                        )
    parser.add_argument('--log-level', type=str, help='any level in logging, default: INFO',
                        default='INFO',
                        )
    args = parser.parse_args()
    timeout = args.timeout
    task_module = args.task
    qos = args.qos
    amqp_url = args.amqp_url
    log_level = args.log_level.upper()
    if log_level not in logging._nameToLevel:
        raise Exception('invalid log level')
    log_level = logging._nameToLevel[log_level]
    coro_main(timeout, task_module, qos, amqp_url, log_level)
    return


if __name__ == '__main__':
    main()
