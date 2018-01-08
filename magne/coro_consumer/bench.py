'''
reference: dramatiq.benchmarks.bench
'''
import pika
import redis
import os
import argparse
import subprocess
import time

from magne.coro_consumer.coro_consumer import main as coro_main


counter_key = 'magne_coro_consumer'


routing_key = exchange_name = queue_name = 'magne_latency_bench'.upper()


def en_queue(n):
    print('starting en_queue...')
    parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    print('decalaring exchange and queue')
    try:
        channel.exchange_declare(exchange_name)
        channel.queue_declare(queue_name)
        print('bind')
        channel.queue_bind(queue_name, exchange_name, routing_key)
    except Exception as e:
        print('decalare exchange and queue error: %s' % e)
        raise e
    print('config exchange and queue done')
    print('staring send tasks into rabbitmq')
    for _ in range(n):
        channel.basic_publish('MAGNE_LATENCY_BENCH',
                              'MAGNE_LATENCY_BENCH',
                              '{"func": "magne_latency_bench", "args": []}',
                              )
    print('en_queue done')
    return


def setup(count):
    print('benchmark magne coro_consumer...')
    en_queue(count)
    print('%s tasks in rabbitmq' % count)
    return


def run_magne(workers):
    coro_main(workers, 200, 'magne.benchmark.bench_tasks', amqp_url='amqp://guest:guest@localhost:5672//',
              qos=workers, logger_level="INFO")
    return


def parse_argv():
    parser = argparse.ArgumentParser(prog='magne-bench', description='benchmark magne, reference: dramatiq.benchmarks.bench')
    parser.add_argument('--count', type=int, help='worker count, default: 100',
                        default=100,
                        )
    parser.add_argument('--only-setup', type=int, help='any non zero means that just push tasks into rabbitmq, do not run bench',
                        default=0,
                        )
    parser.add_argument('--workers', type=int, help='workers, default 1',
                        default=1,
                        )
    args = parser.parse_args()
    return args.count, args.only_setup, args.workers


def main():
    print('pid: %s' % os.getpid())
    count, only_setup, workers = parse_argv()
    print('task count: %s' % (count))
    setup(count)
    if only_setup != 0:
        return
    rs = redis.StrictRedis()
    start_time = time.time()
    rs.set(counter_key, 0)
    cm = ['env', 'PYTHONPATH=/opt/curio:/opt/magne:/opt/magne/magne', 'python3.6',
          '/opt/magne/magne/coro_consumer/run.py', '--task=magne.coro_consumer.bench_tasks', '--log-level=INFO',
          '--curio-debug=1']
    print(' '.join(cm))
    procs = []
    for _ in range(workers):
        proc = subprocess.Popen(cm)
        procs.append(proc)
    processed = 0
    while processed < count:
        processed = int(rs.get(counter_key).decode('utf-8'))
        print(f"{processed}/{count} messages processed\r", end="")
        time.sleep(0.1)

    duration = time.time() - start_time
    for proc in procs:
        proc.terminate()
        proc.wait()
    print(f"coro_consumer took {duration} seconds to process {count} messages.")
    return


if __name__ == '__main__':
    main()
