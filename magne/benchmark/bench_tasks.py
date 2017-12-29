import random
import time
import pylibmc

from magne.helper import register

counter_key = "magne-latench-bench-counter"
memcache_client = pylibmc.Client(["localhost"], binary=True)
memcache_pool = pylibmc.ClientPool(memcache_client, 8)


@register
def magne_latency_bench():
    p = random.randint(1, 100)
    if p == 1:
        duration = 10
    elif p <= 30:
        duration = 5
    elif p <= 50:
        duration = 3
    else:
        duration = 1

    time.sleep(duration)
    with memcache_pool.reserve() as client:
        client.incr(counter_key)


@register
def fib_bench(n):
    p, q = 0, 1
    while n > 0:
        p, q = q, p + q
        n -= 1

    with memcache_pool.reserve() as client:
        client.incr(counter_key)

    return p
