import random
import time
import redis

from magne.helper import register

counter_key = "magne-thread-bench-counter"

rclient = redis.StrictRedis()


@register
def magne_latency_bench():
    p = random.randint(1, 100)
    if p <= 30:
        duration = 5
    elif p <= 50:
        duration = 3
    else:
        duration = 1

    start = time.time()
    time.sleep(duration)
    rclient.incr(counter_key)
    info = 'duration is %s, sleep %s(s)' % (duration, time.time() - start)
    return info
