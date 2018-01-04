import curio
import random

from magne.helper import register
from magne.coro_consumer.utils import DummyRedis

dummy_redis = None

counter_key = 'magne_coro_consumer'


async def get_redis_connection():
    global dummy_redis
    dr = DummyRedis()
    await dr.connect()
    dummy_redis = dr
    return


@register
async def magne_latency_bench():
    p = random.randint(1, 100)
    if p == 1:
        duration = 10
    elif p <= 30:
        duration = 5
    elif p <= 50:
        duration = 3
    else:
        duration = 1
    await curio.sleep(duration)
    data = await dummy_redis.send_command('INCR', counter_key)
    return data

curio.run(get_redis_connection)
