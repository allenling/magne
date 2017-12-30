import curio
import random

from magne.helper import register


@register
async def sleep(n):
    await curio.sleep(n)
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
    return
