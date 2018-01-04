import curio

from magne.helper import register


@register
async def sleep(n):
    await curio.sleep(n)
    return
