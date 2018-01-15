
import time

from magne.helper import register


@register
def thread_sleep(n):
    start = time.time()
    time.sleep(n)
    info = 'n is %s, sleep %s(s)' % (n, time.time() - start)
    return info
