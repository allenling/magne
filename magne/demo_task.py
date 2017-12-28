'''
demo task module
'''
import time

from magne.helper import register


@register
def sleep(n):
    time.sleep(n)
    return 'i am sleep func, and i had sleep %s sec(s)' % n


def no_task():
    return 'should not be call'
