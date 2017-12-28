
def register(func):
    '''
    register as a task
    '''
    tasks[func.__qualname__.upper()] = func
    return func


tasks = {}
