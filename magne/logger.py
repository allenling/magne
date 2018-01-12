import logging


class TripleCLogger:

    def __init__(self, cms=['logger', 'worker', 'connection', 'master'], level=logging.INFO):
        '''
        利用闭包和property实现的一个logger, 初始化组件之后, 直接.xxx_logger, 就会加上xxx的大写这个前缀
        比如: logger.worker_logger, 打印的是会加上WORKER前缀, worker是默认组件
        logger = TripleCLogger(['xxx']), logger.xxx_logger会打印出XXX前缀

        In [1]: logger = TripleCLogger()

        In [2]: logger.worker_logger.info('this is worker info')
        INFO 2017-12-27 19:11:14,290 WORKER: this is worker info

        In [3]: logger.connection_logger.info('this is connection info')
        INFO 2017-12-27 19:11:23,370 CONNECTION: this is connection info

        In [4]: logger.master_logger.info('this is master info')
        INFO 2017-12-27 19:11:30,611 MASTER: this is master info

        add WORKER, CONNECTION, MASTER component name into message
        just for fun
        '''
        self.name = 'TripleCLogger'
        self.level = level
        logger = logging.getLogger(self.name)
        log_handler = logging.StreamHandler()
        log_format = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)
        logger.setLevel(self.level)
        self._logger = logger
        self.component_prefix = None
        self._components = cms
        for c in self._components:
            self._add_component_logger(c)
        self.set_component_prefix(self._components[0])
        return

    def __str__(self):
        return '%s<%s, %s>' % (self.name, self.component_prefix, logging.getLevelName(self.level))

    def full_msg(self, msg):
        return '%s: %s' % (self.component_prefix, msg)

    def debug(self, msg, *args, **kwargs):
        return self._logger.debug(self.full_msg(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        return self._logger.info(self.full_msg(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        return self._logger.error(self.full_msg(msg), *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        return self._logger.warning(self.full_msg(msg), *args, **kwargs)

    def add_component(self, component_name):
        lower_component_name = component_name.lower()
        if lower_component_name not in self.components:
            self._components.append(lower_component_name)
            self._add_component_logger(lower_component_name)
        return

    def _add_component_logger(self, component_name):
        # closure and property

        def get_logger(self):
            self.component_prefix = component_name.upper()
            return self
        cls = self.__class__
        setattr(cls, '%s_logger' % component_name, property(get_logger))
        return

    def set_component_prefix(self, component_name):
        lower_component_name = component_name.lower()
        if lower_component_name not in self._components:
            return
        self.component_prefix = lower_component_name.upper()
        return


def get_logger(name, level):
    logger = logging.getLogger(name)
    log_handler = logging.StreamHandler()
    log_format = logging.Formatter('%(levelname)-8s %(asctime)s %(message)s')
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(level)
    return logger


class WrapLogger:

    def __init__(self, name, level):
        self._logger = get_logger(name, level)
        self.name = name
        self.prefix = name.split('-')[1] if '-' in name else name
        self.level = level
        return

    def __str__(self):
        return '<%s, %s>' % (self.name, logging.getLevelName(self.level))

    def full_msg(self, msg):
        return '[{prefix}]: {msg}'.format(prefix=self.prefix, msg=msg)

    def debug(self, msg, *args, **kwargs):
        return self._logger.debug(self.full_msg(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        return self._logger.info(self.full_msg(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        return self._logger.error(self.full_msg(msg), *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        return self._logger.warning(self.full_msg(msg), *args, **kwargs)


def get_component_log(name, level):
    return WrapLogger(name, level)


def test_triplec_logger():
    tl = TripleCLogger(level=logging.DEBUG)
    print(tl)
    tl.info('------heiheihei-------\n')
    tl.worker_logger.debug('worker debug')
    tl.worker_logger.info('worker info')
    tl.worker_logger.error('worker error\n')
    tl.connection_logger.debug('connection debug')
    tl.connection_logger.info('connection info')
    tl.connection_logger.error('connection error\n')
    tl.master_logger.debug('master debug')
    tl.master_logger.info('master info')
    tl.master_logger.error('master error')
    return


def main():
    # for test
    l1 = get_component_log('Magne-Master', logging.DEBUG)
    print(l1)
    l1.debug('master debug')
    l1.info('master info')
    l2 = get_component_log('Magne-Connection', logging.DEBUG)
    print(l2)
    l2.debug('connection debug')
    l2.info('connection info')
    l3 = get_component_log('Magne-WorkerPool', logging.DEBUG)
    print(l3)
    l3.debug('workerpool debug')
    l3.info('workerpool info')
    return


if __name__ == '__main__':
    main()
