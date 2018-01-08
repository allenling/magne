'''
多个coroutine同时recv是不好的, 如果一次recv了多个命令的返回, 丢弃还是? 并且curio中会报read busy异常

然后一般多个线程读取同一个socket的话是加锁, 一般的做法是加锁, 拿锁, send, recv, 解锁

但是这样就限制了一次只能send一次, recv一次, 浪费recv的size大小, 比如每次recv都只能拿到1kb, 就是send不加锁,
recv加锁, 如果recv一次拿到了多个命令返回, 那么当前的coroutine需要唤醒其他coroutine, 通知它们结果已经回来了么, 还是丢弃?

可以spawn一个专门recv的corotine, 这样send不受限制, 然后可以一次拿到多个命令的返回:

1. 每次send之前, 把自己的event加入带等待唤醒的queue中(加入list不太好?), 然后send, 然后等待event被唤醒
   ev, ev_id = get_ev()
   await ev_queue.put((ev_id, ev))
   await send(cmd)
   # 等待event
   await ev.wait()

2. 然后当调度到recv的时候, 有可能recv多个命令的返回:
   # 以resps的长度为准, 因为担心有可能有一个coroutine发送了命令, 但是这一次的recv没有获取到, 此时ev_queue的长度就大于resps
   while resps:
       for resp in resps:
           # 拿到event的对象和id
           ev, ev_id = ev_queue.get
           # 存储结果
           res[ev_id] = resp
           唤醒event
           await ev.set()

3. event受信, 那么说明结果返回了
   await ev.wait()
   res = res[ev_id]
   del res[ev_id]

该任务最好设置为daemon, 因为我们没办法去主动join它(在__del__中join?), 然后curio结束的时候, 此时该任务
是非daemon, 并且没有被cancel, 没有join的, 所以会报never joined的warning

其实应该用future对象而不是ev_id, ev这样的组合, 因为redis服务一般是一直开着的, 如果ev_id一直自增的话
就找出大整数的内存一直膨胀, python的整数内存不会返回给os的, 然后就发现整个程序的内存消耗会越来越大
'''
from redis.connection import Token, SYM_EMPTY, SYM_STAR, SYM_CRLF, SYM_DOLLAR
from redis._compat import b as rcb, imap

import curio


class DummyRedis:
    '''
    curio redis utils for benchmark
    using lock is a common way, but still not good enough, too many tasks waiting for lock still cause ReadResourceBusy exception
    any better way?
    if spawn a task to get response, how could we join(cancel) it?
    setting daemon to True can avoid join, but it is a good way?
    '''

    def __init__(self, host='localhost', port='6379'):
        self.host = host
        self.port = port
        self.should_spawn = True
        self._evs = curio.Queue()
        self._ev_seq = 0
        self._res = {}
        return

    async def connect(self):
        self.sock = await curio.open_connection(self.host, self.port)
        return

    async def distribute_response(self):
        # daemon=True!
        last_resp = ''
        while True:
            data = await self.sock.recv(1024)
            # last element is empty string
            dlist = data.split(b'\r\n')
            if last_resp:
                dlist[0] = last_resp + dlist[0]
                last_resp = ''
            if dlist[-1] != b'':
                last_resp = dlist[-1]
            dlist = dlist[:-1]
            while dlist:
                rdata = dlist.pop(0)
                if b'$' in rdata:
                    continue
                # int
                rdata = rdata.decode('utf-8')
                if rdata[0] == ':':
                    rdata = int(rdata[1:])
                ev, ev_seq = await self._evs.get()
                self._res[ev_seq] = rdata
                await ev.set()
        return

    def encode(self, value):
        if isinstance(value, Token):
            return rcb(value.value)
        elif isinstance(value, bytes):
            return value
        elif isinstance(value, (int)):
            value = rcb(str(value))
        elif isinstance(value, float):
            value = rcb(repr(value))
        elif isinstance(value, str):
            value = value.encode('utf-8')
        return value

    async def just_send(self, *args):
        # do not wait for response
        # for benchmark, it is enough
        data = self.pack_command(*args)
        await self.sock.send(data[0])
        return

    async def wait_for_cmd_resp(self, *args):
        data = self.pack_command(*args)
        await self.sock.send(data[0])
        # lock is not ok, too many waiting event will cause ReadResourceBusy exception
        async with curio.Lock():
            res_data = await self.sock.recv(1024)
        return res_data

    async def send_and_spawn_rsp(self, *args):
        # maybe spawn task to get resp?
        cmd = self.pack_command(*args)
        ev, ev_seq = curio.Event(), self._ev_seq
        self._ev_seq += 1
        await self._evs.put([ev, ev_seq])
        await self.sock.send(cmd[0])
        if self.should_spawn is True:
            self.should_spawn = False
            # daemon=True, we do not have to join task
            self.resp_task = await curio.spawn(self.distribute_response, daemon=True)
        await ev.wait()
        data = self._res.pop(ev_seq)
        return data

    async def send_command(self, *args):
        # just for test, use just_send
        # data = await self.just_send(*args)
        # how to recv data?
        # data = await self.wait_for_cmd_resp(*args)
        # maybe spawn task to get resp?
        data = await self.send_and_spawn_rsp(*args)
        return data

    def pack_command(self, *args):
        "Pack a series of arguments into the Redis protocol"
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        command = args[0]
        if ' ' in command:
            args = tuple([Token(s) for s in command.split(' ')]) + args[1:]
        else:
            args = (Token(command),) + args[1:]

        buff = SYM_EMPTY.join(
            (SYM_STAR, rcb(str(len(args))), SYM_CRLF))

        for arg in imap(self.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values
            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, rcb(str(len(arg))), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, rcb(str(len(arg))),
                                       SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output
