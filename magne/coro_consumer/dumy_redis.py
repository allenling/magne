'''

如何读?

batch recv的话是因为调度的关系, 可以尽可能recv

并且多个coroutine同时recv是不好的, 比如a.send, b.send, c.send, 然后a.recv

这样如果a一次recv了多个命令的返回, 丢弃还是需要唤醒b, c? 并且b.recv, c.recv这样多个task对一个fd进行wait read的话, curio中会报read busy异常

然后一般多个线程读取同一个socket的话是加锁, 一般的做法是加锁, 流程是拿锁, send, recv, 解锁

但是这样就限制了一次只能send一次, recv一次, 浪费recv的size大小, 比如每次recv都只能拿到1kb

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

如何写?

因为socket.send的时候, 如果网络很繁忙的话, 不能发完所有的数据, 比如命令有1kb, 结果只发送了5kb怎么办?

具体来说, 比如命令x='get a', 发送的数据是: b'*2\r\n$3\r\nget\r\n$1\r\na\r\n'

针对这种情况, redis会处理的, 就像我们处理接收到的数据不是完整的时候改怎么办, redis一样处理了命令不完整的情况

主要是如何保证每一个coroutine发送命令不会错乱, 比如协程a发送了上面的命令, 只发了5kb, 因为send也是用await, 所以有可能发送之后被切换到其他协程上
那么这个时候可能协程b也开始发送命令x, 此时redis就会收到错乱的命令, 会报错, 所以最好不要直接send, 统一用一个协程来send

就像线程一样, 线程中, send/recv是交个一个线程统一处理, 这个线程称为io线程, 然后协程的话, 孵化出send和recv各一个或者统一一个都可以, 两个感觉好点
因为协程的spawn和调度都那么便宜, 多spawn几个协程无所谓了, 分离send/recv为两个协程这个也简单一点

读的时候是尽量读, 写的时候也是一样, 当queue.get被唤醒的时候, 或许直接获取queue中所有的数据, 然后调用sendall算了

必须send完才能把ev加入到recv_queue中

然后curio这层, 如果网络繁忙, 同一个socket中, 仍然有write任务在等待, 那么会报write busy, 所以

在协程下, batch send/recv是因为调度的关系, 不要浪费发送/接收缓存区. 比如a, b, c调用send的时候
a.send发送出去了, 然后可以调度到b.send, 然后同样b之后是c.send, 这样每次都只能发送一点数据
如果用统一的协程序send的话, 比如a.send, b.send, c.send都是把数据发送到一个queue, 由协程d来统一发送, 调度关系如果是
a.send -> b.send -> c.send的话, d一下就能从queue拿到所有的数据, 然后尽可能的发送出去, 一次性不阻塞地获取queue的数据需要一点小修改


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
        try:
            while True:
                tmp_data = None
                tmp_last = None
                data = await self.sock.recv(1024)
                tmp_data = data
                # last element is empty string
                if last_resp:
                    data = last_resp + data
                    tmp_last = last_resp
                    last_resp = ''
                dlist = data.split(b'\r\n')
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
        except curio.CancelledError:
            pass
        except Exception as e:
            print('tmp data: %s, tmp last: %s' % (tmp_data, tmp_last))
            raise e
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


def test_pack_command():
    dr = DummyRedis()
    x = dr.pack_command('get a')
    print(x)
    return

def main():
    test_pack_command()
    return

if __name__ == '__main__':
    main()
