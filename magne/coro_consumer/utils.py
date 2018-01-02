from redis.connection import Token, SYM_EMPTY, SYM_STAR, SYM_CRLF, SYM_DOLLAR
from redis._compat import b as rcb, imap

import curio


class DummyRedis:
    '''
    curio redis utils
    for benchmark
    '''

    def __init__(self, host='localhost', port='6379'):
        self.host = host
        self.port = port
        return

    async def connect(self):
        self.sock = await curio.open_connection(self.host, self.port)
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

    async def send_command(self, *args):
        data = self.pack_command(*args)
        await self.sock.send(data[0])
        return

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
