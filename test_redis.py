from redis.connection import Token, SYM_EMPTY, SYM_STAR, SYM_CRLF, SYM_DOLLAR
from redis._compat import b as rcb, imap

import socket


def encode(value):
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


def pack_command(*args):
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

    for arg in imap(encode, args):
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


def rsend(cmds):
    s = socket.create_connection(('127.0.0.1', 6379))
    for c in cmds:
        s.send(c)
    data = s.recv(1024)
    return data


def main():
    c1 = pack_command('GET', 'a')[0]
    c2 = pack_command('GET', 'b')[0]
    cs = [c1, c2]
    print(rsend(cs))
    return


if __name__ == '__main__':
    main()
