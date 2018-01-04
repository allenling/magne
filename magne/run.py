import sys
import argparse

import magne
from magne.process_worker.run import main as process_main
from magne.coro_consumer.run import main as coro_main

desc = '''magne distributed task queue, v%s
-----------------------------
process  : process worker

coroutine: coroutine worker
-----------------------------
''' % magne.__version__


def main():
    parser = argparse.ArgumentParser(prog='magne',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=desc,)

    cmds = ['process', 'coroutine']
    parser.add_argument('command', choices=cmds, help='run worker')

    sub_help = False
    if len(sys.argv) == 1:
        sys.argv.append('--help')
    elif sys.argv[1] in cmds:
        if '--help' in sys.argv:
            sub_help = True
            sys.argv.remove('--help')
        elif '--h' in sys.argv:
            sub_help = True
            sys.argv.remove('-h')
    a = parser.parse_args()
    if not a.__dict__:
        print(parser.print_help())
        return
    if sub_help is True:
        sys.argv.append('--help')
    del sys.argv[1]
    if a.command == 'process':
        process_main()
    elif a.command == 'coroutine':
        coro_main()
    return


if __name__ == '__main__':
    main()
