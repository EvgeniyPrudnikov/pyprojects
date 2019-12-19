import os
import sys
import time
from datetime import timedelta
import threading
from queue import Queue
from lib import *
try:
    import numpy as np
    import pyodbc
    import cx_Oracle as cx
except Exception as e:
    print(e)

PRINT_HEADER = []
PRINT_FOOTER = []
PRINT_LOAD = '(...)'


def print_header_footer(l):
    for h in l:
        print(h)


def connect_to_db(conn_str, env):
    db = None
    for _ in range(50):  # 50 attempts
        try:
            if env == 'oracle':
                db = cx.connect(conn_str)
            else:
                db = pyodbc.connect(conn_str, autocommit=True, timeout=0)
            if db:
                break
        except Exception as e:
            raise Exception(e)
    if not db:
        raise Exception('\nCant connect in 50 attempts. Exit 1\n')
    PRINT_HEADER.append('\n[{0}] Connected to {1}\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), env))
    return db


def fetch_data(cur, res, fetch_num=100, with_header=False):
    headers = tuple([i[0].lower() for i in cur.description])
    result = cur.fetchmany(fetch_num)
    if with_header:
        result.insert(0, headers)
    res += result
    if len(result) == 0 or len(result) <= fetch_num - 1:
        return -1
    return len(result)


def exec_query(cur, fetch_num):
    result = []
    rows_cnt = fetch_data(cur, result, fetch_num=fetch_num, with_header=True)

    print_header_footer(PRINT_HEADER)
    pretty_print_result(result)
    print_header_footer(PRINT_FOOTER)
    sys.stdout.flush()

    if rows_cnt < 0:
        cur.close()
        os._exit(0)

    print(PRINT_LOAD, flush=True)
    PRINT_FOOTER.append(PRINT_LOAD)

    # default timeout 30 sec
    timeout = time.time() + 30
    input_msgs = Queue()

    def read_input(msg_q):
        while True:
            msg_q.put(sys.stdin.readline())

    input_t = threading.Thread(target=read_input, args=(input_msgs,))
    input_t.daemon = True
    input_t.start()

    while time.time() < timeout:
        try:
            if input_msgs.empty():
                time.sleep(0.2)
                continue

            cmd = input_msgs.get().split(':')

            if cmd[0] == 'load':
                rows_cnt = fetch_data(cur, result, fetch_num=int(cmd[1]))
                print_header_footer(PRINT_HEADER)
                pretty_print_result(result)
                print_header_footer(PRINT_FOOTER)
                sys.stdout.flush()
                if rows_cnt < 0:
                    break
                timeout += 10
            else:
                break
        except Exception as e:
            raise Exception(str(e))
    cur.close()


def main():
    env = sys.argv[1]
    conn_str = sys.argv[2]
    query_file_name = sys.argv[3]
    qtype = sys.argv[4]  # query, explain
    fetch_num = int(sys.argv[5])
    query = ''
    sys.path.append(os.path.dirname(query_file_name))

    try:
        with open(query_file_name, 'r') as f:
            query = f.read()

        PRINT_HEADER.append(query)

        db = connect_to_db(conn_str, env)
        cur = db.cursor()
        start = time.time()
        cur.execute(query)
        end = time.time()
        PRINT_FOOTER.append('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))
        exec_query(cur, fetch_num)

    except Exception as e:
        e_msg = str(e) + '\n'
        print_header_footer(PRINT_HEADER)
        print(e_msg, flush=True)
        os._exit(1)

    sys.stdout.flush()
    db.close()
    os._exit(0)


if __name__ == '__main__':
    main()
