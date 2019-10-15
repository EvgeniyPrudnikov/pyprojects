import pyodbc
import sys
import os
import time
from datetime import timedelta
import threading
import lib


def connect_to_db(conn_str, client_id=''):
    db = None
    for x in range(50):  # 50 attempts
        try:
            db = pyodbc.connect(conn_str)
            if db: break
        except Exception as e:
            print(e)
    if not db:
        print('\nCant connect in 50 attempts. Exit 1\n')
        exit(1)
    print('\n[{0}] Connected to Oracle\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    return db


def execute_query(db, query_file_name, full_res=0):
    with open(query_file_name, 'r') as qf:
        query = qf.read()

    print('{0}\n'.format(query))

    cur = db.cursor()
    start = time.time()

    try:
        cur.execute(query)
    except pyodbc.Error as e:
        msg = e.args[1]
        print(msg[:msg.find('\n')])

    if full_res:
        return cur

    headers = tuple([i[0].lower() for i in cur.description])
    result = cur.fetchmany(50)
    result.insert(0, headers)
    lib.pretty_print_result(result)
    end = time.time()
    print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))

    cur.close()
    db.close()


def main():

    query_file_name = sys.argv[1]
    conn_str = sys.argv[2]
    client_id = sys.argv[3]
    full_res = int(sys.argv[4])
    sys.path.append(os.path.dirname(query_file_name))

    db = connect_to_db(conn_str, client_id)

    if not full_res:
        execute_query(db, query_file_name, full_res)
        exit(0)

    res_cur = execute_query(db, query_file_name, full_res)

    print('See result in gui')

    lib.show_result_in_gui(res_cur)

if __name__ == '__main__':
    main()
