import pyodbc
import sys
import os
import time
from datetime import timedelta
import lib


def connect_to_db(conn_str):
    db = None
    for x in range(50):  # 50 attempts
        try:
            db = pyodbc.connect(conn_str, autocommit=True, timeout=0)
            if db: break
        except Exception as e:
            print(e)
    if not db:
        print('\nCant connect in 50 attempts. Exit 1\n')
        exit(1)
    else:
        db.setencoding(encoding='utf-8')
        print('\n[{0}] Connected to Impala;\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    return db


def execute_query(db, query_file_name, qtype, full_res):

    q_type = ''

    with open(query_file_name, 'r') as qf:
        query = qf.read().strip('\n')

    for qq in list(filter(None, query.split(';'))):
        exec_qq = qq.strip('\n')
        print('{0}\n'.format(exec_qq))

        if qtype == 'impala_explain':
            q_type = 'explain'
        elif exec_qq.lower().startswith('select') or exec_qq.lower().startswith('with'):
            q_type = 'sql'
        else:
            q_type ='ddl'

        start = time.time()
        cur = db.cursor()
        try:
            number_of_rows = cur.execute(exec_qq)
            if full_res and q_type == 'sql':
                return cur
        except Exception as e:
            print(e.args[1])
            end = time.time()
            print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))
            db.close()
            exit(1)

        if q_type == 'sql':
            headers = tuple([i[0].lower() for i in cur.description])
            result = cur.fetchmany(50)
            result.insert(0, headers)
            lib.pretty_print_result(result)
        elif q_type == 'explain':
            result = cur.fetchall()
            result = [res[0] + '\n' for res in result]
            print (''.join(result))
        elif q_type == 'ddl' and (exec_qq.lower().startswith('describe') or exec_qq.lower().startswith('show table stats')):
            headers = tuple([i[0].lower() for i in cur.description])
            result = cur.fetchall()
            result.insert(0, headers)
            lib.pretty_print_result(result)
        elif q_type == 'ddl' and exec_qq.lower().startswith('show'):
            result = cur.fetchall()
            result = [res[0] + '\n' for res in result]
            print (''.join(result))

        end = time.time()
        print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))
        cur.close()

    db.close()

    return 'ok'


def main():

    query_file_name = sys.argv[1]
    conn_str = sys.argv[2]
    query_type = sys.argv[3]
    full_res = int(sys.argv[4])
    sys.path.append(os.path.dirname(query_file_name))

    db = connect_to_db(conn_str)
    res_cur = execute_query(db, query_file_name, query_type, full_res)

    if full_res:
        lib.show_result_in_gui(res_cur)
        print('See result in gui')


if __name__ == '__main__':
    main()


