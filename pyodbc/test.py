import pyodbc
import time


def connect_to_db(conn_str):
    db = None
    for x in range(50):  # 50 attempts
        try:
            db = pyodbc.connect(conn_str)
            if db:break
        except Exception as e:
            raise Exception(e)
    if not db:
        raise Exception('\nCant connect in 50 attempts. Exit 1\n')
    print('\n[{0}] Connected to Oracle\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    return db


def execute_query(db, query_text, qtype):

    cur = db.cursor()

    try:
        cur.execute(query_text)
    except pyodbc.Error as e:
        msg = e.args[1]
        if msg.find('\n') > 0:
            msg = msg[:msg.find('\n')]
        raise Exception(msg)

    return cur

query3 = '''EXPLAIN PLAN FOR 
select  LISTAGG(lvl1, '&') WITHIN GROUP (ORDER BY lvl1) from (
select lvl1 from (
select lvl1, mod(lvl1,lvl2) as md
from (
    select level as lvl1 
    from dual 
    connect by level <=10000
) inner join ( 
    select level as lvl2
    from dual 
    connect by level <=10000
) on lvl1 > lvl2 and lvl2 > 1
) group by lvl1 having min(md) > 0
union all 
select 2 as   lvl1 from dual
)
'''
query1 = '''EXPLAIN PLAN FOR select * from lool__1 where id = 6'''
query2 = '''
select plan_table_output from table(DBMS_XPLAN.DISPLAY);
'''

query = 'select * from lool__1  order by id'

db1 = connect_to_db('DSN=oracle_odbc;UID=js;PWD=js')
db2 = connect_to_db('DSN=oracle_odbc;UID=js;PWD=js')

cur1 = db1.cursor()
cur2 = db2.cursor()

# exit(0)
cur1.execute(query1)
cur2.execute(query3)
cur1.execute(query2)
cur2.execute(query2)

res1 = cur1.fetchall()
res2 = cur2.fetchall()

for line in res1:
    print(line[0])

for line in res2:
    print(line[0])
