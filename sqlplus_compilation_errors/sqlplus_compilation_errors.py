"""
For compiling objects like packages, triggers, procedures and so on
there are no error or cmd in sqlplus if something goes wrong (like for SQL: WHENEVER SQLERROR EXIT SQL.SQLCODE )
only message like
"Warning: {Object} created with compilation errors."

To handle such errors we can use PAUSE in sql script
1) analyze output lines for Warning: {Object} created with compilation errors.
2) if found set was_compilation_err = True
3) wait for pause statement
4) check for was_compilation_err : if True -> fails else write sqlplus b'OK\n'

"""


import subprocess

oracle_dsn = 'js/js@orcl'

cmd = ['sqlplus', '-L', oracle_dsn, '@', 'lol.sql']

popen = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

try:
    was_compilation_err = False
    stdout_lines = iter(popen.stdout.readline, b'')
    for stdout_line in stdout_lines:
        line = stdout_line.decode('utf-8').strip('\r\n')
        print(line)
        if line.startswith('Warning: '):
            was_compilation_err = True
        if line == 'check for errors':
            if was_compilation_err:
                raise AssertionError('COMPILATION ERRORS!!!')
            else:
                popen.stdin.write('OK\n'.encode('utf-8'))
                popen.stdin.flush()
except AssertionError as ae:
    print(ae)
    popen.terminate()
    exit(1)