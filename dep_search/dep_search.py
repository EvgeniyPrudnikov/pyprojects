import os
import pickle
import re

EXCLUDE_DIR_NAMES = ['scripts', 'json', 'dev_kiev', 'dev_kiev_pr']



trg_re = re.compile('@?insert@(.*?)@?into@(.*?)[@|(]', re.DOTALL | re.MULTILINE | re.IGNORECASE)
src_re = re.compile('@?(from|inner@join|left@join|right@join|full@join|cross@join|join)@(.*?)@', re.DOTALL | re.MULTILINE | re.IGNORECASE)

s = '''
insert  into lol1 
select * from LOOOL
union all
insert /*+append*/ into s_arenas(asd)
            SELECT *
          FROM    wotx_log_arenas wla
          ,dual9 d1
          left join dual1 on a = b
          rigth join dual2 an c = g 
          full join dual3 on g =o 
          join dual4 on r = t
          inner join dual5 on r = t
         WHERE dt = TO_CHAR( lStartDt, 'YYYY-MM-DD')
     
'''

s2 = '@'.join(s.split())
print(s2)

result = trg_re.findall(s2)
result2 = src_re.findall(s2)

print(result)
print(result2)

exit(1)

a = {'Python': '.py', 'C++': '.cpp', 'Java': '.java'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)


def process_file(file_path, ):
    ind_part = {}

    f = open(file_path, 'r')
    data = f.read()
    data = '@'.join(data.split())
    trg_object = trg_re.search(data).group(2).strip()
    ind_part[trg_object] = 'insert'
    src_object = src_re.search(s).group(3).strip()
    ind_part[src_object] = 'from'

    print(ind_part)
    f.close()


def create_index(root_dir_path, exclude_dir_names=None):
    for path, subdirs, files in os.walk(root_dir_path):
        subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
        if not files:
            continue
        print('{0} - {1} - {2}'.format(path, os.path.basename(os.path.dirname(path)), files))


# create_index(ROOT, EXCLUDE_DIR_NAMES)

process_file(lol_path)
