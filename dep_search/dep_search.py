import os
import pickle
import re

EXCLUDE_DIR_NAMES = ['scripts', 'json', 'dev_kiev', 'dev_kiev_pr']

trg_re = re.compile('@?insert@(.*?)@?into@(.*?)[@|(]', re.DOTALL | re.MULTILINE | re.IGNORECASE)
src_re = re.compile('@?(from|inner@join|left@join|right@join|full@join|cross@join|join)@(.*?)@',
                    re.DOTALL | re.MULTILINE | re.IGNORECASE)
# @?from@(.*?)@?,@(.*)@

sr = """select * from lol1 l1, lol2 l2"""
print('@'.join(sr.split()))

s = '''

with lol1 as (
-- lol
select 1 as a from dual1 union all -- lol1
select 2 as a from dual1 union all /* + asd */
select 3 /* as a from dual1
asd 
sad */ LLLOOL
) 
, lol2 as ( -- COMMENT!
select 2 as b from dual2 /* comment2 */ 
)
select * from lol1 l1, lol2 l2 
where l1.a = l2.b(+)
     
'''


def clear_data(text):
    # lines clearing
    text_lines = [line for line in text.split('\n') if
                  line.strip() and not line.strip().startswith('--') and not line.strip().startswith(
                      '/*') and not line.strip().startswith('pkg_')]
    #  comments clearing
    cl_data = []
    flag_1 = 0
    for line in text_lines:
        comm1 = line.find('--')
        comm2_start = line.find('/*')
        comm2_end = line.find('*/')
        if comm1 > -1:
            line = line[:comm1]
        elif comm2_start > -1 and comm2_end > -1:
            line = line[:comm2_start] + line[comm2_end + 2:]
        elif comm2_start > -1 and flag_1 == 0:
            line = line[:comm2_start]
            flag_1 = 1
        elif flag_1 == 1 and comm2_end > -1:
            line = line[comm2_end + 2:]
            flag_1 = 0
        elif flag_1 == 1:
            continue
        if line:
            cl_data.append(line)
    return '\n'.join(cl_data)


#
#
# data = clear_data(raw_data)
#
# s2 = '@'.join(data.split())
# res1 = trg_re.findall(s2)
# res2 = src_re.findall(s2)
#
# print(res1)
# print(res2)

#
# exit(1)

a = {'Python': '.py', 'C++': '.cpp', 'Java': '.java'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)


lol_path = r''


def process_file(file_path):
    ind_part = {}

    f = open(file_path, 'r')
    data = f.read()
    cl_data = clear_data(data)
    cl_data = '@'.join(cl_data.split())

    trg_objects = trg_re.findall(cl_data)
    for trg in trg_objects:
        ind_part[trg[1]] = 'insert'
    src_objects = src_re.findall(cl_data)
    for src in src_objects:
        if src[1] != '(' and src[1].lower() != '(select':
            ind_part[src[1]] = src[0].lower()

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
