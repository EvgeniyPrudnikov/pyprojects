import os
import pickle
import re
import networkx
import sys
import time
from datetime import timedelta
import numpy as np

INDEX = {}
EXCLUDE_DIR_NAMES = ['dev_dw', 'dev_pr', 'tables', 'sequences','functions', 'synonym', 'types', 'application', 'queries', 'scripts', 'json', 'dev_kiev', 'dev_kiev_pr']
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']

trg_re = re.compile('@?insert@?(.*(@table|@into))@([\(\)\._a-zA-Z0-9]+?)[@|(]', re.DOTALL | re.MULTILINE)
trg_view_re = re.compile('create([or@replace]*)@view@([\.\$_a-zA-Z0-9]+?)@', re.DOTALL | re.MULTILINE)
src_re = re.compile('@(from|inner@join|left@join|right@join|full@join|cross@join|join)@([\(\)\.\$\_a-zA-Z0-9]+?)@', re.DOTALL | re.MULTILINE)
src_with_catch = re.compile('@?(with|,)@([_a-zA-Z0-9]+?)@as@\(', re.DOTALL | re.MULTILINE)


def clear_data(text):
    # lines clearing
    text_lines = [line.strip().lower().replace('"','') for line in text.split('\n') if
                  line.strip() and not line.strip().startswith('--')]
    # print(text_lines)
    if text_lines is None:
        return None

    #  comments clearing
    cl_data = []
    is_multiline_comment = 0
    for line in text_lines:
        comm1 = line.find('--')
        comm2_start = line.find('/*')
        comm2_end = line.find('*/')
        if comm1 > -1:
            line = line[:comm1]
        elif comm2_start > -1 and comm2_end > -1:
            line = line[:comm2_start] + line[comm2_end + 2:]
        elif comm2_start > -1 and is_multiline_comment == 0:
            line = line[:comm2_start]
            is_multiline_comment = 1
        elif is_multiline_comment == 1 and comm2_end > -1:
            line = line[comm2_end + 2:]
            is_multiline_comment = 0
        elif is_multiline_comment == 1:
            continue
        if line:
            cl_data.append(line)
    return '\n'.join(cl_data)


# [DWH specific]
def if_queal_tables(t1_name, t2_name):
    equal_prefix = ['t_', 'v_', 'c_', 'd_', 'ld_']
    if t1_name[:2] in equal_prefix and t2_name[:2] in equal_prefix:
        return t1_name[2:] == t2_name[2:]
    else:
        return False


def process_prefix_postfix(object_name, op_type='pf'):
    dot = object_name.find('.') + 1 if op_type == 'pr' else 0
    if object_name.find('@') > -1:
        return object_name[dot:object_name.find('@')]
    else:
        return object_name[dot:]


def process_file(file_path, schema_name):
    ind_part = {}
    # print(schema_name)
    f = open(file_path, 'rb')
    try:
        data = f.read().decode('utf-8', 'ignore')
    except Exception as e:
        print(e)
        print(file_path + '-- broken')
        return

    for stm in data.split(';'):
        stm = stm.strip().lower()
        cl_data = clear_data(stm)
        if cl_data:
            if not (cl_data.startswith('insert') or cl_data.startswith('merge') or cl_data.startswith('create')):
                continue

        cl_data = '@'.join(cl_data.split())

        l_trg_objects = trg_re.findall(cl_data)
        if l_trg_objects:
            trg_object = l_trg_objects[0][2].strip().lower()
        else:
            l_trg_objects = trg_view_re.findall(cl_data)
            if l_trg_objects:
                trg_object = l_trg_objects[0][1].strip().lower()
            else:
                continue

        if schema_name != 'jenkins':
            trg_object = (schema_name, process_prefix_postfix(trg_object, op_type='pr'))
        elif schema_name == 'jenkins':
            trg_object = (trg_object[:trg_object.find('.')], trg_object[trg_object.find('.') + 1:])

        src_objects = src_re.findall(cl_data)
        with_objects = tuple([item[1].strip().lower() for item in src_with_catch.findall(cl_data)])

        s_sources = set()
        for src in src_objects:
            val = process_prefix_postfix(src[1].strip(' ();').lower())
            if val and 'select' not in val and 'dual' not in val and val != trg_object[1] and val not in with_objects:
                s_sources.add(val)

        ind_part[trg_object] = s_sources

    # print(ind_part)
    f.close()

    return ind_part


def add_to_index(index, ind_part):
    if ind_part is None:
        return
    if len(ind_part) == 0:
        return
    else:
        for k in ind_part:
            if k not in index:
                index[k] = ind_part[k]
            else:
                res_val = index[k] | ind_part[k]  # merge sets
                index[k] = res_val
    del ind_part
    # print(index)


def create_index(root_dir_path, exclude_dir_names=[]):
    start = time.time()
    for path, subdirs, files in os.walk(root_dir_path):
        subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
        if not files:
            continue
        for f in files:
            if f[f.rfind('.'):] in ACCEPTED_FILES_TYPES:
                ind_part = process_file(os.path.join(path, f), os.path.basename(os.path.dirname(path)))
                add_to_index(INDEX, ind_part)
        print('{0} - {1} files processed.'.format(path, len(files)))

    with open('index.pkl', 'wb') as pkl:
        pickle.dump(INDEX, pkl)

    end = time.time()
    print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))



root_dir_path = r''

# create_index(root_dir_path, EXCLUDE_DIR_NAMES)

with open('index.pkl', 'rb') as pkl:
    INDEX = pickle.load(pkl)

# print(INDEX)
# exit(1)

def get_sources(ind, search_object):
    l_sources = []
    if not search_object:
        return l_sources

    if isinstance(search_object, tuple):
        l_sources = (search_object, ind[search_object])
    elif isinstance(search_object, str):
        for k, v in ind.items():
            if k[1] == search_object:
                l_sources = (k, v)
    if l_sources:
        return l_sources
    else:
        if isinstance(search_object, tuple):
            return search_object
        elif isinstance(search_object, str):
            return (('', search_object),)



def find_source_path(ind, search_object, lvl=-1, res =[]):
    # res = []
    lvl += 1
    src_objs = get_sources(ind, search_object)
    if len(src_objs) == 1:
        # print(' '*lvl + str(lvl) + str(src_objs))
        res.append([lvl, src_objs[0]])
        return
    for o in src_objs[1]:
        # print(' '*lvl + str(lvl) + str(src_objs[0]) +' <= '+ str(o))
        find_source_path(ind, o, lvl, res)
        res.append([lvl, src_objs[0]])
    return res

sys.setrecursionlimit(30)

res = find_source_path(INDEX, '')

res2 = {}

for r in res:
    if r[0] not in res2:
        res2[r[0]] = [r[1][0] + '.' + r[1][1]]
    else:
        res_val = res2[r[0]] + [r[1][0] + '.' + r[1][1]]  # merge sets
        res2[r[0]] = res_val


print(res2)
# r4 = sorted(res2, key=lambda x: x[0], reverse=True)
# r5 = np.array(r4)

def pretty_print(output):
    # to_str = np.vectorize(str)
    # get_length = np.vectorize(len)
    # max_col_length = np.amax(get_length(to_str(output)), axis=1)
    # print(max_col_length)
    for k in output:
        for v in output[k]:
            print(' '*k*10 + str(k) + ' ' + v)


pretty_print(res2)


def pretty_print_result(output):
    to_str = np.vectorize(str)
    get_length = np.vectorize(len)
    max_col_length = np.amax(get_length(to_str(output)), axis=0)
    print(max_col_length)

    # print result
    print('+' + ''.join(['-' * x + '--+' for x in max_col_length]))
    for row_index, row in enumerate(output):
        print('|' + ''.join([' ' + str(value).replace('None', 'NULL') + ' ' * (max_col_length[index] - len(str(value))) + ' |' for index, value in enumerate(row)]))
        if row_index == 0 or row_index == len(output) - 1:
            print('+' + ''.join(['-' * x + '--+' for x in max_col_length]))
    print('\nFetched {0} rows'.format(np.size(output, 0) - 1))

# pretty_print_result(r5.transpose())