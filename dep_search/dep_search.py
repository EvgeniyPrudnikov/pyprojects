import matplotlib as mpl
import os
import pickle
from collections import namedtuple
import re
import networkx as nx
import matplotlib.pyplot as plt
import sys
import time
from datetime import timedelta
import numpy as np

INDEX = {}
EXCLUDE_DIR_NAMES = ['dev_dw', 'dev_pr', 'tables', 'sequences', 'functions', 'synonym',
                     'types', 'application', 'queries', 'scripts', 'json', 'dev_kiev', 'dev_kiev_pr', 'deploy']
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']

schema_re = re.compile('use@([0-9_a-zA-Z]*);?', re.DOTALL | re.MULTILINE)
trg_re = re.compile(
    '@?insert@?(.*(@table|@into))@([\(\)\._a-zA-Z0-9]+?)[@|(]', re.DOTALL | re.MULTILINE)
trg_view_re = re.compile(
    'create([or@replace]*)@view@([\.\$_a-zA-Z0-9]+?)@', re.DOTALL | re.MULTILINE)
src_re = re.compile(
    '@(from|inner@join|left@join|right@join|full@join|cross@join|join)@([\(\)\.\$\_a-zA-Z0-9]+?)@', re.DOTALL | re.MULTILINE)
src_with_catch = re.compile(
    '@?(with|,)@([_a-zA-Z0-9]+?)@as@\(', re.DOTALL | re.MULTILINE)


trg_obj_props = namedtuple('trg_obj_props', ['schemas', 'sources'])


def clear_data(text):
    # lines clearing
    text_lines = [line.strip().lower().replace('"', '') for line in text.split('\n') if
                  line.strip() and not line.strip().startswith('--')]
    # print(text_lines)
    if text_lines is None:
        return None

    #  comments clearing
    cl_data = []
    is_multiline_comment = 0
    for line in text_lines:
        comm2_start = line.find('/*')
        comm2_end = line.find('*/')

        if comm2_start > -1 and comm2_end > -1:
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
def if_equal_tables(t1_name, t2_name):
    equal_prefix = ['t_', 'v_', 'c_', 'd_', 'ld_']
    if t1_name[:2] in equal_prefix and t2_name[:2] in equal_prefix:
        return t1_name[2:] == t2_name[2:]
    else:
        return False


def process_prefix_postfix(object_name):
    dot = object_name.find('.') + 1
    if object_name.find('@') > -1:
        return object_name[dot:object_name.find('@')]
    else:
        return object_name[dot:]


def process_file(file_path, schema_name):
    ind_part = {}

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
        if len(cl_data) > 0:
            if not (cl_data.startswith('insert') or cl_data.startswith('merge') or cl_data.startswith('create') or cl_data.startswith('use')):
                continue

        cl_data = '@'.join(cl_data.split())

        if cl_data.startswith('use'):
            try:
                res = schema_re.findall(cl_data).pop()
                if len(res) > 0:
                    schema_name = res
                continue
            except IndexError:
                continue

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
            trg_object = process_prefix_postfix(trg_object)
        elif schema_name in ('jenkins', 'hive_sql', 'impala_sql'):
            schema_name = trg_object[:trg_object.find('.')]
            trg_object = process_prefix_postfix(trg_object)

        if len(trg_object) == 0:
            continue
        # if trg_object == 'af_tourn_aug_intersect_ww':
        #     print(1)

        src_objects = src_re.findall(cl_data)
        with_objects = tuple([item[1].strip().lower()
                              for item in src_with_catch.findall(cl_data)])

        s_sources = set()
        for src in src_objects:
            val = process_prefix_postfix(src[1].strip(' ();').lower())
            if val and 'select' not in val and 'dual' not in val and val != trg_object and val not in with_objects:
                s_sources.add(val)

        top = trg_obj_props(schemas={schema_name}, sources=s_sources)
        ind_part[trg_object] = top

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
                res_val = trg_obj_props(
                    sources=index[k].sources | ind_part[k].sources, schemas=index[k].schemas | ind_part[k].schemas)  # merge sets
                index[k] = res_val
    del ind_part


def create_index(root_dir_path, exclude_dir_names=[]):
    start = time.time()
    for path, subdirs, files in os.walk(root_dir_path):
        subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
        if not files:
            continue
        for f in files:
            if f[f.rfind('.'):] in ACCEPTED_FILES_TYPES:
                ind_part = process_file(os.path.join(
                    path, f), os.path.basename(os.path.dirname(path)))
                add_to_index(INDEX, ind_part)
        print('{0} - {1} files processed.'.format(path, len(files)))

    with open('index.pkl', 'wb') as pkl:
        pickle.dump(INDEX, pkl)

    end = time.time()
    print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))


root_dir_path = r''

# create_index(root_dir_path, EXCLUDE_DIR_NAMES)
# exit(1)

with open('index.pkl', 'rb') as pkl:
    INDEX = pickle.load(pkl)

with open('idx', 'w') as f:
    f.write(str(INDEX))

# exit(0)


def find_source_path(ind, search_object, x=0, y=-1, res=[], seen=[], pos={}):

    x -= 1
    y += 1

    try:
        src_objs = ind[search_object].sources
    except KeyError:
        return
    if len(src_objs) == 0:
        return
    if len(src_objs) == 1:
        o = src_objs.pop()
        res.append((o, search_object,))
        seen.append(o)
        pos[o] = (x, abs(y),)
        print(' ' * abs(x) * 5 + '(' + str(x) +
              ', ' + str(abs(y )) + ' )' + ' ' + o)
        return
    for i, o in enumerate(src_objs):
        if o not in seen:
            res.append((o, search_object,))
            seen.append(o)
            pos[o] = (x, y + i,)
            print(' ' * abs(x) * 5 + '(' + str(x) + ', ' + str(y + i) + ' )' + ' ' + o)
            find_source_path(ind, o, x, y + i, res, seen, pos)

    return res, pos


def find_target_path(ind, search_object, lvl=-1, res=[], seen=[]):

    lvl += 1
    trgs = []

    for k, v in ind.items():
        if search_object in v.sources:
            trgs.append(k)

    if len(trgs) == 0:
        return

    for t in trgs:
        if t not in seen:
            print(' ' * lvl * 5 + str(lvl) + ' ' + t)
            res.append((search_object, t,))
            seen.append(t)
            find_target_path(ind, t, lvl, res, seen)
    return res


sys.setrecursionlimit(100)

res_source, pos_source = find_source_path(INDEX, 'cl_wot_acc_actions')
# res_target = find_target_path(INDEX, 'cl_wot_acc_actions')

# x = round(sum([i[1] for i in pos_source.values() if i[0] == -1 ])/len(pos_source))
x = 4
print(x)

pos_source['cl_wot_acc_actions'] = (0, x)

print(pos_source)

# print('LEN=', len(res_source) + len(res_target))

g = nx.DiGraph(directed=True)

g.add_edges_from(res_source)
# g.add_edges_from(res_target)

# graph_pos = nx.shell_layout(g)

# nx.draw_networkx_nodes(g, graph_pos, node_size=1000, node_color='blue', alpha=0.3)
# nx.draw_networkx_edges(g, graph_pos)
# nx.draw_networkx_labels(g, graph_pos, font_size=10, font_family='sans-serif')


nx.draw(g, pos_source, with_labels=True, arrows=True, alpha=0.3)
plt.draw()
plt.show()
