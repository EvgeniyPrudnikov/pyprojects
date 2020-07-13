import os
import argparse
import pickle
from collections import namedtuple
import re
import traceback
from igraph import Graph
import matplotlib.pyplot as plt
import networkx as nx
import time
import csv
from datetime import timedelta

INDEX = {}
INDEX2 = {}

EXCLUDE_DIR_NAMES = {'dev_dw', 'dev_pr', 'tables', 'sequences', 'functions', 'synonym',
                     'types', 'application', 'queries', 'scripts', 'json', 'dev_kiev', 'dev_kiev_pr', 'deploy',
                     'hive_metastore', 'kafka', 'reports', 'triggers', 'additional_tools', 'oemm'}
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']

FLAGS = re.DOTALL | re.MULTILINE

SQL_REG = {
    'schema_re': r'use@([0-9_a-zA-Z]*);?',
    'trg_re': r'@?(insert|merge)@?(.*(@table|@into))@([\(\)\._a-zA-Z0-9]+?)[@|(]',
    'trg_view_re': r'create([or@replace]*)@view@([\.\$_a-zA-Z0-9]+?)@',
    'src_re': r'@(from|inner@join|left@join|right@join|full@join|cross@join|join)@([\(\)\.\$\_a-zA-Z0-9]+?)@',
    'src_with_catch': r'@?(with|,)@([_a-zA-Z0-9]+?)@as@\('
}

SQL_REG = {k: re.compile(v, FLAGS) for k, v in SQL_REG.items()}


trg_obj_props = namedtuple('trg_obj_props', ['schemas', 'sources'])


def result_to_csv(output, file_name='result.csv'):
    with open(file_name, 'w+') as f:
        writer = csv.writer(f, dialect='excel', delimiter=',', lineterminator='\n', escapechar='\\')
        writer.writerows(output)


def clear_data(text):
    # lines clearing
    text_lines = [line.strip().lower().replace('"', '') for line in text.split('\n') if
                  line.strip() and not line.strip().startswith('--')]

    if len(text_lines) == 0:
        return ''

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
    return '\n'.join(cl_data) + '@'

# TODO merge tables by [3:] symbols
# [DWH specific]


def merge_equal_tables(t_name):
    equal_prefix = ['c_', 'd_', 'ld_']
    if any(map(t_name.startswith, equal_prefix)):
        return 't' + t_name[t_name.find('_'):]
    return t_name


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

    starts = ['insert', 'merge', 'create', 'use', 'if']

    for stm in data.split(';'):
        stm = stm.strip().lower()
        cl_data = clear_data(stm)

        if len(cl_data) <= 1:
            continue

        if len(cl_data) > 1 and not any(map(cl_data.startswith, starts)):
            continue

        cl_data = '@'.join(cl_data.split())

        if cl_data.startswith('use'):
            try:
                res = SQL_REG['schema_re'].findall(cl_data).pop()
                if len(res) > 0:
                    schema_name = res
                continue
            except IndexError:
                continue

        l_trg_objects = SQL_REG['trg_re'].findall(cl_data)
        if l_trg_objects:
            trg_object = l_trg_objects[0][3].strip().lower()
        else:
            l_trg_objects = SQL_REG['trg_view_re'].findall(cl_data)
            if l_trg_objects:
                trg_object = l_trg_objects[0][1].strip().lower()
            else:
                continue

        if schema_name != 'jenkins':
            trg_object = process_prefix_postfix(trg_object)
        elif schema_name in ('jenkins', 'hive_sql', 'impala_sql'):
            schema_name = trg_object[:trg_object.find('.')]
            trg_object = process_prefix_postfix(trg_object)

        trg_object = merge_equal_tables(trg_object)

        if not trg_object:
            continue

        src_objects = SQL_REG['src_re'].findall(cl_data)
        with_objects = tuple([item[1].strip().lower() for item in SQL_REG['src_with_catch'].findall(cl_data)])

        s_sources = set()
        for src in src_objects:
            val = merge_equal_tables(process_prefix_postfix(src[1].strip(' ();').lower()))
            if val and 'select' not in val and 'dual' not in val and val != trg_object and val not in with_objects:
                s_sources.add(val)

        add_to_index(ind_part, {trg_object: trg_obj_props(schemas={schema_name}, sources=s_sources)})

    f.close()

    return ind_part


def add_to_index(index, ind_part):
    if not ind_part:
        return
    else:
        for k in ind_part:
            if k not in index:
                index[k] = ind_part[k]
            else:
                res_val = trg_obj_props(sources=index[k].sources | ind_part[k].sources, schemas=index[k].schemas | ind_part[k].schemas)  # merge sets
                index[k] = res_val
    del ind_part


def create_index(root_dir_path, exclude_dir_names=[]):
    start = time.time()
    for path, subdirs, files in os.walk(root_dir_path):
        subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
        if not files:
            continue
        cnt = 0
        for f in files:
            if f[f.rfind('.'):] in ACCEPTED_FILES_TYPES:
                ind_part = process_file(os.path.join(path, f), os.path.basename(os.path.dirname(path)))
                add_to_index(INDEX, ind_part)
                cnt += 1
        print('{0} - {1} files processed.'.format(path, cnt))

    INDEX['METADATA'] = {'objects': len(INDEX), 'last_update_date': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}

    print(INDEX['METADATA'])

    with open('index.pkl', 'wb') as pkl:
        pickle.dump(INDEX, pkl)

    end = time.time()
    print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))

    return INDEX


def find_source_path(idx, search_object, exclude_source, depth=999, x=-1, res=[], seen=[]):
    '''
    find and return objects who is a source for the search_object (like obj1 , obj2, obj3 --> search_object)
    '''

    try:
        src_objs = sorted(list(idx[search_object].sources))
    except KeyError:
        res.append(('NULL', search_object,))
        return

    if not src_objs or abs(x) > depth or search_object in exclude_source:
        res.append(('NULL', search_object,))
        return

    for o in src_objs:
        if o not in seen:
            res.append((o, search_object,))
            seen.append(o)
            find_source_path(idx, o, exclude_source, depth, x - 1, res, seen)
        else:
            res.append((o, search_object,))

    return res


def find_target_path(idx, search_object, depth=999, x=1, res=[], seen=[]):
    '''
    find and return objects who is a target for the search_object (like search_object --> obj1 , obj2, obj3)
    '''

    trgs = []

    for k, v in idx.items():
        if k != 'METADATA' and search_object in v.sources:
            trgs.append(k)

    if not trgs or abs(x) > depth:
        return

    for t in trgs:
        if t not in seen:
            res.append((search_object, t,))
            seen.append(t)
            find_target_path(idx, t, depth, x + 1, res, seen)
        else:
            res.append((search_object, t,))
    return res


def open_index():
    global INDEX

    if not INDEX:
        try:
            with open('index.pkl', 'rb') as pkl:
                INDEX = pickle.load(pkl)
        except FileNotFoundError:
            print('index.pkl file not found.')
            print('Please create index using --create_index PATH_TO_SVN_TRUNC_FOLDER flag')
            exit(1)
        except Exception:
            print(traceback.format_exc())
            exit(1)

    print('INDEX:{0}\n'.format(INDEX['METADATA']))

    return INDEX


def swap_index():
    global INDEX

    INDEX = open_index()

    with open('index.csv', 'w+') as f:
        cw = csv.writer(f, delimiter=',', lineterminator='\n')
        for k, v in INDEX.items():
            if k == 'METADATA':
                continue
            for vi in v.sources:
                cw.writerow([vi, k])


def get_vertexs(output):
    vertexs = set()

    for i in output:
        vertexs.add(i[0])
        vertexs.add(i[1])

    return sorted(list(vertexs))


def show_dataflow2(search_objects, search_result):
    vertexs = get_vertexs(search_result)
    nr_vertices = len(vertexs)
    G = Graph.Tree(0, 5, mode=1)  # 2 stands for children number
    G.add_vertices(vertexs)
    G.add_edges(search_result)

    lay = G.layout('rt')
    position = {k: (lay[k][1], lay[k][0]) for k in range(nr_vertices)}  # rotate 90 deg

    vx = {v: k for k, v in G.vs._name_index.items()}

    gr = nx.DiGraph(directed=True)
    E = [(vx[e.tuple[0]], vx[e.tuple[1]]) for e in G.es]  # list of edges
    position = {vx[k]: v for k, v in position.items()}

    gr.add_edges_from(E)

    nx.draw(gr, position, with_labels=False, arrows=True, alpha=0.5, font_size=10,
            node_shape='s', node_size=250, connectionstyle='arc3, rad = 0.15',)

    for k, v in position.items():
        if k in search_objects:
            plt.annotate(k, v, textcoords="offset points", xytext=(0, 11), ha='center', color='red')
        else:
            plt.annotate(k,  # this is the text
                         v,  # this is the point to label
                         textcoords="offset points",  # how to position the text
                         xytext=(0, 11),  # distance from text to points (x,y)
                         ha='center',  # horizontal alignment can be left, right or center
                         )
    plt.draw()
    plt.show()


def main():

    global EXCLUDE_DIR_NAMES
    global INDEX

    parser = argparse.ArgumentParser(description='Dependencies search')
    parser.add_argument('-ci', "--create_index", metavar='root_dir_path', action="store", help="create or update index from localFS (svn trunc)")
    parser.add_argument('-e', "--exclude_dir_names", metavar='dir_name', action="store", nargs='+', required=False, help='exclude dirs')
    parser.add_argument('-f', "--find", metavar='search_object', nargs='+', help="find dependencies")
    parser.add_argument('-sw', "--swap_index", metavar='FILE_NAME', help="swap index to csv file")
    parser.add_argument('-d', "--depth", action="store", type=int, help="depth of search in both directions", default=999)
    parser.add_argument('-es', "--exclude_source", action="store", metavar='exclude_source', nargs='+', help="depth of search in both directions")

    args = parser.parse_args()

    if not args.create_index and not args.find and not args.swap_index:
        print('Nothing to do...')
        parser.print_help()
        exit(0)

    search_depth = args.depth

    exclude_source = []
    if args.exclude_source:
        exclude_source = set(args.exclude_source)
        print('exclude_sources:\n')
        print(exclude_source)

    if args.exclude_dir_names:
        EXCLUDE_DIR_NAMES = set(args.exclude_dir_names)
        print('EXCLUDE_DIR_NAMES:\n')
        print(EXCLUDE_DIR_NAMES)

    if args.create_index:
        root_dir_path = args.create_index.strip("'").strip('"')
        if os.path.isdir(root_dir_path):
            print('Creating index ...')
            INDEX = create_index(root_dir_path, EXCLUDE_DIR_NAMES)
        else:
            print('Path "{0}" is not a directory'.format(root_dir_path))

    if args.swap_index:
        swap_index()

    if args.find:
        search_objects = list(map(lambda x: x.lower(), args.find))

        INDEX = open_index()

        # with open('idx', 'w') as f:
        #     f.write(str(INDEX))

        result_source = set()
        result_target = set()

        for so in search_objects:
            res_source = find_source_path(
                INDEX, so, exclude_source, depth=search_depth)
            if res_source:
                result_source = result_source | set(res_source)

            res_target = find_target_path(INDEX, so, depth=search_depth)
            if res_target:
                result_target = result_target | set(res_target)

        result = result_source | result_target

        print(result)
        print('Done.')

        result_to_csv(result)

        show_dataflow2(search_objects, result)


if __name__ == '__main__':
    main()
