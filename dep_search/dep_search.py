import os
import argparse
import pickle
from collections import namedtuple
import re
import traceback
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch
import time
import csv
from datetime import timedelta


class MyLines:

    def __init__(self, points):
        self.points = points
        self.lines = []

    def add_lines(self, ln):
        self.lines.append(ln)

    def get_dep_line(self, xy):
        moveble_line_idx = {}
        for i, l in enumerate(self.lines):
            psab = l._posA_posB
            if psab[0] == xy:
                moveble_line_idx[i] = 0
            if psab[1] == xy:
                moveble_line_idx[i] = 1
        return moveble_line_idx


class DragableLineAnn:
    lock = None

    def __init__(self, myline, an):
        self.myline = myline
        self.line = myline.points
        self.an = an
        self.press = None
        self.background = None
        # self.background_an = None
        self.xs = list(myline.points.get_xdata())
        self.ys = list(myline.points.get_ydata())
        self.moving_point_idx = -1
        self.cidpress = myline.points.figure.canvas.mpl_connect('button_press_event', self.on_press)
        self.cidmotion = myline.points.figure.canvas.mpl_connect('motion_notify_event', self.on_motion)
        self.cidrelease = myline.points.figure.canvas.mpl_connect('button_release_event', self.on_release)

    def on_press(self, event):
        'on button press we will see if the mouse is over us and store some data'

        if event.inaxes != self.line.axes:
            return
        if DragableLineAnn.lock is not None:
            return
        contains, attrd = self.line.contains(event)
        if not contains:
            return
        # print(event)
        DragableLineAnn.lock = self

        x0 = min(self.xs, key=lambda x: abs(x - event.xdata))
        y0 = min(self.ys, key=lambda x: abs(x - event.ydata))

        try:
            self.moving_point_idx = list(zip(self.xs, self.ys)).index((x0, y0))
        except Exception as e:
            # print(e)
            self.moving_point_idx = -1
            self.on_release(event)
            return

        self.dep = self.myline.get_dep_line((x0, y0))
        self.dep_lines = [self.myline.lines[k] for k, _ in self.dep.items()]

        self.press = x0, y0, event.xdata, event.ydata
        # print(self.press)

        canvas = self.line.figure.canvas
        canvases = []
        for dep_line in self.dep_lines:
            canvases.append(dep_line.figure.canvas)

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)

        axes_an = self.an[self.moving_point_idx].axes

        self.line.set_animated(True)
        for dep_line in self.dep_lines:
            dep_line.set_animated(True)

        self.an[self.moving_point_idx].set_animated(True)
        canvas.draw()

        self.background = canvas.copy_from_bbox(self.line.axes.bbox)
        axes.draw_artist(self.line)
        for i, dep_line in enumerate(self.dep_lines):
            axeses[i].draw_artist(dep_line)
        axes_an.draw_artist(self.an[self.moving_point_idx])

        # and blit just the redrawn area
        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes_an.bbox)

    def on_motion(self, event):
        'on motion we will move the line if the mouse is over us'
        if DragableLineAnn.lock is not self:
            return
        if event.inaxes != self.line.axes:
            return
        x0, y0, xpress, ypress = self.press
        dx = event.xdata - xpress
        dy = event.ydata - ypress
        self.xs[self.moving_point_idx] = (x0 + dx)
        self.ys[self.moving_point_idx] = (y0 + dy)
        self.line.set_xdata(self.xs)
        self.line.set_ydata(self.ys)
        self.an[self.moving_point_idx].xy = ((x0 + dx), (y0 + dy))
        # print(self.dep)
        for k, v in self.dep.items():
            dline = self.myline.lines[k]
            psab = dline._posA_posB
            psab[v] = ((x0 + dx), (y0 + dy))
            dline.set_positions(*psab)

        canvas = self.line.figure.canvas
        canvases = []
        # for dep_line in self.dep_lines:
        #     canvases.append(dep_line.figure.canvas)

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)
        axes2 = self.an[self.moving_point_idx].axes
        # restore the background region
        canvas.restore_region(self.background)
        # for i, c in enumerate(canvases):
        #     c.restore_region(self.backgroundes[i])

        # canvas.restore_region(self.background_an)

        # redraw just the current rectangle
        axes.draw_artist(self.line)
        for i, dl in enumerate(self.dep_lines):
            axeses[i].draw_artist(dl)
        axes2.draw_artist(self.an[self.moving_point_idx])

        # blit just the redrawn area
        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes2.bbox)

    def on_release(self, event):
        'on release we reset the press data'
        if DragableLineAnn.lock is not self:
            return

        self.press = None

        # turn off the rect animation property and reset the background
        self.line.set_animated(False)
        for dl in self.dep_lines:
            dl.set_animated(False)
        self.an[self.moving_point_idx].set_animated(False)
        self.background = None

        self.line.figure.canvas.draw()
        for dl in self.dep_lines:
            dl.figure.canvas.draw()
        self.an[self.moving_point_idx].figure.canvas.draw()

        DragableLineAnn.lock = None


INDEX = {}
INDEX2 = {}

EXCLUDE_DIR_NAMES = {'dev_dw', 'dev_pr', 'tables', 'sequences', 'functions', 'synonym',
                     'types', 'application', 'queries', 'scripts', 'json', 'dev_kiev', 'dev_kiev_pr', 'deploy',
                     'hive_metastore', 'kafka', 'reports', 'triggers', 'additional_tools', 'oemm', 'snowflake', 'tidal', 'oracle_dwmeta'}
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']

FLAGS = re.DOTALL | re.MULTILINE

SQL_REG = {
    'schema_re': r'use@([0-9_a-zA-Z]*);?',
    'trg_re': r'@?(insert|merge)@?(.*(@table|@into))@([\(\)\._a-zA-Z0-9]+?)[@|(]',
    'trg_view_re': r'create([or@replace@force]*)@view@([\.\$_a-zA-Z0-9]+?)@',
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


def find_source_path(ind, search_object, exclude_source, depth=999, x=-1, res=[], seen=[], pos={}):

    try:
        src_objs = sorted(list(ind[search_object].sources))
    except KeyError:
        return

    if len(src_objs) == 0 or abs(x) > depth or search_object in exclude_source:
        return

    for o in src_objs:
        if o not in seen:
            res.append((o, search_object,))
            seen.append(o)
            pos[o] = (x, 0,)
            find_source_path(ind, o, exclude_source, depth, x - 1, res, seen, pos)
        else:
            res.append((o, search_object,))
            pos[o] = (x, 0,)

    return res, pos


def find_target_path(ind, search_object, depth=999, x=1, res=[], seen=[], pos={}):

    trgs = []

    for k, v in ind.items():
        if k != 'METADATA' and search_object in v.sources:
            trgs.append(k)

    if len(trgs) == 0 or abs(x) > depth:
        return

    for t in trgs:
        if t not in seen:
            # print(' ' * lvl * 5 + str(lvl) + ' ' + t)
            res.append((search_object, t,))
            seen.append(t)
            pos[t] = (x, 0,)
            find_target_path(ind, t, depth, x + 1, res, seen, pos)
        else:
            res.append((search_object, t,))
            pos[t] = (x, 0,)
    return res, pos


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


def get_vertexs_len(output):
    vertexs = set()

    for i in output:
        vertexs.add(i[0])
        vertexs.add(i[1])

    return len(vertexs)


def show_dataflow2(search_objects, search_result, pos):

    vertexs_len = get_vertexs_len(search_result)

    E = [(pos[e[0]], pos[e[1]]) for e in search_result]  # list of edges

    fig = plt.figure()
    ax = fig.add_subplot(111)

    x = []
    y = []
    for _, v in pos.items():
        x.append(v[0])
        y.append(v[1])

    lines, = ax.plot(x, y, 's', ms=16)

    ml = MyLines(lines)

    for ex in E:
        arrow = FancyArrowPatch(ex[0], ex[1],
                                arrowstyle='-|>',
                                shrinkA=11,
                                shrinkB=11,
                                mutation_scale=10,
                                color='black',
                                linewidth=0.9,
                                connectionstyle='arc3,rad=0.15',
                                zorder=1)

        ax.add_patch(arrow)
        ml.add_lines(arrow)

    ax.axes.get_xaxis().set_ticks([])
    ax.axes.get_yaxis().set_ticks([])

    an = []

    for k, v in pos.items():
        if k in search_objects:
            an.append(ax.annotate(k, v, textcoords="offset points", xytext=(0, 11), ha='center', color='red'))
        else:
            an.append(ax.annotate(k,  # this is the text
                                  v,  # this is the point to label
                                  textcoords="offset points",  # how to position the text
                                  xytext=(0, 11),  # distance from text to points (x,y)
                                  ha='center',  # horizontal alignment can be left, right or center
                                  ))
    print('len(vertexs) = ' + str(vertexs_len))
    if vertexs_len <= 64:
        ax.set_title('Interactable Data flow for {0}'.format(*search_objects))
        dl = DragableLineAnn(ml, an)
    else:
        ax.set_title('NonInteractable Data flow for {0}'.format(*search_objects))
        del ml
    plt.show()


def update_y_position(pos):
    ln = {}
    ln2 = {}
    for x in pos.values():
        if x[0] not in ln:
            ln[x[0]] = 1
        else:
            ln[x[0]] += 1

    for j in ln:
        ln2[j] = [i + 1 for i in range(ln[j])]

    for j in ln:
        ln[j] = ln[j] // 2

    for p in pos.keys():
        pnt = pos[p]
        pos[p] = (pnt[0], ln2[pnt[0]].pop() - ln[pnt[0]])


def main():

    global EXCLUDE_DIR_NAMES
    global INDEX

    parser = argparse.ArgumentParser(description='Dependencies search')
    parser.add_argument('-ci', "--create_index", metavar='root_dir_path', action="store", help="create or update index from localFS (svn trunc)")
    parser.add_argument('-e', "--exclude_dir_names", metavar='dir_name', action="store", nargs='+', required=False, help='exclude dirs')
    parser.add_argument('-f', "--find", metavar='search_object', nargs='+', help="find dependencies")
    parser.add_argument('-fs', "--find_source", metavar='search_object', nargs='+', help="find sources dependencies")
    parser.add_argument('-ft', "--find_target", metavar='search_object', nargs='+', help="find target dependencies")
    parser.add_argument('-sw', "--swap_index", metavar='FILE_NAME', help="swap index to csv file")
    parser.add_argument('-d', "--depth", action="store", type=int, help="depth of search in both directions", default=999)
    parser.add_argument('-es', "--exclude_source", action="store", metavar='exclude_source', nargs='+', help="depth of search in both directions")

    args = parser.parse_args()

    # if not args.create_index and not args.find and not args.swap_index:
    #     print('Nothing to do...')
    #     parser.print_help()
    #     exit(0)

    search_depth = args.depth

    exclude_source = []
    if args.exclude_source:
        exclude_source = set(args.exclude_source)
        print('exclude_sources:\n')
        print(exclude_source)

    print('Excluded directories:')
    print(*EXCLUDE_DIR_NAMES, sep=', ')

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

    if args.find_source:
        search_objects = list(map(lambda x: x.lower(), args.find_source))
        INDEX = open_index()
        result_source = set()
        position_source = dict()
        for so in search_objects:
            res_source, pos_source = find_source_path(INDEX, so, exclude_source, depth=search_depth)
            if res_source:
                result_source = result_source | set(res_source)
                position_source.update(pos_source)

        for so in search_objects:
            position_source[so] = (0, 0)

        update_y_position(position_source)
        print(position_source)

        result_to_csv(result_source)

        show_dataflow2(search_objects, result_source, position_source)

    if args.find_target:
        search_objects = list(map(lambda x: x.lower(), args.find_target))
        INDEX = open_index()
        result_target = set()
        position_target = dict()
        for so in search_objects:
            res_target, pos_target = find_target_path(INDEX, so, depth=search_depth)
            if res_target:
                result_target = result_target | set(res_target)
                position_target.update(pos_target)

        for so in search_objects:
            position_target[so] = (0, 0)

        update_y_position(position_target)
        print(position_target)

        result_to_csv(result_target)

        show_dataflow2(search_objects, result_target, position_target)

    if args.find:
        search_objects = list(map(lambda x: x.lower(), args.find))

        INDEX = open_index()

        with open('idx', 'w') as f:
            f.write(str(INDEX))

        result_source = set()
        position_source = dict()
        result_target = set()
        position_target = dict()
        pos = dict()

        for so in search_objects:
            res_source, pos_source = find_source_path(INDEX, so, exclude_source, depth=search_depth)
            if res_source:
                result_source = result_source | set(res_source)
                position_source.update(pos_source)

            res_target, pos_target = find_target_path(INDEX, so, depth=search_depth)
            if res_target:
                result_target = result_target | set(res_target)
                position_target.update(pos_target)

        result = result_source | result_target

        pos = {**pos_source, **pos_target}

        for so in search_objects:
            pos[so] = (0, 0)

        update_y_position(pos)
        print(pos)

        result_to_csv(result)

        show_dataflow2(search_objects, result, pos)


if __name__ == '__main__':
    main()
