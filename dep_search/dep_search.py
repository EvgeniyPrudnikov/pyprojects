import os
import argparse
import pickle
from collections import namedtuple
import re
import traceback
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch
from matplotlib.text import Annotation
import numpy as np
import time
import csv
from datetime import timedelta
import tkinter as tk

trg_obj_props = namedtuple('trg_obj_props', ['schemas', 'sources'])

EXCLUDE_DIR_NAMES = {'dev_dw', 'dev_pr', 'tables', 'sequences', 'functions', 'synonym',
                     'types', 'application', 'queries', 'scripts', 'json', 'dev_kiev', 'dev_kiev_pr', 'deploy',
                     'hive_metastore', 'kafka', 'reports', 'triggers', 'additional_tools', 'oemm', 'snowflake', 'tidal', 'oracle_dwmeta'}
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']


class IndexStorage:

    def __init__(self, path_to_root='', ):
        self.path_to_root = path_to_root
        self.index = {}
        self._init_re()

    def _init_re(self):
        flags = re.DOTALL | re.MULTILINE
        sql_reg = {
            'schema_re': r'use@([0-9_a-zA-Z]*);?',
            'trg_re': r'@?(insert|merge)@?(.*(@table|@into))@([\(\)\._a-zA-Z0-9]+?)[@|(]',
            'trg_view_re': r'create([or@replace@force]*)@view@([\.\$_a-zA-Z0-9]+?)@',
            'src_re': r'@(from|inner@join|left@join|right@join|full@join|cross@join|join)@([\(\)\.\$\_a-zA-Z0-9]+?)@',
            'src_with_catch': r'@?(with|,)@([_a-zA-Z0-9]+?)@as@\('
        }

        self.SQL_REG = {k: re.compile(v, flags) for k, v in sql_reg.items()}

    def result_to_csv(self, output, file_name='result.csv'):
        with open(file_name, 'w+') as f:
            writer = csv.writer(f, dialect='excel', delimiter=',', lineterminator='\n', escapechar='\\')
            writer.writerows(output)

    def _clear_data(self, text):
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

    def _merge_equal_tables(self, t_name):
        equal_prefix = ['c_', 'd_', 'ld_']
        if any(map(t_name.startswith, equal_prefix)):
            return 't' + t_name[t_name.find('_'):]
        return t_name

    def _process_prefix_postfix(self, object_name):
        dot = object_name.find('.') + 1
        if object_name.find('@') > -1:
            return object_name[dot:object_name.find('@')]
        else:
            return object_name[dot:]

    def _process_file(self, file_path, schema_name):
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
            cl_data = self.clear_data(stm)

            if len(cl_data) <= 1:
                continue

            if len(cl_data) > 1 and not any(map(cl_data.startswith, starts)):
                continue

            cl_data = '@'.join(cl_data.split())

            if cl_data.startswith('use'):
                try:
                    res = self.SQL_REG['schema_re'].findall(cl_data).pop()
                    if len(res) > 0:
                        schema_name = res
                    continue
                except IndexError:
                    continue

            l_trg_objects = self.SQL_REG['trg_re'].findall(cl_data)
            if l_trg_objects:
                trg_object = l_trg_objects[0][3].strip().lower()
            else:
                l_trg_objects = self.SQL_REG['trg_view_re'].findall(cl_data)
                if l_trg_objects:
                    trg_object = l_trg_objects[0][1].strip().lower()
                else:
                    continue

            if schema_name != 'jenkins':
                trg_object = self._process_prefix_postfix(trg_object)
            elif schema_name in ('jenkins', 'hive_sql', 'impala_sql'):
                schema_name = trg_object[:trg_object.find('.')]
                trg_object = self._process_prefix_postfix(trg_object)

            trg_object = self._merge_equal_tables(trg_object)

            if not trg_object:
                continue

            src_objects = self.SQL_REG['src_re'].findall(cl_data)
            with_objects = tuple([item[1].strip().lower() for item in self.SQL_REG['src_with_catch'].findall(cl_data)])

            s_sources = set()
            for src in src_objects:
                val = self._merge_equal_tables(self._process_prefix_postfix(src[1].strip(' ();').lower()))
                if val and 'select' not in val and 'dual' not in val and val != trg_object and val not in with_objects:
                    s_sources.add(val)

            self._add_to_index(ind_part, {trg_object: trg_obj_props(schemas={schema_name}, sources=s_sources)})

        f.close()

        return ind_part

    def _add_to_index(self, index, ind_part):
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

    def create_index(self, root_dir_path, exclude_dir_names=[]):
        start = time.time()
        for path, subdirs, files in os.walk(root_dir_path):
            subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
            if not files:
                continue
            cnt = 0
            for f in files:
                if f[f.rfind('.'):] in ACCEPTED_FILES_TYPES:
                    ind_part = self._process_file(os.path.join(path, f), os.path.basename(os.path.dirname(path)))
                    self._add_to_index(self.index, ind_part)
                    cnt += 1
            print('{0} - {1} files processed.'.format(path, cnt))

        self.index['METADATA'] = {'objects_num': len(self.index), 'last_update_date': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}

        print(self.index['METADATA'])

        with open('index.pkl', 'wb') as pkl:
            pickle.dump(self.index, pkl)

        end = time.time()
        print('\nElapsed {0} s\n'.format(str(timedelta(seconds=end - start))))

    def get_source_path(self, search_objects, exclude_source, depth=999):
        result_source = set()
        position_source = dict()

        for so in search_objects:
            res_source, pos_source = self._find_source_path(so, exclude_source, depth=depth) or (None, None)
            if res_source:
                result_source = result_source | set(res_source)
                position_source.update(pos_source)

        for so in search_objects:
            position_source[so] = (0, 0)

        self._update_y_position(position_source)
        vertexs = position_source
        edges = [(position_source[e[0]], position_source[e[1]]) for e in result_source]

        return PointsList(vertexs, edges)

    def _find_source_path(self, search_object, exclude_source, depth=999, x=-1, res=[], seen=[], pos={}):

        try:
            src_objs = sorted(list(self.index[search_object].sources))
        except KeyError:
            return

        if len(src_objs) == 0 or abs(x) > depth or search_object in exclude_source:
            return

        for o in src_objs:
            if o not in seen:
                res.append((o, search_object,))
                seen.append(o)
                pos[o] = (x, 0,)
                self._find_source_path(o, exclude_source, depth, x - 1, res, seen, pos)
            else:
                res.append((o, search_object,))
                pos[o] = (x, 0,)

        return res, pos

    def get_target_path(self, search_objects, depth=999):
        result_target = set()
        position_target = dict()

        for so in search_objects:
            res_target, pos_target = self._find_target_path(so, depth=depth) or (None, None)
            if res_target:
                result_target = result_target | set(res_target)
                position_target.update(pos_target)

        for so in search_objects:
            position_target[so] = (0, 0)

        self._update_y_position(position_target)

        vertexs = position_target
        edges = [(position_target[e[0]], position_target[e[1]]) for e in result_target]

        return PointsList(vertexs, edges)

    def _find_target_path(self, search_object, depth, x=1, res=[], seen=[], pos={}):

        trgs = []

        for k, v in self.index.items():
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
                self._find_target_path(t, depth, x + 1, res, seen, pos)
            else:
                res.append((search_object, t,))
                # pos[t] = (x, 0,)
        return res, pos

    def open_index(self):

        if self.index:
            return

        try:
            with open('index.pkl', 'rb') as pkl:
                self.index = pickle.load(pkl)
        except FileNotFoundError:
            print('index.pkl file not found.')
            print('Please create index using --create_index PATH_TO_SVN_TRUNC_FOLDER flag')
            exit(1)
        except Exception:
            print(traceback.format_exc())
            exit(1)

        print('INDEX:{0}\n'.format(self.index['METADATA']))

    def swap_index(self):
        with open('index.csv', 'w+') as f:
            cw = csv.writer(f, delimiter=',', lineterminator='\n')
            for k, v in self.index.items():
                if k == 'METADATA':
                    continue
                for vi in v.sources:
                    cw.writerow([vi, k])

    def get_vertexs_len(self, output):
        vertexs = set()

        for i in output:
            vertexs.add(i[0])
            vertexs.add(i[1])

        return len(vertexs)

    def _update_y_position(self, pos):
        points_num_per_x_ax = {}
        new_coord_for_x_ax = {}
        for x in pos.values():
            if x[0] not in points_num_per_x_ax:
                points_num_per_x_ax[x[0]] = 1
            else:
                points_num_per_x_ax[x[0]] += 1

        for j in points_num_per_x_ax:
            new_coord_for_x_ax[j] = [i + 0.20 for i in range(points_num_per_x_ax[j])]

        for j in points_num_per_x_ax:
            points_num_per_x_ax[j] = points_num_per_x_ax[j] // 2

        for p in pos.keys():
            pnt = pos[p]
            pos[p] = (pnt[0], new_coord_for_x_ax[pnt[0]].pop() - points_num_per_x_ax[pnt[0]])


class Point():
    def __init__(self, name, position):
        self.name = name
        self.xy = position

    def __str__(self):
        return '{0} {1}'.format(self.name, self.xy)


class PointsList():

    def __init__(self, points, edges):
        self.points = {i: Point(v[0], v[1]) for i, v in enumerate(points.items())}
        self.points_len = len(points.keys())
        self.x_y_arr = np.array(list(points.values()))
        self.edges_to_points = {}
        self.edges = edges
        self._connect_edges_to_points()

    def __add__(self, other):
        vert = self.points.update(other.points)
        ed = self.edges + other.edges
        print(vert)
        print(ed)
        return PointsList(vert, ed)

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)

    def add_eadges(self, ed):
        self.edges.append(ed)

    def get_x_y_arr(self):
        return self.x_y_arr[:, 0], self.x_y_arr[:, 1]

    def get_dep_edges(self, idx):
        return self.edges_to_points[idx]

    def set_point_by_id(self, idx, xy):
        self.points[idx].xy = xy
        self.x_y_arr[idx] = xy

    def get_point_idx(self, coord):
        idx, p = min(self.points.items(), key=lambda p: abs(p[1].xy[0] - coord[0]) + abs(p[1].xy[1] - coord[1]))
        return idx, p

    def _connect_edges_to_points(self):

        for pk, pv in self.points.items():
            self.edges_to_points[pk] = {}
            for i, edge in enumerate(self.edges):
                if edge[0] == pv.xy:
                    self.edges_to_points[pk][i] = 0
                if edge[1] == pv.xy:
                    self.edges_to_points[pk][i] = 1


class DetailedPlot():
    def __init__(self, obj_name):
        self.obj_name = obj_name
        self.root = tk.Tk()

    def show(self):

        self.root.title('Table info for {0}'.format(self.obj_name))
        txt = tk.Text(self.root)
        txt.pack()
        txt.insert(tk.END, "TBD\ntable info for {0}".format(self.obj_name))
        txt.config(state=tk.DISABLED)
        tk.mainloop()


class PlotView():

    lock = None

    def __init__(self, plot_presenter):
        self.plot_presenter = plot_presenter
        fig = plt.figure()
        self.ax = fig.add_subplot(111)
        self.ax.axes.get_xaxis().set_ticks([])
        self.ax.axes.get_yaxis().set_ticks([])
        self.press = None
        self.background = None
        self.moving_point_idx = -1

    def _create_plot(self):
        self.plot_presenter.transform_data()
        self.line, = self.ax.plot(self.plot_presenter.points_list.get_x_y_arr()[0], self.plot_presenter.points_list.get_x_y_arr()[1], 's', ms=16)

        for ar in self.plot_presenter.arrows:
            self.ax.add_patch(ar)

        for an in self.plot_presenter.annotations:
            self.ax.add_artist(an)

    def _connect(self):
        self.cidpress = self.line.figure.canvas.mpl_connect('button_press_event', self._on_press)
        self.cidmotion = self.line.figure.canvas.mpl_connect('motion_notify_event', self._on_motion)
        self.cidrelease = self.line.figure.canvas.mpl_connect('button_release_event', self._on_release)

    def show(self):
        self._create_plot()
        if self.plot_presenter.points_list.points_len <= 100:
            self._connect()
            self.ax.set_title('Interactable Data Flow for {0}'.format(*self.plot_presenter.search_objects))
        else:
            self.ax.set_title('NonInteractable Data Flow for {0}'.format(*self.plot_presenter.search_objects))

        plt.show()

    def _on_pick(self, obj_name):
        dp = DetailedPlot(obj_name)
        dp.show()

    def _on_press(self, event):

        if event.inaxes != self.line.axes:
            return
        if PlotView.lock is not None:
            return
        if PlotView.lock is self:
            return
        contains, attrd = self.line.contains(event)
        if not contains:
            return

        PlotView.lock = self

        try:
            self.moving_point_idx, p = self.plot_presenter.points_list.get_point_idx((event.xdata, event.ydata))
        except Exception as e:
            print(e)
            self.moving_point_idx = -1
            self._on_release(event)
            return

        if event.dblclick:
            self._on_pick(self.plot_presenter.annotations[self.moving_point_idx].get_text())
            return

        self.dep_lines = [self.plot_presenter.arrows[k] for k, _ in self.plot_presenter.points_list.get_dep_edges(self.moving_point_idx).items()]
        self.press = p.xy[0], p.xy[1], event.xdata, event.ydata

        canvas = self.line.figure.canvas
        canvases = []
        for dep_line in self.dep_lines:
            canvases.append(dep_line.figure.canvas)

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)

        axes_an = self.plot_presenter.annotations[self.moving_point_idx].axes

        self.line.set_animated(True)
        for dep_line in self.dep_lines:
            dep_line.set_animated(True)

        self.plot_presenter.annotations[self.moving_point_idx].set_animated(True)
        canvas.draw()

        self.background = canvas.copy_from_bbox(self.line.axes.bbox)
        axes.draw_artist(self.line)
        for i, dep_line in enumerate(self.dep_lines):
            axeses[i].draw_artist(dep_line)
        axes_an.draw_artist(self.plot_presenter.annotations[self.moving_point_idx])

        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes_an.bbox)

    def _on_motion(self, event):

        if PlotView.lock is not self:
            return
        if event.inaxes != self.line.axes:
            return
        try:
            x0, y0, xpress, ypress = self.press
        except Exception as e:
            print(e)

        dx = event.xdata - xpress
        dy = event.ydata - ypress
        self.plot_presenter.points_list.set_point_by_id(self.moving_point_idx, ((x0 + dx), (y0 + dy)))

        self.line.set_xdata(self.plot_presenter.points_list.get_x_y_arr()[0])
        self.line.set_ydata(self.plot_presenter.points_list.get_x_y_arr()[1])
        self.plot_presenter.annotations[self.moving_point_idx].xy = ((x0 + dx), (y0 + dy))

        for k, v in self.plot_presenter.points_list.get_dep_edges(self.moving_point_idx).items():
            ar = self.plot_presenter.arrows[k]
            psab = ar._posA_posB
            psab[v] = ((x0 + dx), (y0 + dy))
            ar.set_positions(*psab)

        canvas = self.line.figure.canvas
        canvases = []

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)
        axes2 = self.plot_presenter.annotations[self.moving_point_idx].axes

        canvas.restore_region(self.background)

        axes.draw_artist(self.line)
        for i, dl in enumerate(self.dep_lines):
            axeses[i].draw_artist(dl)
        axes2.draw_artist(self.plot_presenter.annotations[self.moving_point_idx])

        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes2.bbox)

    def _on_release(self, event):
        'on release we reset the press data'
        if PlotView.lock is not self:
            return

        self.press = None

        self.line.set_animated(False)
        for dl in self.dep_lines:
            dl.set_animated(False)
        self.plot_presenter.annotations[self.moving_point_idx].set_animated(False)
        self.background = None

        self.line.figure.canvas.draw()
        for dl in self.dep_lines:
            dl.figure.canvas.draw()
        self.plot_presenter.annotations[self.moving_point_idx].figure.canvas.draw()

        PlotView.lock = None


class PlotPresenter():

    def __init__(self, search_objects, exclude_sources, depth, idx, cmd):
        self.search_objects = search_objects
        self.cmd = cmd
        self.exclude_sources = exclude_sources
        self.depth = depth
        self.idx = idx
        self.points_list = None
        self.arrows = []
        self.annotations = []

    def transform_data(self):

        if self.cmd == 'find_source':
            self.points_list = self.idx.get_source_path(self.search_objects, self.exclude_sources, self.depth)
        elif self.cmd == 'find_target':
            self.points_list = self.idx.get_target_path(self.search_objects, self.depth)
        elif self.cmd == 'find':
            pl1 = self.idx.get_source_path(self.search_objects, self.exclude_sources, self.depth)
            pl2 = self.idx.get_target_path(self.search_objects, self.depth)
            self.points_list = pl1 + pl2
        else:
            print('lol')
            exit(1)

        for ex in self.points_list.edges:
            arrow = FancyArrowPatch(ex[0], ex[1],
                                    arrowstyle='-|>',
                                    shrinkA=11,
                                    shrinkB=11,
                                    mutation_scale=10,
                                    color='black',
                                    linewidth=0.9,
                                    connectionstyle='arc3,rad=0.02',
                                    zorder=1)

            self.arrows.append(arrow)

        for point in self.points_list.points.values():
            if point.name in self.search_objects:
                self.annotations.append(Annotation(point.name, point.xy, textcoords="offset points", xytext=(0, 11), ha='center', color='red', annotation_clip=True))
            else:
                self.annotations.append(Annotation(point.name,  # this is the text
                                                   point.xy,  # this is the point to label
                                                   textcoords="offset points",  # how to position the text
                                                   xytext=(0, 11),  # distance from text to points (x,y)
                                                   ha='center',  # horizontal alignment can be left, right or center
                                                   annotation_clip=True
                                                   ))


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

    idx = IndexStorage()
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
            idx.create_index(root_dir_path, EXCLUDE_DIR_NAMES)
        else:
            print('Path "{0}" is not a directory'.format(root_dir_path))

    if args.swap_index:
        idx.swap_index()
        return

    if args.find_source:
        search_objects = list(map(lambda x: x.lower(), args.find_source))
        cmd = 'find_source'
    if args.find_target:
        search_objects = list(map(lambda x: x.lower(), args.find_target))
        cmd = 'find_target'
    if args.find:
        search_objects = list(map(lambda x: x.lower(), args.find))
        cmd = 'find'

    idx.open_index()
    plt_press = PlotPresenter(search_objects, exclude_source, search_depth, idx, cmd)
    plt_view = PlotView(plt_press)
    plt_view.show()


if __name__ == '__main__':
    main()
