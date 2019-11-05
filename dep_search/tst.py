
import numpy as np
from datetime import timedelta
import sys
import matplotlib.pyplot as plt
import networkx as nx
import re
import pickle
import matplotlib as mpl
import time
import datetime
from collections import namedtuple
import os

table = namedtuple("Table", ['name', 'schemas'])

t_target = table('3', ('2', '1'))
t_source = table('1', ('2', '3'))

print(t_target)
print(t_source)

d = {}


class myTable(object):
    name = ''
    schemas = []

    def __init__(self, name, schemas):
        self.name = name
        self.schemas.append(schemas)


mt = myTable('1',  ['2', '3'])

s = {1, 2, 3}

s.pop()

# d[mt] = 0
# print(d)

# mt.schemas = ['', '']
d[t_target] = [t_source]
lol = d.get(t_target)[0].name

print(d)
print(lol)


def process_prefix_postfix(object_name):
    dot = object_name.find('.') + 1
    if object_name.find('@') > -1:
        return object_name[dot:object_name.find('@')]
    else:
        return object_name[dot:]


l = [
    'asd.lol@pup', 'asd.lol', 'lol', 'lol@pop'
]

# for i in l:
#    print(i,process_prefix_postfix(i), sep=' -> ')

path = r''

s = 'use (f);'

print(len(s))

print(os.path.basename(os.path.dirname(path)))

# import re

# schema_re = re.compile('use@([0-9_a-zA-Z]*);?', re.DOTALL | re.MULTILINE)

# res = schema_re.findall(s).pop()
# print(res)
# if len(res) > 0:
#     schema_name = res


# def draw_graph(graph):
#     # create networkx graph
#     G = nx.Graph()

#     # add edges
#     for edge in graph:
#         G.add_edge(edge[0], edge[1])

#     # There are graph layouts like shell, spring, spectral and random.
#     # Shell layout usually looks better, so we're choosing it.
#     # I will show some examples later of other layouts
#     graph_pos = nx.spring_layout(G)

#     # draw nodes, edges and labels
#     nx.draw_networkx_nodes(G, graph_pos, node_size=1000,
#                            node_color='blue', alpha=0.3)
#     nx.draw_networkx_edges(G, graph_pos)
#     nx.draw_networkx_labels(G, graph_pos, font_size=12,
#                             font_family='sans-serif')

#     # show graph
#     plt.show()


# # draw example
# # graph is a list of tuples of nodes. Each tuple defining the
# # connection between 2 nodes
# graph = [(20, 21), (21, 22), (22, 23), (23, 24), (24, 25), (25, 20)]

# draw_graph(graph)

# [DWH specific]
def merge_equal_tables(t_name):
    equal_prefix = ['c_', 'd_', 'ld_']
    if any(map(t_name.startswith, equal_prefix)):
        return 't' + t_name[t_name.find('_'):]
    return t_name


l = ['c_wt_acc', 'd_lol', 'ld_ld_acc', 't_t_acc', 'asd']
for i in l:
    print(i, merge_equal_tables(i), sep=' -> ')


res_source = [('A','B'), ('B','C')]

print()

g = nx.DiGraph(directed=True)

g.add_edges_from(res_source)
# g.add_edges_from(res_target)


# nx.draw_networkx_nodes(g, graph_pos, node_size=1000, node_color='blue', alpha=0.3)
# nx.draw_networkx_edges(g, graph_pos)
# nx.draw_networkx_labels(g, graph_pos, font_size=10, font_family='sans-serif')


nx.draw(g, with_labels=True, arrows=True, alpha=0.5,
        font_size=10, node_shape='o', node_size=100)
plt.draw()
plt.show()
