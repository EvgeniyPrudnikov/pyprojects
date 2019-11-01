
from collections import namedtuple

table = namedtuple("Table" , ['name', 'schemas'])

t_target = table('3', ('2', '1'))
t_source = table('1', ('2', '3'))

print(t_target)
print(t_source)

d = {}

class myTable(object):
    name = ''
    schemas = []
    def __init__(self, name, schemas ):
        self.name = name
        self.schemas.append(schemas)

mt = myTable('1',  ['2', '3'])

s = {1,2,3}

s.pop()

# d[mt] = 0
# print(d)

# mt.schemas = ['src_wot_ru', 'src_wot_eu']
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
    'asd.lol@pup'
    , 'asd.lol'
    , 'lol'
    , 'lol@pop'
]

#for i in l:
#    print(i,process_prefix_postfix(i), sep=' -> ')