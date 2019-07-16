import os
import pickle
import re

EXCLUDE_DIR_NAMES = ['scripts', 'json', 'dev_kiev', 'dev_kiev_pr']
ACCEPTED_FILES_TYPES = ['.pkb', '.sql']

trg_re = re.compile('@?insert@(.*?)@?into@([_a-zA-Z0-9]+?)[@|(]', re.DOTALL | re.MULTILINE | re.IGNORECASE)
src_re = re.compile('@(from|inner@join|left@join|right@join|full@join|cross@join|join)@([_a-zA-Z0-9]+?)@', re.DOTALL | re.MULTILINE | re.IGNORECASE)
src_with_catch = re.compile('@?(with|,)@([_a-zA-Z0-9]+?)@as@\(', re.DOTALL | re.MULTILINE | re.IGNORECASE)


a = {'Python': '.py', 'C++': '.cpp', 'Java': '.java'}

with open('filename.pickle', 'wb') as handle:
    pickle.dump(a, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('filename.pickle', 'rb') as handle:
    b = pickle.load(handle)

lol_path = r''


def clear_data(text):
    # lines clearing
    text_lines = [line for line in text.split('\n') if line.strip() and not line.strip().startswith('--')]
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


def process_prefix(object_name):
    return object_name[object_name.find('.') + 1:]


def process_file(file_path):
    ind_part = {}

    f = open(file_path, 'r')
    try:
        data = f.read()
    except Exception as e:
        print(e)
        print(file_path + '-- broken')
        return

    for stm in data.split(';'):
        stm = stm.strip().lower()
        if not (stm.startswith('insert') or stm.startswith('merge')):
            continue
        cl_data = clear_data(stm)
        cl_data = '@'.join(cl_data.split())
        trg_object = None
        try:
            trg_object = trg_re.findall(cl_data)[0][1].strip().lower()
        except:
            pass
        if trg_object:
            src_objects = src_re.findall(cl_data)
            with_objects = tuple([item[1].strip().lower() for item in src_with_catch.findall(cl_data)])

            l_sources = [src[1].strip(' ()').lower() for src in src_objects if src[1].strip(' ()') \
                         and 'select' not in src[1].strip(' ()').lower() \
                         and 'dual' not in src[1].strip(' ()').lower() \
                         and src[1].strip(' ()').lower() != trg_object \
                         and src[1].strip(' ()').lower() not in with_objects]
            s_sources = set(l_sources)
            t_sources = tuple([process_prefix(s) for s in s_sources])

            ind_part[trg_object] = t_sources

    print(ind_part)
    f.close()

    return ind_part


root_dir_path = r''


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
                res_val = tuple(set(index[k] + ind_part[k]))
                index[k] = res_val

    # print(index)


def create_index(root_dir_path, exclude_dir_names=None):
    index = {}
    for path, subdirs, files in os.walk(root_dir_path):
        subdirs[:] = [d for d in subdirs if d not in exclude_dir_names]
        if not files:
            continue
        print('{0} - {1} - {2}'.format(path, os.path.basename(os.path.dirname(path)), files))
        for f in files:
            if not f.endswith('.pks'):
                # print(os.path.join(path, f))
                ind_part = process_file(os.path.join(path, f))
                add_to_index(index, ind_part)

    print(index)


# create_index(root_dir_path, EXCLUDE_DIR_NAMES)

# process_file(lol_path)