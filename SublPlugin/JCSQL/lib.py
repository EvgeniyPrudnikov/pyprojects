# Oracle/IMPALA utilities functions
from random import randint
import os
import csv
from datetime import datetime
import time
import string
from decimal import Decimal
try:
    import wx
    import wx.dataview
    import numpy as np
except:
    pass

RAND_MAX = 999999999999999
# SELECT 'client_id = ' || SYS_CONTEXT('userenv', 'client_identifier') AS client_identifier FROM dual;
SQLPLUS_DEFAULT_PARAMS = '''
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
SET SERVEROUTPUT ON;
SET TAB OFF;
SET PAGESIZE 0;
SET SQLBLANKLINES ON;
SET TIMING ON;
SET ECHO ON;
'''

def get_schema_pass(schema_name, pass_file_name, tool):

    password = ''
    host = ''
    s = 'oracle {0}'.format(schema_name) if schema_name.find('@') < 0 else 'impala {0}'.format(schema_name)

    try:
        with open(pass_file_name, 'r') as pass_file:
            for line in pass_file:
                if line.startswith(s):
                    password = line[len(s) + 3:].strip(' \n\t')
                    break
    except:
        pass

    if s.startswith('oracle') and tool == 'sqlplus':
        return 'js/js@orcl'.format(schema_name, password)
    elif s.startswith('oracle'):
        return 'DSN=oracle_odbc; UID=js; PWD=js'.format(UID=schema_name, PWD=password)

    # return 'DRIVER={0};HOST={1};PORT={2};AuthMech=3;SSL=0;UID={3};PWD={4};'.format('{Cloudera ODBC Driver for Impala}',host, 999, schema_name, password.strip(' \n\t'))
    return 'DSN=impala_odbc'


def get_text(view):
    selected_text = ''
    if view.sel():
        for region in view.sel():
            if region.empty():
                selected_text += view.substr(view.line(region))
                view.sel().add(view.line(region))
            else:
                selected_text += view.substr(region)

    if not selected_text:
        return ''

    text = selected_text.strip('; \t\n\r')
    text = ''.join([t for t in text if t in string.printable])
    return text


def prepare_query(query):

    tool = 'sqlplus'
    client_id = str(randint(0, RAND_MAX))

    is_query_dml = query.lower().startswith('select') or query.lower().startswith('with')

    if is_query_dml:
        tool = 'exec_cx.py'
    else:
        # CLIENT_ID_PARAM = "EXEC dbms_session.set_identifier('{0}');".format(client_id)
        CLIENT_ID_PARAM = ''
        if query.endswith('/'):
            query = '{0}{1}{2}\n{3}'.format(CLIENT_ID_PARAM, SQLPLUS_DEFAULT_PARAMS, query, 'exit')
        else:
            query = '{0}{1}{2};\n{3}'.format(CLIENT_ID_PARAM, SQLPLUS_DEFAULT_PARAMS, query, 'exit')

    return tool, query


def prepare_explain(query):

    LINE_SIZE_PARAM = '''SET LINESIZE 1000;'''
    tool = 'sqlplus'
    query = "{0}{1}EXPLAIN PLAN FOR\n{2};\nSELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);".format(LINE_SIZE_PARAM, SQLPLUS_DEFAULT_PARAMS, query.strip(';'))

    return tool, query


def prepare_impala_query(query):

    tool = 'exec_impl.py'

    return tool, query


def prepare_impala_explain(query):

    EXPLAIN_PARAM = '''EXPLAIN '''
    tool = 'exec_impl.py'
    query = '{0}{1}'.format(EXPLAIN_PARAM, query.strip(';'))

    return tool, query


def prepare_query_file (view, flag):

    sel_text = get_text(view)
    if not sel_text:
        print('nothing selected')
        return

    if flag == 'oracle_query':
        tool, cl_query = prepare_query(sel_text)
    elif flag == 'oracle_explain':
        tool, cl_query = prepare_explain(sel_text)
    elif flag == 'impala_query':
        tool, cl_query = prepare_impala_query(sel_text)
    elif flag == 'impala_explain':
        tool, cl_query = prepare_impala_explain(sel_text)

    tmp_file_name = '{path}\\tmp\\tmp_{tool}_dt_{dt}.sql'.format(path=os.path.dirname(__file__), tool=tool[:tool.find('.')], dt=time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()))

    try:
        with open(tmp_file_name, 'w+') as tf:
            tf.write(cl_query)
    except Exception as e:
        print(e)

    return tool, tmp_file_name


def pretty_print_result(output):

    l_output = np.array(output)
    to_str = np.vectorize(str)
    get_length = np.vectorize(len)
    max_col_length = np.amax(get_length(to_str(l_output)), axis=0)

    # print result
    print('+' + ''.join(['-' * x + '--+' for x in max_col_length]))
    for row_index, row in enumerate(l_output):
        print('|' + ''.join([' ' + str(value).replace('None', 'NULL') + ' ' * (max_col_length[index] - len(str(value))) + ' |' for index, value in enumerate(row)]))
        if row_index == 0 or row_index == len(l_output) - 1:
            print('+' + ''.join(['-' * x + '--+' for x in max_col_length]))
    print('\nFetched {0} rows'.format(np.size(l_output, 0) - 1))


def result_to_csv(file_path, headers, dataset):
    file_path = file_path + '.csv' if not file_path.endswith('.csv') else file_path
    output_file = open(file_path, 'w', newline='', encoding='utf-8')
    writer = csv.writer(output_file, dialect='excel', delimiter=',', lineterminator='\n', quoting=csv.QUOTE_ALL, escapechar='\\')
    try:
        writer.writerow(headers)
        writer.writerows(dataset)
    except Exception as e:
        print(e)
    output_file.close()


def show_result_in_gui(res_cur):
    headers = tuple([i[0].lower() for i in res_cur.description])

    def ask_file_path():
        with wx.FileDialog(frame, "Save csv file", wildcard="CSV files (*.csv)|*.csv",
                           style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT) as fileDialog:
            if fileDialog.ShowModal() == wx.ID_CANCEL:
                return ''
            return fileDialog.GetPath()

    def save_to_csv(event):
        file_path = ask_file_path()
        if file_path:
            update_table(-1)
            result_to_csv(file_path, headers, data_store)
            dialog = wx.MessageDialog(frame, 'Done.')
            dialog.ShowModal()
        event.Skip()


    def on_char_event(event):
        if event.GetKeyCode() == wx.WXK_END:
            update_table(-1)
        event.Skip()

    def show_popup_menu(event):
        popupID1 = wx.NewId()
        menu = wx.Menu()
        item = wx.MenuItem(menu, popupID1, "Save to csv")
        menu.Append(item)
        menu.Bind(wx.EVT_MENU, save_to_csv, id=popupID1)
        wx.Window.PopupMenu(frame, menu)
        menu.Destroy()
        event.Skip()

    def scroll_event_handler(event):
        r = data_view.GetScrollRange(wx.VERTICAL)
        s = data_view.GetScrollThumb(wx.VERTICAL)
        pos = data_view.GetScrollPos(wx.VERTICAL)
        if r - s == pos:
            update_table()
        event.Skip()

    def update_table(fetch_num=50):
        if fetch_num == -1:
            result = res_cur.fetchall()
        else:
            result = res_cur.fetchmany(fetch_num)

        for row in result:
            row = tuple([str(r).replace('None', 'NULL') if type(r) == datetime or type(r) == Decimal or r is None else r for r in row])
            data_view.AppendItem(row)
            data_store.append(row)

    app = wx.App(False)
    width, height = wx.GetDisplaySize()
    frame = wx.Frame(None, wx.ID_ANY, "result", size=wx.Size(width * 0.8, height * 0.50), pos=(width * 0.10, height * 0.4))

    data_view = wx.dataview.DataViewListCtrl(frame, wx.ID_ANY, style=wx.dataview.DV_MULTIPLE | wx.dataview.DV_ROW_LINES)
    data_store = []

    for header_name in headers:
        data_view.AppendTextColumn(header_name, width=wx.COL_WIDTH_AUTOSIZE, flags=wx.dataview.DATAVIEW_COL_SORTABLE | wx.dataview.DATAVIEW_COL_RESIZABLE | wx.dataview.DATAVIEW_COL_REORDERABLE)

    data_view.Bind(wx.EVT_SCROLLWIN, scroll_event_handler)
    data_view.Bind(wx.dataview.EVT_DATAVIEW_COLUMN_HEADER_RIGHT_CLICK, show_popup_menu)
    data_view.Bind(wx.EVT_CHAR, on_char_event)

    update_table()

    frame.Show(True)
    app.MainLoop()