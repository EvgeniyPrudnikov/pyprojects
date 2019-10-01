import sublime, sublime_plugin
import os
import subprocess
import threading
import time
import sys
from random import randint
import JCSQL.lib as lib

SETTINGS_FILE_NAME = 'JCSQL.sublime-settings'
RAND_MAX = 999999999999999


class ExecThreadCommand(sublime_plugin.WindowCommand):

    def run(self, schema_name="", is_full_res="", tool="", qtype="", tmp_file_name="", **kwargs):

        settings = sublime.load_settings(SETTINGS_FILE_NAME)
        client_id = str(randint(0, RAND_MAX))

        dsn = lib.get_schema_pass(schema_name, settings.get("pass_file_full_path"), tool)
        output_view = self.create_view(schema_name if schema_name.find('@') < 0 else 'impala')

        cmd = []
        if tool == 'sqlplus':
            cmd = [tool,'-L', dsn, '@', tmp_file_name]
        elif tool == 'exec_cx.py':
            cmd = ['python', settings.get("orcl_query_exec_py_path"), tmp_file_name, dsn, client_id, is_full_res]
        elif tool == 'exec_impl.py':
            cmd = ['python', settings.get("impl_query_exec_py_path"), tmp_file_name, dsn, qtype, is_full_res]

        self.exec_thread = ExecThread(cmd, output_view, tmp_file_name)
        self.exec_thread.daemon = True
        self.exec_thread.start()


    def create_view(self, view_name):

        self.window.set_layout({
            "cols": [0.0, 1.0],
            "rows": [0.0, 0.5, 1.0],
            "cells": [[0, 0, 1, 1], [0, 1, 1, 2]]
            })

        result_focus_group = 1      # Focus in output group
        self.window.focus_group(result_focus_group)

        new_view_index = len(self.window.views_in_group(result_focus_group))
        view = self.window.new_file()   # New view in group
        self.window.set_view_index(view, result_focus_group, new_view_index)

        view.settings().set("line_numbers", True)
        view.settings().set("gutter", True)
        view.settings().set("word_wrap", False)
        view.settings().set("scroll_past_end", False)
        view.set_name(view_name + '_' + str(new_view_index))
        view.set_read_only(True)

        self.window.focus_group(0) # Focus in main group

        return view


class ExecThread(threading.Thread):
    def __init__(self, cmd, view, tmp_file_name):
        self.cmd = cmd
        self.view = view
        self.tmp_file_name = tmp_file_name
        threading.Thread.__init__(self)


    def run(self):
        try:

            CREATE_NO_WINDOW = 0x08000000 # hide cmd window
            popen = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, creationflags=CREATE_NO_WINDOW)
            check_thread = threading.Thread(target = self.check_view_proc, args = (self.view, popen, ))
            check_thread.start()

            stdout_lines = iter(popen.stdout.readline, b'')
            for stdout_line in stdout_lines:
                ok = self.append_data('{0}\n'.format(stdout_line.decode('cp437').strip('\r\n')))
                if not ok:
                    break
            popen.stdout.close()

        except Exception as e:
            print(str(e))
        finally:
            os.remove(self.tmp_file_name)


    def append_data(self, data):

        self.view.set_read_only(False)
        self.view.run_command('append', {'characters': data})
        self.view.set_read_only(True)

        data_len = len(data)
        view_size = self.view.size()
        if data.startswith('ORA-') or data.startswith('SP2-') or data.startswith('PL/SQL:') or data.startswith('ERROR') or data.startswith('PLS-') or '[Oracle][ODBC][Ora]' in data:
            self.view.sel().add(self.view.line(sublime.Region(view_size - data_len, view_size)))
            try:
                self.view.run_command("token_style", {"style_index" : 0})
            except:
                pass

        self.view.run_command('move_to', {'to': 'eof'})

        return True


    def check_view_proc(self, view, popen):
        while True:
            if not view.name():
                popen.terminate()
                sys.exit(1)
            time.sleep(5)

    def flash_window():
        pass
        # from ctypes import *
        # import win32con
        # import win32gui as w
        # cur_window = w.GetForegroundWindow() #just get the handler/ID for the current window

        # class FLASHWINFO(Structure):
        #         _fields_ = [('cbSize', c_uint),
        #                 ('hwnd', c_uint),
        #                 ('dwFlags', c_uint),
        #                 ('uCount', c_uint),
        #                 ('dwTimeout', c_uint)]

        # '''Flash a window with caption and tray.'''
        # info = FLASHWINFO(0, hwnd, win32con.FLASHW_ALL | win32con.FLASHW_TIMERNOFG, 0, 0)
        # info.cbSize = sizeof(info)
        # FlashWindowEx(byref(info))
