import sublime
import sublime_plugin
import os
import subprocess
import threading
import time
import sys
from .lib import *

kernel32 = None
user32 = None
HWND = None


if os.name == 'nt':
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    user32 = ctypes.WinDLL('user32', use_last_error=True)

    FLASHW_STOP = 0
    FLASHW_CAPTION = 0x00000001
    FLASHW_TRAY = 0x00000002
    FLASHW_ALL = 0x00000003
    FLASHW_TIMER = 0x00000004
    FLASHW_TIMERNOFG = 0x0000000C

    class FLASHWINFO(ctypes.Structure):
        _fields_ = (('cbSize', wintypes.UINT),
                    ('hwnd', wintypes.HWND),
                    ('dwFlags', wintypes.DWORD),
                    ('uCount', wintypes.UINT),
                    ('dwTimeout', wintypes.DWORD))

        def __init__(self, hwnd, flags=FLASHW_TRAY | FLASHW_STOP, count=5, timeout_ms=0):
            self.cbSize = ctypes.sizeof(self)
            self.hwnd = hwnd
            self.dwFlags = flags
            self.uCount = count
            self.dwTimeout = timeout_ms

    HWND = user32.GetForegroundWindow()

    def flash_start_icon(count=2):
        if os.name != 'nt':
            return
        if not HWND:
            raise ctypes.WinError(ctypes.get_last_error())
        winfo = FLASHWINFO(HWND, count=count)
        previous_state = user32.FlashWindowEx(ctypes.byref(winfo))
        return previous_state


settings = sublime.load_settings('JCSQL.sublime-settings')

VIEW_THREADS = {}


class EraseCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        self.view.erase(edit, sublime.Region(0, self.view.size()))


class ExecQueryCommand(sublime_plugin.WindowCommand):

    def run(self, view_id="", conn="", qtype="query"):

        if view_id != '':
            self.load_data(view_id)
        else:
            self.new_thread(conn, qtype)

    def new_thread(self, conn, qtype):

        env = conn['environment']

        tool, tmp_file_name = prepare_query_file(self.window.active_view(), env, qtype) or (None, None)
        if tool is None:
            return

        output_view_name = '{0}_{1}'.format(conn['connection_name'], env)
        output_view = self.create_view(output_view_name)

        cmd = []
        if tool == 'sqlplus':
            cmd = [tool, '-L', conn['connection_string'], '@', tmp_file_name]
        elif tool == 'python':
            cmd = [tool, settings.get("client"), env, conn['connection_string'], tmp_file_name, qtype, settings.get('fetch_num')]

        self.exec_thread = ExecThread(cmd, output_view, tmp_file_name)
        self.exec_thread.daemon = True
        self.exec_thread.start()

        VIEW_THREADS[output_view.id()] = self.exec_thread

    def load_data(self, vid):
        try:
            view_thread = VIEW_THREADS[vid]
            view = view_thread.view
            popen = view_thread.popen

            # view.sel().clear()
            view.set_read_only(False)
            view.run_command('erase')

            popen.stdin.write('load:{0}\n'.format(settings.get('fetch_num')).encode('utf-8'))
            popen.stdin.flush()
        except Exception as e:
            print(e)
            return

    def create_view(self, view_name):

        self.window.set_layout({
            "cols": [0.0, 1.0],
            "rows": [0.0, 0.5, 1.0],
            "cells": [[0, 0, 1, 1], [0, 1, 1, 2]]
        })

        result_focus_group = 1
        self.window.focus_group(result_focus_group)  # Focus in output group

        new_view_index = len(self.window.views_in_group(result_focus_group))
        view = self.window.new_file()   # New view in group
        self.window.set_view_index(view, result_focus_group, new_view_index)

        view.settings().set("line_numbers", True)
        view.settings().set("gutter", True)
        view.settings().set("word_wrap", False)
        view.settings().set("scroll_past_end", False)
        view.set_name('{0}_{1}'.format(view_name, str(new_view_index)))
        view.set_read_only(True)

        self.window.focus_group(0)  # Focus in main group

        return view


class ExecThread(threading.Thread):
    def __init__(self, cmd, view, tmp_file_name):
        self.cmd = cmd
        self.view = view
        self.tmp_file_name = tmp_file_name
        self.popen = None
        threading.Thread.__init__(self)

    def run(self):
        try:

            CREATE_NO_WINDOW = 0x08000000  # hide cmd window
            self.popen = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, creationflags=CREATE_NO_WINDOW)
            check_thread = threading.Thread(target=self.check_view_proc, args=(self.view, self.popen, ))
            check_thread.start()

            stdout_lines = iter(self.popen.stdout.readline, b'')
            for stdout_line in stdout_lines:
                ok = self.append_data('{0}\n'.format(stdout_line.decode('cp437').strip('\r\n')))
                if not ok:
                    break
            self.popen.stdout.close()

        except Exception as e:
            print(e)
        finally:
            flash_start_icon()
            os.remove(self.tmp_file_name)

    def append_data(self, data):

        self.view.set_read_only(False)
        self.view.run_command('append', {'characters': data})
        self.view.set_read_only(True)

        errors_start_text = ['ORA-', 'SP2-', 'PL/SQL:', 'ERROR', 'PLS-']
        odbc_err_text = '[Oracle][ODBC][Ora]'
        if any(map(data.startswith, errors_start_text)) or odbc_err_text in data:
            self.view.sel().add(self.view.line(sublime.Region(self.view.size() - len(data), self.view.size())))
            try:
                self.view.run_command("token_style", {"style_index": 0})
            except Exception:
                pass

        self.view.run_command('move_to', {'to': 'eof'})
        return True

    def check_view_proc(self, view, popen):
        while True:
            rc = popen.poll()
            if (not view.name()) or (not rc is None):
                popen.terminate()
                if view.id() in VIEW_THREADS:
                    del VIEW_THREADS[view.id()]
                sys.exit(0)
            time.sleep(5)


class LoadDataEvent(sublime_plugin.ViewEventListener):
    def __init__(self, view):
        sublime_plugin.ViewEventListener.__init__(self, view)

    def on_selection_modified_async(self):
        if self.view.window().active_group() != 1:
            return
        sublime.set_timeout(self.check, 500)

    def check(self):
        if self.view.id() in VIEW_THREADS:
            view_thread = VIEW_THREADS[self.view.id()]
        else:
            return
        rc = view_thread.popen.poll()
        if not rc is None:
            return

        sel = self.view.sel()[0]
        size = self.view.size()
        reg = sublime.Region(size - 5, size - 2)
        if reg.contains(sel) and self.view.is_read_only():
            self.view.window().run_command("exec_query", {"view_id": self.view.id()})
