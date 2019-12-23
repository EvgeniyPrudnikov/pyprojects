import os
import sublime
import sublime_plugin
from .connection_store import *
import time


settings = None
conn_store = None


def plugin_loaded():
    global settings, conn_store
    settings = sublime.load_settings('JCSQL.sublime-settings')
    if not settings.get('client'):
        settings.set('client', os.path.join(sublime.packages_path(), 'JCSQL', 'client.py'))
        settings.set('pass', os.path.join(sublime.packages_path(), 'JCSQL', 'pass'))
        settings.set('fetch_num', '65')
        sublime.save_settings('JCSQL.sublime-settings')
    conn_store = ConnectionStore(settings.get('pass'))


class AddConnectionCommand(sublime_plugin.WindowCommand):
    def run(self, **kwargs):
        conn_store.add_connection()


class DeleteConnectionCommand(sublime_plugin.WindowCommand):
    def run(self, **kwargs):
        conn_store.delete_connection()


class ModifyConnectionCommand(sublime_plugin.WindowCommand):
    def run(self, **kwargs):
        conn_store.modify_connection()


class RunCodeCommand(sublime_plugin.WindowCommand):
    def run(self):
        las_used_conn = conn_store.get_last_used_conn()
        self.window.run_command("run_code_in", {"conn": las_used_conn})


class RunCodeInCommand(sublime_plugin.WindowCommand):
    def run(self, conn=None, qtype="query"):
        if conn:
            self.window.run_command("exec_query", {"conn": conn, "qtype": qtype})
        else:
            conn_list = conn_store.get_all_connections()
            if len(conn_list) == 0:
                sublime.message_dialog('No connections.')
                return

            def on_con(conn_idx):
                conn = conn_store.get_connection(conn_list[conn_idx])
                self.window.run_command("exec_query", {"conn": conn, "qtype": qtype})
                conn_store.upd_last_used_conn(conn)

            self.window.show_quick_panel(conn_list, on_con, 0, 0, None)


class ExplainPlanCommand(sublime_plugin.WindowCommand):
    def run(self):
        self.window.run_command("run_code_in", {"qtype": 'explain'})
