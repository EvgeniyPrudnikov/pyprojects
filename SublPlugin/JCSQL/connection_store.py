import json
import os
import sublime
import sublime_plugin

settings = sublime.load_settings('JCSQL.sublime-settings')


class ConnectionStore(object):

    _last_used_connection = None
    _connection_store = None
    _template = """
{
      "connection_name": "CONNECTION_NAME_HERE"
    , "environment": "ENVIRONMENT_HERE(oracle/impala)"
    , "connection_string": "CONNECTION_STRING_HERE"
}
"""

    def __init__(self, pass_path):
        self._pass_path = pass_path

        if os.path.isfile(pass_path):
            with open(pass_path, 'r') as pass_file:
                d = pass_file.read()
                if len(d) > 0:
                    self._connection_store = json.loads(d)
                else:
                    self._connection_store = {}
        else:
            self._connection_store = {}

    def get_last_used_conn(self):
        return self._last_used_connection

    def upd_last_used_conn(self, conn):
        self._last_used_connection = conn

    def add_connection(self):

        w = sublime.active_window()
        w.show_input_panel('Add New Connection: ', self._template, self._on_add_modify_conn, None, None)

    def modify_connection(self):
        def _on_modify_conn(conn_name_idx):
            conn = self.get_connection(conn_list[conn_name_idx])

            w = sublime.active_window()
            w.show_input_panel('Modify Connection: ',
                               self._template.replace("ENVIRONMENT_HERE(oracle/impala)", conn['environment']).replace('CONNECTION_NAME_HERE', conn['connection_name']).replace('CONNECTION_STRING_HERE', conn['connection_string']),
                               self._on_add_modify_conn,
                               None,
                               None)

        conn_list = self.get_all_connections()
        if len(conn_list) == 0:
            sublime.message_dialog('Nothing to modify.')
            return
        w = sublime.active_window()
        w.show_quick_panel(conn_list, _on_modify_conn, 0, 0, None)

    def _on_add_modify_conn(self, conn_json):
        print(conn_json)
        if len(conn_json) > 0:
            try:
                j = json.loads(conn_json)
            except Exception as e:
                sublime.error_message('Incorrect Connection JSON. Please try again.')
        else:
            sublime.error_message('Empty connection.')
            return
        self._connection_store[j["connection_name"]] = j
        self._flush_connections()
        sublime.message_dialog('Succsess!')

    def _flush_connections(self):
        with open(self._pass_path, 'w+') as f:
            json.dump(self._connection_store, f)

    def get_connection(self, conn_name):
        return self._connection_store[conn_name]

    def get_all_connections(self):
        return list(self._connection_store.keys())

    def delete_connection(self):

        def _on_del(conn_name_idx):
            conn_name = conn_list[conn_name_idx]
            try:
                del self._connection_store[conn_name]
            except Exception as e:
                sublime.error_message('Empty connection.')
                return
            sublime.message_dialog('Connection {0} has been deleted.'.format(conn_name))
            self._flush_connections()

        conn_list = self.get_all_connections()
        if len(conn_list) == 0:
            sublime.message_dialog('Nothing to delete.')
            return

        w = sublime.active_window()
        w.show_quick_panel(conn_list, _on_del, 0, 0, None)
