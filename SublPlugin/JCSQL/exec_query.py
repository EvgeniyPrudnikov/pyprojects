import sublime
import sublime_plugin
import sys
import os
import ImapalaOracleSQL.lib as lib


SETTINGS_FILE_NAME = 'JCSQL.sublime-settings'


class ExecQueryCommand(sublime_plugin.WindowCommand):
    def run(self, schema_name="", is_full_res="0", qtype="oracle_query", **kwargs):
        settings = sublime.load_settings(SETTINGS_FILE_NAME)

        tool = lib.prepare_query_file(self.window.active_view(), settings.get("query_tmp_file_full_path"), qtype)

        dsn = lib.get_schema_pass(schema_name, settings.get("pass_file_full_path"), tool)

        if not tool:
            print('Some errors occurred')
            return

        self.window.run_command("exec_thread", {"dsn":dsn, "tool":tool, "qtype": qtype, "is_full_res":is_full_res})