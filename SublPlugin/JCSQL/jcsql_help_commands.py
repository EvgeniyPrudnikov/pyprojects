import os
import sublime
import sublime_plugin


class FetchAllCommand(sublime_plugin.WindowCommand):
    def run(self):
        if self.window.active_group() != 1:
            return
        view = self.window.active_view()
        self.window.run_command("exec_query", {"view_id": view.id(), "fetch": -1})


class SaveToCsvCommand(sublime_plugin.WindowCommand):
    def run(self):
        if self.window.active_group() != 1:
            return
        view = self.window.active_view()
        self.window.run_command("exec_query", {"view_id": view.id(), "fetch": -1, "qtype":'csv'})


class EraseCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        self.view.erase(edit, sublime.Region(0, self.view.size()))


class DoneOrExpiredCommand(sublime_plugin.TextCommand):
    def run(self, edit):
        size = self.view.size()
        reg = sublime.Region(size - 6, size - 1)
        check_text = self.view.substr(reg)
        if check_text == '(...)':
            self.view.set_read_only(False)
            self.view.replace(edit, reg, 'Fetched all rows or timeout expired.')
            self.view.set_read_only(True)


class CloseWithoutSavingCommand(sublime_plugin.WindowCommand):
    def run(self):
        view = self.window.active_view()
        view.set_scratch(True)
        view.close()
