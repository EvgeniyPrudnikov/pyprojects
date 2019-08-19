import sublime_plugin


class CloseWithoutSavingCommand(sublime_plugin.WindowCommand):
    def run(self):
        view = self.window.active_view()
        view.set_scratch(True)
        view.close()
