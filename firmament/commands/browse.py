from firmament.commands.base import BaseCommand


class BrowseCommand(BaseCommand):
    """
    Interactive file tree browser.

    Displays a TUI showing all local and remote files in a tree view, with their path
    request statuses.
    """

    def run(self, remote=None):
        if remote is None:
            remote = self.config.default_remote

        from firmament.commands.browse_tui import BrowseApp

        app = BrowseApp(self.config, remote)
        app.run()
