from firmament.commands.base import BaseCommand


class ListCommand(BaseCommand):
    """
    Listing command.
    """

    def run(self, remote=None):
        if remote is None:
            remote = self.config.default_remote
        print(list(self.config.rclone.get_all_files(remote)))
