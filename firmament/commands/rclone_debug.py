from firmament.commands.base import BaseCommand


class RCloneDebugCommand(BaseCommand):
    """
    RClone debug command.
    """

    def run(self, command, remote=None):
        if remote is None:
            remote = self.config.default_remote
        self.config.rclone.run_command(command + [f"{remote}:"])
