import click

from firmament.commands.base import BaseCommand
from firmament.constants import PATH_REQUEST_ON_DEMAND


class SyncCommand(BaseCommand):
    """
    Synchronisation command.

    Copies up changes from any ON_DEMAND and DOWNLOAD_ONCE folders, and fully
    synchronises any SYNC folders
    """

    def run(self, remote=None, max_transfer=None):
        if remote is None:
            remote = self.config.default_remote
        if max_transfer is None:
            max_transfer = "100T"
        # First, do an upward copy (of OD, DO and SY)
        click.echo(click.style("Nondestructive upload", fg="cyan", bold=True))
        self.config.rclone.run_command(
            [
                "copy",
                str(self.config.root_path),
                f"{remote}:",
                "--progress",
                "--max-transfer",
                max_transfer,
            ],
            filter_text=self.config.path_requests.generate_rclone_filters(
                type="up-copy"
            ),
            request_combined=True,
        )
        # Run a download
        click.echo(click.style("\nNondestructive download", fg="cyan", bold=True))
        self.config.rclone.run_command(
            [
                "copy",
                f"{remote}:",
                str(self.config.root_path),
                "--progress",
                "--max-transfer",
                max_transfer,
            ],
            filter_text=self.config.path_requests.generate_rclone_filters(
                type="down-copy"
            ),
            request_combined=True,
        )
        # Mark all download_onces as done as a result
        for path in self.config.path_requests.download_once_paths():
            print(f"Download once completed: {path}")
            self.config.path_requests.set(path, PATH_REQUEST_ON_DEMAND)
        self.config.path_requests.save()
