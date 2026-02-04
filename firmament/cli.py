from pathlib import Path

import click

from firmament.commands.browse import BrowseCommand
from firmament.commands.ls import ListCommand
from firmament.commands.sync import SyncCommand
from firmament.config import Config


@click.group()
@click.option(
    "-r",
    "--root-path",
    type=click.Path(exists=True, path_type=Path),
    default=".",
)
@click.pass_context
def main(ctx, root_path: Path):
    # Traverse up directories until we find our meta dir
    for i in range(100):
        meta_path = root_path / ".firmament"
        if meta_path.is_dir():
            break
        # Check if we've reached the root directory
        if root_path.parent == root_path:
            raise ValueError("No Firmament root found in directory hierarchy")
        root_path = root_path.parent
    else:
        raise ValueError("No Firmament root found in directory hierarchy")
    # Setup config object
    ctx.obj = Config(root_path)


@main.command()
@click.pass_obj
@click.option("--nondestructive/--destructive")
def sync(config, nondestructive=False):
    """
    One-shot sync.
    """
    SyncCommand(config).run(nondestructive=nondestructive)


@main.command()
@click.pass_obj
def ls(config):
    """
    General static ls command.
    """
    ListCommand(config).run()


@main.command()
@click.option(
    "-r",
    "--remote",
    type=str,
    default=None,
    help="Remote to browse (defaults to default_remote)",
)
@click.pass_obj
def browse(config, remote):
    """
    Interactive file tree browser.
    """
    BrowseCommand(config).run(remote=remote)


if __name__ == "__main__":
    main()
