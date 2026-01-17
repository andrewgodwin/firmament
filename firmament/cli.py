import logging
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from firmament.config import Config
from firmament.server import Server


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter with colors for different log levels
    """

    COLORS = {
        logging.DEBUG: "\033[90m",  # Grey
        logging.INFO: "\033[37m",  # White
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",  # Red
        logging.CRITICAL: "\033[31m",  # Red
    }
    RESET = "\033[0m"

    ABBREVIATIONS = {
        "DEBUG": "DBG",
        "INFO": "INF",
        "WARNING": "WRN",
        "ERROR": "ERR",
        "CRITICAL": "CRT",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        levelname_abbr = self.ABBREVIATIONS.get(record.levelname, record.levelname[:3])
        record.levelname = f"{color}{levelname_abbr:>3}{self.RESET}"
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
)
@click.option(
    "-r",
    "--root-path",
    type=click.Path(exists=True, path_type=Path),
    default=".",
)
@click.pass_context
def main(ctx, log_level: str, root_path: Path):
    # Configure logging with custom colored formatter
    handler = logging.StreamHandler()
    handler.setFormatter(
        ColoredFormatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M")
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=[handler],
    )
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
def server(config):
    """
    One-shot sync for debugging
    """
    Server(config).run()


@main.command()
@click.pass_obj
def tui(config: Config):
    """
    Launch the Terminal User Interface
    """
    from firmament.tui import FirmamentTUI

    FirmamentTUI(config).run()


@main.group()
@click.pass_context
def debug(ctx):
    """
    Debug commands for inspecting internal state
    """
    pass


@debug.command("list-fv")
@click.pass_obj
def list_fv(config: Config):
    """
    List all file versions in the datastore
    """
    console = Console()
    table = Table()

    table.add_column("Path", style="cyan")
    table.add_column("Content Hash", style="green")
    table.add_column("Size", justify="right", style="magenta")
    table.add_column("Modified", style="yellow")

    for path, versions in config.file_versions.items():
        for content_hash, meta in versions.items():
            mtime = datetime.fromtimestamp(meta["mtime"]).strftime("%Y-%m-%d %H:%M:%S")
            size = _format_size(meta["size"])
            table.add_row(path, content_hash[:12] + "...", size, mtime)

    console.print(table)


@debug.command("list-lv")
@click.pass_obj
def list_lv(config: Config):
    """
    List all local versions in the datastore
    """
    console = Console()
    table = Table()

    table.add_column("Path", style="cyan")
    table.add_column("Content Hash", style="green")
    table.add_column("Size", justify="right", style="magenta")
    table.add_column("Modified", style="yellow")

    for path, data in config.local_versions.items():
        mtime = datetime.fromtimestamp(data["mtime"]).strftime("%Y-%m-%d %H:%M:%S")
        size = _format_size(data["size"])
        content_hash = data["content_hash"]
        hash_display = content_hash[:12] + "..." if content_hash else "[dim]None[/dim]"
        table.add_row(path, hash_display, size, mtime)

    console.print(table)


def _format_size(size: float) -> str:
    """
    Format size in human-readable units
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


if __name__ == "__main__":
    main()
