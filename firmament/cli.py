import logging
from pathlib import Path

import click

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


if __name__ == "__main__":
    main()
