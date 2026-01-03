import logging
from pathlib import Path

import click

from firmament.syncer import Syncer


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
)
def main(log_level):
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


@main.command()
@click.option(
    "-d",
    "--directory",
    type=click.Path(exists=True, path_type=Path),
    default=".",
)
def sync(directory: Path):
    """
    One-shot sync for debugging
    """
    Syncer(directory).run()


if __name__ == "__main__":
    main()
