import logging
from pathlib import Path

import click

from firmament.backends.base import BaseBackend
from firmament.checkout import Checkout
from firmament.config import Config

logger = logging.getLogger(__name__)


class Daemon:
    """
    Main daemon.

    Handles knowing what backends there are and what blocks are currently
    available.
    """

    backends: dict[str, BaseBackend] = {}
    checkouts: list[Checkout] = []

    def __init__(self, config_path: Path):
        # Load configuration
        self.config = Config(config_path)
        logger.info(f"Config file: {config_path}")

        # Create backends
        for name, backend_config in self.config.backends.items():
            backend_class = BaseBackend.implementation_get(backend_config.type)
            self.backends[name] = backend_class(**backend_config.options)
            logger.info(f"Backend {name}: {self.backends[name]}")

        # Create checkouts
        for checkout_config in self.config.checkouts:
            self.checkouts.append(
                Checkout(
                    local_path=checkout_config.local_path,
                    remote_path=checkout_config.remote_path,
                )
            )

    def run(self):
        """
        Main daemon loop.
        """
        logging.info("Main loop starting")
        # Collect all local files
        local_files = []
        for checkout in self.checkouts:
            local_files.extend(checkout.scan())
        # Get their blocks
        for file in local_files:
            print(file)
            print([x for x, y in file.blocks()])
        logging.info("Stopping")


@click.command()
@click.argument(
    "config",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
)
def main(config: Path, log_level: str):
    """
    Firmament main daemon
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Create and run daemon
    daemon = Daemon(config_path=config)
    daemon.run()


if __name__ == "__main__":
    main()
