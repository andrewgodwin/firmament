import logging
from pathlib import Path

import click

from firmament.backends.base import BaseBackend
from firmament.config import Config

logger = logging.getLogger(__name__)


class Syncer:
    """
    Main sync logic.

    Looks at both backends and the local checkout, and uploads/downloads as needed.
    """

    backends: dict[str, BaseBackend] = {}

    def __init__(self, checkout_path: Path):

        # Check checkout_path is a reasonable directory
        checkout_path = checkout_path.expanduser().resolve()
        if not checkout_path.is_dir():
            raise ValueError("Non-directory passed as checkout_path")

        # Go through and find the actual checkout root
        while True:
            db_path = checkout_path / ".firmament.db"
            if db_path.is_file():
                break
            # Check if we've reached the root directory
            if checkout_path.parent == checkout_path:
                raise ValueError("No Firmament checkout found in directory hierarchy")
            checkout_path = checkout_path.parent

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
