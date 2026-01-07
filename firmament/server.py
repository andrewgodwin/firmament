import logging
import time

from firmament.config import Config
from firmament.operators import (
    BaseOperator,
    LocalHasherOperator,
    LocalScannerOperator,
    LocalVersionCreationOperator,
)

logger = logging.getLogger(__name__)


class Server:
    """
    Main server.

    Runs a series of operator loops.
    """

    operators: list[type[BaseOperator]] = [
        LocalScannerOperator,
        LocalHasherOperator,
        LocalVersionCreationOperator,
    ]

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        """
        Main daemon loop.
        """
        logging.debug("Main loop starting")

        # Create a thread per operator and start it
        threads = []
        for operator in self.operators:
            threads.append(operator(self.config))
        [thread.start() for thread in threads]

        # Wait for a shutdown signal
        logging.info("Running. Ctrl-C to exit.")
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
