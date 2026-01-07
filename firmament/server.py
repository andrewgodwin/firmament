import logging
import time

from firmament.config import Config
from firmament.operators import BaseOperator, LocalScannerOperator

logger = logging.getLogger(__name__)


class Server:
    """
    Main server.

    Runs a series of operator loops.
    """

    operators: list[type[BaseOperator]] = [
        LocalScannerOperator,
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
        [thread.run() for thread in threads]
        # Wait for a shutdown signal
        try:
            while True:
                logging.info("Running. Ctrl-C to exit.")
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
        logging.debug("Stopping")
