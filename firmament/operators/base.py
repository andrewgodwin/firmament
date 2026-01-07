import logging
import threading
import time

from firmament.config import Config


class BaseOperator(threading.Thread):
    """
    Base class for all operators
    """

    interval: float = 1
    log_name = "base-operator"

    def __init__(self, config: Config):
        self.config = config
        self.running: bool = False
        self.logger = logging.getLogger(self.log_name)
        super().__init__(daemon=True)

    def run(self):
        self.running = True
        while self.running:
            self.step()
            time.sleep(self.interval)

    def step(self):
        raise NotImplementedError()
