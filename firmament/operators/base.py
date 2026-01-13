import logging
import threading
import time

from firmament.config import Config


class BaseOperator(threading.Thread):
    """
    Base class for all operators
    """

    interval_short: float = 1
    interval_long: float = 30
    log_name = "base-operator"

    def __init__(self, config: Config):
        self.config = config
        self.running: bool = False
        self.logger = logging.getLogger(self.log_name)
        super().__init__(daemon=True)

    def run(self):
        self.running = True
        interval = self.interval_short
        while self.running:
            try:
                active = self.step()
                if active:
                    interval = self.interval_short
                else:
                    interval = min(interval * 2, self.interval_long)
                time.sleep(interval)
            except BaseException as e:
                self.logger.exception(f"{self.log_name}: {e}")
                time.sleep(30)

    def step(self) -> bool:
        """
        Called to do an iteration of the loop.
        Returns if it did work or not; if True, then the next loop is quick.
        If False, then a backoff happens.
        """
        raise NotImplementedError()
