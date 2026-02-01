import logging
import threading
import time

from firmament.config import Config


class BaseOperator(threading.Thread):
    """
    Base class for all operators.
    """

    interval_short: float = 1
    interval_long: float = 10
    log_name = "base-operator"

    def __init__(self, config: Config, operator_index: int = 0):
        self.config = config
        self.running: bool = False
        self.operator_index = operator_index
        self.status: str | None = None
        if operator_index > 0:
            self.log_name = f"{self.log_name}-{operator_index}"
        self.logger = logging.getLogger(self.log_name)
        super().__init__(daemon=True)
        self.post_init()

    def post_init(self):
        pass

    def run(self):
        self.running = True
        interval = self.interval_short
        while self.running:
            try:
                active = self.step()
                self.status = None
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

        Returns if it did work or not; if True, then the next loop is quick. If False,
        then a backoff happens.
        """
        raise NotImplementedError()
