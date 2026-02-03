from firmament.config import Config


class BaseCommand:
    """
    Base command class.
    """

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        raise NotImplementedError()
