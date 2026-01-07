import hashlib

from ..database import LocalFile
from .base import BaseOperator


class LocalHasherOperator(BaseOperator):
    """
    Looks for LocalFiles without a content hash, and hashes them.
    """

    interval = 10

    def step(self):
        with self.config.database.session_factory() as session:
            for local_file in LocalFile.without_content(session):
                with open(self.config.root_path / local_file.path, "rb") as fh:
                    local_file.content = hashlib.sha256(fh.read()).hexdigest()
                self.logger.debug(
                    f"Hashed file {local_file.path} as {local_file.content}"
                )
            session.commit()
