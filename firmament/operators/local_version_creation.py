from ..database import FileVersion, LocalFile
from .base import BaseOperator


class LocalVersionCreationOperator(BaseOperator):
    """
    Looks for hashed LocalFiles that don't have a corresponding FileVersion
    and creates them.
    """

    interval = 10

    def step(self):
        with self.config.database.session_factory() as session:
            for local_file in LocalFile.without_fileversion(session):
                instance = FileVersion(
                    path=local_file.path,
                    content=local_file.content,
                    mtime=local_file.mtime,
                    size=local_file.size,
                )
                session.add(instance)
                self.logger.debug(
                    f"Added file version {local_file.path}@{local_file.content}"
                )
            session.commit()
