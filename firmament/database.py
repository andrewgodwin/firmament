from pathlib import Path

from sqlalchemy import (
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class FileVersion(DeclarativeBase):
    """
    Represents what is at a path at a given time - either a file (as defined by content), or nothing (if the content ID is the special value "__deleted__").

    The most recent mtime is always considered to be what should "currently" be at the path.
    """

    __tablename__ = "FileVersion"

    path: Mapped[str] = mapped_column(primary_key=True)
    content: Mapped[str]
    mtime: Mapped[int]
    size: Mapped[int]


class LocalFile(DeclarativeBase):
    """
    Represents what our local files on disk are

    If content is NULL, it requires hashing still.
    """

    __tablename__ = "LocalFile"

    path: Mapped[str] = mapped_column(primary_key=True)
    content: Mapped[str | None]
    mtime: Mapped[int]
    size: Mapped[int]

    @classmethod
    def by_path(cls, session: Session, path: str) -> "LocalFile | None":
        return (
            session.execute(select(LocalFile).where(LocalFile.path == path))
            .scalars()
            .one_or_none()
        )

    @classmethod
    def insert_new(
        cls, session: Session, path: str, mtime: int, size: int
    ) -> "LocalFile":
        """
        Adds a new LocalFile for the given path with empty content
        """
        local_file = cls(path=path, content=None, mtime=mtime, size=size)
        session.add(local_file)
        return local_file

    def update_new(self, mtime: int, size: int):
        """
        Updates the LocalFile with new mtime and size, resetting content to None
        """
        self.mtime = mtime
        self.size = size
        self.content = None


class Database:
    """
    Simplistic database access wrapper that handles schemas and types
    """

    def __init__(self, path: Path):
        # Check paths
        self.path = path

        # Create engine and session
        self.engine = create_engine(f"sqlite:///{self.path}")
        self.session_factory = sessionmaker(bind=self.engine)

        # Make sure all tables are good
        self.check_schema()

    def check_schema(self):
        # Create all tables defined in Base
        DeclarativeBase.metadata.create_all(self.engine)
