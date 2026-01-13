from pathlib import Path
from typing import ClassVar


class BaseBackend:
    """
    Root backend class that defines the main interfaces.

    The methods are defined in terms of single blocks, but implementations
    can (and should) implement their own caching layer, as block_exists
    specifically may get called a lot of times quickly.

    Implementations should be thread-safe.
    """

    name: str
    type_aliases: list[str] = []

    implementation_registry: ClassVar[dict[str, type["BaseBackend"]]] = {}

    def __init__(self, name: str):
        self.name = name

    def __init_subclass__(cls) -> None:
        if not cls.type_aliases:
            raise RuntimeError(
                "You must define at least one type alias per backend implementation"
            )
        for alias in cls.type_aliases:
            BaseBackend.implementation_registry[alias] = cls

    @classmethod
    def implementation_get(cls, alias: str):
        return cls.implementation_registry[alias]

    def run_maintenance(self):
        """
        Entrypoint to do cache rebuilds or the like.
        Will be called periodically in its own thread.
        """
        pass

    def content_exists(self, sha256sum: str) -> bool:
        """
        Returns if the FileContent is available from this backend.
        """
        raise NotImplementedError()

    def content_upload(self, sha256sum: str, disk_path: Path):
        """
        Adds a FileContent to this backend. Blocks until complete.
        """
        raise NotImplementedError()

    def content_download(self, sha256sum: str, disk_path: Path):
        """
        Retrieves contents of a block
        """
        raise NotImplementedError()

    def content_delete(self, sha256sum: str):
        """
        Deletes a block from this backend
        """
        raise NotImplementedError()

    def content_list(self) -> set[str]:
        """
        Returns a set of all content blocks stored on this backend.

        Probably should use some form of caching.
        """
        raise NotImplementedError()


class BackendError(BaseException):
    """
    Custom error class for internal-backend errors.
    """

    pass
