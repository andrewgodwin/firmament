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

    def block_exists(self, sha256sum: str) -> bool:
        """
        Returns if the block is available from this backend.
        """
        raise NotImplementedError()

    def block_store(self, sha256sum: str, content: bytes):
        """
        Adds a block to this backend. Blocks until complete.
        """
        raise NotImplementedError()

    def block_retrieve(self, sha256sum: str) -> bytes:
        """
        Retrieves contents of a block
        """
        raise NotImplementedError()

    def block_delete(self, sha256sum: str):
        """
        Deletes a block from this backend
        """
        raise NotImplementedError()


class BackendError(BaseException):
    """
    Custom error class for internal-backend errors.
    """

    pass
