from pathlib import Path
from typing import ClassVar, TypedDict

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESSIV
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class FileVersionMeta(TypedDict):
    mtime: int
    size: int


FileVersionSet = dict[str, dict[str, FileVersionMeta]]


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

    encryption_key: bytes | None = None
    encryption_aessiv: AESSIV | None = None

    def __init__(self, name: str, encryption_key: str | None = None):
        self.name = name
        self._encryption_init(encryption_key)

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

    def _encryption_init(self, password: str | None = None):
        """
        Takes a user-supplied string and derives an AES-SIV key from it and
        an AES-SIV function, if we need to
        """
        if password is not None:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=64,  # 512 bits / 64 bytes
                salt=b"NaCl",
                iterations=600_000,
            )
            self.encryption_key = kdf.derive(password.encode("utf8"))
            self.encryption_aessiv = AESSIV(self.encryption_key)

    def _encrypt_sha256sum(self, sha256sum: str) -> str:
        if self.encryption_aessiv is not None:
            return self.encryption_aessiv.encrypt(
                sha256sum.encode("utf8"), associated_data=None
            ).decode("utf8")
        return sha256sum

    def _decrypt_sha256sum(self, crypttext: str) -> str:
        if self.encryption_aessiv is not None:
            return self.encryption_aessiv.decrypt(
                crypttext.encode("utf8"), associated_data=None
            ).decode("utf8")
        return crypttext

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

    def file_version_download(self) -> FileVersionSet:
        """
        Returns a set of FileVersionEntries for all fileversions this remote
        knows about.
        """
        raise NotImplementedError()

    def file_version_upload(self, file_versions: FileVersionSet):
        """
        Sets the current set of remote file versions to include the given ones.

        Backends should support this being called in parallel from different
        checkouts/machines, and union the results together.
        """
        raise NotImplementedError()


class BackendError(BaseException):
    """
    Custom error class for internal-backend errors.
    """

    pass
