import logging
import time
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, ClassVar, cast

import msgpack

from firmament.encryptors.aes import AESEncryptor
from firmament.encryptors.base import BaseEncryptor
from firmament.encryptors.null import NullEncryptor
from firmament.types import FileVersionData

FileVersionSet = dict[str, FileVersionData]


class VersionError(BaseException):
    """
    Exception for when the version you said you wanted to overwrite isn't present.
    """


class BaseBackend:
    """
    Root backend class that defines the main interfaces.

    Handles storing both file content and database content locally. As file
    contents are content-addressed, no locking or overwrite protection is
    performed; however, for database content, we need some.

    This is done using basic overwrite-if-not-changed logic - when you read
    a file from the backend, it comes with a version, and you supply that
    version when you are writing it, and will get an error if you are
    overwriting a different version than you passed. The version is an opaque
    string; what it is depends on the implementation (locally it can be mtime,
    remotely it could be an ETag).

    Implementations should be thread-safe and multi=process safe.
    """

    name: str
    type_aliases: list[str] = []

    implementation_registry: ClassVar[dict[str, type["BaseBackend"]]] = {}

    encryptor: BaseEncryptor

    content_rebuild_interval: int = 60

    def __init__(self, name: str, encryption_key: str | None = None):
        self.name = name
        # Stores content hashes we know we've uploaded but which aren't in the DB yet
        self.extra_content_known: set[str] = set()
        self.last_content_rebuild = 0
        # Set up encryptor
        if encryption_key is None:
            self.encryptor = NullEncryptor()
        else:
            self.encryptor = AESEncryptor(encryption_key)

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

    ### Remote file writing/reading ###

    def remote_read_bytes(self, path: str) -> tuple[bytes, str]:
        """
        Reads encrypted contents stored at "path" and returns a tuple of (content,
        version)
        """
        result = BytesIO()
        version = self.remote_read_io(path, result)
        return result.getvalue(), version

    def remote_write_bytes(
        self,
        path: str,
        content: bytes,
        over_version: str | None = None,
        is_content: bool = False,
    ):
        """
        Writed passed bytes into encrypted contents stored at "path".
        """
        buffer = BytesIO(content)
        self.remote_write_io(
            path, buffer, over_version=over_version, is_content=is_content
        )

    def remote_read_io(
        self,
        path: str,
        target_handle: BinaryIO,
    ) -> str:
        """
        Reads encrypted contents stored at "path" into the passed file handle.

        Returns an identifier that can be passed back to _write_* to ensure the
        underlying file has not changed.
        """
        raise NotImplementedError()

    def remote_write_io(
        self,
        path: str,
        source_handle: BinaryIO,
        over_version: str | None = None,
        is_content: bool = False,
    ):
        """
        Writes encrypted contents from the passed file handle into "path".

        If lock is True, tries to lock the file to ensure nobody else writes it at the
        same time as us. If a lock cannot be established (either synchronously or via a
        update-if-version-unchanged system), raises IOError.

        If is_content is True, this is a content block write (as opposed to a database
        file). Backends may use this to apply different storage policies.
        """
        raise NotImplementedError()

    def remote_exists(self, path: str) -> bool:
        """
        Returns if the given remote path exists.
        """
        raise NotImplementedError()

    def remote_delete(self, path: str):
        """
        Deletes the remote path.
        """
        raise NotImplementedError()

    def remote_content_walk(self) -> Iterator[str]:
        """
        Yields the set of content hashes that are stored on this backend, usually by
        walking the file tree.
        """
        raise NotImplementedError()

    def remote_content_path(self, sha256sum: str) -> str:
        """
        Works out the remote path for given sha256sum.
        """
        raise NotImplementedError()

    def remote_database_path(self, db_name: str) -> str:
        """
        Works out the remote path for given database name.
        """
        raise NotImplementedError()

    ### High-level API ###

    def run_maintenance(self):
        """
        Entrypoint to do cache rebuilds or the like.

        Will be called periodically in its own thread.
        """
        pass

    def content_exists(self, sha256sum: str) -> bool:
        """
        Returns if the given content is available from this backend.
        """
        return self.remote_exists(self.remote_content_path(sha256sum))

    def content_upload(self, sha256sum: str, disk_path: Path):
        """
        Adds the given content, sourced from the local path "disk_path", to this
        backend.

        Blocks until complete.
        """
        content_path = self.remote_content_path(sha256sum)
        with open(disk_path, "rb") as orig_fh:
            self.remote_write_io(content_path, orig_fh, is_content=True)
        self.extra_content_known.add(sha256sum)

    def content_download(self, sha256sum: str, disk_path: Path):
        """
        Retrieves content into local path "disk_path".
        """
        with open(disk_path, "wb") as local_fh:
            self.remote_read_io(self.remote_content_path(sha256sum), local_fh)

    def content_delete(self, sha256sum: str):
        """
        Deletes the content from this backend.
        """
        self.remote_delete(self.remote_content_path(sha256sum))
        self.extra_content_known.discard(sha256sum)

    def content_list(self) -> set[str]:
        """
        Returns a set of all content blocks stored on this backend.

        Probably should use some form of caching.
        """
        # If the database is old, rebuild it
        if (time.time() - self.last_content_rebuild) > self.content_rebuild_interval:
            self.content_database_rebuild()
            self.last_content_rebuild = int(time.time())
        # Return database content merged with extra knowns
        remote_path = self.remote_database_path("contents")
        if not self.remote_exists(remote_path):
            content_hashes = []
        else:
            content_hashes = cast(
                list[str], msgpack.unpackb(self.remote_read_bytes(remote_path)[0])
            )
        result = set(content_hashes)
        result.update(self.extra_content_known)
        return result

    def content_database_rebuild(self):
        """
        Rebuilds the content database file.
        """
        # Capture current extra known hashes before walking the filesystem
        # so we only remove those that existed before the walk started
        extra_to_clear = set(self.extra_content_known)
        # Walk all remote content hashes
        content_hashes = list(self.remote_content_walk())
        # Write that to the remote database (no need to download it first, since contents are append-only)
        remote_path = self.remote_database_path("contents")
        self.remote_write_bytes(remote_path, cast(bytes, msgpack.packb(content_hashes)))
        # Clear only the extra content hashes that existed before we started
        self.extra_content_known -= extra_to_clear
        logging.debug(f"Backend {self.name} content database rebuilt")

    def file_version_download(self) -> FileVersionSet:
        """
        Returns a set of FileVersionEntries for all fileversions this remote knows
        about.
        """
        remote_path = self.remote_database_path("file-versions")
        if not self.remote_exists(remote_path):
            return {}
        packed_db = self.remote_read_bytes(remote_path)[0]
        return cast(FileVersionSet, msgpack.unpackb(packed_db))

    def file_version_upload(self, file_versions: FileVersionSet):
        """
        Sets the current set of remote file versions to include the given ones.

        Uses the version locking for upload to merge the remote version in if it has
        changed.
        """
        remote_path = self.remote_database_path("file-versions")
        for i in range(100):
            try:
                # Read current remote version
                if self.remote_exists(remote_path):
                    packed_db, db_version = self.remote_read_bytes(remote_path)
                    existing_db = cast(FileVersionSet, msgpack.unpackb(packed_db))
                else:
                    existing_db = {}
                    db_version = None

                # Merge in with our passed one
                for path, contents in file_versions.items():
                    if path not in existing_db:
                        existing_db[path] = {}
                    for content, meta in contents.items():
                        existing_db[path][content] = meta

                # Write out the new one with a version assertion
                self.remote_write_bytes(
                    remote_path,
                    cast(bytes, msgpack.packb(existing_db)),
                    over_version=db_version,
                )
            except VersionError:
                continue
            else:
                break
        else:
            raise OSError("Could not write clean version of file version database")


class BackendError(BaseException):
    """
    Custom error class for internal-backend errors.
    """

    pass
