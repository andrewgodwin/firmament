import fcntl
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from .base import BackendError, BaseBackend, VersionError


class LocalBackend(BaseBackend):
    """
    A backend that uses a local filesystem directory to store blocks and files.

    It requires that the filesystem support flock()
    """

    type_aliases = ["local"]

    def __init__(self, root: str, name: str, encryption_key: str | None = None):
        super().__init__(name=name, encryption_key=encryption_key)
        self.root = Path(root).expanduser()
        self.content_root = self.root / "content"
        # Make storage directories if they're not there already; but if they
        # have to be made, ensure the root is empty
        if not self.content_root.is_dir():
            if list(self.root.iterdir()):
                raise BackendError("Cannot initialize storage root - not empty")
            self.content_root.mkdir(parents=True)

    def __str__(self):
        return f"Local (root {self.root})"

    def remote_read_io(
        self,
        path: str,
        target_handle: BinaryIO,
    ):
        """
        Reads encrypted contents stored at "path" into the passed file handle.

        Returns an identifier that can be passed back to _write_* to ensure the
        underlying file has not changed.
        """
        enc_handle = self.encryptor.decrypt_file(open(path, "rb"))
        while True:
            chunk = enc_handle.read(self.encryptor.chunk_size)
            if chunk:
                target_handle.write(chunk)
            else:
                break
        enc_handle.close()

    def remote_write_io(
        self,
        path: str,
        source_handle: BinaryIO,
        over_version: str | None = None,
    ):
        """
        Writes encrypted contents from the passed file handle into "path".

        If lock is True, tries to lock the file to ensure nobody else writes it at
        the same time as us. If a lock cannot be established (either synchronously
        or via a update-if-version-unchanged system), raises IOError.
        """
        enc_handle = self.encryptor.encrypt_file(source_handle)
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as fh:
            if over_version:
                fcntl.flock(fh, fcntl.LOCK_EX)
                current_version = str(path_obj.stat().st_mtime_ns)
                if current_version != over_version:
                    fcntl.flock(fh, fcntl.LOCK_UN)
                    raise VersionError(
                        f"Requested {over_version}, got {current_version}"
                    )
            try:
                while True:
                    chunk = enc_handle.read(self.encryptor.chunk_size)
                    if chunk:
                        fh.write(chunk)
                    else:
                        break
                fh.flush()
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
        enc_handle.close()

    def remote_exists(self, path: str) -> bool:
        return Path(path).is_file()

    def remote_delete(self, path: str):
        path_obj = Path(path)
        try:
            path_obj.unlink()
        except FileNotFoundError:
            pass

    def remote_content_path(self, sha256sum: str) -> str:
        """
        Works out storage path for given sha256sum.

        As we're on an actual filesystem, use a 3-char prefix so the top level
        has a max of 4096 entries.
        """
        cryptsum = self.encryptor.encrypt_identifier(sha256sum)
        return str(self.content_root / cryptsum[:3] / cryptsum)

    def remote_database_path(self, db_name: str) -> str:
        """
        Works out storage path for given database name
        """
        return str(self.root / f"database-{db_name}")

    def remote_content_walk(self) -> Iterator[str]:
        """
        Yields the set of content hashes that are stored on this backend,
        usually by walking the file tree.
        """
        for path, _, filenames in self.content_root.walk():
            for filename in filenames:
                # TODO: Check instead that we're in the right hash-prefix subdir rather than relying on length
                if len(filename) > 4:
                    yield self.encryptor.decrypt_identifier(filename)
