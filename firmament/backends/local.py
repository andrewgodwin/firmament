import fcntl
import hashlib
import shutil
import time
from pathlib import Path
from typing import cast

import msgpack

from .base import BackendError, BaseBackend, FileVersionSet


class LocalBackend(BaseBackend):
    """
    A backend that uses a local filesystem directory to store blocks and files.

    It requires that the filesystem support flock()
    """

    type_aliases = ["local"]

    def __init__(self, root: str):
        self.root = Path(root).expanduser()
        self.content_root = self.root / "content"
        self.content_db_path = self.root / "content-database"
        self.file_versions_db_path = self.root / "file-versions-database"
        # Stores content hashes we know we've uploaded but which aren't in the DB yet
        self.extra_content_known: set[str] = set()
        # Make storage directories if they're not there already; but if they
        # have to be made, ensure the root is empty
        if not self.content_root.is_dir():
            if list(self.root.iterdir()):
                raise BackendError("Cannot initialize storage root - not empty")
            self.content_root.mkdir(parents=True)

    def __str__(self):
        return f"Local (root {self.root})"

    def content_path(self, sha256sum: str) -> Path:
        """
        Works out storage path for given sha256sum.

        As we're on an actual filesystem, use a 3-char prefix so the top level
        has a max of 4096 entries.
        """
        return self.content_root / sha256sum[:3] / sha256sum

    def content_exists(self, sha256sum: str) -> bool:
        return self.content_path(sha256sum).is_file()

    def content_upload(self, sha256sum: str, disk_path: Path):
        content_path = self.content_path(sha256sum)
        content_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(disk_path, content_path)
        # Verify the copied file has the expected hash
        actual_hash = hashlib.sha256(content_path.read_bytes()).hexdigest()
        if actual_hash != sha256sum:
            content_path.unlink()
            raise BackendError(
                f"Hash mismatch after copy: expected {sha256sum}, got {actual_hash}"
            )
        self.extra_content_known.add(sha256sum)

    def content_download(self, sha256sum: str, disk_path: Path):
        shutil.copyfile(self.content_path(sha256sum), disk_path)

    def content_delete(self, sha256sum: str):
        try:
            self.content_path(sha256sum).unlink()
        except OSError:
            pass

    def content_list(self) -> set[str]:
        # If the database is old, rebuild it
        if (
            not self.content_db_path.is_file()
            or (time.time() - self.content_db_path.stat().st_mtime) > 60
        ):
            self._build_content_db()
        # Return database content merged with extra knowns
        with open(self.content_db_path, "rb") as fh:
            content_hashes = cast(list[str], msgpack.unpack(fh))
        result = set(content_hashes)
        result.update(self.extra_content_known)
        return result

    def _build_content_db(self):
        """
        Builds the content database file on disk
        """
        # Capture current extra known hashes before walking the filesystem
        # so we only remove those that existed before the walk started
        extra_to_clear = set(self.extra_content_known)
        content_hashes = []
        for path, _, filenames in self.content_root.walk():
            for filename in filenames:
                # TODO: Check instead that we're in the right hash-prefix subdir rather than relying on length
                if len(filename) == 64:
                    content_hashes.append(filename)
        with open(self.content_db_path, "wb") as fh:
            msgpack.pack(content_hashes, fh)
        # Clear only the extra content hashes that existed before we started
        self.extra_content_known -= extra_to_clear

    def file_version_download(self) -> FileVersionSet:
        if not self.file_versions_db_path.is_file():
            return {}
        with open(self.file_versions_db_path, "rb") as fh:
            return cast(FileVersionSet, msgpack.unpack(fh))

    def file_version_upload(self, file_versions: FileVersionSet):
        with open(self.file_versions_db_path, "a+b") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                # Read existing data
                fh.seek(0)
                existing_data = fh.read()
                if existing_data:
                    existing: FileVersionSet = msgpack.unpackb(existing_data)
                else:
                    existing = {}

                # Merge: for each path/content in file_versions, add to existing
                for path, contents in file_versions.items():
                    if path not in existing:
                        existing[path] = {}
                    for content, meta in contents.items():
                        existing[path][content] = meta

                # Write merged result
                fh.seek(0)
                fh.truncate()
                msgpack.pack(existing, fh)
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
