import shutil
import time
from pathlib import Path

from firmament import firmament_pb2

from .base import BackendError, BaseBackend


class LocalBackend(BaseBackend):
    """
    A backend that uses a local filesystem directory to store blocks and files.
    """

    type_aliases = ["local"]

    def __init__(self, root: str):
        self.root = Path(root).expanduser()
        self.content_root = self.root / "content"
        self.content_db_path = self.root / "content-database"
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
        content_db = firmament_pb2.ContentDatabase()
        with open(self.content_db_path, "rb") as fh:
            content_db.ParseFromString(fh.read())
        result = {c.sha256sum for c in content_db.contents}
        result.update(self.extra_content_known)
        return result

    def _build_content_db(self):
        """
        Builds the content protobuf file on disk
        """
        # Capture current extra known hashes before walking the filesystem
        # so we only remove those that existed before the walk started
        extra_to_clear = set(self.extra_content_known)
        content_db = firmament_pb2.ContentDatabase()
        for path, _, filenames in self.content_root.walk():
            for filename in filenames:
                # TODO: Check instead that we're in the right hash-prefix subdir rather than relying on length
                if len(filename) == 64:
                    content_db.contents.add().sha256sum = filename
        with open(self.content_db_path, "wb") as fh:
            fh.write(content_db.SerializeToString())
        # Clear only the extra content hashes that existed before we started
        self.extra_content_known -= extra_to_clear
