import shutil
from pathlib import Path

from .base import BackendError, BaseBackend


class LocalBackend(BaseBackend):
    """
    A backend that uses a local filesystem directory to store blocks and files.
    """

    type_aliases = ["local"]

    def __init__(self, root: str):
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

    def content_path(self, sha256sum: str) -> Path:
        return self.content_root / sha256sum[:2] / sha256sum[:4] / sha256sum

    def content_exists(self, sha256sum: str) -> bool:
        return self.content_path(sha256sum).is_file()

    def content_store(self, sha256sum: str, local_path: Path):
        shutil.copyfile(local_path, self.content_path(sha256sum))

    def content_retrieve(self, sha256sum: str, local_path: Path):
        shutil.copyfile(self.content_path(sha256sum), local_path)

    def content_delete(self, sha256sum: str):
        try:
            self.content_path(sha256sum).unlink()
        except OSError:
            pass
