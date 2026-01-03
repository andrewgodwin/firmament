from pathlib import Path

from .base import BackendError, BaseBackend


class LocalBackend(BaseBackend):
    """
    A backend that uses a local filesystem directory to store blocks and files.
    """

    type_aliases = ["local"]

    def __init__(self, root: str):
        self.root = Path(root).expanduser()
        self.block_root = self.root / "blocks"
        # Make storage directories if they're not there already; but if they
        # have to be made, ensure the root is empty
        if not self.block_root.is_dir():
            if list(self.root.iterdir()):
                raise BackendError("Cannot initialize storage root - not empty")
            self.block_root.mkdir(parents=True)

    def __str__(self):
        return f"Local (root {self.root})"

    def block_path(self, sha256sum: str) -> Path:
        return self.block_root / sha256sum[:2] / sha256sum[:4] / sha256sum

    def block_exists(self, sha256sum: str) -> bool:
        return self.block_path(sha256sum).is_file()

    def block_retrieve(self, sha256sum: str) -> bytes:
        with open(self.block_path(sha256sum), "rb") as fh:
            return fh.read()

    def block_store(self, sha256sum: str, content: bytes):
        with open(self.block_path(sha256sum), "wb") as fh:
            fh.write(content)

    def block_delete(self, sha256sum: str):
        try:
            self.block_path(sha256sum).unlink()
        except OSError:
            pass
