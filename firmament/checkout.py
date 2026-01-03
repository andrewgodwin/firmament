import hashlib
from collections.abc import Iterable
from pathlib import Path

from firmament.constants import BLOCK_SIZE


class Checkout:
    """
    Represents some subset (by directory) of the Filesystem that is present on
    a local disk.
    """

    def __init__(
        self,
        local_path: Path,
        remote_path: Path,
        upload: bool = True,
        download: bool = True,
    ):
        self.remote_path = remote_path
        if not str(self.remote_path).startswith("/"):
            raise RuntimeError("Remote path must start with /")
        self.local_path = local_path
        if not self.local_path.is_dir():
            raise RuntimeError("Local checkout directory does not exist")
        self.upload = upload
        self.download = download

    def __str__(self):
        return f"Checkout ({self.local_path} of {self.remote_path})"

    def scan(self) -> list["LocalFile"]:
        """
        Returns a LocalFile for each file present in this checkout.
        """
        result = []
        for directory, _, filenames in self.local_path.walk():
            for filename in filenames:
                file_local_path = directory / filename
                file_relative_path = file_local_path.relative_to(self.local_path)
                result.append(
                    LocalFile(
                        local_path=file_local_path,
                        remote_path=Path(self.remote_path).joinpath(file_relative_path),
                    )
                )
        return result


class LocalFile:
    """
    Represents a local file inside a checkout
    """

    def __init__(self, local_path: Path, remote_path: Path):
        self.local_path = local_path
        self.remote_path = remote_path

    def __str__(self):
        return f"LocalFile ({self.local_path} of {self.remote_path})"

    def __repr__(self):
        return f"<{self}>"

    def mtime(self) -> int:
        return int(self.local_path.stat().st_mtime)

    def blocks(self) -> Iterable[tuple[str, bytes]]:
        """
        Returns a list of blocks for this file, in order, with their SHA256
        hexdigest and raw bytes.
        """
        with open(self.local_path, "rb") as fh:
            while True:
                data = fh.read(BLOCK_SIZE)
                if not data:
                    return
                sha256 = hashlib.sha256(data).hexdigest()
                yield (sha256, data)
