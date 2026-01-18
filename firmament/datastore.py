from collections.abc import Iterator
from pathlib import Path
from typing import Generic, TypeVar, cast

import lmdb
import msgpack

from firmament.types import (
    FileVersionData,
    FileVersionMeta,
    LocalVersionData,
    PathRequestType,
)

T = TypeVar("T")


class DiskDatastore(Generic[T]):
    """
    A generic key-value store backed by LMDB.

    Keys are strings (encoded as UTF-8), values are serialized with msgpack.
    """

    def __init__(self, path: Path, map_size: int = 1024 * 1024 * 1024):
        """
        Initialize the datastore.

        Args:
            path: Path to the LMDB environment directory.
            map_size: Maximum size of the database in bytes (default 1GB).
        """
        self.path = path
        path.mkdir(parents=True, exist_ok=True)
        self.env = lmdb.open(str(path), map_size=map_size)

    def _validate_key(self, key: str):
        pass

    def get(self, key: str, default: T | None = None) -> T | None:
        """
        Get a value by key, returning default if not found.
        """
        with self.env.begin() as txn:
            value = txn.get(key.encode("utf-8"))
            if value is None:
                return default
            return cast(T, msgpack.unpackb(value))

    def set(self, key: str, value: T) -> None:
        """
        Set a value and persist to disk.
        """
        self._validate_key(key)
        with self.env.begin(write=True) as txn:
            txn.put(key.encode("utf-8"), msgpack.packb(value))

    def delete(self, key: str) -> None:
        """
        Delete a key. Raises KeyError if not found.
        """
        self._validate_key(key)
        with self.env.begin(write=True) as txn:
            if not txn.delete(key.encode("utf-8")):
                raise KeyError(key)

    def __getitem__(self, key: str) -> T:
        with self.env.begin() as txn:
            value = txn.get(key.encode("utf-8"))
            if value is None:
                raise KeyError(key)
            return cast(T, msgpack.unpackb(value))

    def __setitem__(self, key: str, value: T) -> None:
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        self.delete(key)

    def __contains__(self, key: str) -> bool:
        with self.env.begin() as txn:
            return txn.get(key.encode("utf-8")) is not None

    def keys(self) -> Iterator[str]:
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, _ in cursor:
                yield key.decode("utf-8")

    def values(self) -> Iterator[T]:
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                yield cast(T, msgpack.unpackb(value))

    def items(self) -> Iterator[tuple[str, T]]:
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                yield key.decode("utf-8"), cast(T, msgpack.unpackb(value))

    def all(self) -> dict[str, T]:
        result: dict[str, T] = {}
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                result[key.decode("utf-8")] = cast(T, msgpack.unpackb(value))
        return result

    def set_all(self, value: dict[str, T]):
        """
        Overwrite the entire database to match value
        """
        with self.env.begin(write=True) as txn:
            txn.drop(self.env.open_db(), delete=False)
            for key, val in value.items():
                self._validate_key(key)
                txn.put(key.encode("utf-8"), msgpack.packb(val))

    def __len__(self) -> int:
        with self.env.begin() as txn:
            return txn.stat()["entries"]

    def close(self) -> None:
        self.env.close()


class LocalVersion(DiskDatastore[LocalVersionData]):
    """
    Storage of LocalVersions (what things we have on-disk in our checkout)
    """

    def _validate_key(self, key: str):
        if not key.startswith("/"):
            raise ValueError("LocalVersion paths must start with /")

    def by_content_hash(self, content_hash: str) -> tuple[str, LocalVersionData]:
        """
        Returns the first path key that has this content
        """
        for path, meta in self.items():
            if meta["content_hash"] == content_hash:
                return path, meta
        raise KeyError(f"No entry with content hash {content_hash}")

    def all_content_hashes(self) -> set[str]:
        result = set()
        for data in self.values():
            if data["content_hash"] is not None:
                result.add(data["content_hash"])
        return result

    def without_content_hashes(self) -> Iterator[str]:
        for path, data in self.items():
            if data["content_hash"] is None:
                yield path

    def not_in_file_versions(
        self, file_versions: "FileVersion"
    ) -> Iterator[tuple[str, LocalVersionData]]:
        """
        Returns all LocalVersions which have a content_hash but do not have a
        matching FileVersion (i.e. there is no FileVersion with their path and contenthash)
        """
        for path, local_data in self.items():
            content_hash = local_data["content_hash"]
            if content_hash is None:
                continue
            file_version_data = file_versions.get(path)
            if file_version_data is None or content_hash not in file_version_data:
                yield path, local_data


class FileVersion(DiskDatastore[FileVersionData]):
    """
    Storage of FileVersions (global concept of what exists).

    Path is the overall key, then the value is a dict of {content_hash: meta}
    """

    def _validate_key(self, key: str):
        if not key.startswith("/"):
            raise ValueError("FileVersion paths must start with /")

    def set_with_content(self, path: str, content_hash: str, meta: FileVersionMeta):
        """
        Sets the path and content entry, making the path's value dict if it does
        not exists already.
        """
        path_value = cast(FileVersionData, self.get(path, default={}))
        path_value[content_hash] = meta
        self.set(path, path_value)

    def most_recent_content(
        self, path: str
    ) -> tuple[str | None, FileVersionMeta | None]:
        """
        Returns the most recent content hash and its meta for a given path
        """
        try:
            candidates = list(self[path].items())
            candidates.sort(key=lambda v: v[1]["mtime"], reverse=True)
            return candidates[0]
        except (KeyError, IndexError):
            return None, None


class PathRequest(DiskDatastore[PathRequestType]):
    """
    Storage of path requests (how we should download/upload a path or not)
    """

    def _validate_key(self, key: str):
        if not key.startswith("/"):
            raise ValueError("PathRequest paths must start with /")

    def resolve_status(self, path: str) -> PathRequestType:
        """
        Tries the path and each of its parents until a status is found.
        """
        path_obj = Path(path)
        while path_obj != path_obj.parent:
            path_config = self.get(str(path_obj))
            if path_config is not None:
                return path_config
            path_obj = path_obj.parent
        # Default is on-demand (to avoid mass downloads on new checkout)
        return "on-demand"


class ContentBackends(DiskDatastore[list[str]]):
    """
    Storage of what backend names each content hash is on
    """
