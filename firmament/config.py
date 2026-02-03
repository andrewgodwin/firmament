import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import AfterValidator, BaseModel
from pydantic.types import PathType

from firmament.backends.base import BaseBackend
from firmament.datastore import (
    ContentBackends,
    FileVersion,
    LocalVersion,
    OperatorStatus,
    PathRequest,
)

DirectoryPath = Annotated[
    Path, AfterValidator(lambda v: v.expanduser()), PathType("dir")
]
FilePath = Annotated[Path, AfterValidator(lambda v: v.expanduser()), PathType("file")]


class BackendSchema(BaseModel):

    type: str
    encryption_key: str | None = None
    options: dict[str, Any]


class PathSchema(BaseModel):

    on_demand: bool | None = None


class ConfigSchema(BaseModel):

    backends: dict[str, BackendSchema]
    paths: dict[str, PathSchema] = {}


class ResourceLock:
    """
    Thread-safe resource locking for coordinating exclusive access across operators.

    Used for both file paths and content hashes.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._locked: set[str] = set()

    def is_locked(self, key: str) -> bool:
        """Peek: check if resource is locked without acquiring."""
        with self._lock:
            return key in self._locked

    def try_acquire(self, key: str) -> bool:
        """
        Try to acquire lock.

        Returns True if acquired, False if already locked.
        """
        with self._lock:
            if key in self._locked:
                return False
            self._locked.add(key)
            return True

    def release(self, key: str) -> None:
        """
        Release lock on resource.
        """
        with self._lock:
            self._locked.discard(key)

    @contextmanager
    def acquire(self, key: str) -> Iterator[bool]:
        """
        Context manager for resource locking.

        Yields True if lock acquired, False if already locked.
        Automatically releases on exit.

        Usage:
            with lock.acquire(key) as acquired:
                if not acquired:
                    continue  # skip, someone else has it
                # do work
        """
        acquired = self.try_acquire(key)
        try:
            yield acquired
        finally:
            if acquired:
                self.release(key)


class Config:
    """
    Config file parser.
    """

    backends: dict[str, BaseBackend]

    def __init__(self, root_path: Path):
        # Calculate paths
        self.root_path = root_path.resolve()
        self.meta_path = self.root_path / ".firmament"
        self.config_path = self.meta_path / "config"
        self.datastore_path = self.meta_path / "datastore"

        # Read main config in
        with open(self.config_path) as fh:
            self.config_data = ConfigSchema(**yaml.safe_load(fh.read()))

        # Set up backend class instances
        self.backends = {}
        for name, backend_config in self.config_data.backends.items():
            backend_class = BaseBackend.implementation_get(backend_config.type)
            self.backends[name] = backend_class(
                name=name,
                encryption_key=backend_config.encryption_key,
                **backend_config.options,
            )

        # Set up datastores
        self.local_versions = LocalVersion(self.datastore_path / "local_versions")
        self.file_versions = FileVersion(self.datastore_path / "file_versions")
        self.path_requests = PathRequest(self.datastore_path / "path_requests")
        self.content_backends = ContentBackends(
            self.datastore_path / "content_backends"
        )
        self.operator_statuses = OperatorStatus(self.datastore_path / "operator_status")

        # Set up locks for operator coordination
        self.path_lock = ResourceLock()
        self.content_lock = ResourceLock()

    def disk_path(self, path: str) -> Path:
        """
        Convert a virtual path (starting with /) to an absolute disk path.
        """
        return self.root_path / path.lstrip("/")
