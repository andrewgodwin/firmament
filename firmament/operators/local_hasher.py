import hashlib
import os
import threading
import time

from .base import BaseOperator


class LocalHasherOperator(BaseOperator):
    """
    Looks for LocalFiles without a content hash, and hashes them.
    """

    log_name = "local-hasher"
    max_per_loop = 100

    # Class-level lock to coordinate between multiple hasher instances
    _hashing_lock = threading.Lock()
    _hashing_paths: set[str] = set()

    def _try_acquire_path(self, path: str) -> bool:
        """
        Try to acquire exclusive access to hash a path.

        Returns True if acquired.
        """
        with self._hashing_lock:
            if path in self._hashing_paths:
                return False
            self._hashing_paths.add(path)
            return True

    def _release_path(self, path: str) -> None:
        """
        Release the lock on a path after hashing is complete.
        """
        with self._hashing_lock:
            self._hashing_paths.discard(path)

    def step(self) -> bool:
        hashed = 0
        to_hash = list(self.config.local_versions.without_content_hashes())
        for i, path in enumerate(to_hash):
            if i > self.max_per_loop:
                break
            # Skip if another hasher is already working on this file
            if not self._try_acquire_path(path):
                continue
            try:
                self.status = f"Hashing {path}"
                try:
                    with open(self.config.disk_path(path), "rb") as fh:
                        content_hash = hashlib.sha256(fh.read()).hexdigest()
                        stat_result = os.stat(fh.fileno())
                except FileNotFoundError:
                    try:
                        del self.config.local_versions[path]
                    except KeyError:
                        # Someone else already got it
                        continue
                    self.logger.debug(f"Removed vanished file {path}")
                    continue
                self.config.local_versions[path] = {
                    "content_hash": content_hash,
                    "size": stat_result.st_size,
                    "mtime": int(stat_result.st_mtime),
                    "last_hashed": int(time.time()),
                }
                hashed += 1
                self.logger.debug(f"Hashed file {path} as {content_hash}")
            finally:
                self._release_path(path)
        return bool(hashed)
