import hashlib
import os
import time

from .base import BaseOperator


class LocalHasherOperator(BaseOperator):
    """
    Looks for LocalFiles without a content hash, and hashes them.
    """

    log_name = "hasher"
    max_per_loop = 100

    def step(self) -> bool:
        hashed = 0
        to_hash = list(self.config.local_versions.without_content_hashes())
        for i, path in enumerate(to_hash):
            if i > self.max_per_loop:
                break

            with self.config.path_lock.acquire(path) as acquired:
                if not acquired:
                    continue

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
                        pass
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

        return bool(hashed)
