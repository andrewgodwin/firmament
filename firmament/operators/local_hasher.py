import hashlib
import os
import time

from .base import BaseOperator


class LocalHasherOperator(BaseOperator):
    """
    Looks for LocalFiles without a content hash, and hashes them.
    """

    log_name = "local-hasher"
    max_per_loop = 100

    def step(self) -> bool:
        hashed = 0
        to_hash = list(self.config.local_versions.without_content_hashes())
        for i, path in enumerate(to_hash):
            self.status = "Hashing {path}"
            if i > self.max_per_loop:
                break
            try:
                with open(self.config.disk_path(path), "rb") as fh:
                    content_hash = hashlib.sha256(fh.read()).hexdigest()
                    stat_result = os.stat(fh.fileno())
            except FileNotFoundError:
                del self.config.local_versions[path]
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
