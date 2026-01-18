from firmament.types import LocalVersionData

from .base import BaseOperator


class LocalScannerOperator(BaseOperator):
    """
    Scans for local files in our root and updates our LocalFile table
    with any discoveries - and handles any missing files as potential deletions.
    """

    log_name = "local-scanner"

    def step(self) -> bool:
        scanned = 0
        new = 0
        deleted = 0
        seen: set[str] = set()
        for directory, subdirs, filenames in self.config.root_path.walk():
            if ".firmament" in subdirs:
                subdirs.remove(".firmament")
            for filename in filenames:
                if filename.startswith(".firmament"):
                    continue
                scanned += 1
                # Calculate what this file is and its mtime
                file_path = (directory / filename).resolve()
                firmament_path = "/" + str(file_path.relative_to(self.config.root_path))
                stat_result = file_path.stat()
                seen.add(firmament_path)
                # See if we have a database entry for that, or if it's older
                local_version_data = self.config.local_versions.get(firmament_path)
                new_version_data: LocalVersionData = {
                    "content_hash": None,
                    "mtime": int(stat_result.st_mtime),
                    "size": stat_result.st_size,
                }
                if local_version_data is None or (
                    local_version_data["mtime"] < new_version_data["mtime"]
                ):
                    self.config.local_versions[firmament_path] = new_version_data
                    self.logger.debug(f"New file found: {firmament_path}")
                    new += 1
        self.logger.debug(f"{scanned} files scanned")
        deleted_paths = set(self.config.local_versions.keys()) - seen
        for path in deleted_paths:
            deleted += 1
            if self.config.path_requests.resolve_status(path) == "full":
                self.logger.debug(f"File deleted (propagating): {firmament_path}")
                # TODO: Propagate this deletion as a new FileVersion with DELETED_CONTENT_HASH as the content hash value
            else:
                self.logger.debug(f"File deleted (not propagating): {firmament_path}")
        if new:
            self.logger.info(f"{new} new files discovered")
        if deleted:
            self.logger.info(f"{deleted} files found deleted")
        return new > 0 or deleted > 0
