from firmament.types import LocalVersionData

from .base import BaseOperator


class LocalScannerOperator(BaseOperator):
    """
    Scans for local files in our root and updates our LocalFile table
    with any discoveries.
    """

    log_name = "local-scanner"

    def step(self) -> bool:
        scanned = 0
        new = 0
        for directory, subdirs, filenames in self.config.root_path.walk():
            if ".firmament" in subdirs:
                subdirs.remove(".firmament")
            for filename in filenames:
                if filename.startswith(".firmament"):
                    continue
                scanned += 1
                # Calculate what this file is and its mtime
                file_path = (directory / filename).resolve()
                relative_file_path = "/" + str(
                    file_path.relative_to(self.config.root_path)
                )
                stat_result = file_path.stat()
                # See if we have a database entry for that, or if it's older
                local_version_data = self.config.local_versions.get(relative_file_path)
                new_version_data: LocalVersionData = {
                    "content_hash": None,
                    "mtime": int(stat_result.st_mtime),
                    "size": stat_result.st_size,
                }
                if local_version_data is None or (
                    local_version_data["mtime"] < new_version_data["mtime"]
                ):
                    self.config.local_versions[relative_file_path] = new_version_data
                    new += 1
        self.logger.debug(f"{scanned} files scanned")
        if new:
            self.logger.info(f"{new} new files discovered")
        return new > 0
