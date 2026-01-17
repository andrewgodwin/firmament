import os
from pathlib import Path

from .base import BaseOperator


class LocalCreateOperator(BaseOperator):
    """
    Creates local files when we have a FileVersion that should have one
    """

    log_name = "local-create"
    interval_short = 0.5
    max_per_loop = 20

    def step(self) -> bool:
        created = 0
        # Calculate which FileVersion paths do not have a LocalVersion
        potential_paths = set(self.config.file_versions.keys())
        potential_paths.difference_update(self.config.local_versions.keys())
        for path in potential_paths:
            if created > self.max_per_loop:
                break
            # Should we even sync this path?
            path_status = self.config.path_requests.resolve_status(Path(path))
            if path_status == "on-demand" or path_status == "ignore":
                continue
            # Find the most recent file version
            most_recent_content, most_recent_meta = (
                self.config.file_versions.most_recent_content(path)
            )
            if most_recent_content is None or most_recent_meta is None:
                continue
            # Download the content to a temporary file
            final_destination = self.config.disk_path(path)
            final_destination.parent.mkdir(parents=True, exist_ok=True)
            temporary_destination = final_destination.with_name(
                f".firmament-temp.{final_destination.name}"
            )
            # TODO: Go through backends in download priority order
            for backend in self.config.backends.values():
                if backend.content_exists(most_recent_content):
                    self.logger.debug(
                        f"Downloading {most_recent_content} to {temporary_destination}"
                    )
                    backend.content_download(most_recent_content, temporary_destination)
                    os.utime(
                        temporary_destination,
                        (most_recent_meta["mtime"], most_recent_meta["mtime"]),
                    )
                    break
            else:
                self.logger.warn(
                    f"Cannot download content {most_recent_content} for {path} - not available on any backend"
                )
                continue
            # Create a LocalVersion with an empty content hash (so it's rechecked)
            self.config.local_versions[path] = {
                "content_hash": None,
                "mtime": most_recent_meta["mtime"],
                "size": most_recent_meta["size"],
            }
            # Move the temporary file into place
            temporary_destination.rename(final_destination)
            self.logger.debug(f"Downloaded {final_destination}")
            created += 1
        return created > 0
