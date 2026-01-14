import os
from pathlib import Path

from ..database import FileVersion, LocalFile
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
        with self.config.database.session_factory() as session:
            potential_paths = FileVersion.paths_without_local(session)
            for path in potential_paths:
                if created > self.max_per_loop:
                    break
                # Should we even sync this path?
                if not self.config.path_is_on_demand(Path(path)):
                    # TODO: Look for a marker file
                    continue
                # Find the most recent file version
                file_version = FileVersion.most_recent_with_path(session, path)
                if file_version is None:
                    continue
                # Download the content to a temporary file
                final_destination = self.config.root_path / path
                temporary_destination = final_destination.with_name(
                    f".firmament-temp.{final_destination.name}"
                )
                # TODO: Go through backends in download priority order
                for backend in self.config.backends.values():
                    if backend.content_exists(file_version.content):
                        self.logger.debug(
                            f"Downloading {file_version.content} to {temporary_destination}"
                        )
                        backend.content_download(
                            file_version.content, temporary_destination
                        )
                        os.utime(
                            temporary_destination,
                            (file_version.mtime, file_version.mtime),
                        )
                        break
                else:
                    self.logger.warn(
                        f"Cannot download content {file_version.content} for {file_version.path} - not available on any backend"
                    )
                    continue
                # Make a LocalFile and move the temporary file into place
                session.add(
                    LocalFile(
                        path=file_version.path,
                        content=file_version.content,
                        mtime=file_version.mtime,
                        size=file_version.size,
                    )
                )
                temporary_destination.rename(final_destination)
                self.logger.debug(f"Downloaded {final_destination}")
                created += 1
            session.commit()
        return created > 0
