from ..database import FileVersion
from .base import BaseOperator


class FileVersionSyncOperator(BaseOperator):
    """
    Uploads and downloads FileVersion sets from backends.
    """

    log_name = "fileversion-sync"
    interval_short = 5

    def step(self) -> bool:
        new = 0
        with self.config.database.session_factory() as session:
            for backend_name, backend in self.config.backends.items():
                # Download and merge the remote fileversions list
                remote_file_versions = backend.file_version_download()
                local_file_versions = FileVersion.all(session)
                for path, contents in remote_file_versions.items():
                    for content, metadata in contents.items():
                        if local_file_versions.get(path, {}).get(content) is None:
                            # We don't have this locally
                            session.add(
                                FileVersion(
                                    path=path,
                                    content=content,
                                    mtime=metadata["mtime"],
                                    size=metadata["size"],
                                )
                            )
                            local_file_versions.setdefault(path, {})[content] = metadata
                            self.logger.debug(
                                f"New remote FileVersion {path}@{content}"
                            )
                            new += 1
            # Now upload the merged fileversions
            for backend_name, backend in self.config.backends.items():
                backend.file_version_upload(local_file_versions)
            session.commit()
        return new > 0
