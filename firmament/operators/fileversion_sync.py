from .base import BaseOperator


class FileVersionSyncOperator(BaseOperator):
    """
    Uploads and downloads FileVersion sets from backends.
    """

    log_name = "fileversion-sync"
    interval_short = 5

    def step(self) -> bool:
        new = 0
        local_file_versions = self.config.file_versions.all()
        for backend_name, backend in self.config.backends.items():
            # Download and merge the remote fileversions list
            remote_file_versions = backend.file_version_download()
            for path, contents in remote_file_versions.items():
                for content, metadata in contents.items():
                    if local_file_versions.get(path, {}).get(content) is None:
                        # We don't have this locally
                        self.config.file_versions.set_with_content(
                            path,
                            content,
                            {
                                "mtime": metadata["mtime"],
                                "size": metadata["size"],
                            },
                        )
                        # Update the in-operator cache
                        local_file_versions.setdefault(path, {})[content] = (
                            self.config.file_versions[path][content]
                        )
                        self.logger.debug(f"New remote FileVersion {path}@{content}")
                        new += 1
        # Now upload the merged fileversions
        for backend_name, backend in self.config.backends.items():
            backend.file_version_upload(local_file_versions)
        return new > 0
