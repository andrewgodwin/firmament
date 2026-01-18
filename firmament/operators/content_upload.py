from ..backends.base import BackendError
from .base import BaseOperator


class ContentUploadOperator(BaseOperator):
    """
    Looks for local Contents that are not in a backend and uploads them.
    """

    log_name = "content-upload"

    def step(self) -> bool:
        uploaded = 0
        # Get the list of everything we have
        local_hashes = self.config.local_versions.all_content_hashes()
        content_locations: dict[str, list[str]] = {}
        # Now for each backend...
        for backend_name, backend in self.config.backends.items():
            # Get the list of everything in the backend
            remote_hashes = backend.content_list()
            # Store it in our local dict for UI reference
            for remote_hash in remote_hashes:
                content_locations.setdefault(remote_hash, []).append(backend_name)
            # Work out what we have that they don't
            missing_hashes = local_hashes.difference(remote_hashes)
            for missing_hash in missing_hashes:
                # Upload it!
                try:
                    local_file_path, local_file_meta = (
                        self.config.local_versions.by_content_hash(missing_hash)
                    )
                except KeyError:
                    self.logger.warning(
                        f"Content {missing_hash} vanished from local database during upload"
                    )
                    continue
                if local_file_path is not None:
                    try:
                        backend.content_upload(
                            missing_hash,
                            self.config.disk_path(local_file_path),
                        )
                    except BackendError as e:
                        self.logger.warning(
                            f"Content {missing_hash} failed upload: {e}"
                        )
                        continue
                    self.logger.debug(
                        f"Uploaded content {missing_hash} to {backend_name}"
                    )
        # Shove our where-are-contents knowledge into the local DB
        self.config.content_backends.set_all(content_locations)
        return uploaded > 0
