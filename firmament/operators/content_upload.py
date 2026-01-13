from typing import cast

from ..backends.base import BackendError
from ..database import LocalFile
from .base import BaseOperator


class ContentUploadOperator(BaseOperator):
    """
    Looks for local Contents that are not in a backend and uploads them.
    """

    interval = 10

    def step(self) -> bool:
        uploaded = 0
        with self.config.database.session_factory() as session:
            # Get the list of everything we have
            # TODO: More efficient?
            local_hashes = LocalFile.all_contents(session)
            # Now for each backend...
            for backend_name, backend in self.config.backends.items():
                # Get the list of everything in the backend
                remote_hashes = backend.content_list()
                # Work out what we have that they don't
                missing_hashes = local_hashes.difference(remote_hashes)
                for missing_hash in missing_hashes:
                    # Upload it!
                    local_file = LocalFile.by_content(session, missing_hash)
                    if local_file is not None:
                        try:
                            backend.content_upload(
                                cast(str, local_file.content),
                                self.config.root_path / local_file.path,
                            )
                        except BackendError as e:
                            self.logger.warning(f"Failed to upload {missing_hash}: {e}")
                            continue
                        self.logger.debug(
                            f"Uploaded content {missing_hash} to {backend_name}"
                        )
        return uploaded > 0
