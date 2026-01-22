from .base import BaseOperator


class LocalVersionCreationOperator(BaseOperator):
    """
    Looks for hashed LocalFiles that don't have a corresponding FileVersion and creates
    them.
    """

    log_name = "local-version-creation"

    def step(self) -> bool:
        added = 0
        for path, data in self.config.local_versions.not_in_file_versions(
            self.config.file_versions
        ):
            # These shouldn't come through anyway, but it's good for typing
            if data["content_hash"] is None:
                continue
            # Make the new FileVersion
            self.config.file_versions.set_with_content(
                path,
                data["content_hash"],
                {"mtime": data["mtime"], "size": data["size"]},
            )
            added += 1
            self.logger.debug(f"Added file version {path}@{data["content_hash"]}")
        return added > 0
