from .base import BaseOperator


class DownloadOnceCleanupOperator(BaseOperator):
    """
    Cleans up DOWNLOAD_ONCE path requests once all files under that path have been
    downloaded (i.e., have a LocalVersion).
    """

    log_name = "download-once-cleanup"
    interval_short = 1

    def step(self) -> bool:
        cleaned = 0
        # Find all DOWNLOAD_ONCE path requests
        for path, request_type in self.config.path_requests.items():
            if request_type != "download-once":
                continue
            # Check if all FileVersions starting with this path have a LocalVersion
            all_downloaded = True
            path_prefix = path + "/"
            for file_path in self.config.file_versions.keys():
                # Match exact path or paths under this directory
                if file_path == path or file_path.startswith(path_prefix):
                    if file_path not in self.config.local_versions:
                        all_downloaded = False
                        break
            if all_downloaded:
                self.logger.debug(
                    f"All files under {path} downloaded, removing DOWNLOAD_ONCE request"
                )
                del self.config.path_requests[path]
                cleaned += 1
        return cleaned > 0
