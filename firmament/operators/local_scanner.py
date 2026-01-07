from ..database import LocalFile
from .base import BaseOperator


class LocalScannerOperator(BaseOperator):
    """
    Scans for local files in our root and updates our LocalFile table
    with any discoveries.
    """

    def step(self):
        self.logger.debug("Starting scan")
        scanned = 0
        new = 0
        with self.config.database.session_factory() as session:
            for directory, subdirs, filenames in self.config.root_path.walk():
                if ".firmament" in subdirs:
                    subdirs.remove(".firmament")
                for filename in filenames:
                    if filename.startswith(".firmament"):
                        continue
                    scanned += 1
                    # Calculate what this file is and its mtime
                    file_path = (directory / filename).resolve()
                    relative_file_path = str(
                        file_path.relative_to(self.config.root_path)
                    )
                    stat_result = file_path.stat()
                    mtime = int(stat_result.st_mtime)
                    size = stat_result.st_size
                    # See if we have a database entry for that, or if it's older
                    local_file = LocalFile.by_path(session, relative_file_path)
                    if local_file is None:
                        LocalFile.insert_new(session, relative_file_path, mtime, size)
                        print("insert-new", relative_file_path)
                        new += 1
                    elif local_file.mtime < mtime:
                        local_file.update_new(mtime, size)
                        print("update-new", relative_file_path)
                        new += 1
            session.commit()
        self.logger.info(f"{scanned} files scanned, {new} files marked for update")
