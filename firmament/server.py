import logging
import sys
import time
from collections import Counter

from firmament.config import Config
from firmament.constants import (
    BOLD,
    GREEN,
    OPERATOR_COLORS,
    RESET,
    WHITE,
    YELLOW,
)
from firmament.operators.base import BaseOperator
from firmament.operators.content_upload import ContentUploadOperator
from firmament.operators.download_once_cleanup import DownloadOnceCleanupOperator
from firmament.operators.fileversion_sync import FileVersionSyncOperator
from firmament.operators.local_create import LocalCreateOperator
from firmament.operators.local_hasher import LocalHasherOperator
from firmament.operators.local_scanner import LocalScannerOperator
from firmament.operators.local_version_creation import LocalVersionCreationOperator

logger = logging.getLogger(__name__)


class Server:
    """
    Main server.

    Runs a series of operator loops.
    """

    operators: list[type[BaseOperator]] = [
        LocalScannerOperator,
        LocalHasherOperator,
        LocalVersionCreationOperator,
        ContentUploadOperator,
        FileVersionSyncOperator,
        LocalCreateOperator,
        DownloadOnceCleanupOperator,
    ]

    def __init__(self, config: Config):
        self.config = config
        self.operator_instances: list[BaseOperator] = []

    def run(self):
        """
        Main daemon loop.
        """
        logging.debug("Main loop starting")

        # Create a thread per operator and start it
        self.operator_instances = []
        for operator in self.operators:
            self.operator_instances.append(operator(self.config))
        [thread.start() for thread in self.operator_instances]

        # Wait for a shutdown signal
        logging.info("Running. Ctrl-C to exit.")
        try:
            self._status_loop()
        except KeyboardInterrupt:
            self._clear_status_lines()

    def _status_loop(self):
        """
        Display live status lines for each operator.
        """
        last_line_count = 0
        while True:
            lines = []

            # Build summary status line
            summary = self._build_summary_line()
            lines.append(summary)

            # Build operator status lines
            for i, op in enumerate(self.operator_instances):
                if op.status is not None:
                    color = OPERATOR_COLORS[i % len(OPERATOR_COLORS)]
                    lines.append(f"{color}{op.log_name}{RESET}: {op.status}")

            # Move cursor up to overwrite previous lines
            if last_line_count > 0:
                sys.stdout.write(f"\033[{last_line_count}A")

            # Clear and write each line
            for line in lines:
                sys.stdout.write("\033[2K" + line + "\n")

            # Clear any extra lines from previous render
            for _ in range(last_line_count - len(lines)):
                sys.stdout.write("\033[2K\n")

            # Move back up if we printed extra clearing lines
            extra = last_line_count - len(lines)
            if extra > 0:
                sys.stdout.write(f"\033[{extra}A")

            sys.stdout.flush()
            last_line_count = len(lines)
            time.sleep(0.1)

    def _build_summary_line(self) -> str:
        """
        Build a summary status line showing file and content counts.
        """
        # Count local files and contents
        num_files = len(self.config.local_versions)
        num_contents = len(self.config.local_versions.all_content_hashes())

        # Count contents per backend
        backend_counts: Counter[str] = Counter()
        for backend_list in self.config.content_backends.values():
            for backend_name in backend_list:
                backend_counts[backend_name] += 1

        # Format backend counts
        backend_parts = []
        for name in sorted(self.config.backends.keys()):
            count = backend_counts.get(name, 0)
            backend_parts.append(f"{YELLOW}{name}{RESET}: {count}")

        backends_str = ", ".join(backend_parts) if backend_parts else "none"

        return (
            f"{BOLD}{WHITE}Files:{RESET} {GREEN}{num_files}{RESET} | "
            f"{BOLD}{WHITE}Contents:{RESET} {GREEN}{num_contents}{RESET} | "
            f"{BOLD}{WHITE}Backends:{RESET} [{backends_str}]"
        )

    def _clear_status_lines(self):
        """
        Clear all status lines on exit.
        """
        # This is handled by the final state of _status_loop
        pass
