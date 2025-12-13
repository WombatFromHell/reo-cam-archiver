"""
Progress reporting for the Camera Archiver application.
"""

import sys
import threading
import time

from .graceful_exit import GracefulExit
from .utils import OUTPUT_LOCK


class ProgressReporter:
    """Simplified progress reporting with strict typing"""

    def __init__(
        self, total_files: int, graceful_exit: GracefulExit, silent: bool = False
    ):
        self.total: int = total_files
        self.graceful_exit: GracefulExit = graceful_exit
        self.silent: bool = silent
        self.current: int = 0
        self.start_time: float = time.time()
        self.current_file_start_time: float = time.time()
        self._lock: threading.Lock = threading.Lock()

    def start_file(self) -> None:
        with self._lock:
            self.current += 1
            self.current_file_start_time = time.time()

    def format_time(self, elapsed):
        """Format time with hours only when needed.

        Args:
            elapsed: Elapsed time in seconds

        Returns:
            Formatted time string in 'MM:SS' format if hours <= 0,
            or 'HH:MM:SS' format if hours > 0
        """
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        return (
            f"{hours:02}:{minutes:02}:{seconds:02}"
            if hours > 0
            else f"{minutes:02}:{seconds:02}"
        )

    def update_progress(self, pct: float) -> None:
        if self.silent or self.graceful_exit.should_exit():
            return

        with OUTPUT_LOCK:  # Use global lock to coordinate with logging
            with self._lock:
                total_elapsed = time.time() - self.start_time
                file_elapsed = time.time() - self.current_file_start_time

                total_elapsed_str = self.format_time(total_elapsed)
                file_elapsed_str = self.format_time(file_elapsed)
                bar_length = 20
                filled = int(bar_length * pct / 100)
                bar = "|" * filled + "-" * (bar_length - filled)

                # If this is 100%, add a newline to separate from subsequent logs
                if pct >= 100.0:
                    sys.stderr.write(
                        f"\rProgress [{self.current}/{self.total}]: {pct:.0f}% [{bar}] {file_elapsed_str} ({total_elapsed_str})\n"
                    )
                    sys.stderr.flush()
                else:
                    sys.stderr.write(
                        f"\rProgress [{self.current}/{self.total}]: {pct:.0f}% [{bar}] {file_elapsed_str} ({total_elapsed_str})"
                    )
                    sys.stderr.flush()

    def finish_file(self) -> None:
        self.update_progress(100.0)

    def finish(self) -> None:
        if not self.silent:
            with OUTPUT_LOCK:  # Use global lock to coordinate with logging
                sys.stderr.write("\n")
                sys.stderr.flush()

    def __enter__(self):
        global ACTIVE_PROGRESS_REPORTER
        ACTIVE_PROGRESS_REPORTER = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global ACTIVE_PROGRESS_REPORTER
        ACTIVE_PROGRESS_REPORTER = None
        self.finish()
