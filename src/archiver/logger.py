"""
Logging setup for the Camera Archiver application.
"""

import logging
import shutil
import sys

from .config import Config
from .utils import ACTIVE_PROGRESS_REPORTER, OUTPUT_LOCK


class ThreadSafeStreamHandler(logging.StreamHandler):
    """A StreamHandler that uses a lock for thread-safe output"""

    def emit(self, record):
        with OUTPUT_LOCK:  # Use global lock to coordinate with progress updates
            # If there's an active progress bar, clear the line first
            global ACTIVE_PROGRESS_REPORTER
            if ACTIVE_PROGRESS_REPORTER is not None:
                # Clear the current progress line by writing spaces and then the log message
                sys.stderr.write(
                    "\r" + " " * 80 + "\r"
                )  # Clear the line (80 chars should be enough)
                sys.stderr.flush()
            super().emit(record)


class Logger:
    """Simplified logging setup with strict typing"""

    @staticmethod
    def setup(config: Config) -> logging.Logger:
        logger = logging.getLogger("camera_archiver")
        logger.setLevel(logging.INFO)

        # Clear existing handlers
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

        fmt = "%(asctime)s - %(levelname)s - %(message)s"

        # File handler with rotation
        if config.log_file:
            try:
                # Try to create the directory structure first if it doesn't exist
                # This handles the case where parent directories don't exist
                config.log_file.parent.mkdir(parents=True, exist_ok=True)

                # Now try to set up rotation and file handler
                Logger._rotate_log_file(config.log_file)
                fh = logging.FileHandler(config.log_file, encoding="utf-8")
                fh.setFormatter(logging.Formatter(fmt))
                logger.addHandler(fh)
            except (OSError, AttributeError):
                # Handle cases like:
                # - OSError when directory doesn't exist or is not writable
                # - AttributeError when config.log_file is a mock object without proper Path methods
                pass

        # Console handler with thread safety
        sh = ThreadSafeStreamHandler(sys.stderr)
        sh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(sh)

        return logger

    @staticmethod
    def _rotate_log_files(log_file_path) -> None:
        """Rotate log files: archiver.log -> archiver.log.0, archiver.log.0 -> archiver.log.1, etc.

        This method implements the new rotation logic where:
        1. Every invocation logs to archiver.log in the input directory
        2. If there's an existing archiver.log, move it to archiver.log.0
        3. If there are existing numbered logs (archiver.log.0 to archiver.log.9), shift them up by one
        4. If there's a naming conflict (e.g., archiver.log.9 exists), delete the largest numbered log
        """
        # First, remove archiver.log.9 if it exists (the highest number we support)
        max_log_path = log_file_path.with_suffix(f"{log_file_path.suffix}.9")
        if max_log_path.exists():
            max_log_path.unlink()  # Remove the file

        # Shift all existing log files up by one number (8->9, 7->8, ..., 0->1)
        for i in range(8, -1, -1):  # From 8 down to 0
            current_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i}")
            next_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i + 1}")

            if current_path.exists():
                # Move current file to next number
                shutil.move(str(current_path), str(next_path))

        # Finally, move the current archiver.log to archiver.log.0
        if log_file_path.exists():
            backup_path = log_file_path.with_suffix(f"{log_file_path.suffix}.0")
            shutil.move(str(log_file_path), str(backup_path))

    @staticmethod
    def _create_new_log_file(log_file_path) -> None:
        """Create new log file if it doesn't exist."""
        try:
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            log_file_path.touch()
        except (OSError, AttributeError):
            # Handle cases where we can't create directory or file
            # This could happen if log_file_path is a MagicMock or path doesn't exist
            pass

    @staticmethod
    def _rotate_log_file(log_file_path) -> None:
        """Rotate log file - every invocation logs to archiver.log, rotating existing files"""
        # Always rotate the log files regardless of size
        # This implements the new logic where every invocation gets its own log file
        if log_file_path.exists():
            Logger._rotate_log_files(log_file_path)

        # Always create a new empty log file after rotation
        Logger._create_new_log_file(log_file_path)
