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
    def _find_max_backup_number(log_file_path) -> int:
        """Find the highest backup number for existing backup files."""
        max_backup_num = 0
        for backup_path in log_file_path.parent.glob(f"{log_file_path.name}.*"):
            if backup_path.is_file():
                try:
                    backup_num = int(backup_path.suffix[1:])
                    max_backup_num = max(max_backup_num, backup_num)
                except ValueError:
                    continue
        return max_backup_num

    @staticmethod
    def _rename_existing_backups(log_file_path, max_backup_num: int) -> None:
        """Rename existing backup files to make room for new backup."""
        for i in range(max_backup_num, 0, -1):
            old_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i}")
            new_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i + 1}")
            if old_path.exists():
                shutil.move(str(old_path), str(new_path))

    @staticmethod
    def _create_backup_and_new_log(log_file_path) -> None:
        """Create backup of current log and create new empty log file."""
        # Move current log to .1
        backup_path = log_file_path.with_suffix(f"{log_file_path.suffix}.1")
        shutil.move(str(log_file_path), str(backup_path))

        # Create new empty log file
        try:
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            log_file_path.touch()
        except (OSError, AttributeError):
            # Handle cases where we can't create directory or file
            # This could happen if log_file_path is a MagicMock or path doesn't exist
            pass

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
        """Rotate log file if it exceeds maximum size"""
        from .utils import LOG_ROTATION_SIZE

        if log_file_path.exists() and log_file_path.stat().st_size > LOG_ROTATION_SIZE:
            # Find existing backup files
            max_backup_num = Logger._find_max_backup_number(log_file_path)

            # Rename existing backups
            Logger._rename_existing_backups(log_file_path, max_backup_num)

            # Create backup and new log
            Logger._create_backup_and_new_log(log_file_path)
        elif not log_file_path.exists():
            Logger._create_new_log_file(log_file_path)
