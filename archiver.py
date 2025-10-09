#!/usr/bin/env python3
"""
Camera Archiver: Transcodes and archives camera footage based on timestamp parsing,
with intelligent cleanup based on size and age thresholds.
"""

import argparse
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Constants
MIN_ARCHIVE_SIZE_BYTES = 1_048_576  # 1MB
DEFAULT_PROGRESS_WIDTH = 30
PROGRESS_UPDATE_INTERVAL = 5  # seconds for non-TTY output
LOG_ROTATION_SIZE = 4_194_304  # 4MB (4096KB) in bytes

# Global lock for coordinating logging and progress updates
OUTPUT_LOCK = threading.Lock()

# Global reference to the active progress reporter to allow clearing
ACTIVE_PROGRESS_REPORTER = None


# Type Definitions
FilePath = Path
Timestamp = datetime
FileSize = int
ProgressCallback = Callable[[float], None]


class Config:
    """Configuration holder with strict typing"""

    def __init__(self, args: argparse.Namespace):
        self.directory: FilePath = Path(args.directory)
        self.output: FilePath = (
            Path(args.output) if args.output else self.directory / "archived"
        )
        self.dry_run: bool = args.dry_run
        self.no_confirm: bool = args.no_confirm
        self.no_skip: bool = args.no_skip
        self.delete: bool = args.delete
        self.trash_root: Optional[FilePath] = (
            None
            if args.delete  # If delete flag is set, don't use trash regardless
            else Path(args.trash_root)
            if args.trash_root
            else (self.directory / ".deleted")
        )
        self.cleanup: bool = args.cleanup
        self.clean_output: bool = args.clean_output
        self.age: int = args.age
        self.log_file: Optional[FilePath] = (
            Path(args.log_file) if args.log_file else self.directory / "archiver.log"
        )


class GracefulExit:
    """Thread-safe flag for graceful exit handling"""

    def __init__(self):
        self._exit_requested = False
        self._lock = threading.Lock()

    def request_exit(self) -> None:
        with self._lock:
            self._exit_requested = True

    def should_exit(self) -> bool:
        with self._lock:
            return self._exit_requested


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

    def update_progress(self, pct: float) -> None:
        if self.silent or self.graceful_exit.should_exit():
            return

        with OUTPUT_LOCK:  # Use global lock to coordinate with logging
            with self._lock:
                total_elapsed = time.time() - self.start_time
                file_elapsed = time.time() - self.current_file_start_time

                # Format time with hours only when needed
                def format_time(elapsed):
                    hours = int(elapsed // 3600)
                    minutes = int((elapsed % 3600) // 60)
                    seconds = int(elapsed % 60)
                    if hours > 0:
                        return f"{hours:02}:{minutes:02}:{seconds:02}"
                    else:
                        return f"{minutes:02}:{seconds:02}"

                total_elapsed_str = format_time(total_elapsed)
                file_elapsed_str = format_time(file_elapsed)
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
            Logger._rotate_log_file(config.log_file)
            fh = logging.FileHandler(config.log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter(fmt))
            logger.addHandler(fh)

        # Console handler with thread safety
        sh = ThreadSafeStreamHandler(sys.stderr)
        sh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(sh)

        return logger

    @staticmethod
    def _rotate_log_file(log_file_path: FilePath) -> None:
        """Rotate log file if it exceeds maximum size"""
        if log_file_path.exists() and log_file_path.stat().st_size > LOG_ROTATION_SIZE:
            # Find existing backup files
            max_backup_num = 0
            for backup_path in log_file_path.parent.glob(f"{log_file_path.name}.*"):
                if backup_path.is_file():
                    try:
                        backup_num = int(backup_path.suffix[1:])
                        max_backup_num = max(max_backup_num, backup_num)
                    except ValueError:
                        continue

            # Rename existing backups
            for i in range(max_backup_num, 0, -1):
                old_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i}")
                new_path = log_file_path.with_suffix(f"{log_file_path.suffix}.{i + 1}")
                if old_path.exists():
                    shutil.move(str(old_path), str(new_path))

            # Move current log to .1
            backup_path = log_file_path.with_suffix(f"{log_file_path.suffix}.1")
            shutil.move(str(log_file_path), str(backup_path))

            # Create new empty log file
            log_file_path.touch()
        elif not log_file_path.exists():
            log_file_path.touch()


class FileDiscovery:
    """Handles file discovery operations with strict typing"""

    @staticmethod
    def discover_files(
        directory: FilePath,
        trash_root: Optional[FilePath] = None,
        output_directory: Optional[FilePath] = None,
        clean_output: bool = False,
    ) -> Tuple[
        List[Tuple[FilePath, Timestamp]], Dict[str, Dict[str, FilePath]], Set[FilePath]
    ]:
        """Discover camera files with valid timestamps"""
        mp4s: List[Tuple[FilePath, Timestamp]] = []
        mapping: Dict[str, Dict[str, FilePath]] = {}
        trash_files: Set[FilePath] = set()

        # Scan base directory
        for p in directory.rglob("*.*"):
            if not p.is_file() or (trash_root and trash_root in p.parents):
                continue

            # Check directory structure: /camera/<YYYY>/<MM>/<DD>/*.*
            try:
                rel_parts = p.relative_to(directory).parts
                if len(rel_parts) < 4:
                    continue

                y, m, d = rel_parts[-4], rel_parts[-3], rel_parts[-2]
                y_int, m_int, d_int = int(y), int(m), int(d)

                if not (
                    1000 <= y_int <= 9999 and 1 <= m_int <= 12 and 1 <= d_int <= 31
                ):
                    continue
            except (ValueError, AttributeError):
                continue

            ts = FileDiscovery._parse_timestamp(p.name)
            if not ts:
                continue

            key = ts.strftime("%Y%m%d%H%M%S")
            ext = p.suffix.lower()
            mapping.setdefault(key, {})[ext] = p
            if ext == ".mp4":
                mp4s.append((p, ts))

        # Scan output directory if clean_output is specified
        if clean_output and output_directory and output_directory.exists():
            for p in output_directory.rglob("*.*"):
                if not p.is_file() or (trash_root and trash_root in p.parents):
                    continue

                # Check directory structure: /camera/archived/<YYYY>/<MM>/<DD>/*.*
                try:
                    rel_parts = p.relative_to(output_directory).parts
                    if len(rel_parts) < 4:
                        continue

                    y, m, d = rel_parts[-4], rel_parts[-3], rel_parts[-2]
                    y_int, m_int, d_int = int(y), int(m), int(d)

                    if not (
                        1000 <= y_int <= 9999 and 1 <= m_int <= 12 and 1 <= d_int <= 31
                    ):
                        continue
                except (ValueError, AttributeError):
                    continue

                ts = FileDiscovery._parse_timestamp(p.name)
                if not ts:
                    # Also try to parse timestamp from archived files that have 'archived-' prefix
                    ts = FileDiscovery._parse_timestamp_from_archived_filename(p.name)
                    if not ts:
                        continue

                key = ts.strftime("%Y%m%d%H%M%S")
                ext = p.suffix.lower()
                mapping.setdefault(key, {})[ext] = p
                if ext == ".mp4":
                    mp4s.append((p, ts))

        # Scan trash directory if enabled
        if trash_root and trash_root.exists():
            for trash_type in ["input", "output"]:
                trash_dir = trash_root / trash_type
                if trash_dir.exists():
                    for p in trash_dir.rglob("*.*"):
                        if not p.is_file():
                            continue

                        ts = FileDiscovery._parse_timestamp(p.name)
                        if not ts:
                            continue

                        key = ts.strftime("%Y%m%d%H%M%S")
                        ext = p.suffix.lower()
                        mapping.setdefault(key, {})[ext] = p
                        trash_files.add(p)
                        if ext == ".mp4":
                            mp4s.append((p, ts))

        return mp4s, mapping, trash_files

    @staticmethod
    def _parse_timestamp(filename: str) -> Optional[Timestamp]:
        """Extract timestamp from filename"""
        TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = TIMESTAMP_RE.search(filename)
        if not m:
            return None

        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None

    @staticmethod
    def _parse_timestamp_from_archived_filename(filename: str) -> Optional[Timestamp]:
        """Extract timestamp from archived filename (e.g., archived-20230115120000.mp4)"""
        # Pattern to match archived files: archived-YYYYMMDDHHMMSS.ext
        ARCHIVED_RE = re.compile(r"archived-(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = ARCHIVED_RE.search(filename)
        if not m:
            return None

        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None


class FileManager:
    """Handles file operations with strict typing"""

    @staticmethod
    def remove_file(
        file_path: FilePath,
        logger: logging.Logger,
        dry_run: bool = False,
        delete: bool = False,
        trash_root: Optional[FilePath] = None,
        is_output: bool = False,
        source_root: Optional[FilePath] = None,
    ) -> None:
        """Remove a file, optionally moving to trash"""
        if dry_run:
            logger.info(f"[DRY RUN] Would remove {file_path}")
            return

        try:
            if source_root is None:
                source_root = file_path.parent

            if not delete and trash_root:  # Use trash by default unless delete is True
                new_dest = FileManager._calculate_trash_destination(
                    file_path, source_root, trash_root, is_output
                )
                new_dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(new_dest))
                logger.info(f"Moved to trash: {file_path} -> {new_dest}")
            else:
                if file_path.is_file():
                    file_path.unlink()
                elif file_path.is_dir():
                    file_path.rmdir()
                else:
                    logger.warning(f"Unsupported file type for removal: {file_path}")
                logger.info(f"Removed: {file_path}")
        except Exception as e:
            logger.error(f"Failed to remove {file_path}: {e}")

    @staticmethod
    def _calculate_trash_destination(
        file_path: FilePath,
        source_root: FilePath,
        trash_root: FilePath,
        is_output: bool = False,
    ) -> FilePath:
        """Calculate the destination path in trash for a given file"""
        dest_sub = "output" if is_output else "input"
        try:
            rel_path = file_path.relative_to(source_root)
        except ValueError:
            rel_path = Path(file_path.name)

        base_dest = trash_root / dest_sub / rel_path
        counter = 0
        new_dest = base_dest

        while new_dest.exists():
            counter += 1
            suffix = f"_{int(time.time())}_{counter}"
            stem = new_dest.stem + suffix
            new_dest = new_dest.parent / (stem + new_dest.suffix)

        return new_dest

    @staticmethod
    def clean_empty_directories(directory: FilePath, logger: logging.Logger) -> None:
        """Remove empty date-structured directories"""
        for dirpath, dirs, files in os.walk(directory, topdown=False):
            p = Path(dirpath)
            if p == directory:
                continue

            try:
                # Check if directory is empty
                if not any(p.iterdir()):
                    p.rmdir()
                    logger.info(f"Removed empty directory: {p}")
            except OSError:
                pass


class Transcoder:
    """Handles video transcoding operations with strict typing"""

    @staticmethod
    def get_video_duration(file_path: FilePath) -> Optional[float]:
        """Get video duration using ffprobe"""
        if not shutil.which("ffprobe"):
            return None

        try:
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(file_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration_str = result.stdout.strip()
            if duration_str and duration_str != "N/A":
                return float(duration_str)
            return None
        except Exception:
            return None

    @staticmethod
    def transcode_file(
        input_path: FilePath,
        output_path: FilePath,
        logger: logging.Logger,
        progress_cb: Optional[ProgressCallback] = None,
        graceful_exit: Optional[GracefulExit] = None,
    ) -> bool:
        """Transcode a video file using ffmpeg with QSV hardware acceleration"""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-hwaccel",
            "qsv",
            "-hwaccel_output_format",
            "qsv",
            "-y",
            "-i",
            str(input_path),
            "-vf",
            "scale_qsv=w=1024:h=768:mode=hq",
            "-global_quality",
            "26",
            "-c:v",
            "h264_qsv",
            "-an",
            str(output_path),
        ]

        # Get video duration
        total_duration = Transcoder.get_video_duration(input_path)
        debug_ffmpeg = logger.isEnabledFor(logging.DEBUG)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError as e:
            logger.error(f"Failed to start ffmpeg process: {e}")
            return False

        log_lines: List[str] = []
        prev_pct = -1.0
        cur_pct = 0.0

        try:
            if proc.stdout is None:
                logger.error("Failed to capture ffmpeg output")
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                return False

            stdout_iter = iter(proc.stdout.readline, "")

            for line in stdout_iter:
                if graceful_exit.should_exit():
                    logger.info("Cancellation requested, terminating ffmpeg process...")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    return False

                if not line:
                    break

                if debug_ffmpeg:
                    logger.debug(f"FFmpeg output: {line.strip()}")

                log_lines.append(line)

                if total_duration and total_duration > 0:
                    time_match = re.search(r"time=([0-9:.]+)", line)
                    if time_match:
                        time_str = time_match.group(1)
                        if ":" in time_str:
                            h, mn, s = map(float, time_str.split(":")[:3])
                            elapsed_seconds = h * 3600 + mn * 60 + s
                        else:
                            elapsed_seconds = float(time_str)
                        cur_pct = min(elapsed_seconds / total_duration * 100, 100.0)
                else:
                    cur_pct = min(cur_pct + 0.5, 99.0)

                if progress_cb and cur_pct != prev_pct:
                    progress_cb(cur_pct)
                    prev_pct = cur_pct

            rc = proc.wait()
            if rc != 0 and not graceful_exit.should_exit():
                msg = (
                    f"FFmpeg failed (code {rc}) for {input_path} -> {output_path}\n"
                    + "".join(log_lines)
                )
                logger.error(msg)

            return rc == 0 and not graceful_exit.should_exit()
        finally:
            if proc and proc.stdout:
                try:
                    proc.stdout.close()
                except Exception:
                    pass

            if proc:
                try:
                    proc.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                except Exception:
                    pass


class FileProcessor:
    """Handles file processing operations with strict typing"""

    def __init__(
        self, config: Config, logger: logging.Logger, graceful_exit: GracefulExit
    ):
        self.config: Config = config
        self.logger: logging.Logger = logger
        self.graceful_exit: GracefulExit = graceful_exit

    def generate_action_plan(
        self,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: Dict[str, Dict[str, FilePath]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a plan of all actions to be performed"""
        transcoding_actions: List[Dict[str, Any]] = []
        removal_actions: List[Dict[str, Any]] = []

        # Calculate age cutoff
        age_cutoff = None
        if self.config.age > 0:
            age_cutoff = datetime.now() - timedelta(days=self.config.age)

        for fp, ts in mp4s:
            if fp in set():  # Would be trash_files in actual implementation
                continue

            outp = self._output_path(fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            # Skip files newer than age cutoff
            if age_cutoff and ts >= age_cutoff:
                self.logger.debug(
                    f"Skipping {fp}: timestamp {ts} is newer than age cutoff {age_cutoff}"
                )
                continue

            # Check if we should skip transcoding
            should_skip = False
            if not self.config.no_skip and outp.exists():
                try:
                    # When exists() returns True, try to get file stats
                    # This might fail in test environments where exists() is broadly mocked
                    file_stat = outp.stat()
                    # Check if file is large enough to skip transcoding
                    should_skip = file_stat.st_size > MIN_ARCHIVE_SIZE_BYTES
                except (OSError, TypeError):
                    # File might not be accessible, or mocking is interfering (e.g., st_mode missing from mock)
                    should_skip = False

            # If cleanup is enabled, always skip transcoding and only add removal actions
            if self.config.cleanup:
                should_skip = True

            if should_skip:
                removal_actions.append(
                    {
                        "type": "source_removal_after_skip",
                        "file": fp,
                        "reason": f"Skipping transcoding: archive exists at {outp}"
                        if not self.config.cleanup
                        else "Skipping transcoding: cleanup mode enabled",
                    }
                )
                if jpg:
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_skip",
                            "file": jpg,
                            "reason": "Skipping transcoding: archive exists for paired MP4"
                            if not self.config.cleanup
                            else "Skipping transcoding: cleanup mode enabled",
                        }
                    )
            else:
                transcoding_actions.append(
                    {
                        "type": "transcode",
                        "input": fp,
                        "output": outp,
                        "jpg_to_remove": jpg,
                    }
                )
                removal_actions.append(
                    {
                        "type": "source_removal_after_transcode",
                        "file": fp,
                        "reason": f"Source file for transcoded archive at {outp}",
                    }
                )
                if jpg:
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_transcode",
                            "file": jpg,
                            "reason": "Paired with transcoded MP4",
                        }
                    )

        return {
            "transcoding": transcoding_actions,
            "removals": removal_actions,
        }

    def execute_plan(
        self, plan: Dict[str, List[Dict[str, Any]]], progress_reporter: ProgressReporter
    ) -> bool:
        """Execute the action plan"""
        transcoding_actions = plan.get("transcoding", [])
        removal_actions = plan.get("removals", [])

        # Execute transcoding actions
        for i, action in enumerate(transcoding_actions, 1):
            if self.graceful_exit.should_exit():
                break

            input_path = action["input"]
            output_path = action["output"]

            self.logger.info(f"Processing {input_path}")
            progress_reporter.start_file()

            # Create progress callback
            def progress_callback(pct: float) -> None:
                if not self.graceful_exit.should_exit():
                    progress_reporter.update_progress(pct)

            # Transcode file
            success = Transcoder.transcode_file(
                input_path,
                output_path,
                self.logger,
                progress_callback,
                self.graceful_exit,
            )

            if success:
                progress_reporter.finish_file()
                self.logger.info(
                    f"Successfully transcoded {input_path} -> {output_path}"
                )

                # Remove paired JPG if exists
                jpg = action.get("jpg_to_remove")
                if jpg:
                    FileManager.remove_file(
                        jpg,
                        self.logger,
                        dry_run=self.config.dry_run,
                        delete=self.config.delete,
                        trash_root=self.config.trash_root,
                        is_output=False,
                        source_root=jpg.parent,
                    )

                # Remove source file after successful transcoding
                source_removal_action = None
                for removal_action in removal_actions:
                    if (
                        removal_action.get("type") == "source_removal_after_transcode"
                        and removal_action["file"] == input_path
                    ):
                        source_removal_action = removal_action
                        break

                if source_removal_action:
                    FileManager.remove_file(
                        source_removal_action["file"],
                        self.logger,
                        dry_run=self.config.dry_run,
                        delete=self.config.delete,
                        trash_root=self.config.trash_root,
                        is_output=False,
                        source_root=source_removal_action["file"].parent,
                    )
                    removal_actions.remove(source_removal_action)
            else:
                self.logger.error(f"Failed to transcode {input_path}")
                continue

        # Execute remaining removal actions
        for action in removal_actions:
            if self.graceful_exit.should_exit():
                break

            file_path = action["file"]
            FileManager.remove_file(
                file_path,
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=False,
                source_root=file_path.parent,
            )

        return True

    def cleanup_orphaned_files(self, mapping: Dict[str, Dict[str, FilePath]]) -> None:
        """Remove orphaned JPG files and clean empty directories"""
        count = 0
        for key, files in mapping.items():
            if self.graceful_exit.should_exit():
                break

            jpg = files.get(".jpg")
            mp4 = files.get(".mp4")
            if not jpg or mp4:
                continue

            self.logger.info(f"Found orphaned JPG (no MP4 pair): {jpg}")
            FileManager.remove_file(
                jpg,
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=False,
                source_root=jpg.parent,
            )
            count += 1

        if not self.graceful_exit.should_exit():
            self.logger.info(f"Removed {count} orphaned JPG files")

        # Clean empty directories
        FileManager.clean_empty_directories(self.config.directory, self.logger)

    def _output_path(self, input_file: FilePath, timestamp: Timestamp) -> FilePath:
        """Generate output path for archived file"""
        return (
            self.config.output
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )


def display_plan(
    plan: Dict[str, List[Dict[str, Any]]], logger: logging.Logger, config: Config
) -> None:
    """Display the action plan to the user"""
    logger.info("=== ACTION PLAN ===")
    logger.info(f"Transcoding {len(plan['transcoding'])} files:")
    for i, action in enumerate(plan["transcoding"], 1):
        logger.info(f"  {i}. {action['input']} -> {action['output']}")
        if action["jpg_to_remove"]:
            logger.info(f"      + Removing paired JPG: {action['jpg_to_remove']}")

    logger.info(f"Removing {len(plan['removals'])} files:")
    for i, action in enumerate(plan["removals"], 1):
        logger.info(f"  {i}. {action['file']} - {action['reason']}")

    # Display age cutoff information if cleanup is enabled
    if config.cleanup:
        age_cutoff = datetime.now() - timedelta(days=config.age)
        logger.info(
            f"Cleanup enabled: Files older than {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')} "
            f"will be removed based on age threshold of {config.age} days"
        )
        if config.clean_output:
            logger.info("Cleanup scope: Source files, archive files, and trash files")
        else:
            logger.info(
                "Cleanup scope: Source files and trash files (archive files excluded)"
            )

    logger.info("=== END PLAN ===")


def confirm_plan(
    plan: Dict[str, List[Dict[str, Any]]], config: Config, logger: logging.Logger
) -> bool:
    """Ask for user confirmation"""
    if config.no_confirm:
        return True

    suffix = " [Y/n]" if False else " [y/N]"
    try:
        response = (
            input(f"Proceed with transcoding and file removals?{suffix}: ")
            .strip()
            .lower()
        )
        if not response:
            return False
        return response in ("y", "yes")
    except KeyboardInterrupt:
        return False


def setup_signal_handlers(graceful_exit: GracefulExit) -> None:
    """Setup signal handlers for graceful exit"""

    def signal_handler(signum: int, frame) -> None:
        graceful_exit.request_exit()

        # Convert signal number to signal name
        signal_name = "unknown"
        if signum == signal.SIGINT:
            signal_name = "SIGINT"
        elif signum == signal.SIGTERM:
            signal_name = "SIGTERM"
        elif signum == signal.SIGHUP:
            signal_name = "SIGHUP"
        else:
            signal_name = f"signal {signum}"

        with OUTPUT_LOCK:  # Use global lock to coordinate with progress updates
            sys.stderr.write(f"\nReceived {signal_name}, shutting down gracefully...\n")
            sys.stderr.flush()

    signals = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
    for sig in signals:
        try:
            signal.signal(sig, signal_handler)
        except (ValueError, OSError):
            pass


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments

    Args:
        args: Optional list of arguments to parse. If None, uses sys.argv[1:]
    """
    parser = argparse.ArgumentParser(description="Camera Archiver")
    parser.add_argument(
        "directory",
        nargs="?",
        default="/camera",
        help="Input directory containing camera footage (defaults to /camera)",
    )
    parser.add_argument("-o", "--output", help="Output directory for archived footage")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    parser.add_argument(
        "-y", "--no-confirm", action="store_true", help="Skip confirmation prompts"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip files that already have archives",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Permanently delete files instead of moving to trash",
    )
    parser.add_argument(
        "--trash-root",
        help="Root directory for trash (defaults to /camera/.deleted)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old files based on age and size",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Also clean output directory during cleanup",
    )
    parser.add_argument(
        "--age", type=int, default=30, help="Age in days for cleanup (default: 30)"
    )
    parser.add_argument("--log-file", help="Log file path")
    return parser.parse_args(args)


def run_archiver(config: Config) -> int:
    """Main pipeline function with strict typing"""
    # Setup logging
    logger = Logger.setup(config)

    # Setup graceful exit
    graceful_exit = GracefulExit()
    setup_signal_handlers(graceful_exit)

    try:
        # Stage 1: Discovery
        logger.info("Discovering files")

        # Check storage
        if not config.directory.exists():
            logger.error(f"Input directory does not exist: {config.directory}")
            return 1

        # Create output directory if needed
        if config.output and not config.output.exists():
            config.output.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output directory: {config.output}")

        # Discover files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            config.directory, config.trash_root, config.output, config.clean_output
        )

        logger.info(f"Discovered {len(mp4s)} MP4 files")
        if not mp4s:
            logger.info("No files to process")
            return 0

        # Stage 2: Planning
        logger.info("Planning operations")

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Display plan
        display_plan(plan, logger, config)

        # If in dry-run mode, exit immediately
        if config.dry_run:
            logger.info("Dry run completed - no transcoding or removals performed")
            return 0

        # Ask for confirmation if needed
        if not confirm_plan(plan, config, logger):
            logger.info("Operation cancelled by user")
            return 0

        # Ask for confirmation if needed
        if not confirm_plan(plan, config, logger):
            logger.info("Operation cancelled by user")
            return 0

        # Stage 3: Processing
        logger.info("Processing files")

        progress_reporter = ProgressReporter(
            total_files=len(plan["transcoding"]),
            graceful_exit=graceful_exit,
            silent=config.dry_run,
        )

        with progress_reporter:
            processor.execute_plan(plan, progress_reporter)

        # Stage 4: Cleanup
        if config.cleanup:
            logger.info("Cleaning up files")
            processor.cleanup_orphaned_files(mapping)

        logger.info("Archiving completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def main() -> int:
    """Main entry point"""
    args = parse_args()
    config = Config(args)
    return run_archiver(config)


if __name__ == "__main__":
    sys.exit(main())
