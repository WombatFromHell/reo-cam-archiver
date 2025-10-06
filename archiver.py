#!/usr/bin/env python3
"""
Camera Archiver: Transcodes and archives camera footage based on timestamp parsing,
with intelligent cleanup based on size and age thresholds.
"""

import argparse
import atexit
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

from abc import ABC, abstractmethod
from enum import Enum

# Constants
MIN_ARCHIVE_SIZE_BYTES = 1_048_576  # 1MB
DEFAULT_PROGRESS_WIDTH = 30
PROGRESS_UPDATE_INTERVAL = 5  # seconds for non-TTY output


class GracefulExit:
    """Thread-safe flag for graceful exit handling"""

    def __init__(self):
        self._exit_requested = False
        self._lock = threading.Lock()

    def request_exit(self):
        with self._lock:
            self._exit_requested = True

    def should_exit(self):
        with self._lock:
            return self._exit_requested


class Config:
    """Configuration class to manage all settings and CLI arguments"""

    def __init__(self):
        self.directory = Path("/camera")
        self.output = Path("/camera/archived")
        self.trashdir: Optional[Path] = None
        self.use_trash = True
        self.age = 30
        self.dry_run = False
        self.max_size = 500
        self.no_skip = False
        self.cleanup = False
        self.clean_output = False
        self.no_confirm = False  # Add the no_confirm flag to Config class
        self.log_file: Optional[Path] = None

    @classmethod
    def from_args(cls, args):
        """Create config from parsed CLI arguments"""
        config = cls()
        config.directory = (
            args.directory if args.directory.exists() else Path("/camera")
        )
        config.output = args.output or (config.directory / "archived")
        config.trashdir = args.trashdir
        config.use_trash = args.use_trash
        config.age = args.age
        config.dry_run = args.dry_run
        config.max_size = args.max_size
        config.no_skip = args.no_skip
        config.cleanup = args.cleanup
        config.clean_output = getattr(args, "clean_output", False)
        config.no_confirm = getattr(args, "no_confirm", False)
        config.log_file = config.directory / "transcoding.log"
        return config

    def get_trash_root(self) -> Optional[Path]:
        if not self.use_trash:
            return None
        if self.trashdir:
            return self.trashdir
        return self.directory / ".deleted"


class FileInfo:
    """Represents a file with its metadata for cleanup decisions."""

    def __init__(
        self,
        path: Path,
        timestamp: datetime,
        size: int,
        is_archive: bool,
        is_trash: bool,
    ):
        self.path = path
        self.timestamp = timestamp
        self.size = size
        self.is_archive = is_archive
        self.is_trash = is_trash


class ConsoleOrchestrator:
    """Thread-safe lock for console output."""

    def __init__(self):
        self._lock = threading.RLock()

    def guard(self):
        return self._lock


class GuardedStreamHandler(logging.StreamHandler):
    """Log handler that coordinates with progress bar"""

    def __init__(self, orchestrator, stream=None, progress_bar=None):
        super().__init__(stream)
        self.orchestrator = orchestrator
        self.progress_bar = progress_bar

    def emit(self, record):
        msg = self.format(record) + self.terminator

        with self.orchestrator.guard():
            if self.progress_bar and self.progress_bar._progress_line:
                self.stream.write(f"\r\x1b[2K{msg}")
                self.progress_bar.redraw()
            else:
                self.stream.write(msg)
            self.stream.flush()


class ProgressReporter:
    """Handles progress reporting for file operations"""

    def __init__(
        self,
        total_files: int,
        graceful_exit: Optional[GracefulExit] = None,
        width: int = DEFAULT_PROGRESS_WIDTH,
        silent: bool = False,
        out=sys.stderr,
    ):
        self.total = total_files
        self.graceful_exit = graceful_exit or GracefulExit()
        self.width = max(10, width)
        self.blocks = self.width - 2
        self.silent = silent or out is None
        self.out = out
        self.orchestrator = ConsoleOrchestrator()
        self.start_time = None
        self.file_start = None
        self._progress_line = ""
        self._last_print_time = time.time()
        self._finished = False  # Track if the progress bar has been finished
        self._original_signal_handlers = {}
        self._register_cleanup_handlers()

    def _is_tty(self) -> bool:
        return hasattr(self.out, "isatty") and self.out.isatty() and not self.silent

    def _register_cleanup_handlers(self):
        atexit.register(self._cleanup_progress_bar)
        signals = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
        for sig in signals:
            try:
                self._original_signal_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, self._signal_handler)
            except (ValueError, OSError):
                pass

    def _unregister_cleanup_handlers(self):
        for sig, handler in self._original_signal_handlers.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass
        atexit.unregister(self._cleanup_progress_bar)

    def _signal_handler(self, signum, frame):
        self.graceful_exit.request_exit()
        self._cleanup_progress_bar()

        signal_name = {
            signal.SIGINT: "SIGINT",
            signal.SIGTERM: "SIGTERM",
            signal.SIGHUP: "SIGHUP",
        }.get(signum, f"signal {signum}")

        sys.stderr.write(f"\nReceived {signal_name}, shutting down gracefully...\n")
        sys.stderr.flush()

    def _cleanup_progress_bar(self):
        if not self._progress_line or self.silent:
            return

        try:
            self.out.write("\r\x1b[2K\n")
            self.out.flush()
            self._progress_line = ""
        except Exception:
            pass

    def finish(self):
        self._cleanup_progress_bar()
        self._unregister_cleanup_handlers()
        self._finished = True  # Mark that this progress bar is finished

    @property
    def has_progress(self) -> bool:
        return bool(self._progress_line)

    def start_processing(self):
        if self._finished:
            return
        if self.start_time is None:
            self.start_time = time.time()

    def start_file(self):
        if self._finished:
            return
        self.file_start = time.time()
        if self.start_time is None:
            self.start_time = time.time()

    def update_progress(self, idx: int, pct: float = 0.0):
        if self.silent or self.graceful_exit.should_exit() or self._finished:
            return
        line = self._format_line(idx, pct)
        if line == self._progress_line:
            return
        self._progress_line = line
        self._display(line)

    def finish_file(self, idx: int):
        if not self.graceful_exit.should_exit() and not self._finished:
            self.update_progress(idx, 100.0)

    def _format_line(self, idx: int, pct: float) -> str:
        now = time.time()
        bar = f"[{'|' * int(pct / 100 * self.blocks)}{'-' * (self.blocks - int(pct / 100 * self.blocks))}]"
        elapsed_file = datetime.fromtimestamp(now - (self.file_start or now)).strftime(
            "%M:%S"
        )

        total_sec = int(now - (self.start_time or now))
        hours, remainder = divmod(total_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed_total = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        return f"Progress [{idx}/{self.total}]: {pct:.0f}% {bar} {elapsed_file} ({elapsed_total})"

    def redraw(self):
        if (
            self.silent
            or not self._progress_line
            or self.graceful_exit.should_exit()
            or self._finished
        ):
            return
        self._display(self._progress_line)

    def _display(self, line: str):
        if not self._is_tty():
            now = time.time()
            if (
                now - self._last_print_time >= PROGRESS_UPDATE_INTERVAL
                or "100%" in line
            ):
                self.out.write(f"{line}\n")
                self.out.flush()
                self._last_print_time = now
            return

        try:
            self.out.write(f"\r\x1b[2K{line}")
            self.out.flush()
        except Exception:
            self.out.write(f"\r{line}")
            self.out.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class Logger:
    """Handles logging setup and operations"""

    @staticmethod
    def setup(
        log_file: Optional[Path], progress_bar: Optional[ProgressReporter] = None
    ) -> logging.Logger:
        logger = logging.getLogger("camera_archiver")
        logger.setLevel(logging.INFO)

        # Properly close and remove existing handlers
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()  # Close the handler to free resources
            except Exception:
                pass  # Ignore errors when closing

        fmt = "%(asctime)s - %(levelname)s - %(message)s"
        fh = (
            logging.FileHandler(log_file, encoding="utf-8")
            if log_file
            else logging.NullHandler()
        )
        fh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(fh)
        stream = progress_bar.out if progress_bar else sys.stdout
        orch = progress_bar.orchestrator if progress_bar else ConsoleOrchestrator()
        sh = GuardedStreamHandler(orch, stream=stream, progress_bar=progress_bar)
        sh.setFormatter(logging.Formatter(fmt))
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)
        logger.propagate = False
        return logger


class FileScanner:
    """Handles scanning directories for files with valid timestamps"""

    @staticmethod
    def parse_timestamp_from_filename(name: str) -> Optional[datetime]:
        """Extract timestamp from filename using REO_*_YYYYMMDDHHMMSS.(mp4|jpg) pattern."""
        TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = TIMESTAMP_RE.search(name)
        if not m:
            return None
        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None

    @staticmethod
    def scan_files(
        base_dir: Path,
        include_trash: bool = False,
        trash_root: Optional[Path] = None,
        *,
        graceful_exit: Optional[GracefulExit] = None,
    ) -> Tuple[List[Tuple[Path, datetime]], Dict[str, Dict[str, Path]], Set[Path]]:
        """Scan for MP4 and JPG files with valid timestamps."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return [], {}, set()

        mp4s = []
        mapping = {}
        trash_files = set()

        # Scan base directory
        for p in base_dir.rglob("*.*"):
            if graceful_exit.should_exit():
                break

            if not p.is_file():
                continue

            # Skip files in trash directory when scanning base directory
            if trash_root and trash_root in p.parents:
                continue

            ts = FileScanner.parse_timestamp_from_filename(p.name)
            if not ts:
                continue
            key = ts.strftime("%Y%m%d%H%M%S")
            ext = p.suffix.lower()
            mapping.setdefault(key, {})[ext] = p
            if ext == ".mp4":
                mp4s.append((p, ts))

        # Scan trash directory if enabled
        if include_trash and trash_root and trash_root.exists():
            for trash_type in ["input", "output"]:
                trash_dir = trash_root / trash_type
                if trash_dir.exists():
                    for p in trash_dir.rglob("*.*"):
                        if graceful_exit.should_exit():
                            break

                        if not p.is_file():
                            continue

                        ts = FileScanner.parse_timestamp_from_filename(p.name)
                        if not ts:
                            continue
                        key = ts.strftime("%Y%m%d%H%M%S")
                        ext = p.suffix.lower()

                        mapping.setdefault(key, {})[ext] = p
                        trash_files.add(p)
                        if ext == ".mp4":
                            mp4s.append((p, ts))

        return mp4s, mapping, trash_files


class Transcoder:
    """Handles video transcoding operations"""

    @staticmethod
    def _build_ffprobe_command(file_path: Path) -> List[str]:
        """Build the ffprobe command to get video duration."""
        return [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
        ]

    @staticmethod
    def get_video_duration(
        file_path: Path, *, graceful_exit: Optional[GracefulExit] = None
    ) -> Optional[float]:
        """Get video duration using ffprobe."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit() or not shutil.which("ffprobe"):
            return None
        try:
            cmd = Transcoder._build_ffprobe_command(file_path)
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration_str = result.stdout.strip()
            if duration_str and duration_str != "N/A":
                return float(duration_str)
            return None
        except Exception:
            return None

    @staticmethod
    def transcode_file(
        input_path: Path,
        output_path: Path,
        logger: logging.Logger,
        progress_cb: Optional[Callable[[float], None]] = None,
        *,
        graceful_exit: Optional[GracefulExit] = None,
    ) -> bool:
        """Transcode a video file using ffmpeg with QSV hardware acceleration."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return False

        # Ensure output directory exists
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
        total_duration = Transcoder.get_video_duration(
            input_path, graceful_exit=graceful_exit
        )

        proc = None
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

        log_lines = []
        prev_pct = -1.0
        cur_pct = 0.0

        try:
            # Make sure proc.stdout is not None before proceeding
            if proc.stdout is None:
                logger.error("Failed to capture ffmpeg output")
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                return False

            # Handle both file-like objects and iterables for stdout
            stdout_iter = None
            if hasattr(proc.stdout, "readline"):
                # File-like object with readline method
                stdout_iter = iter(proc.stdout.readline, "")
            elif hasattr(proc.stdout, "__iter__"):
                # Iterable (like list of strings in tests)
                stdout_iter = proc.stdout
            else:
                logger.error(f"Unsupported stdout type: {type(proc.stdout)}")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                return False

            if stdout_iter:
                try:
                    for line in stdout_iter:
                        if graceful_exit.should_exit():
                            logger.info(
                                "Cancellation requested, terminating ffmpeg process..."
                            )
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()
                            return False

                        if not line:
                            break
                        log_lines.append(line)

                        if total_duration and total_duration > 0:
                            m = re.search(r"time=([0-9:.]+)", line)
                            if m:
                                h, mn, s = map(float, m.group(1).split(":")[:3])
                                cur_pct = min(
                                    (h * 3600 + mn * 60 + s) / total_duration * 100,
                                    100.0,
                                )
                        else:
                            cur_pct = min(cur_pct + 1, 99.0)

                        if progress_cb and cur_pct != prev_pct:
                            progress_cb(cur_pct)
                            prev_pct = cur_pct
                except Exception as e:
                    logger.error(f"Error reading ffmpeg output: {e}")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    return False

            rc = proc.wait()
            if rc != 0 and not graceful_exit.should_exit():
                msg = (
                    f"FFmpeg failed (code {rc}) for {input_path} -> {output_path}\n"
                    + "".join(log_lines)
                )
                logger.error(msg)
            return rc == 0 and not graceful_exit.should_exit()
        finally:
            # Ensure the process is cleaned up in the finally block
            if proc and proc.stdout:
                try:
                    proc.stdout.close()
                except Exception:
                    pass  # Ignore errors when closing
            if proc:
                try:
                    proc.wait(
                        timeout=0.1
                    )  # Give a small timeout to check if already finished
                except subprocess.TimeoutExpired:
                    # If process is still running, terminate it
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                except Exception:
                    # Process might already be finished
                    pass


class FileCleaner:
    """Handles file removal and cleanup operations"""

    @staticmethod
    def remove_one(
        path: Path,
        logger: logging.Logger,
        dry_run: bool,
        use_trash: bool,
        trash_root: Optional[Path],
        is_output: bool,
        source_root: Path,
    ) -> None:
        """Delete *path* once.  If it is already inside the trash tree it is
        permanently removed, otherwise it is moved into the trash directory."""
        if dry_run:
            logger.info("[DRY RUN] Would remove %s", path)
            return
        if use_trash and trash_root and trash_root in path.parents:
            path.unlink(missing_ok=True)
            logger.info("Permanently removed (already in trash): %s", path)
        else:
            FileCleaner.safe_remove(
                path,
                logger,
                dry_run=False,
                use_trash=use_trash,
                trash_root=trash_root,
                is_output=is_output,
                source_root=source_root,
            )

    @staticmethod
    def calculate_trash_destination(
        file_path: Path, source_root: Path, trash_root: Path, is_output: bool = False
    ) -> Path:
        """Calculate the destination path in trash for a given file."""
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
    def safe_remove(
        file_path: Path,
        logger: logging.Logger,
        dry_run: bool = False,
        use_trash: bool = False,
        trash_root: Optional[Path] = None,
        is_output: bool = False,
        source_root: Optional[Path] = None,
        *,
        graceful_exit: Optional[GracefulExit] = None,
    ):
        """Safely remove a file, optionally moving to trash."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return

        if dry_run:
            logger.info(f"[DRY RUN] Would remove {file_path}")
            return

        try:
            if source_root is None:
                source_root = file_path.parent

            if use_trash and trash_root:
                new_dest = FileCleaner.calculate_trash_destination(
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
    def remove_orphaned_jpgs(
        mapping: Dict[str, Dict[str, Path]],
        processed: Set[Path],
        logger: logging.Logger,
        dry_run: bool = False,
        use_trash: bool = False,
        trash_root: Optional[Path] = None,
        *,
        graceful_exit: Optional[GracefulExit] = None,
    ) -> None:
        """Remove JPG files without corresponding MP4 files."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return

        count = 0
        for key, files in mapping.items():
            if graceful_exit.should_exit():
                break
            jpg = files.get(".jpg")
            mp4 = files.get(".mp4")
            if not jpg or jpg in processed:
                continue
            if mp4:
                continue
            if dry_run:
                logger.info("[DRY RUN] Found orphaned JPG (no MP4 pair): %s", jpg)
            else:
                logger.info("Found orphaned JPG (no MP4 pair): %s", jpg)
            FileCleaner.remove_one(
                jpg,
                logger,
                dry_run,
                use_trash,
                trash_root,
                is_output=False,
                source_root=jpg.parent,
            )
            count += 1

        if not graceful_exit.should_exit():
            logger.info(
                "%s %d orphaned JPG files",
                "[DRY RUN] Would remove" if dry_run else "Removed",
                count,
            )

    @staticmethod
    def clean_empty_directories(
        root_dir: Path,
        logger: Optional[logging.Logger] = None,
        use_trash: bool = False,
        trash_root: Optional[Path] = None,
        is_output: bool = False,
        is_trash: bool = False,
        *,
        graceful_exit: Optional[GracefulExit] = None,
    ):
        """Remove empty date-structured directories."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return

        root = Path(root_dir)
        if not root.exists():
            return

        for dirpath, dirs, files in os.walk(root, topdown=False):
            if graceful_exit.should_exit():
                break

            p = Path(dirpath)
            if p == root:
                continue

            # For trash directories, remove empty dirs permanently without validation
            if is_trash:
                if not files and not dirs:
                    try:
                        p.rmdir()
                        if logger:
                            logger.info(f"Removed empty trash directory: {p}")
                    except Exception as e:
                        if logger:
                            logger.error(
                                f"Failed to remove empty trash directory {p}: {e}"
                            )
                continue

            # For non-trash directories, apply date-structure validation
            try:
                rel_parts = p.relative_to(root).parts
            except ValueError:
                continue

            # Only clean directories with exactly 3 parts (year/month/day structure)
            if len(rel_parts) != 3:
                continue

            y, m, d = rel_parts
            try:
                int(y)
                int(m)
                int(d)
            except Exception:
                continue

            # Only remove if directory is actually empty
            if not files and not dirs:
                try:
                    if use_trash and trash_root:
                        new_dest = FileCleaner.calculate_trash_destination(
                            p, root, trash_root, is_output
                        )
                        new_dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(p), str(new_dest))
                        if logger:
                            logger.info(
                                f"Moved empty directory to trash: {p} -> {new_dest}"
                            )
                    else:
                        p.rmdir()
                        if logger:
                            logger.info(f"Removed empty directory: {p}")
                except Exception as e:
                    if logger:
                        logger.error(f"Failed to remove empty directory {p}: {e}")


class Archiver:
    """Main archiver class that orchestrates the entire process"""

    def __init__(self, config: Config, graceful_exit: Optional[GracefulExit] = None):
        self.config = config
        self.graceful_exit = graceful_exit or GracefulExit()
        self.logger: Optional[logging.Logger] = None
        self.progress_bar: Optional[ProgressReporter] = None

    def setup_logging(self, graceful_exit: Optional[GracefulExit] = None):
        """Setup logging with optional progress bar"""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        self.progress_bar = ProgressReporter(
            total_files=0,
            graceful_exit=graceful_exit,
            silent=self.config.dry_run,
            out=sys.stderr,
        )
        self.logger = Logger.setup(self.config.log_file, self.progress_bar)

    def output_path(self, input_file: Path, timestamp: datetime) -> Path:
        out_dir = self.config.output
        # Check if parent directories match YYYY/MM/DD pattern
        if len(input_file.parts) >= 4:
            y, m, d = input_file.parts[-4:-1]
            try:
                int(y)
                int(m)
                int(d)
                # Valid date structure → reuse it
                return (
                    out_dir
                    / y
                    / m
                    / d
                    / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
                )
            except ValueError:
                pass  # Not a valid date structure → fall through
        # Use timestamp-based structure
        return (
            out_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )

    def get_all_archive_files(self) -> List[Path]:
        """Get all archive files including trash if enabled."""
        out_dir = self.config.output
        trash_root = self.config.get_trash_root()
        archive_files = (
            list(out_dir.rglob("archived-*.mp4")) if out_dir.exists() else []
        )

        if trash_root:
            trash_output_dir = trash_root / "output"
            if trash_output_dir.exists():
                archive_files.extend(list(trash_output_dir.rglob("archived-*.mp4")))

        return archive_files

    def collect_file_info(
        self, old_list: List[Tuple[Path, datetime]]
    ) -> List[FileInfo]:
        """Collect file information for all relevant files."""
        all_files = []
        seen_paths = set()
        trash_root = self.config.get_trash_root()

        # Get all archive files
        archive_files = self.get_all_archive_files()

        # Process archive files - mark them as archives and skip if in old_list (source)
        for archive_file in archive_files:
            try:
                if not archive_file.is_file():
                    continue
                size = archive_file.stat().st_size
            except (OSError, IOError):
                continue

            ts_match = re.search(r"archived-(\d{14})\.mp4$", archive_file.name)
            if ts_match:
                try:
                    ts = datetime.strptime(ts_match.group(1), "%Y%m%d%H%M%S")
                    is_trash = trash_root is not None and any(
                        p in archive_file.parents
                        for p in [trash_root, trash_root / "output"]
                    )
                    if archive_file not in seen_paths:
                        all_files.append(
                            FileInfo(
                                archive_file,
                                ts,
                                size,
                                is_archive=True,
                                is_trash=is_trash,
                            )
                        )
                        seen_paths.add(archive_file)
                except ValueError:
                    pass

        # Process source files from old_list - skip if already processed as archives
        for fp, ts in old_list:
            if (
                fp in seen_paths
            ):  # Skip if this is an archive file we've already processed
                continue

            try:
                if not fp.is_file():
                    continue
                size = fp.stat().st_size
            except (OSError, IOError):
                continue

            is_trash = trash_root is not None and any(
                p in fp.parents for p in [trash_root, trash_root / "input"]
            )
            all_files.append(
                FileInfo(fp, ts, size, is_archive=False, is_trash=is_trash)
            )
            seen_paths.add(fp)

        return all_files

    def _categorize_files(self, all_files: List[FileInfo]) -> Dict[int, List[FileInfo]]:
        """Categorize files by location priority (0 = trash, 1 = archive, 2 = source)."""
        categorized_files: Dict[int, List[FileInfo]] = {0: [], 1: [], 2: []}

        for file_info in all_files:
            if file_info.is_trash:
                categorized_files[0].append(file_info)
            elif file_info.is_archive:
                categorized_files[1].append(file_info)
            else:
                categorized_files[2].append(file_info)

        # Sort each category by timestamp (oldest first)
        for category in categorized_files.values():
            category.sort(key=lambda x: x.timestamp)

        return categorized_files

    def _apply_size_cleanup(
        self,
        categorized_files: Dict[int, List[FileInfo]],
        total_size: int,
        size_limit: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply size-based cleanup based on priority."""
        files_to_remove = []
        remaining_size = total_size

        # Ensure logger is not None
        if self.logger is None:
            raise RuntimeError("Logger not initialized. Call setup_logging() first.")

        self.logger.info("Archive size exceeds limit")
        self.logger.info(
            f"Size threshold exceeded, removing files by priority to reach {self.config.max_size} GB..."
        )
        self.logger.info(
            "Priority order: Trash > Archive > Source (oldest first within each)"
        )

        # Remove files starting from highest priority (0) to lowest (2)
        for priority in range(3):
            if remaining_size <= size_limit:
                break

            category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
            category_files = categorized_files[priority]

            if category_files:
                self.logger.info(
                    f"Processing {category_name} files for size cleanup..."
                )

                for file_info in category_files:
                    if remaining_size <= size_limit:
                        break

                    files_to_remove.append(file_info)
                    remaining_size -= file_info.size

                    if self.config.dry_run:
                        self.logger.info(
                            f"[DRY RUN] Would remove {category_name} file for size: {file_info.path} "
                            f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                        )

        self.logger.info(
            f"After size cleanup: {remaining_size / (1024**3):.1f} GB "
            f"({len(files_to_remove)} files marked for removal)"
        )

        return files_to_remove, remaining_size

    def _apply_age_cleanup(
        self,
        categorized_files: Dict[int, List[FileInfo]],
        age_cutoff: datetime,
        total_size: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply age-based cleanup respecting clean_output setting."""
        files_to_remove = []
        remaining_size = total_size

        # Ensure logger is not None
        if self.logger is None:
            raise RuntimeError("Logger not initialized. Call setup_logging() first.")

        files_over_age_by_priority: Dict[int, List[FileInfo]] = {
            0: [],
            1: [],
            2: [],
        }

        for priority in range(3):
            # Skip archive files (priority 1) if clean_output is False
            if priority == 1 and not self.config.clean_output:
                continue

            files_over_age_by_priority[priority] = [
                f for f in categorized_files[priority] if f.timestamp < age_cutoff
            ]

        total_over_age = sum(
            len(files) for files in files_over_age_by_priority.values()
        )

        if total_over_age > 0:
            self.logger.info(
                f"Found {total_over_age} files older than {self.config.age} days"
            )

            # Remove age-eligible files by priority order
            for priority in range(3):
                # Skip archive files (priority 1) if clean_output is False
                if priority == 1 and not self.config.clean_output:
                    continue

                category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
                age_files = files_over_age_by_priority[priority]

                if age_files:
                    self.logger.info(
                        f"Processing {category_name} files for age cleanup..."
                    )

                    for file_info in age_files:
                        files_to_remove.append(file_info)
                        remaining_size -= file_info.size

                        if self.config.dry_run:
                            self.logger.info(
                                f"[DRY RUN] Would remove {category_name} file for age: {file_info.path} "
                                f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                            )

            self.logger.info(f"Added {total_over_age} files for age-based removal")
        else:
            self.logger.info(f"No files older than {self.config.age} days found")

        return files_to_remove, remaining_size

    def intelligent_cleanup(self, all_files: List[FileInfo]) -> List[FileInfo]:
        """
        Select files to remove based on location priority and size/age constraints.
        Priority order: Trash > Archive > Source (oldest first within each category).
        Output/archive files are excluded from age-based removal unless clean_output=True.
        """
        if not all_files:
            return []

        # Ensure logger is set
        if self.logger is None:
            raise RuntimeError("Logger not initialized. Call setup_logging() first.")

        # Calculate totals
        total_size = sum(f.size for f in all_files)
        size_limit = self.config.max_size * (1024**3)
        age_cutoff = datetime.now() - timedelta(days=self.config.age)

        self.logger.info(f"Current total size: {total_size / (1024**3):.1f} GB")
        self.logger.info(f"Size limit: {self.config.max_size} GB")
        self.logger.info(f"Age cutoff: {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
        if not self.config.clean_output:
            self.logger.info("Output files excluded from age-based cleanup")

        # Categorize files by location priority
        categorized_files = self._categorize_files(all_files)

        files_to_remove = []
        remaining_size = total_size

        # PHASE 1: Enforce size limit (if over limit)
        if remaining_size > size_limit:
            files_to_remove, remaining_size = self._apply_size_cleanup(
                categorized_files, total_size, size_limit
            )
        else:
            # PHASE 2: Enforce age limit (only if under size limit AND age_days > 0)
            if self.config.age > 0:
                files_to_remove, remaining_size = self._apply_age_cleanup(
                    categorized_files, age_cutoff, total_size
                )
            else:
                self.logger.info("Age-based cleanup disabled (age_days <= 0)")

        # Remove duplicates and sort by priority then timestamp
        unique_files = {f.path: f for f in files_to_remove}
        files_to_remove = list(unique_files.values())

        def sort_key(file_info: FileInfo):
            if file_info.is_trash:
                priority = 0
            elif file_info.is_archive:
                priority = 1
            else:
                priority = 2
            return (priority, file_info.timestamp)

        files_to_remove.sort(key=sort_key)

        self.logger.info(
            f"Final removal plan: {len(files_to_remove)} files, "
            f"final size: {remaining_size / (1024**3):.1f} GB"
        )

        return files_to_remove

    def generate_action_plan(
        self,
        old_list: List[Tuple[Path, datetime]],
        mapping: Dict[str, Dict[str, Path]],
        trash_files: Set[Path],
    ) -> Dict[str, List]:
        """Generate a plan of all actions to be performed (transcoding and removals)"""
        transcoding_actions = []
        removal_actions = []

        for fp, ts in old_list:
            if fp in trash_files:  # already in trash
                continue

            outp = self.output_path(fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            # Check skip logic
            if (
                not self.config.no_skip
                and outp.exists()
                and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
            ):
                # Will skip transcoding but remove source files
                size = 0
                if fp.exists():
                    try:
                        size = fp.stat().st_size
                    except OSError:
                        size = 0
                removal_actions.append(
                    {
                        "type": "source_removal_after_skip",
                        "file": fp,
                        "reason": f"Skipping transcoding: archive exists at {outp}",
                        "size": size,
                    }
                )
                if jpg:
                    size = 0
                    if jpg.exists():
                        try:
                            size = jpg.stat().st_size
                        except OSError:
                            size = 0
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_skip",
                            "file": jpg,
                            "reason": "Skipping transcoding: archive exists for paired MP4",
                            "size": size,
                        }
                    )
            else:
                # Will transcode the file
                size = 0
                if fp.exists():
                    try:
                        size = fp.stat().st_size
                    except OSError:
                        size = 0
                transcoding_actions.append(
                    {
                        "type": "transcode",
                        "input": fp,
                        "output": outp,
                        "size": size,
                        "jpg_to_remove": jpg,
                    }
                )
                if jpg:
                    size = 0
                    if jpg.exists():
                        try:
                            size = jpg.stat().st_size
                        except OSError:
                            size = 0
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_transcode",
                            "file": jpg,
                            "reason": "Paired with transcoded MP4",
                            "size": size,
                        }
                    )

        # Also get cleanup plan for existing archives if in cleanup mode
        cleanup_removals = []
        if self.config.cleanup:
            all_file_infos = self.collect_file_info(old_list)
            files_to_remove = self.intelligent_cleanup(all_file_infos)
            for file_info in files_to_remove:
                cleanup_removals.append(
                    {
                        "type": "cleanup_removal",
                        "file": file_info.path,
                        "size": file_info.size,
                        "is_archive": file_info.is_archive,
                        "is_trash": file_info.is_trash,
                        "timestamp": file_info.timestamp,
                    }
                )

        return {
            "transcoding": transcoding_actions,
            "removals": removal_actions,
            "cleanup_removals": cleanup_removals,
        }

    def display_action_plan(self, plan: Dict[str, List]) -> None:
        """Display the action plan to the user"""
        logger = self.logger
        if logger:
            logger.info("=== ACTION PLAN ===")
            logger.info(f"Transcoding {len(plan['transcoding'])} files:")
            for i, action in enumerate(plan["transcoding"], 1):
                logger.info(
                    f"  {i}. {action['input']} -> {action['output']} "
                    f"({action['size'] / (1024**2):.1f} MB)"
                )
                if action["jpg_to_remove"]:
                    logger.info(
                        f"      + Removing paired JPG: {action['jpg_to_remove']}"
                    )

            logger.info(f"Removing {len(plan['removals'])} files:")
            for i, action in enumerate(plan["removals"], 1):
                logger.info(
                    f"  {i}. {action['file']} "
                    f"({action['size'] / (1024**2):.1f} MB) - {action['reason']}"
                )

            logger.info(f"Cleanup removing {len(plan['cleanup_removals'])} files:")
            for i, action in enumerate(plan["cleanup_removals"], 1):
                logger.info(
                    f"  {i}. {action['file']} "
                    f"({action['size'] / (1024**2):.1f} MB) - Priority: {'Trash' if action['is_trash'] else 'Archive' if action['is_archive'] else 'Source'}"
                )

            logger.info("=== END PLAN ===")

    def process_files_intelligent(
        self,
        old_list: List[Tuple[Path, datetime]],
        mapping: Dict[str, Dict[str, Path]],
        graceful_exit: Optional[GracefulExit] = None,
        trash_files: Optional[Set[Path]] = None,
    ) -> Set[Path]:
        """Process (transcode) files and return the set of *all* paths that were
        finally removed (source MP4s + paired JPGs)."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if trash_files is None:
            trash_files = set()

        # Ensure logger and progress_bar are set
        if self.logger is None or self.progress_bar is None:
            raise RuntimeError(
                "Logger or progress_bar not initialized. Call setup_logging() first."
            )

        # Store references to avoid type checker issues with lambdas
        logger = self.logger
        progress_bar = self.progress_bar

        logger.info(f"Found {len(old_list)} files to process")
        if not old_list or graceful_exit.should_exit():
            logger.info("No files to process or cancellation requested")
            return set()

        # Generate action plan and ask for confirmation if needed
        plan = self.generate_action_plan(old_list, mapping, trash_files)
        self.display_action_plan(plan)

        if not self.config.no_confirm:
            # Ask for confirmation before proceeding
            confirm = ask_confirmation(
                "Proceed with transcoding and file removals?", default=False
            )
            if not confirm:
                logger.info("Operation cancelled by user")
                return set()

        removed_files: Set[Path] = set()
        for fp, ts in old_list:
            if graceful_exit.should_exit():
                break
            if fp in trash_files:  # already in trash
                continue

            outp = self.output_path(fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            if self.config.dry_run:
                logger.info("[DRY RUN] Would transcode %s -> %s", fp, outp)
                if jpg:
                    logger.info("[DRY RUN] Would remove paired JPG %s", jpg)
                continue

            if (
                not self.config.no_skip
                and outp.exists()
                and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
            ):
                logger.info("[SKIP] Archive exists and is large enough: %s", outp)
                # Remove source files immediately for skipped files too
                FileCleaner.remove_one(
                    fp,
                    logger,
                    self.config.dry_run,
                    self.config.use_trash,
                    self.config.get_trash_root(),
                    is_output=False,
                    source_root=self.config.directory,
                )
                if (
                    not self.config.dry_run
                ):  # Only add to removed_files if not in dry run
                    removed_files.add(fp)
                if jpg:
                    FileCleaner.remove_one(
                        jpg,
                        logger,
                        self.config.dry_run,
                        self.config.use_trash,
                        self.config.get_trash_root(),
                        is_output=False,
                        source_root=self.config.directory,
                    )
                    if (
                        not self.config.dry_run
                    ):  # Only add to removed_files if not in dry run
                        removed_files.add(jpg)
                continue

            progress_bar.start_file()
            logger.info("Transcoding %s -> %s", fp, outp)
            ok = Transcoder.transcode_file(
                fp,
                outp,
                logger,  # Type checker now knows logger is not None
                lambda pct: progress_bar.update_progress(
                    old_list.index((fp, ts)) + 1, pct
                ),
                graceful_exit=graceful_exit,
            )
            if ok:
                progress_bar.finish_file(old_list.index((fp, ts)) + 1)
                # Remove source files immediately after successful transcoding
                FileCleaner.remove_one(
                    fp,
                    logger,
                    self.config.dry_run,
                    self.config.use_trash,
                    self.config.get_trash_root(),
                    is_output=False,
                    source_root=self.config.directory,
                )
                if (
                    not self.config.dry_run
                ):  # Only add to removed_files if not in dry run
                    removed_files.add(fp)
                if jpg:
                    FileCleaner.remove_one(
                        jpg,
                        logger,
                        self.config.dry_run,
                        self.config.use_trash,
                        self.config.get_trash_root(),
                        is_output=False,
                        source_root=self.config.directory,
                    )
                    if (
                        not self.config.dry_run
                    ):  # Only add to removed_files if not in dry run
                        removed_files.add(jpg)
            else:
                logger.error("Transcoding failed for %s – keeping source", fp)

        progress_bar.start_processing()

        if graceful_exit.should_exit():
            logger.info(
                "Transcoding was cancelled - some source files may have been removed if transcoding completed before cancellation"
            )

        progress_bar.finish()
        return removed_files

    def cleanup_archive_size_limit(
        self, graceful_exit: Optional[GracefulExit] = None
    ) -> None:
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        """Comprehensive storage management with location-based priorities."""
        if graceful_exit.should_exit():
            return

        # Ensure logger is set
        if self.logger is None:
            raise RuntimeError("Logger not initialized. Call setup_logging() first.")

        base_dir = self.config.directory
        trash_root = self.config.get_trash_root()

        # Step 1: Complete system discovery
        mp4s, mapping, trash_files = FileScanner.scan_files(
            base_dir,
            include_trash=self.config.use_trash,
            trash_root=trash_root,
            graceful_exit=graceful_exit,
        )

        if self.config.dry_run:
            self.logger.info("[DRY RUN] Would clean empty directories")
            self.logger.info("[DRY RUN] Would enforce storage limits")
            # Simulate the intelligent cleanup to show what would happen
            all_file_infos = self.collect_file_info(mp4s)
            files_to_remove = self.intelligent_cleanup(all_file_infos)
            # Show what would be removed but don't remove
            self.logger.info(
                f"[DRY RUN] Would remove {len(files_to_remove)} files based on size/age limits"
            )
            self.logger.info("[DRY RUN] Would clean up orphaned JPG files")
            return

        # Step 2: Generate action plan for cleanup
        all_file_infos = self.collect_file_info(mp4s)
        files_to_remove = self.intelligent_cleanup(all_file_infos)

        # Create cleanup plan to display
        cleanup_plan = {
            "transcoding": [],  # No transcoding in cleanup mode
            "removals": [],  # No regular removals in cleanup mode
            "cleanup_removals": [
                {
                    "type": "cleanup_removal",
                    "file": file_info.path,
                    "size": file_info.size,
                    "is_archive": file_info.is_archive,
                    "is_trash": file_info.is_trash,
                    "timestamp": file_info.timestamp,
                }
                for file_info in files_to_remove
            ],
        }

        self.display_action_plan(cleanup_plan)

        # Step 3: Ask for confirmation if needed
        if not self.config.dry_run and not self.config.no_confirm:
            confirm = ask_confirmation("Proceed with cleanup removals?", default=False)
            if not confirm:
                self.logger.info("Cleanup operation cancelled by user")
                return

        # Step 4: Execute file removal
        for file_info in files_to_remove:
            FileCleaner.remove_one(
                file_info.path,
                self.logger,  # Type checker now knows logger is not None
                dry_run=False,
                use_trash=self.config.use_trash,
                trash_root=trash_root,
                is_output=file_info.is_archive,
                source_root=base_dir
                if not file_info.is_archive
                else self.config.output,
            )

        # Step 5: Clean up orphaned JPGs
        # Generate a separate plan for orphaned JPG removal
        orphaned_jpgs = []
        for key, files in mapping.items():
            jpg = files.get(".jpg")
            mp4 = files.get(".mp4")
            if jpg and mp4 is None:  # orphaned JPG
                size = 0
                try:
                    if jpg.exists():
                        size = jpg.stat().st_size
                except (OSError, IOError):
                    pass
                orphaned_jpgs.append(
                    {
                        "type": "orphaned_jpg_removal",
                        "file": jpg,
                        "reason": "No corresponding MP4 file",
                        "size": size,
                    }
                )

        # Display orphaned JPG plan
        if orphaned_jpgs:
            self.logger.info(
                f"Found {len(orphaned_jpgs)} orphaned JPG files to remove:"
            )
            for i, jpg_info in enumerate(orphaned_jpgs, 1):
                self.logger.info(
                    f"  {i}. {jpg_info['file']} "
                    f"({jpg_info['size'] / (1024**2):.1f} MB) - {jpg_info['reason']}"
                )

        # Ask for confirmation for orphaned JPGs if needed
        if orphaned_jpgs and not self.config.dry_run and not self.config.no_confirm:
            confirm = ask_confirmation(
                f"Remove {len(orphaned_jpgs)} orphaned JPG files?", default=False
            )
            if not confirm:
                self.logger.info("Orphaned JPG removal cancelled by user")
            else:
                FileCleaner.remove_orphaned_jpgs(
                    mapping,
                    set(),
                    self.logger,
                    False,  # Not dry run, user confirmed
                    self.config.use_trash,
                    trash_root,
                    graceful_exit=graceful_exit,
                )
        elif orphaned_jpgs:
            # No confirmation needed, proceed with removal
            FileCleaner.remove_orphaned_jpgs(
                mapping,
                set(),
                self.logger,
                False,  # Not dry run
                self.config.use_trash,
                trash_root,
                graceful_exit=graceful_exit,
            )

        # Step 6: Clean up empty directories AFTER all file operations are complete
        FileCleaner.clean_empty_directories(
            base_dir,
            self.logger,
            self.config.use_trash,
            trash_root,
            is_output=False,
            is_trash=False,
            graceful_exit=graceful_exit,
        )
        FileCleaner.clean_empty_directories(
            self.config.output,
            self.logger,
            self.config.use_trash,
            trash_root,
            is_output=True,
            is_trash=False,
            graceful_exit=graceful_exit,
        )
        if trash_root and trash_root.exists():
            FileCleaner.clean_empty_directories(
                trash_root,
                self.logger,
                use_trash=False,
                trash_root=None,
                is_output=False,
                is_trash=True,
                graceful_exit=graceful_exit,
            )

    def run(self, graceful_exit: Optional[GracefulExit] = None) -> int:
        """Main archiver logic with proper error handling."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        base_dir = self.config.directory
        if not base_dir.exists():
            print(
                f"Error: Directory {self.config.directory} does not exist and /camera is missing"
            )
            return 1

        trash_root = self.config.get_trash_root()

        if trash_root is not None:
            trash_root.mkdir(parents=True, exist_ok=True)

        # Always perform comprehensive discovery
        mp4s, mapping, trash_files = FileScanner.scan_files(
            base_dir,
            include_trash=self.config.use_trash,
            trash_root=trash_root,
            graceful_exit=graceful_exit,
        )

        # Set up logger and log initial configuration
        if not self.config.cleanup:
            # Normal mode: transcoding files
            cutoff = datetime.now() - timedelta(days=self.config.age)
            old_list = [(p, t) for p, t in mp4s if t < cutoff]
        else:
            # Cleanup mode: no transcoding
            old_list = []  # Define old_list even in cleanup mode to avoid unbound variable

        if not self.config.cleanup:
            self.progress_bar = ProgressReporter(
                total_files=len(old_list),
                graceful_exit=graceful_exit,
                silent=self.config.dry_run,
                out=sys.stderr,
            )
        else:
            # Cleanup mode: no transcoding
            self.progress_bar = ProgressReporter(
                total_files=0, graceful_exit=graceful_exit, silent=True, out=sys.stderr
            )

        self.logger = Logger.setup(self.config.log_file, self.progress_bar)

        if self.config.cleanup:
            self.logger.info(
                "Cleanup mode: skipping transcoding, only performing cleanup operations"
            )

        # Log initial configuration messages
        for msg in [
            "Starting camera archive process...",
            f"Input: {base_dir}",
            f"Output: {self.config.output}",
            f"Trash: {trash_root}",
            f"Age threshold: {self.config.age} days",
            f"Size limit: {self.config.max_size} GB",
            f"Dry run: {self.config.dry_run}",
            f"Cleanup only: {self.config.cleanup}",
            f"Clean output files: {self.config.clean_output}",
        ]:
            if not graceful_exit.should_exit():
                self.logger.info(msg)

        # Now perform the main operations
        user_cancelled = False
        if not self.config.cleanup:
            # For backward compatibility in transcoding logic, keep the old process_files_intelligent
            # but update it to not call intelligent_cleanup internally
            result = self.process_files_intelligent(
                old_list=old_list,
                mapping=mapping,
                graceful_exit=graceful_exit,
                trash_files=trash_files,
            )
            # If the user cancelled during transcoding, we should skip cleanup too
            if result == set() and not graceful_exit.should_exit() and old_list:
                # Operation was cancelled during transcoding (empty result set with non-empty input list)
                user_cancelled = True

        # Perform comprehensive storage management unless user cancelled during previous operation
        if not graceful_exit.should_exit() and not user_cancelled:
            self.cleanup_archive_size_limit(graceful_exit)

        if graceful_exit.should_exit():
            self.logger.info("Archive process was cancelled")
            return 1
        elif self.config.dry_run:
            self.logger.info("[DRY RUN] Done - no files were actually modified")
            return 0
        else:
            self.logger.info("Archive process completed successfully")
            return 0


class State(Enum):
    """Enumeration of possible states in the archiver workflow"""

    INITIALIZATION = "initialization"
    DISCOVERY = "discovery"
    PLANNING = "planning"
    EXECUTION = "execution"
    CLEANUP = "cleanup"
    TERMINATION = "termination"


class BaseState(ABC):
    """Base class for all state implementations"""

    def __init__(self, context: "ArchiveStateMachine"):
        self.context = context

    @abstractmethod
    def execute(self) -> State:
        """Execute the current state and return the next state"""
        pass


# Utility functions to simplify common operations
def execute_with_service_check(
    service_getter: Callable, error_msg: str, action: Callable
):
    """
    Helper function to execute an action with service availability check
    """
    service = service_getter()
    if service is None:
        print(error_msg)
        return State.TERMINATION
    return action(service)


def safe_get_data(data_dict: Dict, key: str, default=None):
    """Safely get data from the context's data dictionary"""
    return data_dict.get(key, default)


# Utility functions to eliminate code duplication
def construct_output_path(
    output_dir: Path, input_file: Path, timestamp: datetime
) -> Path:
    """Get output path for transcoded file based on timestamp"""
    # Check if parent directories match YYYY/MM/DD pattern
    if len(input_file.parts) >= 4:
        y, m, d = input_file.parts[-4:-1]
        try:
            int(y)
            int(m)
            int(d)
            # Valid date structure → reuse it
            return (
                output_dir
                / y
                / m
                / d
                / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
            )
        except ValueError:
            pass  # Not a valid date structure → fall through
    # Use timestamp-based structure
    return (
        output_dir
        / str(timestamp.year)
        / f"{timestamp.month:02d}"
        / f"{timestamp.day:02d}"
        / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
    )


# Service Classes for better modularity and testability
class FileOperationsService:
    """Service class for handling file operations"""

    def __init__(self, logger: logging.Logger, graceful_exit: GracefulExit):
        self.logger = logger
        self.graceful_exit = graceful_exit

    def scan_files(
        self, base_dir: Path, include_trash: bool, trash_root: Optional[Path]
    ) -> Tuple[List[Tuple[Path, datetime]], Dict[str, Dict[str, Path]], Set[Path]]:
        """Scan files using the existing FileScanner"""
        return FileScanner.scan_files(
            base_dir,
            include_trash=include_trash,
            trash_root=trash_root,
            graceful_exit=self.graceful_exit,
        )

    def remove_one(
        self,
        path: Path,
        dry_run: bool,
        use_trash: bool,
        trash_root: Optional[Path],
        is_output: bool,
        source_root: Path,
    ) -> None:
        """Remove a single file using the existing FileCleaner"""
        FileCleaner.remove_one(
            path, self.logger, dry_run, use_trash, trash_root, is_output, source_root
        )

    def remove_orphaned_jpgs(
        self,
        mapping: Dict[str, Dict[str, Path]],
        processed: Set[Path],
        dry_run: bool,
        use_trash: bool,
        trash_root: Optional[Path],
    ) -> None:
        """Remove orphaned JPG files"""
        FileCleaner.remove_orphaned_jpgs(
            mapping,
            processed,
            self.logger,
            dry_run,
            use_trash,
            trash_root,
            graceful_exit=self.graceful_exit,
        )

    def clean_empty_directories(
        self,
        root_dir: Path,
        use_trash: bool,
        trash_root: Optional[Path],
        is_output: bool,
        is_trash: bool,
    ) -> None:
        """Clean empty directories"""
        FileCleaner.clean_empty_directories(
            root_dir,
            self.logger,
            use_trash,
            trash_root,
            is_output,
            is_trash,
            graceful_exit=self.graceful_exit,
        )


class TranscodingService:
    """Service class for handling transcoding operations"""

    def __init__(self, logger: logging.Logger, graceful_exit: GracefulExit):
        self.logger = logger
        self.graceful_exit = graceful_exit

    def transcode_file(
        self,
        input_path: Path,
        output_path: Path,
        progress_cb: Optional[Callable[[float], None]] = None,
    ) -> bool:
        """Transcode a file using the existing Transcoder"""
        return Transcoder.transcode_file(
            input_path,
            output_path,
            self.logger,
            progress_cb,
            graceful_exit=self.graceful_exit,
        )

    def get_video_duration(self, file_path: Path) -> Optional[float]:
        """Get video duration using the existing Transcoder"""
        return Transcoder.get_video_duration(
            file_path, graceful_exit=self.graceful_exit
        )


class ActionPlanner:
    """Service class for planning operations"""

    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger

    def generate_action_plan(
        self,
        old_list: List[Tuple[Path, datetime]],
        mapping: Dict[str, Dict[str, Path]],
        trash_files: Set[Path],
    ) -> Dict[str, List]:
        """Generate action plan for operations"""
        # Create a temporary archiver instance just for planning
        archiver = _ActionPlanGenerator(self.config)
        return archiver.generate_action_plan(old_list, mapping, trash_files)

    def display_action_plan(self, plan: Dict[str, List]) -> None:
        """Display the action plan"""
        archiver = _ActionPlanGenerator(self.config)
        archiver.logger = self.logger
        archiver.display_action_plan(plan)

    def collect_file_info(
        self, old_list: List[Tuple[Path, datetime]]
    ) -> List[FileInfo]:
        """Collect file info for cleanup"""
        archiver = _ActionPlanGenerator(self.config)
        return archiver.collect_file_info(old_list)

    def intelligent_cleanup(self, all_files: List[FileInfo]) -> List[FileInfo]:
        """Perform intelligent cleanup"""
        archiver = _ActionPlanGenerator(self.config)
        archiver.logger = self.logger
        return archiver.intelligent_cleanup(all_files)


class _ActionPlanGenerator:
    """
    Internal utility class containing only the methods needed for planning and info collection
    This eliminates the circular dependency between ActionPlanner and Archiver
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger: Optional[logging.Logger] = None

    def _ensure_logger(self) -> logging.Logger:
        """Ensure logger is initialized, raise error if not"""
        if self.logger is None:
            raise RuntimeError("Logger not initialized.")
        return self.logger

    def output_path(self, input_file: Path, timestamp: datetime) -> Path:
        out_dir = self.config.output
        # Check if parent directories match YYYY/MM/DD pattern
        if len(input_file.parts) >= 4:
            y, m, d = input_file.parts[-4:-1]
            try:
                int(y)
                int(m)
                int(d)
                # Valid date structure → reuse it
                return (
                    out_dir
                    / y
                    / m
                    / d
                    / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
                )
            except ValueError:
                pass  # Not a valid date structure → fall through
        # Use timestamp-based structure
        return (
            out_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )

    def get_all_archive_files(self) -> List[Path]:
        """Get all archive files including trash if enabled."""
        out_dir = self.config.output
        trash_root = self.config.get_trash_root()
        archive_files = (
            list(out_dir.rglob("archived-*.mp4")) if out_dir.exists() else []
        )

        if trash_root:
            trash_output_dir = trash_root / "output"
            if trash_output_dir.exists():
                archive_files.extend(list(trash_output_dir.rglob("archived-*.mp4")))

        return archive_files

    def collect_file_info(
        self, old_list: List[Tuple[Path, datetime]]
    ) -> List[FileInfo]:
        """Collect file information for all relevant files."""
        all_files = []
        seen_paths = set()
        trash_root = self.config.get_trash_root()

        # Get all archive files
        archive_files = self.get_all_archive_files()

        # Process archive files - mark them as archives and skip if in old_list (source)
        for archive_file in archive_files:
            try:
                if not archive_file.is_file():
                    continue
                size = archive_file.stat().st_size
            except (OSError, IOError):
                continue

            ts_match = re.search(r"archived-(\d{14})\.mp4$", archive_file.name)
            if ts_match:
                try:
                    ts = datetime.strptime(ts_match.group(1), "%Y%m%d%H%M%S")
                    is_trash = trash_root is not None and any(
                        p in archive_file.parents
                        for p in [trash_root, trash_root / "output"]
                    )
                    if archive_file not in seen_paths:
                        all_files.append(
                            FileInfo(
                                archive_file,
                                ts,
                                size,
                                is_archive=True,
                                is_trash=is_trash,
                            )
                        )
                        seen_paths.add(archive_file)
                except ValueError:
                    pass

        # Process source files from old_list - skip if already processed as archives
        for fp, ts in old_list:
            if (
                fp in seen_paths
            ):  # Skip if this is an archive file we've already processed
                continue

            try:
                if not fp.is_file():
                    continue
                size = fp.stat().st_size
            except (OSError, IOError):
                continue

            is_trash = trash_root is not None and any(
                p in fp.parents for p in [trash_root, trash_root / "input"]
            )
            all_files.append(
                FileInfo(fp, ts, size, is_archive=False, is_trash=is_trash)
            )
            seen_paths.add(fp)

        return all_files

    def _categorize_files(self, all_files: List[FileInfo]) -> Dict[int, List[FileInfo]]:
        """Categorize files by location priority (0 = trash, 1 = archive, 2 = source)."""
        categorized_files: Dict[int, List[FileInfo]] = {0: [], 1: [], 2: []}

        for file_info in all_files:
            if file_info.is_trash:
                categorized_files[0].append(file_info)
            elif file_info.is_archive:
                categorized_files[1].append(file_info)
            else:
                categorized_files[2].append(file_info)

        # Sort each category by timestamp (oldest first)
        for category in categorized_files.values():
            category.sort(key=lambda x: x.timestamp)

        return categorized_files

    def _apply_size_cleanup(
        self,
        categorized_files: Dict[int, List[FileInfo]],
        total_size: int,
        size_limit: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply size-based cleanup based on priority."""
        files_to_remove = []
        remaining_size = total_size

        logger = self._ensure_logger()

        logger.info("Archive size exceeds limit")
        logger.info(
            f"Size threshold exceeded, removing files by priority to reach {self.config.max_size} GB..."
        )
        logger.info(
            "Priority order: Trash > Archive > Source (oldest first within each)"
        )

        # Remove files starting from highest priority (0) to lowest (2)
        for priority in range(3):
            if remaining_size <= size_limit:
                break

            category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
            category_files = categorized_files[priority]

            if category_files:
                logger.info(f"Processing {category_name} files for size cleanup...")

                for file_info in category_files:
                    if remaining_size <= size_limit:
                        break

                    files_to_remove.append(file_info)
                    remaining_size -= file_info.size

                    if self.config.dry_run:
                        logger.info(
                            f"[DRY RUN] Would remove {category_name} file for size: {file_info.path} "
                            f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                        )

        logger.info(
            f"After size cleanup: {remaining_size / (1024**3):.1f} GB "
            f"({len(files_to_remove)} files marked for removal)"
        )

        return files_to_remove, remaining_size

    def _apply_age_cleanup(
        self,
        categorized_files: Dict[int, List[FileInfo]],
        age_cutoff: datetime,
        total_size: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply age-based cleanup respecting clean_output setting."""
        files_to_remove = []
        remaining_size = total_size

        logger = self._ensure_logger()

        files_over_age_by_priority: Dict[int, List[FileInfo]] = {
            0: [],
            1: [],
            2: [],
        }

        for priority in range(3):
            # Skip archive files (priority 1) if clean_output is False
            if priority == 1 and not self.config.clean_output:
                continue

            files_over_age_by_priority[priority] = [
                f for f in categorized_files[priority] if f.timestamp < age_cutoff
            ]

        total_over_age = sum(
            len(files) for files in files_over_age_by_priority.values()
        )

        if total_over_age > 0:
            logger.info(
                f"Found {total_over_age} files older than {self.config.age} days"
            )

            # Remove age-eligible files by priority order
            for priority in range(3):
                # Skip archive files (priority 1) if clean_output is False
                if priority == 1 and not self.config.clean_output:
                    continue

                category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
                age_files = files_over_age_by_priority[priority]

                if age_files:
                    logger.info(f"Processing {category_name} files for age cleanup...")

                    for file_info in age_files:
                        files_to_remove.append(file_info)
                        remaining_size -= file_info.size

                        if self.config.dry_run:
                            logger.info(
                                f"[DRY RUN] Would remove {category_name} file for age: {file_info.path} "
                                f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                            )

            logger.info(f"Added {total_over_age} files for age-based removal")
        else:
            logger.info(f"No files older than {self.config.age} days found")

        return files_to_remove, remaining_size

    def intelligent_cleanup(self, all_files: List[FileInfo]) -> List[FileInfo]:
        """
        Select files to remove based on location priority and size/age constraints.
        Priority order: Trash > Archive > Source (oldest first within each category).
        Output/archive files are excluded from age-based removal unless clean_output=True.
        """
        if not all_files:
            return []

        logger = self._ensure_logger()

        # Calculate totals
        total_size = sum(f.size for f in all_files)
        size_limit = self.config.max_size * (1024**3)
        age_cutoff = datetime.now() - timedelta(days=self.config.age)

        logger.info(f"Current total size: {total_size / (1024**3):.1f} GB")
        logger.info(f"Size limit: {self.config.max_size} GB")
        logger.info(f"Age cutoff: {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
        if not self.config.clean_output:
            logger.info("Output files excluded from age-based cleanup")

        # Categorize files by location priority
        categorized_files = self._categorize_files(all_files)

        files_to_remove = []
        remaining_size = total_size

        # PHASE 1: Enforce size limit (if over limit)
        if remaining_size > size_limit:
            files_to_remove, remaining_size = self._apply_size_cleanup(
                categorized_files, total_size, size_limit
            )
        else:
            # PHASE 2: Enforce age limit (only if under size limit AND age_days > 0)
            if self.config.age > 0:
                files_to_remove, remaining_size = self._apply_age_cleanup(
                    categorized_files, age_cutoff, total_size
                )
            else:
                logger.info("Age-based cleanup disabled (age_days <= 0)")

        # Remove duplicates and sort by priority then timestamp
        unique_files = {f.path: f for f in files_to_remove}
        files_to_remove = list(unique_files.values())

        def sort_key(file_info: FileInfo):
            if file_info.is_trash:
                priority = 0
            elif file_info.is_archive:
                priority = 1
            else:
                priority = 2
            return (priority, file_info.timestamp)

        files_to_remove.sort(key=sort_key)

        logger.info(
            f"Final removal plan: {len(files_to_remove)} files, "
            f"final size: {remaining_size / (1024**3):.1f} GB"
        )

        return files_to_remove

    def generate_action_plan(
        self,
        old_list: List[Tuple[Path, datetime]],
        mapping: Dict[str, Dict[str, Path]],
        trash_files: Set[Path],
    ) -> Dict[str, List]:
        """Generate a plan of all actions to be performed (transcoding and removals)"""
        transcoding_actions = []
        removal_actions = []

        for fp, ts in old_list:
            if fp in trash_files:  # already in trash
                continue

            outp = self.output_path(fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            # Check skip logic
            if (
                not self.config.no_skip
                and outp.exists()
                and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
            ):
                # Will skip transcoding but remove source files
                size = 0
                if fp.exists():
                    try:
                        size = fp.stat().st_size
                    except OSError:
                        size = 0
                removal_actions.append(
                    {
                        "type": "source_removal_after_skip",
                        "file": fp,
                        "reason": f"Skipping transcoding: archive exists at {outp}",
                        "size": size,
                    }
                )
                if jpg:
                    size = 0
                    if jpg.exists():
                        try:
                            size = jpg.stat().st_size
                        except OSError:
                            size = 0
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_skip",
                            "file": jpg,
                            "reason": "Skipping transcoding: archive exists for paired MP4",
                            "size": size,
                        }
                    )
            else:
                # Will transcode the file
                size = 0
                if fp.exists():
                    try:
                        size = fp.stat().st_size
                    except OSError:
                        size = 0
                transcoding_actions.append(
                    {
                        "type": "transcode",
                        "input": fp,
                        "output": outp,
                        "size": size,
                        "jpg_to_remove": jpg,
                    }
                )
                if jpg:
                    size = 0
                    if jpg.exists():
                        try:
                            size = jpg.stat().st_size
                        except OSError:
                            size = 0
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_transcode",
                            "file": jpg,
                            "reason": "Paired with transcoded MP4",
                            "size": size,
                        }
                    )

        # Also get cleanup plan for existing archives if in cleanup mode
        cleanup_removals = []
        if self.config.cleanup:
            all_file_infos = self.collect_file_info(old_list)
            files_to_remove = self.intelligent_cleanup(all_file_infos)
            for file_info in files_to_remove:
                cleanup_removals.append(
                    {
                        "type": "cleanup_removal",
                        "file": file_info.path,
                        "size": file_info.size,
                        "is_archive": file_info.is_archive,
                        "is_trash": file_info.is_trash,
                        "timestamp": file_info.timestamp,
                    }
                )

        return {
            "transcoding": transcoding_actions,
            "removals": removal_actions,
            "cleanup_removals": cleanup_removals,
        }

    def display_action_plan(self, plan: Dict[str, List]) -> None:
        """Display the action plan to the user"""
        logger = self.logger
        if logger:
            logger.info("=== ACTION PLAN ===")
            logger.info(f"Transcoding {len(plan['transcoding'])} files:")
            for i, action in enumerate(plan["transcoding"], 1):
                logger.info(
                    f"  {i}. {action['input']} -> {action['output']} "
                    f"({action['size'] / (1024**2):.1f} MB)"
                )
                if action["jpg_to_remove"]:
                    logger.info(
                        f"      + Removing paired JPG: {action['jpg_to_remove']}"
                    )

            logger.info(f"Removing {len(plan['removals'])} files:")
            for i, action in enumerate(plan["removals"], 1):
                logger.info(
                    f"  {i}. {action['file']} "
                    f"({action['size'] / (1024**2):.1f} MB) - {action['reason']}"
                )

            logger.info(f"Cleanup removing {len(plan['cleanup_removals'])} files:")
            for i, action in enumerate(
                plan["cleanup_removals"], 1
            ):  # Fixed typo: cleanup_removals
                logger.info(
                    f"  {i}. {action['file']} "
                    f"({action['size'] / (1024**2):.1f} MB) - Priority: {'Trash' if action['is_trash'] else 'Archive' if action['is_archive'] else 'Source'}"
                )

            logger.info("=== END PLAN ===")


class ArchiveStateMachine:
    """Main state machine that manages the archiver workflow"""

    def __init__(self, config: Config, graceful_exit: GracefulExit):
        self.config = config
        self.graceful_exit = graceful_exit
        self.state: Optional[BaseState] = None
        self.data: Dict[str, Any] = {}  # Shared data between states
        self.error_occurred = False  # Track if any error occurred
        self._exit_code: int = 0  # Track exit code for proper termination

        # Initialize service classes as None, will be set in InitializationState
        self.file_ops_service: Optional[FileOperationsService] = None
        self.transcoding_service: Optional[TranscodingService] = None
        self.action_planner: Optional[ActionPlanner] = None

        # Initialize all states
        self._states = {
            State.INITIALIZATION: InitializationState(self),
            State.DISCOVERY: DiscoveryState(self),
            State.PLANNING: PlanningState(self),
            State.EXECUTION: ExecutionState(self),
            State.CLEANUP: CleanupState(self),
            State.TERMINATION: TerminationState(self),
        }

    def run(self) -> int:
        """Run the state machine starting from initialization"""
        # Start with initialization state
        self.state = self._states[State.INITIALIZATION]

        # Continue executing until we reach termination
        while self.state:
            # Check for graceful exit
            if self.graceful_exit.should_exit():
                # Go directly to termination if exit requested
                self.state = self._states[State.TERMINATION]

            try:
                # At this point, self.state is guaranteed not to be None due to the while condition
                assert self.state is not None
                next_state = self.state.execute()
                if next_state == State.TERMINATION:
                    # When we reach termination, the context holds the exit code info
                    # Return appropriate exit code based on context
                    if self.graceful_exit.should_exit():
                        return 1
                    elif self.error_occurred:
                        return 1  # Return error code if any error occurred during processing
                    else:
                        # Check if this was due to dry run completion or normal completion
                        config = self.config
                        if hasattr(self, "_exit_code"):
                            return self._exit_code
                        else:
                            # Default based on dry run status
                            return 0 if config.dry_run else 0
                elif next_state in self._states:
                    self.state = self._states[next_state]
                else:
                    # Invalid state transition, terminate
                    self.state = self._states[State.TERMINATION]
            except Exception as e:
                # On error, transition to termination
                print(
                    f"Error in state {self.state.__class__.__name__ if self.state else 'None'}: {e}"
                )
                # Return error code
                return 1

        return 0


class InitializationState(BaseState):
    """State responsible for initializing the archiver"""

    def execute(self) -> State:
        """Execute initialization and return the next state"""
        config = self.context.config
        graceful_exit = self.context.graceful_exit

        # Validate input directory exists
        base_dir = config.directory
        if not base_dir.exists():
            print(
                f"Error: Directory {config.directory} does not exist and /camera is missing"
            )
            self.context.error_occurred = True
            return State.TERMINATION

        trash_root = config.get_trash_root()
        if trash_root is not None:
            trash_root.mkdir(parents=True, exist_ok=True)

        # Set up logging
        if not config.cleanup:
            # Normal mode: transcoding files - count old files for progress bar
            cutoff = datetime.now() - timedelta(days=config.age)
            mp4s, _, _ = FileScanner.scan_files(
                base_dir,
                include_trash=config.use_trash,
                trash_root=trash_root,
                graceful_exit=graceful_exit,
            )
            old_list = [(p, t) for p, t in mp4s if t < cutoff]
            total_files = len(old_list)
        else:
            # Cleanup mode: no transcoding
            total_files = 0

        # Create progress reporter
        progress_bar = ProgressReporter(
            total_files=total_files,
            graceful_exit=graceful_exit,
            silent=config.dry_run,
            out=sys.stderr,
        )
        self.context.data["progress_bar"] = progress_bar

        # Setup logger
        logger = Logger.setup(config.log_file, progress_bar)
        self.context.data["logger"] = logger

        # Initialize service classes
        self.context.file_ops_service = FileOperationsService(logger, graceful_exit)
        self.context.transcoding_service = TranscodingService(logger, graceful_exit)
        self.context.action_planner = ActionPlanner(config, logger)

        if config.cleanup:
            logger.info(
                "Cleanup mode: skipping transcoding, only performing cleanup operations"
            )

        # Log initial configuration
        for msg in [
            "Starting camera archive process...",
            f"Input: {base_dir}",
            f"Output: {config.output}",
            f"Trash: {trash_root}",
            f"Age threshold: {config.age} days",
            f"Size limit: {config.max_size} GB",
            f"Dry run: {config.dry_run}",
            f"Cleanup only: {config.cleanup}",
            f"Clean output files: {config.clean_output}",
        ]:
            if not graceful_exit.should_exit():
                logger.info(msg)

        # Return the next state based on configuration
        return State.DISCOVERY


class DiscoveryState(BaseState):
    """State responsible for discovering and scanning files"""

    def execute(self) -> State:
        """Execute file discovery and return the next state"""
        config = self.context.config
        logger = self.context.data.get("logger")

        if logger is None:
            print("Error: Logger not available in DiscoveryState")
            return State.TERMINATION

        base_dir = config.directory
        trash_root = config.get_trash_root()

        # Perform file scanning using the service class
        file_ops_service = self.context.file_ops_service
        if file_ops_service is None:
            print("Error: FileOperationsService not available in DiscoveryState")
            return State.TERMINATION

        mp4s, mapping, trash_files = file_ops_service.scan_files(
            base_dir, include_trash=config.use_trash, trash_root=trash_root
        )

        # Store discovered data for later use
        self.context.data["mp4s"] = mp4s
        self.context.data["mapping"] = mapping
        self.context.data["trash_files"] = trash_files

        logger.info(f"Discovered {len(mp4s)} MP4 files, {len(mapping)} file groups")

        return State.PLANNING


class PlanningState(BaseState):
    """State responsible for generating action plans"""

    def execute(self) -> State:
        """Execute planning and return the next state"""
        config = self.context.config
        logger = self.context.data.get("logger")

        if logger is None:
            print("Error: Logger not available in PlanningState")
            return State.TERMINATION

        # Get data from discovery
        mp4s = self.context.data.get("mp4s", [])
        mapping = self.context.data.get("mapping", {})
        trash_files = self.context.data.get("trash_files", set())

        if not config.cleanup:
            # For normal mode, find files that are old enough to process
            cutoff = datetime.now() - timedelta(days=config.age)
            old_list = [(p, t) for p, t in mp4s if t < cutoff]
            self.context.data["old_list"] = old_list
        else:
            # For cleanup mode, no transcoding to plan
            self.context.data["old_list"] = []

        logger.info(
            f"Planning operations for {len(self.context.data['old_list'])} files"
        )

        # Use the action planner service to generate action plan based on configuration
        action_planner = self.context.action_planner
        if action_planner is None:
            print("Error: ActionPlanner not available in PlanningState")
            return State.TERMINATION

        if not config.cleanup:
            # Generate plan for transcoding operations
            plan = action_planner.generate_action_plan(
                self.context.data["old_list"], mapping, trash_files
            )
            self.context.data["action_plan"] = plan
        else:
            # Generate plan for cleanup operations
            all_file_infos = action_planner.collect_file_info(mp4s)
            files_to_remove = action_planner.intelligent_cleanup(all_file_infos)
            cleanup_plan = {
                "transcoding": [],  # No transcoding in cleanup mode
                "removals": [],  # No regular removals in cleanup mode
                "cleanup_removals": [
                    {
                        "type": "cleanup_removal",
                        "file": file_info.path,
                        "size": file_info.size,
                        "is_archive": file_info.is_archive,
                        "is_trash": file_info.is_trash,
                        "timestamp": file_info.timestamp,
                    }
                    for file_info in files_to_remove
                ],
            }
            self.context.data["action_plan"] = cleanup_plan

        # Display plan to user if confirmation is required
        if not config.no_confirm:
            action_planner.display_action_plan(self.context.data["action_plan"])
            confirm = ask_confirmation(
                "Proceed with planned operations?", default=False
            )
            if not confirm:
                logger.info("Operation cancelled by user")
                return State.TERMINATION

        return State.EXECUTION


class ExecutionState(BaseState):
    """State responsible for executing planned actions"""

    def execute(self) -> State:
        """Execute planned actions and return the next state"""
        config = self.context.config
        graceful_exit = self.context.graceful_exit
        logger = self.context.data.get("logger")

        if logger is None:
            print("Error: Logger not available in ExecutionState")
            return State.TERMINATION

        # Get plan and data
        plan = self.context.data.get("action_plan", {})
        old_list = self.context.data.get("old_list", [])
        mapping = self.context.data.get("mapping", {})
        trash_files = self.context.data.get("trash_files", set())
        progress_bar = self.context.data.get("progress_bar")

        # Get service classes
        file_ops_service = self.context.file_ops_service
        transcoding_service = self.context.transcoding_service

        if file_ops_service is None or transcoding_service is None:
            print("Error: Service classes not available in ExecutionState")
            return State.TERMINATION

        if not config.cleanup:
            # Execute transcoding operations
            logger.info(f"Starting transcoding of {len(old_list)} files")

            processed_files = set()
            for i, (fp, ts) in enumerate(old_list):
                if graceful_exit.should_exit():
                    logger.info("Execution cancelled by user")
                    break

                if fp in trash_files:  # already in trash
                    continue

                outp = construct_output_path(config.output, fp, ts)
                jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

                # Check if we should skip transcoding
                if (
                    not config.no_skip
                    and outp.exists()
                    and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
                ):
                    logger.info("[SKIP] Archive exists and is large enough: %s", outp)
                    # Remove source files immediately for skipped files too
                    file_ops_service.remove_one(
                        fp,
                        config.dry_run,
                        config.use_trash,
                        config.get_trash_root(),
                        is_output=False,
                        source_root=config.directory,
                    )
                    if not config.dry_run:
                        processed_files.add(fp)
                    if jpg:
                        file_ops_service.remove_one(
                            jpg,
                            config.dry_run,
                            config.use_trash,
                            config.get_trash_root(),
                            is_output=False,
                            source_root=config.directory,
                        )
                        if not config.dry_run:
                            processed_files.add(jpg)
                    continue

                # Transcode the file
                if not config.dry_run:
                    if progress_bar:
                        progress_bar.start_file()
                    logger.info("Transcoding %s -> %s", fp, outp)

                    ok = transcoding_service.transcode_file(
                        fp,
                        outp,
                        lambda pct: progress_bar.update_progress(i + 1, pct)
                        if progress_bar
                        else None,
                    )

                    if ok:
                        if progress_bar:
                            progress_bar.finish_file(i + 1)
                        # Remove source files after successful transcoding
                        file_ops_service.remove_one(
                            fp,
                            config.dry_run,
                            config.use_trash,
                            config.get_trash_root(),
                            is_output=False,
                            source_root=config.directory,
                        )
                        processed_files.add(fp)
                        if jpg:
                            file_ops_service.remove_one(
                                jpg,
                                config.dry_run,
                                config.use_trash,
                                config.get_trash_root(),
                                is_output=False,
                                source_root=config.directory,
                            )
                            processed_files.add(jpg)
                    else:
                        logger.error("Transcoding failed for %s – keeping source", fp)
                else:
                    logger.info("[DRY RUN] Would transcode %s -> %s", fp, outp)
                    if jpg:
                        logger.info("[DRY RUN] Would remove paired JPG %s", jpg)

            if graceful_exit.should_exit():
                logger.info("Transcoding was cancelled")

        else:
            # Execute cleanup operations
            logger.info("Starting cleanup operations")

            # Execute cleanup removals from plan
            for item in plan.get("cleanup_removals", []):
                if graceful_exit.should_exit():
                    logger.info("Cleanup cancelled by user")
                    break

                file_path = item["file"]
                file_ops_service.remove_one(
                    file_path,
                    config.dry_run,
                    config.use_trash,
                    config.get_trash_root(),
                    is_output=item["is_archive"],
                    source_root=config.directory
                    if not item["is_archive"]
                    else config.output,
                )

            if not config.dry_run and graceful_exit.should_exit():
                logger.info("Cleanup was cancelled")

        return State.CLEANUP


class CleanupState(BaseState):
    """State responsible for final cleanup operations"""

    def execute(self) -> State:
        """Execute cleanup operations and return the next state"""
        config = self.context.config
        logger = self.context.data.get("logger")

        if logger is None:
            print("Error: Logger not available in CleanupState")
            return State.TERMINATION

        # Get data from previous states
        mapping = self.context.data.get("mapping", {})
        base_dir = config.directory
        trash_root = config.get_trash_root()

        logger.info("Starting final cleanup operations")

        # Get service classes
        file_ops_service = self.context.file_ops_service
        if file_ops_service is None:
            print("Error: FileOperationsService not available in CleanupState")
            return State.TERMINATION

        if not config.cleanup:
            # In normal mode, also remove orphaned JPGs
            logger.info("Removing orphaned JPG files")
            file_ops_service.remove_orphaned_jpgs(
                mapping,
                set(),
                config.dry_run,
                config.use_trash,
                trash_root,
            )

        # Always clean empty directories
        logger.info("Cleaning empty directories")
        file_ops_service.clean_empty_directories(
            base_dir,
            config.use_trash,
            trash_root,
            is_output=False,
            is_trash=False,
        )
        file_ops_service.clean_empty_directories(
            config.output,
            config.use_trash,
            trash_root,
            is_output=True,
            is_trash=False,
        )
        if trash_root and trash_root.exists():
            file_ops_service.clean_empty_directories(
                trash_root,
                False,  # use_trash=False
                None,  # trash_root for this operation
                is_output=False,
                is_trash=True,
            )

        return State.TERMINATION


class TerminationState(BaseState):
    """State responsible for proper shutdown and resource cleanup"""

    def execute(self) -> State:
        """Execute termination and return State.TERMINATION to indicate completion"""
        logger = self.context.data.get("logger")
        progress_bar = self.context.data.get("progress_bar")

        graceful_exit = self.context.graceful_exit
        config = self.context.config

        if logger:
            if graceful_exit.should_exit():
                logger.info("Archive process was cancelled")
            elif config.dry_run:
                logger.info("[DRY RUN] Done - no files were actually modified")
            else:
                logger.info("Archive process completed successfully")

        # Clean up progress bar
        if progress_bar:
            progress_bar.finish()

        # The exit code will be handled by the state machine run method based on context
        return State.TERMINATION


def ask_confirmation(prompt: str, default: bool = True) -> bool:
    """Ask the user for confirmation with a prompt."""
    if default:
        choice_str = "Y/n"
    else:
        choice_str = "y/N"

    while True:
        try:
            response = input(f"{prompt} [{choice_str}] ").strip().lower()
            if not response:
                return default
            if response in ["y", "yes"]:
                return True
            if response in ["n", "no"]:
                return False
            print("Please answer 'y' or 'n'.")
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled.")
            return False


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Archive and transcode camera files based on timestamp parsing"
    )
    parser.add_argument(
        "--directory", "-d", type=Path, default="/camera", help="Input directory"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default="/camera/archived",
        help="Destination for archived MP4s",
    )
    parser.add_argument("--age", "-a", type=int, default=30, help="Age in days")
    parser.add_argument(
        "--dry-run", "-n", action="store_true", help="Show actions only"
    )
    parser.add_argument(
        "--max-size", "-m", type=int, default=500, help="Maximum archive size in GB"
    )
    parser.add_argument(
        "--no-skip",
        "-s",
        action="store_true",
        help="Do not skip transcoding when archived copy exists",
    )
    parser.add_argument(
        "--no-trash",
        action="store_true",
        help="Disable trash functionality (permanently delete files)",
    )
    parser.add_argument("--trashdir", type=Path, help="Specify custom trash directory")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Skip transcoding and only perform cleanup (orphaned JPGs, empty dirs, size/age limits)",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Include output files in age-based cleanup (default: exclude output files)",
    )
    parser.add_argument(
        "-y",
        "--no-confirm",
        action="store_true",
        help="Skip confirmation prompts for transcoding and file removal (default: ask for confirmation)",
    )
    args = parser.parse_args()
    # Handle the --no-trash flag
    args.use_trash = not args.no_trash
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    return args


def main():
    """Main entry point with argument parsing."""
    graceful_exit = GracefulExit()
    try:
        args = parse_arguments()

        # Create config and run archiver inside the same try block
        config = Config.from_args(args)
        archiver = Archiver(config)
        exit_code = archiver.run(graceful_exit)
        sys.exit(exit_code)

    except KeyboardInterrupt:
        graceful_exit.request_exit()
        print("\nReceived KeyboardInterrupt, shutting down gracefully...")
        sys.exit(1)
    except Exception as e:
        if not graceful_exit.should_exit():
            logging.getLogger("camera_archiver").error(f"Unexpected error: {e}")
            sys.exit(1)  # Exit with error code instead of re-raising
        else:
            print("Process was cancelled")
            sys.exit(1)


if __name__ == "__main__":
    main()
