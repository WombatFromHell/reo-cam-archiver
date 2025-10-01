#!/usr/bin/env python3
"""
Camera Archiver: Transcodes and archives camera footage based on timestamp parsing,
with intelligent cleanup based on size and age thresholds.
"""

import argparse
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import threading
import os
import shutil
import atexit
import signal
from typing import Dict, List, Optional, Set, Tuple, Callable


# Constants
MIN_ARCHIVE_SIZE_BYTES = 1_048_576  # 1MB
DEFAULT_PROGRESS_WIDTH = 30
PROGRESS_UPDATE_INTERVAL = 5  # seconds for non-TTY output


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


class GracefulExit:
    """Global flag for graceful exit handling"""

    exit_requested = False
    _lock = threading.Lock()

    @classmethod
    def request_exit(cls):
        with cls._lock:
            cls.exit_requested = True

    @classmethod
    def should_exit(cls):
        with cls._lock:
            return cls.exit_requested


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


class ProgressBar:
    def __init__(
        self,
        total_files: int,
        width: int = DEFAULT_PROGRESS_WIDTH,
        silent: bool = False,
        out=sys.stderr,
    ):
        self.total = total_files
        self.width = max(10, width)
        self.blocks = self.width - 2
        self.silent = silent or out is None
        self.out = out
        self.orchestrator = ConsoleOrchestrator()
        self.start_time = None
        self.file_start = None
        self._progress_line = ""
        self._last_print_time = time.time()
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
        GracefulExit.request_exit()
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

    @property
    def has_progress(self) -> bool:
        return bool(self._progress_line)

    def start_processing(self):
        if self.start_time is None:
            self.start_time = time.time()

    def start_file(self):
        self.file_start = time.time()
        if self.start_time is None:
            self.start_time = time.time()

    def update_progress(self, idx: int, pct: float = 0.0):
        if self.silent or GracefulExit.should_exit():
            return
        line = self._format_line(idx, pct)
        if line == self._progress_line:
            return
        self._progress_line = line
        self._display(line)

    def finish_file(self, idx: int):
        if not GracefulExit.should_exit():
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
        if self.silent or not self._progress_line or GracefulExit.should_exit():
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


def setup_logging(
    log_file: Path, progress_bar: Optional[ProgressBar] = None
) -> logging.Logger:
    logger = logging.getLogger("camera_archiver")
    logger.setLevel(logging.INFO)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    fh = logging.FileHandler(log_file, encoding="utf-8")
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
        safe_remove(
            path,
            logger,
            dry_run=False,
            use_trash=use_trash,
            trash_root=trash_root,
            is_output=is_output,
            source_root=source_root,
        )


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


def safe_remove(
    file_path: Path,
    logger: logging.Logger,
    dry_run: bool,
    use_trash: bool = False,
    trash_root: Optional[Path] = None,
    is_output: bool = False,
    source_root: Optional[Path] = None,
):
    """Safely remove a file, optionally moving to trash."""
    if GracefulExit.should_exit():
        return

    if dry_run:
        logger.info(f"[DRY RUN] Would remove {file_path}")
        return

    try:
        if source_root is None:
            source_root = file_path.parent

        if use_trash and trash_root:
            new_dest = calculate_trash_destination(
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


def get_video_duration(file_path: Path) -> Optional[float]:
    """Get video duration using ffprobe."""
    if GracefulExit.should_exit() or not shutil.which("ffprobe"):
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


def transcode_file(
    input_path: Path,
    output_path: Path,
    logger: logging.Logger,
    progress_cb: Optional[Callable[[float], None]] = None,
) -> bool:
    """Transcode a video file using ffmpeg with QSV hardware acceleration."""
    if GracefulExit.should_exit():
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
    total_duration = get_video_duration(input_path)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    log_lines = []
    prev_pct = -1.0
    cur_pct = 0.0

    # Handle both file-like objects and iterables for stdout
    stdout_iter = None
    if proc.stdout:
        if hasattr(proc.stdout, "readline"):
            # File-like object with readline method
            stdout_iter = iter(proc.stdout.readline, "")
        elif hasattr(proc.stdout, "__iter__"):
            # Iterable (like list of strings in tests)
            stdout_iter = proc.stdout
        else:
            logger.error(f"Unsupported stdout type: {type(proc.stdout)}")
            proc.terminate()
            return False
    else:
        return False

    if stdout_iter:
        for line in stdout_iter:
            if GracefulExit.should_exit():
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
            log_lines.append(line)

            if total_duration and total_duration > 0:
                m = re.search(r"time=([0-9:.]+)", line)
                if m:
                    h, mn, s = map(float, m.group(1).split(":")[:3])
                    cur_pct = min(
                        (h * 3600 + mn * 60 + s) / total_duration * 100, 100.0
                    )
            else:
                cur_pct = min(cur_pct + 1, 99.0)

            if progress_cb and cur_pct != prev_pct:
                progress_cb(cur_pct)
                prev_pct = cur_pct

    rc = proc.wait()
    if rc != 0 and not GracefulExit.should_exit():
        msg = (
            f"FFmpeg failed (code {rc}) for {input_path} -> {output_path}\n"
            + "".join(log_lines)
        )
        logger.error(msg)
    return rc == 0 and not GracefulExit.should_exit()


def scan_files(
    base_dir: Path, include_trash: bool = False, trash_root: Optional[Path] = None
) -> Tuple[List[Tuple[Path, datetime]], Dict[str, Dict[str, Path]], Set[Path]]:
    """Scan for MP4 and JPG files with valid timestamps."""
    if GracefulExit.should_exit():
        return [], {}, set()

    mp4s = []
    mapping = {}
    trash_files = set()

    # Scan base directory
    for p in base_dir.rglob("*.*"):
        if GracefulExit.should_exit():
            break

        if not p.is_file():
            continue

        ts = parse_timestamp_from_filename(p.name)
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
                    if GracefulExit.should_exit():
                        break

                    if not p.is_file():
                        continue

                    ts = parse_timestamp_from_filename(p.name)
                    if not ts:
                        continue
                    key = ts.strftime("%Y%m%d%H%M%S")
                    ext = p.suffix.lower()

                    mapping.setdefault(key, {})[ext] = p
                    trash_files.add(p)
                    if ext == ".mp4":
                        mp4s.append((p, ts))

    return mp4s, mapping, trash_files


def output_path(input_file: Path, timestamp: datetime, out_dir: Path) -> Path:
    """Generate output path for archived file."""
    if len(input_file.parts) >= 4:
        y, m, d = input_file.parts[-4:-1]
        return (
            out_dir / y / m / d / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
    else:
        return (
            out_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )


def get_all_archive_files(
    out_dir: Path, trash_root: Optional[Path] = None
) -> List[Path]:
    """Get all archive files including trash if enabled."""
    archive_files = list(out_dir.rglob("archived-*.mp4")) if out_dir.exists() else []

    if trash_root:
        trash_output_dir = trash_root / "output"
        if trash_output_dir.exists():
            archive_files.extend(list(trash_output_dir.rglob("archived-*.mp4")))

    return archive_files


def collect_file_info(
    old_list: List[Tuple[Path, datetime]],
    out_dir: Path,
    trash_root: Optional[Path] = None,
) -> List[FileInfo]:
    """Collect file information for all relevant files."""
    all_files = []
    seen_paths = set()

    # Get all archive files
    archive_files = get_all_archive_files(out_dir, trash_root)

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
                            archive_file, ts, size, is_archive=True, is_trash=is_trash
                        )
                    )
                    seen_paths.add(archive_file)
            except ValueError:
                pass

    # Process source files from old_list - skip if already processed as archives
    for fp, ts in old_list:
        if fp in seen_paths:  # Skip if this is an archive file we've already processed
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
        all_files.append(FileInfo(fp, ts, size, is_archive=False, is_trash=is_trash))
        seen_paths.add(fp)

    return all_files


def intelligent_cleanup(
    all_files: List[FileInfo],
    logger: logging.Logger,
    dry_run: bool,
    max_size_gb: int,
    age_days: int,
) -> List[FileInfo]:
    """
    Select files to remove based on location priority and size/age constraints.
    Priority order: Trash > Archive > Source (oldest first within each category).
    """
    if not all_files:
        return []

    # Calculate totals
    total_size = sum(f.size for f in all_files)
    size_limit = max_size_gb * (1024**3)
    age_cutoff = datetime.now() - timedelta(days=age_days)

    logger.info(f"Current total size: {total_size / (1024**3):.1f} GB")
    logger.info(f"Size limit: {max_size_gb} GB")
    logger.info(f"Age cutoff: {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')}")

    # Categorize files by location priority (0 = highest priority for removal)
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

    files_to_remove = []
    remaining_size = total_size

    # PHASE 1: Enforce size limit (if over limit)
    if remaining_size > size_limit:
        logger.info("Archive size exceeds limit")
        logger.info(
            f"Size threshold exceeded, removing files by priority to reach {max_size_gb} GB..."
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

                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would remove {category_name} file for size: {file_info.path} "
                            f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                        )

        logger.info(
            f"After size cleanup: {remaining_size / (1024**3):.1f} GB "
            f"({len(files_to_remove)} files marked for removal)"
        )
    else:
        # PHASE 2: Enforce age limit (only if under size limit AND age_days > 0)
        if age_days > 0:
            files_over_age_by_priority: Dict[int, List[FileInfo]] = {
                0: [],
                1: [],
                2: [],
            }

            for priority in range(3):
                files_over_age_by_priority[priority] = [
                    f for f in categorized_files[priority] if f.timestamp < age_cutoff
                ]

            total_over_age = sum(
                len(files) for files in files_over_age_by_priority.values()
            )

            if total_over_age > 0:
                logger.info(f"Found {total_over_age} files older than {age_days} days")

                # Remove age-eligible files by priority order
                for priority in range(3):
                    category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
                    age_files = files_over_age_by_priority[priority]

                    if age_files:
                        logger.info(
                            f"Processing {category_name} files for age cleanup..."
                        )

                        for file_info in age_files:
                            files_to_remove.append(file_info)
                            remaining_size -= file_info.size

                            if dry_run:
                                logger.info(
                                    f"[DRY RUN] Would remove {category_name} file for age: {file_info.path} "
                                    f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                                )

                logger.info(f"Added {total_over_age} files for age-based removal")
            else:
                logger.info(f"No files older than {age_days} days found")
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


def process_files_intelligent(
    old_list: List[Tuple[Path, datetime]],
    out_dir: Path,
    logger: logging.Logger,
    dry_run: bool,
    no_skip: bool,
    mapping: Dict[str, Dict[str, Path]],
    bar: ProgressBar,
    trash_files: Optional[Set[Path]] = None,
    use_trash: bool = False,
    trash_root: Optional[Path] = None,
    source_root: Optional[Path] = None,
    max_size_gb: int = 500,
    age_days: int = 30,  # kept for backward compat, but unused
) -> Set[Path]:
    """Process (transcode) files and return the set of *all* paths that were
    finally removed (source MP4s + paired JPGs)."""
    if trash_files is None:
        trash_files = set()

    logger.info(f"Found {len(old_list)} files to process")
    if not old_list or GracefulExit.should_exit():
        logger.info("No files to process or cancellation requested")
        return set()

    to_delete: Set[Path] = set()
    for fp, ts in old_list:
        if GracefulExit.should_exit():
            break
        if fp in trash_files:  # already in trash
            continue

        outp = output_path(fp, ts, out_dir)
        jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

        if dry_run:
            logger.info("[DRY RUN] Would transcode %s -> %s", fp, outp)
            if jpg:
                logger.info("[DRY RUN] Would remove paired JPG %s", jpg)
            continue

        if (
            not no_skip
            and outp.exists()
            and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
        ):
            logger.info("[SKIP] Archive exists and is large enough: %s", outp)
            to_delete.add(fp)
            if jpg:
                to_delete.add(jpg)
            continue

        bar.start_file()
        logger.info("Transcoding %s -> %s", fp, outp)
        ok = transcode_file(
            fp,
            outp,
            logger,
            lambda pct: bar.update_progress(old_list.index((fp, ts)) + 1, pct),
        )
        if ok:
            bar.finish_file(old_list.index((fp, ts)) + 1)
            to_delete.add(fp)
            if jpg:
                to_delete.add(jpg)
        else:
            logger.error("Transcoding failed for %s – keeping source", fp)

    bar.start_processing()

    # Actually remove everything once
    for p in sorted(to_delete, key=lambda _p: _p.name):
        remove_one(
            p,
            logger,
            dry_run,
            use_trash,
            trash_root,
            is_output=False,
            source_root=source_root or Path("/camera"),
        )

    bar.finish()
    return to_delete


def remove_orphaned_jpgs(
    mapping: Dict[str, Dict[str, Path]],
    processed: Set[Path],
    logger: logging.Logger,
    dry_run: bool = False,
    use_trash: bool = False,
    trash_root: Optional[Path] = None,
) -> None:
    """Remove JPG files without corresponding MP4 files."""
    if GracefulExit.should_exit():
        return

    count = 0
    for key, files in mapping.items():
        if GracefulExit.should_exit():
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
        remove_one(
            jpg,
            logger,
            dry_run,
            use_trash,
            trash_root,
            is_output=False,
            source_root=jpg.parent,
        )
        count += 1

    if not GracefulExit.should_exit():
        logger.info(
            "%s %d orphaned JPG files",
            "[DRY RUN] Would remove" if dry_run else "Removed",
            count,
        )


def clean_empty_directories(
    root_dir: Path,
    logger: Optional[logging.Logger] = None,
    use_trash: bool = False,
    trash_root: Optional[Path] = None,
    is_output: bool = False,
):
    """Remove empty date-structured directories."""
    if GracefulExit.should_exit():
        return

    root = Path(root_dir)
    for dirpath, dirs, files in os.walk(root, topdown=False):
        if GracefulExit.should_exit():
            break

        p = Path(dirpath)
        if p == root:
            continue

        try:
            rel_parts = p.relative_to(root).parts
        except ValueError:
            continue

        if len(rel_parts) != 3:
            continue

        y, m, d = rel_parts
        try:
            int(y)
            int(m)
            int(d)
        except Exception:
            continue

        if not files and not dirs:
            try:
                if use_trash and trash_root:
                    new_dest = calculate_trash_destination(
                        p, root, trash_root, is_output
                    )
                    new_dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(p), str(new_dest))
                else:
                    p.rmdir()
            except Exception as e:
                if logger:
                    logger.error(f"Failed to remove empty directory {p}: {e}")


def cleanup_archive_size_limit(
    base_dir: Path,
    out_dir: Path,
    logger: logging.Logger,
    max_size_gb: int,
    dry_run: bool,
    use_trash: bool = False,
    trash_root: Optional[Path] = None,
    age_days: int = 30,
) -> None:
    """Comprehensive storage management with location-based priorities."""
    if GracefulExit.should_exit():
        return

    # Step 1: Complete system discovery
    mp4s, mapping, trash_files = scan_files(
        base_dir, include_trash=use_trash, trash_root=trash_root
    )

    # Step 2: Empty directory cleanup (highest priority)
    if not dry_run:
        clean_empty_directories(
            base_dir, logger, use_trash, trash_root, is_output=False
        )
        clean_empty_directories(out_dir, logger, use_trash, trash_root, is_output=True)
        if trash_root:
            clean_empty_directories(
                trash_root, logger, use_trash, trash_root, is_output=False
            )

    # Step 3: File-based cleanup
    if dry_run:
        logger.info("[DRY RUN] Would enforce storage limits")
        return

    # Collect info for ALL files (not just old ones)
    all_file_infos = collect_file_info(mp4s, out_dir, trash_root)

    # Step 4: Apply intelligent cleanup
    files_to_remove = intelligent_cleanup(
        all_file_infos, logger, dry_run, max_size_gb, age_days
    )

    # Step 5: Execute file removal
    for file_info in files_to_remove:
        remove_one(
            file_info.path,
            logger,
            dry_run=False,
            use_trash=use_trash,
            trash_root=trash_root,
            is_output=file_info.is_archive,
            source_root=base_dir if not file_info.is_archive else out_dir,
        )

    # Step 6: Clean up orphaned JPGs
    remove_orphaned_jpgs(mapping, set(), logger, False, use_trash, trash_root)


def run_archiver(args) -> int:
    """Main archiver logic with proper error handling."""
    base_dir = args.directory if args.directory.exists() else Path("/camera")
    if not base_dir.exists():
        print(
            f"Error: Directory {args.directory} does not exist and /camera is missing"
        )
        return 1

    out_dir = args.output or (base_dir / "archived")
    trash_root = (
        args.trashdir
        if args.trashdir
        else (base_dir / ".deleted" if args.use_trash else None)
    )

    if trash_root is not None:
        trash_root.mkdir(parents=True, exist_ok=True)

    # Always perform comprehensive discovery
    mp4s, mapping, trash_files = scan_files(
        base_dir, include_trash=args.use_trash, trash_root=trash_root
    )

    if not args.cleanup:
        # Normal mode: transcoding files
        cutoff = datetime.now() - timedelta(days=args.age)
        old_list = [(p, t) for p, t in mp4s if t < cutoff]
        bar = ProgressBar(
            total_files=len(old_list), silent=args.dry_run, out=sys.stderr
        )
        logger = setup_logging(base_dir / "transcoding.log", progress_bar=bar)

        # For backward compatibility in transcoding logic, keep the old process_files_intelligent
        # but update it to not call intelligent_cleanup internally
        _ = process_files_intelligent(
            old_list=old_list,
            out_dir=out_dir,
            logger=logger,
            dry_run=args.dry_run,
            no_skip=args.no_skip,
            mapping=mapping,
            bar=bar,
            trash_files=trash_files,
            use_trash=args.use_trash,
            trash_root=trash_root,
            source_root=base_dir,
            max_size_gb=args.max_size,
            age_days=args.age,
        )
    else:
        # Cleanup mode: no transcoding
        bar = ProgressBar(total_files=0, silent=True, out=sys.stderr)
        logger = setup_logging(base_dir / "transcoding.log", progress_bar=bar)
        logger.info(
            "Cleanup mode: skipping transcoding, only performing cleanup operations"
        )

    for msg in [
        "Starting camera archive process...",
        f"Input: {base_dir}",
        f"Output: {out_dir}",
        f"Trash: {trash_root}",
        f"Age threshold: {args.age} days",
        f"Size limit: {args.max_size} GB",
        f"Dry run: {args.dry_run}",
        f"Cleanup only: {args.cleanup}",
    ]:
        if not GracefulExit.should_exit():
            logger.info(msg)

    # Always perform comprehensive storage management
    if not GracefulExit.should_exit():
        cleanup_archive_size_limit(
            base_dir,
            out_dir,
            logger,
            args.max_size,
            args.dry_run,
            use_trash=args.use_trash,
            trash_root=trash_root,
            age_days=args.age,
        )

    if GracefulExit.should_exit():
        logger.info("Archive process was cancelled")
        return 1
    elif args.dry_run:
        logger.info("[DRY RUN] Done - no files were actually modified")
        return 0
    else:
        logger.info("Archive process completed successfully")
        return 0


def main():
    """Main entry point with argument parsing."""
    try:
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
            "--use-trash",
            action="store_true",
            help="Move deleted items to trash instead of deleting",
        )
        parser.add_argument(
            "--trashdir", type=Path, help="Specify custom trash directory"
        )
        parser.add_argument(
            "--cleanup",
            action="store_true",
            help="Skip transcoding and only perform cleanup (orphaned JPGs, empty dirs, size/age limits)",
        )
        args = parser.parse_args()
        if len(sys.argv) == 1:
            parser.print_help()
            sys.exit(1)

        exit_code = run_archiver(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        GracefulExit.request_exit()
        print("\nReceived KeyboardInterrupt, shutting down gracefully...")
        sys.exit(1)
    except Exception as e:
        if not GracefulExit.should_exit():
            logging.getLogger("camera_archiver").error(f"Unexpected error: {e}")
            raise
        else:
            print("Process was cancelled")
            sys.exit(1)


if __name__ == "__main__":
    main()
