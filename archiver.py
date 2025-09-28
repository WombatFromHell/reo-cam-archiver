#!/usr/bin/env python3
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
    """Thread‑safe lock for console output."""

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
    def __init__(self, total_files, width=30, silent=False, out=sys.stderr):
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

    def _is_tty(self):
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
    def has_progress(self):
        return bool(self._progress_line)

    def start_processing(self):
        if self.start_time is None:
            self.start_time = time.time()

    def start_file(self):
        self.file_start = time.time()
        if self.start_time is None:
            self.start_time = time.time()

    def update_progress(self, idx, pct=0.0):
        if self.silent or GracefulExit.should_exit():
            return
        line = self._format_line(idx, pct)
        if line == self._progress_line:
            return
        self._progress_line = line
        self._display(line)

    def finish_file(self, idx):
        if not GracefulExit.should_exit():
            self.update_progress(idx, 100.0)

    def _format_line(self, idx, pct):
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

    def _display(self, line):
        if not self._is_tty():
            now = time.time()
            if now - self._last_print_time >= 5 or "100%" in line:
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


def setup_logging(log_file, progress_bar=None):
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


def parse_timestamp_from_filename(name):
    TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
    m = TIMESTAMP_RE.search(name)
    if not m:
        return None
    try:
        ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
        return ts if 2000 <= ts.year <= 2099 else None
    except ValueError:
        return None


def safe_remove(
    p: Path,
    logger,
    dry_run,
    use_trash=False,
    trash_root=None,
    is_output=False,
    source_root: Path | None = None,
):
    if GracefulExit.should_exit():
        return

    if dry_run:
        logger.info(f"[DRY RUN] Would remove {p}")
        return

    try:
        if source_root is None:
            source_root = p.parent

        if use_trash and trash_root:
            dest_sub = "output" if is_output else "input"
            rel_path = p.relative_to(source_root) if source_root else Path(p.name)
            base_dest = trash_root / dest_sub / rel_path

            counter = 0
            new_dest = base_dest
            while new_dest.exists():
                counter += 1
                suffix = f"_{int(time.time())}_{counter}"
                stem = new_dest.stem + suffix
                new_dest = new_dest.parent / (stem + new_dest.suffix)

            new_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(new_dest))
            logger.info(f"Moved to trash: {p} -> {new_dest}")
        else:
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                p.rmdir()
            else:
                logger.warning(f"Unsupported file type for removal: {p}")
            logger.info(f"Removed: {p}")
    except Exception as e:
        logger.error(f"Failed to remove {p}: {e}")


def get_video_duration(inp):
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
            str(inp),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration_str = result.stdout.strip()
        if duration_str and duration_str != "N/A":
            return float(duration_str)
        return None
    except Exception:
        return None


def transcode_file(inp, outp, logger, progress_cb=None):
    if GracefulExit.should_exit():
        return False

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-hwaccel",
        "qsv",
        "-hwaccel_output_format",
        "qsv",
        "-y",
        "-i",
        str(inp),
        "-vf",
        "scale_qsv=w=1024:h=768:mode=hq",
        "-global_quality",
        "26",
        "-c:v",
        "h264_qsv",
        "-an",
        str(outp),
    ]
    total = get_video_duration(inp)

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
    if proc.stdout:
        for line in iter(proc.stdout.readline, ""):
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
            if total and total > 0:
                m = re.search(r"time=([0-9:.]+)", line)
                if m:
                    h, mn, s = map(float, m.group(1).split(":")[:3])
                    cur_pct = min((h * 3600 + mn * 60 + s) / total * 100, 100.0)
            else:
                cur_pct = min(cur_pct + 1, 99.0)
            if progress_cb and cur_pct != prev_pct:
                progress_cb(cur_pct)
                prev_pct = cur_pct

    rc = proc.wait()
    if rc != 0 and not GracefulExit.should_exit():
        msg = f"FFmpeg failed (code {rc}) for {inp} -> {outp}\n" + "".join(log_lines)
        logger.error(msg)
    return rc == 0 and not GracefulExit.should_exit()


def scan_files(base_dir, include_trash=False, trash_root=None):
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

                    # Add to mapping and mark as trash
                    mapping.setdefault(key, {})[ext] = p
                    trash_files.add(p)
                    if ext == ".mp4":
                        mp4s.append((p, ts))

    return mp4s, mapping, trash_files


def output_path(fp: Path, ts: datetime, out_dir: Path) -> Path:
    if len(fp.parts) >= 4:
        y, m, d = fp.parts[-4:-1]
        return out_dir / y / m / d / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
    else:
        return (
            out_dir
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )


def get_all_archive_files(out_dir, trash_root=None):
    """Get all archive files including trash if enabled."""
    archive_files = list(out_dir.rglob("archived-*.mp4")) if out_dir.exists() else []

    if trash_root:
        trash_output_dir = trash_root / "output"
        if trash_output_dir.exists():
            archive_files.extend(list(trash_output_dir.rglob("archived-*.mp4")))

    return archive_files


def calculate_total_size(files):
    """Calculate total size of files, handling potential errors."""
    total_size = 0
    valid_files = []

    for file_path in files:
        if file_path.is_file():
            try:
                size = file_path.stat().st_size
                total_size += size
                valid_files.append((file_path, size))
            except (OSError, IOError):
                pass  # Skip files we can't access

    return total_size, valid_files


def intelligent_cleanup(
    old_list,
    mapping,
    out_dir,
    logger,
    dry_run,
    max_size_gb,
    age_days,
    use_trash=False,
    trash_root=None,
    source_root=None,
):
    """
    Intelligent cleanup that removes the minimum number of files to meet thresholds.
    Priority: Size threshold first, then age threshold (but only if size allows).
    """
    if not old_list:
        return set(), []  # processed_files, files_to_remove

    # Get all archive files
    archive_files = get_all_archive_files(out_dir, trash_root)

    # Combine archive files and old_list for total size calculation
    all_files = []
    for archive_file in archive_files:
        if archive_file.is_file():
            try:
                size = archive_file.stat().st_size
            except (OSError, IOError):
                continue
            # Parse timestamp from archive filename
            ts_match = re.search(r"archived-(\d{14})\.mp4$", archive_file.name)
            if ts_match:
                try:
                    ts = datetime.strptime(ts_match.group(1), "%Y%m%d%H%M%S")
                    all_files.append(
                        {
                            "path": archive_file,
                            "timestamp": ts,
                            "size": size,
                            "is_archive": True,
                            "is_trash": trash_root
                            and archive_file.is_relative_to(trash_root),
                        }
                    )
                except ValueError:
                    pass

    # Add source files from old_list
    for fp, ts in old_list:
        if fp.is_file():
            try:
                size = fp.stat().st_size
            except (OSError, IOError) as e:
                logger.warning(f"Could not access file {fp}: {e}")
                continue
            all_files.append(
                {
                    "path": fp,
                    "timestamp": ts,
                    "size": size,
                    "is_archive": False,
                    "is_trash": trash_root and fp.is_relative_to(trash_root),
                }
            )

    size_limit = max_size_gb * (1024**3)
    age_cutoff = datetime.now() - timedelta(days=age_days)

    logger.info(
        f"Current total size: {sum(f['size'] for f in all_files) / (1024**3):.1f} GB"
    )
    logger.info(f"Size limit: {max_size_gb} GB")
    logger.info(f"Age cutoff: {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')}")

    # Sort files by timestamp (oldest first) for consistent removal order
    all_files.sort(key=lambda x: x["timestamp"])

    files_to_remove = []
    processed_files = set()
    remaining_size = sum(f["size"] for f in all_files)

    # PHASE 1: Remove files to meet SIZE threshold (priority)
    if remaining_size > size_limit:
        logger.info(
            f"Size threshold exceeded, removing oldest files to reach {max_size_gb} GB..."
        )

        for file_info in all_files:
            if remaining_size <= size_limit:
                break

            files_to_remove.append(file_info)
            remaining_size -= file_info["size"]

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would remove for size: {file_info['path']} "
                    f"({file_info['size'] / (1024**2):.1f} MB, {file_info['timestamp']})"
                )

        logger.info(
            f"After size cleanup: {remaining_size / (1024**3):.1f} GB "
            f"({len(files_to_remove)} files marked for removal)"
        )

    # PHASE 2: Remove files older than age threshold (but only if we're still under size limit)
    files_over_age = [
        f for f in all_files if f["timestamp"] < age_cutoff and f not in files_to_remove
    ]

    if files_over_age:
        logger.info(f"Found {len(files_over_age)} files older than {age_days} days")

        # Check if removing age-violating files would exceed size limit
        size_of_old_files = sum(f["size"] for f in files_over_age)
        size_after_age_removal = remaining_size - size_of_old_files

        if (
            size_after_age_removal >= 0
        ):  # We can remove old files without going negative
            # But check if we'd exceed our size budget
            temp_remaining = remaining_size
            age_removals = []

            for file_info in files_over_age:
                temp_remaining -= file_info["size"]
                age_removals.append(file_info)

                # If removing this old file would make our total size too small,
                # we need to be more selective
                if temp_remaining < size_limit * 0.8:  # Keep some buffer
                    logger.info(
                        "Stopping age-based removal to maintain reasonable archive size"
                    )
                    break

            files_to_remove.extend(age_removals)
            remaining_size = temp_remaining

            if age_removals:
                logger.info(f"Added {len(age_removals)} files for age-based removal")

    else:
        logger.info(f"No files older than {age_days} days found")

    # Remove duplicates and sort by timestamp for orderly processing
    files_to_remove = list({f["path"]: f for f in files_to_remove}.values())
    files_to_remove.sort(key=lambda x: x["timestamp"])

    logger.info(
        f"Final removal plan: {len(files_to_remove)} files, "
        f"final size: {remaining_size / (1024**3):.1f} GB"
    )

    return processed_files, files_to_remove


def process_files_intelligent(
    old_list,
    out_dir,
    logger,
    dry_run,
    no_skip,
    mapping,
    bar,
    trash_files=None,
    use_trash=False,
    trash_root=None,
    source_root=None,
    max_size_gb=500,
    age_days=30,
):
    """
    Enhanced process_files that uses intelligent cleanup logic
    """
    if trash_files is None:
        trash_files = set()

    logger.info(f"Found {len(old_list)} files to process")
    if not old_list or GracefulExit.should_exit():
        logger.info("No files to process or cancellation requested")
        return set()

    # Get intelligent removal plan
    processed_files, files_to_remove = intelligent_cleanup(
        old_list,
        mapping,
        out_dir,
        logger,
        dry_run,
        max_size_gb,
        age_days,
        use_trash,
        trash_root,
        source_root,
    )

    removal_paths = {f["path"] for f in files_to_remove}

    bar.start_processing()

    for idx, (fp, ts) in enumerate(old_list, 1):
        if GracefulExit.should_exit():
            logger.info("Cancellation requested, stopping file processing...")
            break

        # Check if this file should be removed based on intelligent cleanup
        should_remove = fp in removal_paths
        is_trash_file = fp in trash_files or (
            trash_root and fp.is_relative_to(trash_root)
        )

        if should_remove:
            if is_trash_file:
                # Permanently remove trash files
                if not dry_run:
                    try:
                        fp.unlink()
                        logger.info(f"Permanently removed: {fp}")
                    except Exception as e:
                        logger.error(f"Failed to remove trash file {fp}: {e}")
            else:
                # Move regular files to trash or delete
                logger.info(f"Removing file (threshold): {fp}")
                safe_remove(
                    fp,
                    logger,
                    dry_run,
                    use_trash=use_trash,
                    trash_root=trash_root,
                    source_root=source_root,
                )

            # Handle paired JPG
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")
            if jpg and jpg.exists():
                if is_trash_file and jpg in trash_files:
                    # Permanently remove trash JPG
                    if not dry_run:
                        try:
                            jpg.unlink()
                            logger.info(f"Permanently removed paired trash JPG: {jpg}")
                        except Exception as e:
                            logger.error(f"Failed to remove trash JPG {jpg}: {e}")
                else:
                    safe_remove(
                        jpg,
                        logger,
                        dry_run,
                        use_trash=use_trash,
                        trash_root=trash_root,
                        source_root=source_root,
                    )
                processed_files.add(jpg)

            bar.update_progress(idx, 100.0)
            continue

        # Skip if already in trash but not marked for removal
        if is_trash_file:
            bar.update_progress(idx, 100.0)
            continue

        # Regular processing for files that should be transcoded
        outp = output_path(fp, ts, out_dir)
        outp.parent.mkdir(parents=True, exist_ok=True)
        jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

        if dry_run:
            logger.info(f"[DRY RUN] Would transcode {fp}->{outp}")
            if jpg:
                logger.info(f"[DRY RUN] Would remove paired JPG: {jpg}")
            bar.update_progress(idx, 100.0)
            continue

        if not no_skip and outp.exists() and outp.stat().st_size > 1_048_576:
            logger.info(f"[SKIP] Existing archive large enough: {outp}")
            bar.update_progress(idx, 100.0)
            safe_remove(fp, logger, dry_run, use_trash=use_trash, trash_root=trash_root)
            if jpg and jpg.exists():
                safe_remove(
                    jpg,
                    logger,
                    dry_run,
                    use_trash=use_trash,
                    trash_root=trash_root,
                    source_root=source_root,
                )
                processed_files.add(jpg)
            continue

        bar.start_file()
        logger.info(f"Transcoding {fp}->{outp}")
        ok = transcode_file(fp, outp, logger, lambda pct: bar.update_progress(idx, pct))
        if ok:
            bar.finish_file(idx)
            safe_remove(
                fp,
                logger,
                dry_run,
                use_trash=use_trash,
                trash_root=trash_root,
                source_root=source_root,
            )
            if jpg and jpg.exists():
                safe_remove(
                    jpg,
                    logger,
                    dry_run,
                    use_trash=use_trash,
                    trash_root=trash_root,
                    source_root=source_root,
                )
                processed_files.add(jpg)
        else:
            if not GracefulExit.should_exit():
                logger.error(f"Transcoding failed: {fp}")
                if jpg:
                    logger.info(f"Keeping paired JPG due to transcoding failure: {jpg}")
            break

    bar.finish()
    return processed_files


def remove_orphaned_jpgs(
    mapping, processed, logger, dry_run=False, use_trash=False, trash_root=None
):
    if GracefulExit.should_exit():
        return

    count = 0
    for _, files in mapping.items():
        if GracefulExit.should_exit():
            break

        jpg = files.get(".jpg")
        mp4 = files.get(".mp4")
        if not jpg or jpg in processed:
            continue
        if not mp4:
            if dry_run:
                logger.info(f"[DRY RUN] Found orphaned JPG (no MP4 pair): {jpg}")
            else:
                logger.info(f"Found orphaned JPG (no MP4 pair): {jpg}")
            safe_remove(
                jpg, logger, dry_run, use_trash=use_trash, trash_root=trash_root
            )
            count += 1

    if dry_run and not GracefulExit.should_exit():
        logger.info(f"[DRY RUN] Would remove {count} orphaned JPG files")
    elif not GracefulExit.should_exit():
        logger.info(f"Removed {count} orphaned JPG files")


def clean_empty_directories(
    root_dir, logger=None, use_trash=False, trash_root=None, is_output=False
):
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
                    dest_sub = "output" if is_output else "input"
                    rel_path = p.relative_to(root)
                    base_dest = trash_root / dest_sub / rel_path
                    counter = 0
                    new_dest = base_dest
                    while new_dest.exists():
                        counter += 1
                        suffix = f"_{int(time.time())}_{counter}"
                        stem = new_dest.stem + suffix
                        new_dest = new_dest.parent / (stem + new_dest.suffix)
                    new_dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(p), str(new_dest))
                else:
                    p.rmdir()
            except Exception as e:
                if logger:
                    logger.error(f"Failed to remove empty directory {p}: {e}")


def cleanup_archive_size_limit(
    out_dir, logger, max_size_gb, dry_run, use_trash=False, trash_root=None
):
    """
    Legacy function to maintain compatibility with tests.
    This is now a wrapper around the intelligent cleanup logic.
    """
    if GracefulExit.should_exit():
        return

    if dry_run:
        logger.info(
            f"[DRY RUN] Would check archive size limit ({max_size_gb} GB) and remove old files if needed"
        )
        return

    # Get archive files
    archive_files = list(out_dir.rglob("archived-*.mp4"))

    # Include trash output files if use_trash is enabled
    if use_trash and trash_root:
        trash_output_dir = trash_root / "output"
        if trash_output_dir.exists():
            trash_files = list(trash_output_dir.rglob("archived-*.mp4"))
            archive_files.extend(trash_files)
            if trash_files and not GracefulExit.should_exit():
                logger.info(
                    f"Including {len(trash_files)} trash files in size calculation"
                )

    if not archive_files:
        return

    # Calculate current size
    total_size = 0
    valid_files = []
    for f in archive_files:
        if f.is_file():
            try:
                size = f.stat().st_size
                total_size += size
                valid_files.append((f, size))
            except (OSError, IOError):
                continue

    limit = max_size_gb * (1024**3)

    logger.info(
        f"Current archive size (including trash): {total_size / (1024**3):.1f} GB"
    )
    if total_size > limit:
        logger.info(
            f"Archive size exceeds limit ({max_size_gb} GB), removing oldest files..."
        )
        # Sort by modification time (oldest first)
        for f, size in sorted(valid_files, key=lambda x: x[0].stat().st_mtime):
            if GracefulExit.should_exit():
                logger.info("Cancellation requested during archive cleanup")
                break

            if use_trash and trash_root:
                # Check if file is in trash already
                if trash_root in f.parents:
                    # Permanently remove from trash
                    try:
                        f.unlink()
                        logger.info(f"Permanently removed from trash: {f}")
                    except Exception as e:
                        logger.error(f"Failed to remove from trash: {f}: {e}")
                else:
                    # Move to trash
                    safe_remove(
                        f,
                        logger,
                        dry_run=False,
                        use_trash=True,
                        trash_root=trash_root,
                        is_output=True,
                        source_root=out_dir,
                    )
            else:
                f.unlink()
                logger.info(f"Removed old archive: {f}")
            total_size -= size
            if total_size <= limit:
                break
        logger.info(f"Final archive size: {total_size / (1024**3):.1f} GB")


def run_archiver(args):
    base_dir = args.directory if args.directory.exists() else Path("/camera")
    if not base_dir.exists():
        print(
            f"Error: Directory {args.directory} does not exist and /camera is missing"
        )
        return 1  # Return error code instead of sys.exit(1)

    out_dir = args.output or (base_dir / "archived")
    trash_root = None
    if args.trashdir:
        trash_root = args.trashdir
    elif args.use_trash:
        trash_root = base_dir / ".deleted"

    if trash_root is not None:
        trash_root.mkdir(parents=True, exist_ok=True)

    # Include trash files in scan when use_trash is enabled
    mp4s, mapping, trash_files = scan_files(
        base_dir, include_trash=args.use_trash, trash_root=trash_root
    )
    cutoff = datetime.now() - timedelta(days=args.age)
    old_list = [(p, t) for p, t in mp4s if t < cutoff]

    # Create progress bar and logger
    bar = ProgressBar(total_files=len(old_list), silent=args.dry_run, out=sys.stderr)
    logger = setup_logging(base_dir / "transcoding.log", progress_bar=bar)

    for msg in [
        "Starting camera archive process...",
        f"Input: {base_dir}",
        f"Output: {out_dir}",
        f"Trash: {trash_root}",
        f"Age threshold: {args.age} days",
        f"Size limit: {args.max_size} GB",
        f"Dry run: {args.dry_run}",
    ]:
        if not GracefulExit.should_exit():
            logger.info(msg)

    processed = process_files_intelligent(
        old_list=old_list,
        out_dir=out_dir,
        logger=logger,
        dry_run=args.dry_run,
        no_skip=args.no_skip,
        mapping=mapping,
        bar=bar,
        use_trash=args.use_trash,
        trash_root=trash_root,
        source_root=base_dir,
        max_size_gb=args.max_size,
        age_days=args.age,
    )

    if not GracefulExit.should_exit():
        remove_orphaned_jpgs(
            mapping, processed, logger, args.dry_run, args.use_trash, trash_root
        )
        clean_empty_directories(
            base_dir, logger, args.use_trash, trash_root, is_output=False
        )
        clean_empty_directories(
            out_dir, logger, args.use_trash, trash_root, is_output=True
        )
        cleanup_archive_size_limit(
            out_dir,
            logger,
            args.max_size,
            args.dry_run,
            use_trash=args.use_trash,
            trash_root=trash_root,
        )

    if GracefulExit.should_exit():
        logger.info("Archive process was cancelled")
        return 1  # Return error code for cancelled process
    elif args.dry_run:
        logger.info("[DRY RUN] Done - no files were actually modified")
        return 0  # Return success code for dry run
    else:
        logger.info("Archive process completed successfully")
        return 0  # Return success code for normal completion


def main():
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
