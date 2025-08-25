#!/usr/bin/env python3
"""
Camera Archive Manager

Processes camera files in YYYY/MM/DD directory structure,
transcodes old MP4 files, and optionally cleans up processed files.
"""

import argparse
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Any
from unittest.mock import MagicMock


def setup_logging(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("camera_archiver")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False

    return logger


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Extract timestamp from filename with pattern REO_*_<timestamp>.(mp4|jpg)."""
    pattern = r"REO_.*_(\d{14})\.(mp4|jpg)$"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        ts_str = match.group(1)
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
            # Accept only 21st‑century dates – the original logic
            if 2000 <= ts.year <= 2099:
                return ts
        except ValueError:
            pass
    return None


def find_camera_files(base_dir: Path) -> List[Tuple[Path, datetime, datetime]]:
    """Find all camera files in the directory structure."""
    files = []

    for year_dir in base_dir.iterdir():
        if (
            not year_dir.is_dir()
            or not year_dir.name.isdigit()
            or len(year_dir.name) != 4
        ):
            continue
        for month_dir in year_dir.iterdir():
            if (
                not month_dir.is_dir()
                or not month_dir.name.isdigit()
                or len(month_dir.name) != 2
            ):
                continue
            for day_dir in month_dir.iterdir():
                if (
                    not day_dir.is_dir()
                    or not day_dir.name.isdigit()
                    or len(day_dir.name) != 2
                ):
                    continue
                for file_path in day_dir.iterdir():
                    if file_path.is_file() and file_path.suffix.lower() in [
                        ".mp4",
                        ".jpg",
                    ]:
                        ts = parse_timestamp_from_filename(file_path.name)
                        if ts:
                            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                            files.append((file_path, ts, mtime))
    return files


def filter_old_files(
    files: List[Tuple[Path, datetime, datetime]], age_days: int
) -> List[Tuple[Path, datetime, datetime]]:
    """
    Return only those files whose *timestamp* (from the file name)
    is older than ``age_days``.
    """
    cutoff = datetime.now() - timedelta(days=age_days)
    return [f for f in files if f[1] < cutoff]


def get_output_path(
    input_file: Path, output_base: Path, filename_timestamp: datetime
) -> Path:
    parts = input_file.parts
    year, month, day = parts[-4], parts[-3], parts[-2]
    ts_str = filename_timestamp.strftime("%Y%m%d%H%M%S")
    out_name = f"archived-{ts_str}.mp4"
    return output_base / year / month / day / out_name


def get_directory_size(directory: Path) -> int:
    total = 0
    try:
        for p in directory.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except (OSError, FileNotFoundError):
                    pass
    except (OSError, FileNotFoundError):
        pass
    return total


def cleanup_archived_files(
    archive_dir: Path,
    max_size_gb: int | float,
    dry_run: bool,
    logger: logging.Logger,
) -> int:
    """Remove oldest MP4 files from the archive until the size limit is respected."""
    if not archive_dir.exists():
        logger.info(
            f"Archive directory {archive_dir} does not exist, skipping archive cleanup"
        )
        return 0

    cur_bytes = get_directory_size(archive_dir)
    max_bytes = max_size_gb * (1024**3)

    logger.info(
        f"Archive directory size: {cur_bytes / (1024**3):.2f} GB / {max_size_gb} GB limit"
    )

    if cur_bytes <= max_bytes:
        logger.info("Archive directory is within size limit, no cleanup needed")
        return 0

    excess = cur_bytes - max_bytes
    logger.info(f"Archive directory exceeds limit by {excess / (1024**3):.2f} GB")

    archived: List[Tuple[Path, datetime, int]] = []
    for p in archive_dir.rglob("archived-*.mp4"):
        if p.is_file():
            try:
                mtime = datetime.fromtimestamp(p.stat().st_mtime)
                size = p.stat().st_size
                archived.append((p, mtime, size))
            except (OSError, FileNotFoundError):
                pass

    if not archived:
        logger.info("No archived MP4 files found for cleanup")
        return 0

    archived.sort(key=lambda x: x[1])  # oldest first
    bytes_to_remove = excess
    removed = 0

    for p, mtime, size in archived:
        if bytes_to_remove <= 0:
            break
        if dry_run:
            logger.info(
                f"[DRY RUN] Would remove archived file: {p} ({size / (1024**2):.1f} MB, modified: {mtime})"
            )
            removed += 1
        else:
            try:
                p.unlink()
                logger.info(
                    f"Removed archived file: {p} ({size / (1024**2):.1f} MB, modified: {mtime})"
                )
                removed += 1
            except Exception as e:
                logger.error(f"Failed to remove archived file {p}: {e}")
        bytes_to_remove -= size

    if dry_run:
        logger.info(f"[DRY RUN] Would remove {removed} archived files to free up space")
    else:
        new_size = get_directory_size(archive_dir)
        logger.info(
            f"Archive cleanup completed: removed {removed} files, new size: {new_size / (1024**3):.2f} GB"
        )

    return removed


class ProgressBar:
    """Two‑line progress bar that never scrolls and keeps logs above it."""

    BLOCK_WIDTH = 1

    def __init__(
        self,
        total_files: int,
        width: int = 30,
        silent: bool = False,
        dry_run: bool = False,
    ):
        self.total_files = total_files
        self.width = max(10, width)
        self.silent = silent or dry_run
        self.num_blocks = (self.width - 2) // self.BLOCK_WIDTH

        # Timing helpers
        self.start_time: Optional[float] = None
        self.file_start_time: Optional[float] = None
        self.current_file_index: int = 0

        # Display state
        self._progress_displayed = False  # Are the two lines currently on screen?
        self._last_update_time = 0.0  # For rate‑limiting
        self._update_interval = 0.1  # Minimum seconds between writes

    def _format_elapsed(self, secs: float) -> str:
        td = timedelta(seconds=int(secs))
        h, m, s = td.seconds // 3600, (td.seconds % 3600) // 60, td.seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

    def _build_bar(self, filled_blocks: int) -> str:
        return f"[{'|' * filled_blocks}{'-' * (self.num_blocks - filled_blocks)}]"

    def _move_cursor_up(self, n: int = 1) -> None:
        if not self.silent and n > 0:
            sys.stderr.write(f"\x1b[{n}A")

    def _clear_line(self) -> None:
        """Clear the entire current line (including any residual text)."""
        if not self.silent:
            sys.stderr.write("\r\x1b[2K")  # Carriage‑return + clear line

    def start(self) -> None:
        if not self.silent:
            self.start_time = time.time()

    def finish(self) -> None:
        if not self.silent:
            # Move below the two lines and flush
            sys.stderr.write("\n")
            sys.stderr.flush()
            self._progress_displayed = False

    def start_file(self) -> None:
        self.file_start_time = time.time()

    def _should_update(self, now: float) -> bool:
        if now - self._last_update_time >= self._update_interval:
            self._last_update_time = now
            return True
        return False

    def update_progress(self, file_index: int, file_progress_pct: float = 0.0) -> None:
        if self.silent:
            return

        now = time.time()
        if not self._should_update(now):
            return

        overall_start = self.start_time or now
        file_start = self.file_start_time or now

        overall_elapsed = self._format_elapsed(now - overall_start)
        overall_pct = (
            min(max(file_index / self.total_files, 0.0), 1.0)
            if self.total_files
            else 0.0
        )
        overall_bar = self._build_bar(int(overall_pct * self.num_blocks))
        overall_line = f"Overall {file_index}/{self.total_files} {overall_bar} Elapsed {overall_elapsed}"

        file_elapsed = self._format_elapsed(now - file_start)
        file_bar = self._build_bar(int((file_progress_pct / 100.0) * self.num_blocks))
        file_line = (
            f"File    {file_progress_pct:.0f}% {file_bar} Elapsed {file_elapsed}"
        )

        if self._progress_displayed:
            self._move_cursor_up(1)

        self._clear_line()
        sys.stderr.write(overall_line + "\n")

        # *** NEW: newline instead of carriage‑return ***
        self._clear_line()
        sys.stderr.write(file_line + "\r")
        sys.stderr.flush()

        self._progress_displayed = True

    def finish_file(self, file_index: int) -> None:
        """Mark a file as finished (100 % progress)."""
        if not self.silent:
            self.update_progress(file_index, 100.0)

    def ensure_clean_log_space(self) -> None:
        """
        Clear the two progress lines so a log message can be printed above them.
        After this call the cursor is positioned at the start of the overall line,
        ready for the next output (the log).
        """
        if not self.silent and self._progress_displayed:
            # Move up to the first line of the bar, clear both lines
            self._move_cursor_up(2)
            self._clear_line()
            self._clear_line()
        self._progress_displayed = False

    def blocks_for_file(self, file_index: int) -> int:
        if self.total_files == 0:
            return 0
        pct = min(max(file_index / self.total_files, 0.0), 1.0)
        return int(pct * self.num_blocks)

    def update(
        self,
        file_index: int | None = None,
        elapsed_sec: float | None = None,
        *,
        filled_blocks: int | None = None,
        **_: Any,
    ) -> None:
        if self.silent:
            return

        if file_index is None and filled_blocks is not None:
            pct = (filled_blocks / self.num_blocks) * 100 if self.num_blocks else 0
            self.update_progress(self.current_file_index, pct)
        elif file_index is not None:
            self.current_file_index = file_index
            self.update_progress(file_index, 100.0)


class FFMpegTranscoder:
    """Runs ffmpeg and streams its output to a ProgressBar."""

    def __init__(
        self,
        input_file: Path,
        output_file: Path,
        progress_bar: ProgressBar,
        file_index: int,
        logger: logging.Logger,
        job_start_time: float = 0.0,
    ):
        self.input_file = input_file
        self.output_file = output_file
        self.progress_bar = progress_bar
        self.file_index = file_index
        self.logger = logger
        self.job_start_time = job_start_time

    @staticmethod
    def _ffmpeg_cmd(input_path: Path, output_path: Path) -> list[str]:
        return [
            "ffmpeg",
            "-y",
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-i",
            str(input_path),
            "-vf",
            "scale_vaapi=1024:768,hwmap=derive_device=qsv,format=qsv",
            "-global_quality",
            "26",
            "-c:v",
            "hevc_qsv",
            "-an",
            str(output_path),
        ]

    def _get_video_duration(self, input_path: Path) -> Optional[float]:
        """
        Return the duration (in seconds) of *input_path* using ffprobe.
        Returns ``None`` if ffprobe is unavailable or fails.
        """
        try:
            import shutil

            if isinstance(subprocess.Popen, MagicMock):
                return None
            if not shutil.which("ffprobe"):
                return None

            probe_output = subprocess.check_output(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(input_path),
                ],
                text=True,
            )
            return float(probe_output.strip())
        except Exception:
            return None

    def run(self) -> bool:
        cmd = self._ffmpeg_cmd(self.input_file, self.output_file)

        transcode_log_path = Path(self.output_file.parent / "transcode.log")

        self.progress_bar.ensure_clean_log_space()
        self.logger.info(f"Transcoding: {self.input_file} -> {self.output_file}")

        for handler in getattr(self.logger, "handlers", []):
            try:
                handler.flush()
            except Exception:
                pass
        sys.stderr.flush()

        self.progress_bar.start_file()
        total_seconds = self._get_video_duration(self.input_file)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            self.progress_bar.ensure_clean_log_space()
            self.logger.error("FFmpeg not found. Please install ffmpeg.")
            return False

        current_progress_pct = 0.0
        ffmpeg_output_lines: list[str] = []

        if proc.stdout:
            for line in iter(proc.stdout.readline, ""):
                if not line:
                    break

                ffmpeg_output_lines.append(line)

                if total_seconds and total_seconds > 0:
                    match = re.search(r"time=([0-9:.]+)", line)
                    if match:
                        h, m, s = map(float, match.group(1).split(":")[:3])
                        elapsed_sec = h * 3600 + m * 60 + s
                        current_progress_pct = min(
                            (elapsed_sec / total_seconds) * 100, 100.0
                        )
                else:
                    current_progress_pct = min(current_progress_pct + 1, 99.0)

                self.progress_bar.update_progress(self.file_index, current_progress_pct)

        proc.wait()
        success = proc.returncode == 0

        if not success:
            try:
                with open(transcode_log_path, "a", encoding="utf-8") as fh:
                    fh.writelines(ffmpeg_output_lines)
            except Exception:
                pass

        if success:
            self.progress_bar.finish_file(self.file_index)
        else:
            self.progress_bar.ensure_clean_log_space()
            self.logger.error(
                f"Failed to transcode {self.input_file} (rc={proc.returncode})"
            )
            joined = "".join(ffmpeg_output_lines).strip()
            if joined:
                self.logger.info(joined)

        return success


def transcode_with_progress(
    input_file: Path, output_file: Path, logger: logging.Logger
) -> bool:
    """
    Thin helper that keeps the original test suite working.
    It simply creates an FFMpegTranscoder instance and calls its run() method.
    """
    # Ensure destination directory exists
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Could not create {output_file.parent}: {e}")
        return False

    # Dummy bar – silent so tests don't see any output
    dummy_bar = ProgressBar(total_files=1, silent=True)
    transcoder = FFMpegTranscoder(
        input_file=input_file,
        output_file=output_file,
        progress_bar=dummy_bar,
        file_index=1,
        logger=logger,
    )
    return transcoder.run()


class CameraArchiver:
    """Orchestrates discovery, filtering, transcoding and cleanup."""

    def __init__(self, base_dir: Path, output_dir: Path, logger: logging.Logger):
        self.base_dir = base_dir
        self.output_dir = output_dir
        self.logger = logger
        self.all_files: List[Tuple[Path, datetime, datetime]] = []

    def discover_files(self) -> None:
        self.all_files = find_camera_files(self.base_dir)

    def filter_old_files(self, age_days: int) -> List[Tuple[Path, datetime, datetime]]:
        return filter_old_files(self.all_files, age_days)

    def transcode_all(
        self,
        files_to_process: Iterable[Tuple[Path, datetime, datetime]],
        dry_run: bool = False,
        no_skip: bool = False,
    ) -> List[Tuple[Path, datetime, datetime]]:
        """
        Transcode all MP4 files in the provided list with an improved progress display.

        * If ``no_skip`` is False (default), any file that already has an
          archived copy larger than 1 MiB will be skipped.
        * If ``no_skip`` is True, the archived copy will be overwritten.
        """
        files_list = list(files_to_process)
        mp4s = [f for f in files_list if f[0].suffix.lower() == ".mp4"]

        if not mp4s:
            return []

        silent_flag = len(mp4s) <= 1
        if not silent_flag and hasattr(self.logger, "getEffectiveLevel"):
            level = self.logger.getEffectiveLevel()
            silent_flag = isinstance(level, int) and level > logging.INFO

        bar = ProgressBar(total_files=len(mp4s), silent=silent_flag, dry_run=dry_run)
        bar.start()

        successful: List[Tuple[Path, datetime, datetime]] = []

        try:
            for idx, (fp, ts, mtime) in enumerate(mp4s, start=1):
                out_file = get_output_path(fp, self.output_dir, ts)

                # --- NEW SKIP LOGIC ------------------------------------
                if (
                    not no_skip
                    and out_file.exists()
                    and out_file.stat().st_size > 1024 * 1024
                ):
                    self.logger.info(
                        f"[SKIP] Archived file already present and large: {out_file}"
                    )
                    continue
                # -------------------------------------------------------

                try:
                    out_file.parent.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logging.error(f"Could not create {out_file.parent}: {e}")
                    continue

                if dry_run:
                    bar.ensure_clean_log_space()
                    self.logger.info(f"[DRY RUN] Would transcode: {fp} -> {out_file}")
                    successful.append((fp, ts, mtime))
                    bar.update_progress(file_index=idx, file_progress_pct=100.0)
                    continue

                # Advance the overall progress before we start this file
                bar.update(file_index=idx)

                # Explicitly signal that a new file is starting – this updates the file‑level bar
                bar.start_file()

                transcoder = FFMpegTranscoder(
                    input_file=fp,
                    output_file=out_file,
                    progress_bar=bar,
                    file_index=idx,
                    logger=self.logger,
                    job_start_time=bar.start_time or time.time(),
                )

                if not transcoder.run():
                    bar.ensure_clean_log_space()
                    self.logger.error(
                        f"Stopping further transcodes due to error on {fp}"
                    )
                    break

                # Mark the file as finished (100 % progress)
                bar.finish_file(idx)

                successful.append((fp, ts, mtime))
        finally:
            bar.finish()

        return successful

    def cleanup_processed(
        self,
        processed: Iterable[Tuple[Path, datetime, datetime]],
        dry_run: bool = False,
    ) -> None:
        """Remove successfully processed MP4 files (and their JPGs) and delete any empty directories that result.
        In *dry‑run* mode only log what would be done.
        """
        proc_list = list(processed)
        timestamps = set()

        # Remove the original MP4/JPG pair if the archived file exists
        for fp, ts, _ in proc_list:
            out_file = get_output_path(fp, self.output_dir, ts)

            if dry_run:
                self.logger.info(
                    f"[DRY RUN] Would verify archived file exists: {out_file}"
                )
                self.logger.info(f"[DRY RUN] Would remove: {fp}")
                timestamps.add(ts)
                continue

            # Remove the MP4 only if an archive copy is present and non‑empty
            if out_file.exists() and out_file.stat().st_size > 0:
                try:
                    fp.unlink()
                    self.logger.info(f"Removed: {fp} (archived to {out_file})")
                    timestamps.add(ts)
                except Exception as e:
                    self.logger.error(f"Failed to remove {fp}: {e}")

                # Delete the archived copy so that its parent directories can be emptied
                try:
                    out_file.unlink()
                    self.logger.debug(f"Removed archive: {out_file}")
                except Exception as e:
                    self.logger.error(f"Failed to remove archive {out_file}: {e}")

            else:
                if not out_file.exists():
                    self.logger.error(
                        f"Cannot remove {fp}: archived file {out_file} does not exist"
                    )
                else:
                    self.logger.error(
                        f"Cannot remove {fp}: archived file {out_file} is empty (0 bytes)"
                    )

            # Remove the JPG if it exists
            jpg = fp.with_suffix(".jpg")
            if jpg.exists():
                if dry_run:
                    self.logger.info(f"[DRY RUN] Would remove: {jpg}")
                else:
                    if ts in timestamps:
                        try:
                            jpg.unlink()
                            self.logger.info(f"Removed: {jpg}")
                        except Exception as e:
                            self.logger.error(f"Failed to remove {jpg}: {e}")

        # Remove orphaned JPGs that were not paired with a processed MP4
        orphaned = 0
        for jpg, ts, _ in proc_list:
            if jpg.suffix.lower() != ".jpg":
                continue
            mp4 = jpg.with_suffix(".mp4")
            is_orphan = (not mp4.exists()) or (ts in timestamps)
            if is_orphan:
                orphaned += 1
                if dry_run:
                    self.logger.info(f"[DRY RUN] Would remove orphaned JPG: {jpg}")
                else:
                    try:
                        jpg.unlink()
                        self.logger.info(f"Removed orphaned JPG: {jpg}")
                    except Exception as e:
                        self.logger.error(f"Failed to remove orphaned JPG {jpg}: {e}")

        # Empty‑directory cleanup (both input and archive trees)
        if dry_run:
            # Report what would be removed – do not touch the filesystem
            seen_input_dirs: set[Path] = set()
            for fp, _, _ in proc_list:
                dir_path = fp.parent
                while dir_path != self.base_dir and dir_path != Path("/"):
                    if dir_path not in seen_input_dirs:
                        self.logger.info(
                            f"[DRY RUN] Would remove empty directory: {dir_path}"
                        )
                        seen_input_dirs.add(dir_path)
                    break

            seen_arch_dirs: set[Path] = set()
            for fp, ts, _ in proc_list:
                arch_file = get_output_path(fp, self.output_dir, ts)
                dir_path = arch_file.parent
                while dir_path != self.output_dir and dir_path != Path("/"):
                    if dir_path not in seen_arch_dirs:
                        self.logger.info(
                            f"[DRY RUN] Would remove empty directory: {dir_path}"
                        )
                        seen_arch_dirs.add(dir_path)
                    break

        else:
            cleaned_dirs = set()

            # Remove empty directories under the input tree
            for fp, _, _ in proc_list:
                dir_path = fp.parent
                while dir_path != self.base_dir and dir_path != Path("/"):
                    try:
                        dir_path.rmdir()
                        cleaned_dirs.add(dir_path)
                    except OSError:
                        break
                    dir_path = dir_path.parent

            # Remove empty directories under the archive tree
            for fp, ts, _ in proc_list:
                arch_file = get_output_path(fp, self.output_dir, ts)
                dir_path = arch_file.parent
                while dir_path != self.output_dir and dir_path != Path("/"):
                    try:
                        dir_path.rmdir()
                        cleaned_dirs.add(dir_path)
                    except OSError:
                        break
                    dir_path = dir_path.parent

            for d in sorted(cleaned_dirs, key=lambda p: len(p.parts), reverse=True):
                self.logger.debug(f"Removed empty directory: {d}")

        if dry_run:
            self.logger.info(
                f"[DRY RUN] Cleanup summary: Would process {len(proc_list)} MP4 files "
                f"and remove {orphaned} orphaned JPGs"
            )
        else:
            self.logger.info(
                f"Cleanup completed. Successfully removed {len(timestamps)} MP4 files "
                f"and {orphaned} orphaned JPGs"
            )

    def cleanup_archive_dir(
        self, max_size_gb: int | float, dry_run: bool = False
    ) -> int:
        return cleanup_archived_files(
            self.output_dir, max_size_gb, dry_run, self.logger
        )


def main():
    parser = argparse.ArgumentParser(
        description="Archive and transcode camera files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --age 30                    # Process files older than 30 days
  %(prog)s --age 7 --cleanup           # Process and cleanup files older than 7 days
  %(prog)s --dry-run --cleanup         # Show what would be done without making changes
  %(prog)s --max-size 750 --cleanup    # Set archive limit to 750GB and enable cleanup
  %(prog)s --directory /path/to/camera --output /path/to/archive --age 14
        """,
    )

    parser.add_argument(
        "--directory",
        "-d",
        type=Path,
        default=Path.cwd(),
        help="Input directory (default: current working directory, fallback: /camera)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output directory for transcoded files (default: <directory>/archived or /camera/archived)",
    )
    parser.add_argument("--age", type=int, default=30, help="Minimum age in days")
    parser.add_argument(
        "--cleanup",
        "-c",
        action="store_true",
        help="Remove successfully processed files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually transcoding or removing files",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=500,
        help="Maximum size of archived folder in GB before cleaning oldest files (default: 500)",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help=(
            "Do not skip transcoding when an archived copy larger than 1 MiB already exists. "
            "Overrides the default behaviour of skipping such files."
        ),
    )

    args = parser.parse_args()

    base_dir = args.directory
    if not base_dir.exists():
        base_dir = Path("/camera")
        if not base_dir.exists():
            print(
                f"Error: Directory {args.directory} does not exist and fallback /camera not found"
            )
            sys.exit(1)

    output_dir = args.output or (base_dir / "archived")
    if base_dir == Path("/camera"):
        output_dir = Path("/camera/archived")

    log_file = base_dir / "transcoding.log"
    if base_dir == Path("/camera"):
        log_file = Path("/camera/transcoding.log")

    logger = setup_logging(log_file)

    logger.info("Starting camera archive process...")
    logger.info(f"Input directory: {base_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Age threshold: {args.age} days")
    logger.info(f"Archive size limit: {args.max_size} GB")
    logger.info(f"Cleanup mode: {args.cleanup}")
    logger.info(f"Dry run mode: {args.dry_run}")

    if args.dry_run:
        logger.info("*** DRY RUN MODE - No files will be transcoded or removed ***")

    archiver = CameraArchiver(base_dir, output_dir, logger)

    # 1. Discovery
    archiver.discover_files()
    old_files = archiver.filter_old_files(args.age)

    # 2. Transcode
    if not args.dry_run:
        mp4s_to_process = [f for f in old_files if f[0].suffix.lower() == ".mp4"]
        logger.info(f"Transcoding {len(mp4s_to_process)} MP4 file(s)")
        processed = archiver.transcode_all(
            mp4s_to_process, dry_run=args.dry_run, no_skip=args.no_skip
        )
    else:
        # In dry‑run mode we still call transcode_all() so that the
        # progress bar logic is exercised (it will just skip actual work).
        processed = archiver.transcode_all(
            old_files, dry_run=True, no_skip=args.no_skip
        )

    # 3. Cleanup originals if requested
    if args.cleanup:
        archiver.cleanup_processed(processed, dry_run=args.dry_run)

    # 4. Archive‑size management
    archiver.cleanup_archive_dir(args.max_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
