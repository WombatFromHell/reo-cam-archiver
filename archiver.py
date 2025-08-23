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
from typing import Iterable, List, Tuple, Optional


def setup_logging(log_file: Path) -> logging.Logger:
    """Set up logging to both console and file."""
    logger = logging.getLogger("camera_archiver")
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Extract timestamp from filename with pattern REO_*_<timestamp>.(mp4|jpg)."""
    pattern = r"REO_.*_(\d{14})\.(mp4|jpg)$"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        ts_str = match.group(1)
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
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
    cutoff = datetime.now() - timedelta(days=age_days)
    return [f for f in files if f[2] < cutoff]


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
    archive_dir: Path, max_size_gb: int | float, dry_run: bool, logger: logging.Logger
) -> int:
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
    """
    Dependency‑free progress bar that renders a single line using *columns*.

    Each column is five characters wide: a block of five ``|`` when it is
    filled, or five ``-`` when it is empty.  The total width of the bar,
    including the surrounding brackets, defaults to **80 columns** (the
    typical terminal width).  This gives a much smoother visual effect than
    a single pipe per step.

    Parameters
    ----------
    total_files : int
        How many files are being processed – used only for the “File X/Y”
        prefix.
    width : int, optional (default=80)
        Total number of columns to display, **including** the opening and
        closing brackets.  The actual number of *blocks* is calculated as
        ``(width - 2) // block_width`` where ``block_width`` is 5.
    silent : bool, optional (default=False)
        When ``True`` the progress bar prints nothing; useful for unit tests
        or when you only want to run the transcoder without any console noise.
    """

    BLOCK_WIDTH = 5  # number of chars per block (||||| or ----)

    def __init__(self, total_files: int, width: int = 80, silent: bool = False):
        self.total_files = total_files
        self.width = max(10, width)  # guard against too‑small widths
        self.silent = silent

        # Number of blocks that fit inside the brackets
        self.num_blocks = (self.width - 2) // self.BLOCK_WIDTH

        self.start_time: Optional[float] = None
        self._last_filled = 0

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        td = timedelta(seconds=int(seconds))
        return f"{td.seconds // 60:02d}:{td.seconds % 60:02d}"

    def _build_bar(self, filled_blocks: int) -> str:
        """
        Build a bar string consisting of *filled_blocks* blocks of five
        ``|`` characters followed by the remaining empty blocks of five
        ``-`` characters.
        """
        filled = "|||||" * filled_blocks
        empty = "-----" * (self.num_blocks - filled_blocks)
        return f"[{filled}{empty}]"

    def start(self):
        self.start_time = time.time()

    def update(
        self,
        file_index: int,
        elapsed_sec: float,
        filled_blocks: Optional[int] = None,
    ) -> None:
        if self.silent:
            return

        if self.start_time is None:
            self.start()

        if filled_blocks is None:
            filled_blocks = self._last_filled

        bar_str = self._build_bar(filled_blocks)
        elapsed_str = self._format_elapsed(elapsed_sec)

        line = f"File {file_index}/{self.total_files} {bar_str} Elapsed {elapsed_str}"
        sys.stdout.write("\r" + line)
        sys.stdout.flush()

        self._last_filled = filled_blocks

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


class FFMpegTranscoder:
    """Runs ffmpeg and streams its output to a ProgressBar."""

    def __init__(
        self,
        input_file: Path,
        output_file: Path,
        progress_bar: ProgressBar,
        file_index: int,
        logger: logging.Logger,
    ):
        self.input_file = input_file
        self.output_file = output_file
        self.progress_bar = progress_bar
        self.file_index = file_index
        self.logger = logger

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

    @staticmethod
    def _extract_time(line: str) -> Optional[float]:
        if "time=" not in line:
            return None
        try:
            t = line.split("time=", 1)[1].split()[0]
            h, m, s = t.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
        except Exception:
            return None

    def run(self) -> bool:
        cmd = self._ffmpeg_cmd(self.input_file, self.output_file)
        self.logger.info(f"Transcoding: {self.input_file} -> {self.output_file}")

        try:
            # Capture ffmpeg output in a buffer – we’ll read it ourselves.
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,  # line‑buffered
            )
        except FileNotFoundError:
            self.logger.error("FFmpeg not found. Please install ffmpeg.")
            return False

        start = time.time()
        filled = 0

        # Read the output line by line **without printing it**.
        for raw in iter(proc.stdout.readline, ""):  # type: ignore[arg-type]
            if not raw:
                continue
            elapsed = time.time() - start
            t_sec = self._extract_time(raw)
            if t_sec is not None:
                filled = min(int(t_sec / max(1, t_sec)), self.progress_bar.num_blocks)

            # The progress bar prints *only* the status line.
            self.progress_bar.update(
                file_index=self.file_index,
                elapsed_sec=elapsed,
                filled_blocks=filled,
            )

        proc.wait()
        success = proc.returncode == 0
        if not success:
            self.logger.error(
                f"Failed to transcode {self.input_file} (rc={proc.returncode})"
            )

        # Final bar – fully filled.
        self.progress_bar.update(
            file_index=self.file_index,
            elapsed_sec=time.time() - start,
            filled_blocks=self.progress_bar.num_blocks,
        )
        self.progress_bar.finish()
        return success


def transcode_with_progress(
    input_file: Path, output_file: Path, logger: logging.Logger
) -> bool:
    """
    Thin helper that keeps the original test suite working.
    It simply creates an FFMpegTranscoder instance and calls its run() method.
    """
    # Ensure destination directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

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

    # Discovery & filtering
    def discover_files(self) -> None:
        self.all_files = find_camera_files(self.base_dir)

    def filter_old_files(self, age_days: int) -> List[Tuple[Path, datetime, datetime]]:
        return filter_old_files(self.all_files, age_days)

    # Transcoding
    def transcode_all(
        self,
        files_to_process: Iterable[Tuple[Path, datetime, datetime]],
        dry_run: bool = False,
    ) -> List[Tuple[Path, datetime, datetime]]:
        """
        Transcode all MP4 files in *files_to_process*.

        If a single file fails to transcode the method stops immediately
        and returns the list of files that were processed successfully up
        until that point.  This behaviour matches the new requirement.
        """
        mp4s = [f for f in files_to_process if f[0].suffix.lower() == ".mp4"]
        bar = ProgressBar(total_files=len(mp4s))
        successful: List[Tuple[Path, datetime, datetime]] = []

        for idx, (fp, ts, mtime) in enumerate(mp4s, start=1):
            out_file = get_output_path(fp, self.output_dir, ts)

            if dry_run:
                self.logger.info(f"[DRY RUN] Would transcode: {fp} -> {out_file}")
                successful.append((fp, ts, mtime))
                continue

            transcoder = FFMpegTranscoder(
                input_file=fp,
                output_file=out_file,
                progress_bar=bar,
                file_index=idx,
                logger=self.logger,
            )
            if not transcoder.run():
                # A failure – stop processing the rest of the batch
                self.logger.error(f"Stopping further transcodes due to error on {fp}")
                break

            successful.append((fp, ts, mtime))

        return successful

    # Cleanup of originals
    def cleanup_processed(
        self,
        processed: Iterable[Tuple[Path, datetime, datetime]],
        dry_run: bool = False,
    ) -> None:
        proc_list = list(processed)
        timestamps = set()

        for fp, ts, _ in proc_list:
            out_file = get_output_path(fp, self.output_dir, ts)

            if dry_run:
                self.logger.info(
                    f"[DRY RUN] Would verify archived file exists: {out_file}"
                )
                self.logger.info(f"[DRY RUN] Would remove: {fp}")
                timestamps.add(ts)
                continue

            if out_file.exists() and out_file.stat().st_size > 0:
                try:
                    fp.unlink()
                    self.logger.info(f"Removed: {fp} (archived to {out_file})")
                    timestamps.add(ts)
                except Exception as e:
                    self.logger.error(f"Failed to remove {fp}: {e}")
            else:
                if not out_file.exists():
                    self.logger.error(
                        f"Cannot remove {fp}: archived file {out_file} does not exist"
                    )
                else:
                    self.logger.error(
                        f"Cannot remove {fp}: archived file {out_file} is empty (0 bytes)"
                    )

            # Remove corresponding JPG
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

        # Orphaned JPGs
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

        if dry_run:
            self.logger.info(
                f"[DRY RUN] Cleanup summary: Would process {len(proc_list)} MP4 files and remove {orphaned} orphaned JPGs"
            )
        else:
            self.logger.info(
                f"Cleanup completed. Successfully removed {len(timestamps)} MP4 files and {orphaned} orphaned JPGs"
            )

    # Archive size management
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
    processed = archiver.transcode_all(old_files, dry_run=args.dry_run)

    # 3. Cleanup originals if requested
    if args.cleanup:
        archiver.cleanup_processed(processed, dry_run=args.dry_run)

    # 4. Archive‑size management
    archiver.cleanup_archive_dir(args.max_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
