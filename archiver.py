#!/usr/bin/env python3
"""
Camera Archive Manager

Processes camera files in YYYY/MM/DD directory structure, transcodes old MP4 files,
and optionally cleans up processed files.
"""

import argparse
import logging
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional


def setup_logging(log_file: Path) -> logging.Logger:
    """Set up logging to both console and file."""
    logger = logging.getLogger("camera_archiver")
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatters
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """
    Extract timestamp from filename with pattern REO_*_<timestamp>.(mp4|jpg)
    Examples: REO_DRIVEWAY_01_20250821211345.mp4, REO_BACKYARD_CAM2_20250821211345.jpg
    """
    # Pattern matches: REO_<any characters>_<14-digit timestamp>.<extension>
    pattern = r"REO_.*_(\d{14})\.(mp4|jpg)$"
    match = re.search(pattern, filename, re.IGNORECASE)
    if match:
        timestamp_str = match.group(1)
        try:
            # Validate timestamp format: YYYYMMDDHHMSS
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            # Additional sanity check: ensure year is reasonable (between 2000-2099)
            if 2000 <= timestamp.year <= 2099:
                return timestamp
            else:
                return None
        except ValueError:
            return None
    return None


def find_camera_files(base_dir: Path) -> List[Tuple[Path, datetime, datetime]]:
    """
    Find all camera files in the directory structure.
    Returns list of (file_path, filename_timestamp, modification_time) tuples.
    """
    files = []

    # Walk through YYYY/MM/DD structure
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

                # Find camera files in this day directory
                for file_path in day_dir.iterdir():
                    if file_path.is_file() and file_path.suffix.lower() in [
                        ".mp4",
                        ".jpg",
                    ]:
                        filename_timestamp = parse_timestamp_from_filename(
                            file_path.name
                        )
                        if filename_timestamp:
                            mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                            files.append((file_path, filename_timestamp, mod_time))

    return files


def filter_old_files(
    files: List[Tuple[Path, datetime, datetime]], age_days: int
) -> List[Tuple[Path, datetime, datetime]]:
    """Filter files older than specified age in days."""
    cutoff_date = datetime.now() - timedelta(days=age_days)
    return [f for f in files if f[2] < cutoff_date]  # f[2] is modification time


def transcode_mp4_file(
    input_file: Path, output_file: Path, logger: logging.Logger
) -> bool:
    """
    Transcode MP4 file using ffmpeg with hardware acceleration.
    Returns True if successful, False otherwise.
    """
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-y",  # overwrite without asking
        "-hwaccel",
        "vaapi",
        "-hwaccel_output_format",
        "vaapi",
        "-i",
        str(input_file),
        "-vf",
        "scale_vaapi=1024:768,hwmap=derive_device=qsv,format=qsv",
        "-global_quality",
        "26",
        "-c:v",
        "hevc_qsv",
        "-an",
        str(output_file),
    ]

    logger.info(f"Transcoding: {input_file} -> {output_file}")

    proc = None
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )

        # Stream output in real-time to prevent blocking
        output_lines = []
        if proc.stdout:
            for line in iter(proc.stdout.readline, ""):
                line = line.rstrip()
                if line:
                    # Log FFmpeg progress/info lines at debug level to avoid spam
                    if any(
                        keyword in line.lower()
                        for keyword in ["frame=", "fps=", "time=", "speed="]
                    ):
                        logger.debug(f"FFmpeg: {line}")
                    else:
                        logger.info(f"FFmpeg: {line}")
                    output_lines.append(line)

        # Wait for process to complete
        proc.wait()

        if proc.returncode == 0:
            logger.info(f"Successfully transcoded: {output_file}")
            return True
        else:
            logger.error(
                f"Failed to transcode {input_file} (exit code: {proc.returncode})"
            )
            # Log last few lines of output for debugging
            if output_lines:
                logger.error("Last FFmpeg output:")
                for line in output_lines[-5:]:  # Show last 5 lines
                    logger.error(f"  {line}")
            return False

    except FileNotFoundError:
        logger.error("FFmpeg not found. Please install ffmpeg.")
        return False
    except Exception as e:
        logger.error(f"Error transcoding {input_file}: {e}")
        return False
    finally:
        # Ensure child process is properly reaped to prevent zombies
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"FFmpeg process did not terminate gracefully for {input_file}, killing forcefully"
                )
                proc.kill()
                proc.wait()


def get_output_path(
    input_file: Path, output_base: Path, filename_timestamp: datetime
) -> Path:
    """Generate output path maintaining directory structure."""
    # Extract year, month, day from the input file path
    parts = input_file.parts
    year = parts[-4]  # Assuming structure: .../YYYY/MM/DD/file
    month = parts[-3]
    day = parts[-2]

    # Create timestamp string for filename
    timestamp_str = filename_timestamp.strftime("%Y%m%d%H%M%S")
    output_filename = f"archived-{timestamp_str}.mp4"

    return output_base / year / month / day / output_filename


def get_directory_size(directory: Path) -> int:
    """Get total size of directory in bytes."""
    total_size = 0
    try:
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                try:
                    total_size += file_path.stat().st_size
                except (OSError, FileNotFoundError):
                    # Skip files that can't be accessed
                    pass
    except (OSError, FileNotFoundError):
        # Directory doesn't exist or can't be accessed
        pass
    return total_size


def cleanup_archived_files(
    archive_dir: Path, max_size_gb: int | float, dry_run: bool, logger: logging.Logger
) -> int:
    """
    Clean up archived files if directory exceeds maximum size.
    Returns number of files that were (or would be) removed.
    """
    if not archive_dir.exists():
        logger.info(
            f"Archive directory {archive_dir} does not exist, skipping archive cleanup"
        )
        return 0

    # Calculate current size
    current_size_bytes = get_directory_size(archive_dir)
    current_size_gb = current_size_bytes / (1024**3)  # Convert bytes to GB
    max_size_bytes = max_size_gb * (1024**3)

    logger.info(
        f"Archive directory size: {current_size_gb:.2f} GB / {max_size_gb} GB limit"
    )

    if current_size_bytes <= max_size_bytes:
        logger.info("Archive directory is within size limit, no cleanup needed")
        return 0

    # Need to clean up
    excess_bytes = current_size_bytes - max_size_bytes
    logger.info(f"Archive directory exceeds limit by {excess_bytes / (1024**3):.2f} GB")

    # Find all archived MP4 files with their modification times
    archived_files = []
    for file_path in archive_dir.rglob("archived-*.mp4"):
        if file_path.is_file():
            try:
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                file_size = file_path.stat().st_size
                archived_files.append((file_path, mod_time, file_size))
            except (OSError, FileNotFoundError):
                # Skip files that can't be accessed
                pass

    if not archived_files:
        logger.info("No archived MP4 files found for cleanup")
        return 0

    # Sort by modification time (oldest first)
    archived_files.sort(key=lambda x: x[1])

    logger.info(
        f"Found {len(archived_files)} archived MP4 files to evaluate for cleanup"
    )

    # Remove files until we're under the limit
    bytes_to_remove = excess_bytes
    files_removed = 0

    for file_path, mod_time, file_size in archived_files:
        if bytes_to_remove <= 0:
            break

        if dry_run:
            logger.info(
                f"[DRY RUN] Would remove archived file: {file_path} ({file_size / (1024**2):.1f} MB, modified: {mod_time})"
            )
            files_removed += 1
        else:
            try:
                file_path.unlink()
                logger.info(
                    f"Removed archived file: {file_path} ({file_size / (1024**2):.1f} MB, modified: {mod_time})"
                )
                files_removed += 1
            except Exception as e:
                logger.error(f"Failed to remove archived file {file_path}: {e}")
                # Don't increment files_removed for failed removals
                continue

        bytes_to_remove -= file_size

    # Final reporting
    if dry_run:
        logger.info(
            f"[DRY RUN] Would remove {files_removed} archived files to free up space"
        )
    else:
        # Recalculate size after cleanup
        new_size_bytes = get_directory_size(archive_dir)
        new_size_gb = new_size_bytes / (1024**3)
        logger.info(
            f"Archive cleanup completed: removed {files_removed} files, new size: {new_size_gb:.2f} GB"
        )

    return files_removed


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

    parser.add_argument(
        "--age",
        type=int,
        default=30,
        help="Minimum age in days for files to be processed (default: 30)",
    )

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

    # Set up directories
    base_dir = args.directory
    if not base_dir.exists():
        # Try fallback to /camera
        base_dir = Path("/camera")
        if not base_dir.exists():
            print(
                f"Error: Directory {args.directory} does not exist and fallback /camera not found"
            )
            sys.exit(1)

    # Set up output directory
    if args.output:
        output_dir = args.output
    else:
        output_dir = base_dir / "archived"
        if base_dir == Path("/camera"):
            output_dir = Path("/camera/archived")

    # Set up logging
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

    # Find all camera files
    logger.info("Scanning for camera files...")
    all_files = find_camera_files(base_dir)
    logger.info(f"Found {len(all_files)} total camera files")

    # Filter by age
    old_files = filter_old_files(all_files, args.age)
    logger.info(f"Found {len(old_files)} files older than {args.age} days")

    # Separate MP4 and JPG files
    mp4_files = [f for f in old_files if f[0].suffix.lower() == ".mp4"]
    jpg_files = [f for f in old_files if f[0].suffix.lower() == ".jpg"]

    logger.info(f"MP4 files to transcode: {len(mp4_files)}")
    logger.info(f"JPG files found: {len(jpg_files)}")

    # Process MP4 files
    successfully_processed = []

    for file_path, filename_timestamp, mod_time in mp4_files:
        output_path = get_output_path(file_path, output_dir, filename_timestamp)

        if args.dry_run:
            logger.info(f"[DRY RUN] Would transcode: {file_path} -> {output_path}")
            # In dry run mode, assume all transcodes would succeed
            successfully_processed.append((file_path, filename_timestamp, mod_time))
        else:
            if transcode_mp4_file(file_path, output_path, logger):
                successfully_processed.append((file_path, filename_timestamp, mod_time))

    if args.dry_run:
        logger.info(
            f"[DRY RUN] Would transcode {len(successfully_processed)} MP4 files"
        )
    else:
        logger.info(f"Successfully transcoded {len(successfully_processed)} MP4 files")

    # Cleanup if requested
    if args.cleanup:
        if args.dry_run:
            logger.info("[DRY RUN] Starting cleanup simulation...")
        else:
            logger.info("Starting cleanup process...")

        # Remove successfully processed MP4 files and their corresponding JPGs
        processed_timestamps = set()
        for file_path, filename_timestamp, mod_time in successfully_processed:
            output_path = get_output_path(file_path, output_dir, filename_timestamp)

            # Verify archived file exists and is not empty before removing original
            if args.dry_run:
                logger.info(
                    f"[DRY RUN] Would verify archived file exists: {output_path}"
                )
                logger.info(f"[DRY RUN] Would remove: {file_path}")
                processed_timestamps.add(filename_timestamp)
            else:
                # Check if archived file exists and has content
                if output_path.exists() and output_path.stat().st_size > 0:
                    try:
                        file_path.unlink()
                        logger.info(f"Removed: {file_path} (archived to {output_path})")
                        processed_timestamps.add(filename_timestamp)
                    except Exception as e:
                        logger.error(f"Failed to remove {file_path}: {e}")
                else:
                    if not output_path.exists():
                        logger.error(
                            f"Cannot remove {file_path}: archived file {output_path} does not exist"
                        )
                    else:
                        logger.error(
                            f"Cannot remove {file_path}: archived file {output_path} is empty (0 bytes)"
                        )
                    # Don't add to processed_timestamps since we didn't remove it
                    continue

            # Look for corresponding JPG file
            jpg_path = file_path.with_suffix(".jpg")
            if jpg_path.exists():
                if args.dry_run:
                    logger.info(f"[DRY RUN] Would remove: {jpg_path}")
                else:
                    # Only remove JPG if the MP4 was successfully removed (i.e., archived file is valid)
                    if filename_timestamp in processed_timestamps:
                        try:
                            jpg_path.unlink()
                            logger.info(f"Removed: {jpg_path}")
                        except Exception as e:
                            logger.error(f"Failed to remove {jpg_path}: {e}")
                    else:
                        logger.info(
                            f"Preserving JPG {jpg_path} because MP4 was not removed"
                        )

        # Find and remove orphaned JPG files (JPGs without matching MP4s)
        if args.dry_run:
            logger.info("[DRY RUN] Scanning for orphaned JPG files...")
        else:
            logger.info("Scanning for orphaned JPG files...")

        # Get all JPG files from the filtered old files
        jpg_files_to_check = [f for f in old_files if f[0].suffix.lower() == ".jpg"]

        orphaned_count = 0
        for jpg_path, jpg_timestamp, jpg_mod_time in jpg_files_to_check:
            # Check if there's a corresponding MP4 file with the same timestamp
            mp4_path = jpg_path.with_suffix(".mp4")

            # JPG is orphaned if:
            # 1. No corresponding MP4 exists, OR
            # 2. The MP4 was successfully processed and removed
            is_orphaned = (not mp4_path.exists()) or (
                jpg_timestamp in processed_timestamps
            )

            if is_orphaned:
                orphaned_count += 1
                if args.dry_run:
                    logger.info(f"[DRY RUN] Would remove orphaned JPG: {jpg_path}")
                else:
                    try:
                        jpg_path.unlink()
                        logger.info(f"Removed orphaned JPG: {jpg_path}")
                    except Exception as e:
                        logger.error(f"Failed to remove orphaned JPG {jpg_path}: {e}")

        if args.dry_run:
            logger.info(
                f"[DRY RUN] Cleanup summary: Would process {len(successfully_processed)} MP4 files and remove {orphaned_count} orphaned JPGs"
            )
        else:
            actually_removed = len(processed_timestamps)
            logger.info(
                f"Cleanup completed. Successfully removed {actually_removed} MP4 files and {orphaned_count} orphaned JPGs"
            )

    # Archive size management - clean up old archived files if directory is too large
    logger.info("Checking archive directory size...")
    archived_files_cleaned = cleanup_archived_files(
        output_dir, args.max_size, args.dry_run, logger
    )

    if args.dry_run:
        logger.info("Archive process simulation completed")
    else:
        logger.info("Archive process completed")


if __name__ == "__main__":
    main()
