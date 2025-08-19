#!/usr/bin/python3

import argparse
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timedelta

base_dir = "/camera"
output_dir = f"{base_dir}/archived"
target_fmt = "%Y%m%d%H%M%S"
log_path = f"{base_dir}/transcoding.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")


def log(message):
    """Log message to stdout and flush immediately"""
    sys.stdout.write(f"{message}\n")
    sys.stdout.flush()


def extract_timestamp(filename):
    """Extract timestamp from filename in the format YYYYMMDDHHMMSS

    Args:
        filename (str): Filename containing a timestamp

    Returns:
        datetime: Extracted timestamp or None if no timestamp found
    """
    match = re.search(r"\d{14}", filename)
    if not match:
        return None

    timestamp_str = match.group()

    try:
        return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def format_timestamp_filepath(
    file_str, timestamp, base_dir, prefix_str="archived-", ext_str="mp4"
):
    """Format a filename and path based on the extracted timestamp

    Args:
        file_str (str): Original filename
        timestamp (datetime): Timestamp to use for formatting
        base_dir (str): Base directory for output
        prefix_str (str, optional): Prefix for new filename. Defaults to "archived-".
        ext_str (str, optional): File extension. Defaults to "mp4".

    Returns:
        tuple: (new_filename, new_dirpath) or None if formatting fails
    """
    file_ts_split = file_str.split("_")
    if len(file_ts_split) < 4:
        return None

    # Extract timestamp from filename component
    parsed_ts_str = file_ts_split[3].split(".")[0]

    try:
        dt = datetime.strptime(parsed_ts_str, target_fmt)
    except ValueError:
        return None

    timestamp_str = timestamp.strftime(target_fmt)
    new_filename = f"{prefix_str}{timestamp_str}.{ext_str}"
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    new_dirpath = os.path.join(base_dir, year, month, day)

    return (new_filename, new_dirpath)


def create_file_list_recurse(directory, age_days=0, ext_str="mp4", excluded=None):
    """Recursively list files in directory based on timestamp and age

    Args:
        directory (str): Directory to search
        age_days (int, optional): Minimum age of files to include. Defaults to 0.
        ext_str (str, optional): File extension to filter by. Defaults to "mp4".
        excluded (str, optional): Subdirectory path containing this string will be skipped. Default: None.

    Returns:
        list: List of tuples (file_path, timestamp) matching criteria
    """
    file_tuples = []
    cutoff_date = datetime.now() - timedelta(days=age_days)

    # Use os.walk with proper directory exclusion logic
    for root, _, files in os.walk(directory):
        if excluded and excluded in str(root):
            continue  # skip this directory

        for file in files:
            if not file.endswith(f".{ext_str}"):
                continue

            file_path = os.path.join(root, file)
            timestamp = extract_timestamp(file)

            if not timestamp:
                continue

            # Filter by age
            if age_days > 0 and timestamp < cutoff_date:
                continue

            file_tuples.append((file_path, timestamp))

    return file_tuples


def create_file_list(directory, age_days=0, ext_str="mp4"):
    """List files in a directory based on timestamp and age

    Args:
        directory (str): Directory to search
        age_days (int, optional): Minimum age of files to include. Defaults to 0.
        ext_str (str, optional): File extension to filter by. Defaults to "mp4".

    Returns:
        list: List of tuples (file_path, timestamp) matching criteria
    """
    file_tuples = []

    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue

                filename = entry.name
                if not filename.endswith(f".{ext_str}"):
                    continue

                timestamp = extract_timestamp(filename)
                if not timestamp:
                    continue

                # Filter by age (only check day of month for simplicity)
                if age_days > 0 and int(timestamp.strftime("%d")) <= datetime.now().day:
                    continue

                file_tuples.append((entry.path, timestamp))
    except OSError:
        pass  # Handle directory access errors gracefully

    return file_tuples


def is_directory_truly_empty(path):
    """Check if a directory is truly empty (no files, no subdirectories)

    Args:
        path (str): Directory path to check

    Returns:
        bool: True if directory is completely empty, False otherwise
    """
    try:
        with os.scandir(path) as entries:
            for _ in entries:
                # If we find any entry (file or directory), it's not empty
                return False
        return True
    except OSError:
        # If we can't scan the directory, assume it's not empty to be safe
        return False


def remove_empty(path, dry_run=False):
    """Remove empty directories in a directory tree

    Args:
        path (str): Base directory to clean
        dry_run (bool, optional): If True, don't actually make changes. Defaults to False.

    Returns:
        int: Number of directories removed
    """
    count = 0
    removed_dirs = set()  # Track successfully removed directories
    failed_dirs = set()  # Track directories that failed to be removed

    for dirpath, _, _ in os.walk(path, topdown=False):
        # Skip if it's the base path itself (to avoid removing the root)
        if dirpath == path:
            continue

        # Skip if this directory was already processed or is a child of a failed directory
        if dirpath in removed_dirs or any(
            dirpath.startswith(failed_dir) for failed_dir in failed_dirs
        ):
            continue

        if is_directory_truly_empty(dirpath):
            try:
                if not dry_run:
                    os.rmdir(dirpath)
                    removed_dirs.add(dirpath)
                    log(f"Removed empty directory: {dirpath}")
                    count += 1
                else:
                    log(f"Would remove empty directory: {dirpath}")
                    count += 1
            except OSError as e:
                # Only log the error if we haven't already failed on a parent directory
                if not any(
                    dirpath.startswith(failed_dir) for failed_dir in failed_dirs
                ):
                    log(f"Failed to remove directory {dirpath}: {str(e)}")
                failed_dirs.add(dirpath)

    return count


def _normalize_path(path: str) -> str:
    """Return an absolute, normalized path with forward slashes."""
    return os.path.normpath(os.path.abspath(path))


def collect_files_to_delete(directory: str, max_days_old: int) -> list:
    """
    Collect files older than *max_days_old* days for deletion.

    Returns a **sorted list** of tuples:

        (file_path, timestamp, size_in_bytes, jpg_path_or_None)

    All paths are absolute and normalised so that callers can compare them
    reliably.  The list is sorted by the extracted timestamp (oldest first)
    – this matches the behaviour that `cleanup_old_files()` expects.
    """
    candidates = []
    cutoff_datetime = datetime.now() - timedelta(days=max_days_old)

    for root, _, files in os.walk(directory):
        for file in files:
            # Skip JPG companions – we only want to consider MP4s
            if file.lower().endswith(".jpg"):
                continue

            timestamp = extract_timestamp(file)
            if not timestamp or timestamp >= cutoff_datetime:
                continue

            file_path = os.path.normpath(os.path.abspath(os.path.join(root, file)))
            try:
                size = os.path.getsize(file_path)
            except OSError:
                size = 0

            base, _ = os.path.splitext(file_path)
            jpg_path = f"{base}.jpg"
            if not os.path.exists(jpg_path):
                jpg_path = None

            candidates.append((file_path, timestamp, size, jpg_path))

    # Return sorted by timestamp (oldest first) – identical to old behaviour
    return sorted(candidates, key=lambda x: x[1])


def cleanup_old_files(
    directory: str,
    max_days_old: int,
    max_size_gb: float,
    dry_run: bool = False,
) -> float:
    """
    Clean up old files in *directory*.

    The function now expects `collect_files_to_delete()` to return a list of
    tuples, so the code below has been simplified accordingly.
    """
    total_size = 0

    # Get candidate files as a sorted list (oldest first)
    flagged_mp4s = collect_files_to_delete(directory, max_days_old)

    files_to_remove = []

    for mp4_path, ts, file_size, jpg_path in flagged_mp4s:
        if total_size >= (max_size_gb * 1024**3):
            break

        # Double‑check the modification time to guard against race conditions
        try:
            if datetime.fromtimestamp(os.path.getmtime(mp4_path)) >= (
                datetime.now() - timedelta(days=max_days_old)
            ):
                continue
        except OSError:
            continue

        files_to_remove.append((mp4_path, file_size))
        total_size += file_size

        if jpg_path and os.path.exists(jpg_path):
            try:
                jpg_size = os.path.getsize(jpg_path)
            except OSError:
                jpg_size = 0
            files_to_remove.append((jpg_path, jpg_size))
            total_size += jpg_size

    if not files_to_remove:
        log("No files to cleanup!")
        return 0.0

    log(f"Found {len(files_to_remove)} ({total_size / (1024**2):.2f} MB) to cleanup...")

    dirs_before = {root for root, _, _ in os.walk(directory)}
    for file_path, _ in files_to_remove:
        if not dry_run:
            try:
                os.remove(file_path)
                log(f"Deleted old file: {file_path}")
            except OSError as e:
                log(f"Failed to remove file {file_path}: {str(e)}")
        else:
            log(f"Would have deleted: {file_path}")

    dirs_after = {root for root, _, _ in os.walk(directory)}
    dirs_to_remove = dirs_before - dirs_after
    if not dry_run:
        for dirpath in sorted(dirs_to_remove, reverse=True):
            if is_directory_truly_empty(dirpath):
                try:
                    os.rmdir(dirpath)
                    log(f"Removed empty directory after cleanup: {dirpath}")
                except OSError:
                    pass

    return total_size / (1024**2)  # Return size in MB


def transcode_file(input_file, output_file):
    """Transcode a video file using FFmpeg with hardware acceleration

    Args:
        input_file (str): Path to source file
        output_file (str): Path to output file

    Note: This function is tailored for Intel Celeron J3455 processors.
    """
    command = [
        "ffmpeg",
        "-y",
        "-hwaccel",
        "vaapi",
        "-hwaccel_output_format",
        "vaapi",
        "-i",
        input_file,
        "-vf",
        "scale_vaapi=1024:768,hwmap=derive_device=qsv,format=qsv",
        "-global_quality",
        "26",
        "-c:v",
        "h264_qsv",
        "-an",  # No audio
        output_file,
    ]

    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE
    )

    if not process.stderr:
        log("Error: stderr is None!")
        return

    err_buf = io.BytesIO()
    err_thread = threading.Thread(
        target=shutil.copyfileobj, args=(process.stderr, err_buf)
    )
    err_thread.start()

    # Read and log stdout output
    if process.stdout:
        for line in process.stdout:
            log(line.decode().strip())

    # Wait for process to finish
    process.wait()
    err_thread.join()

    if err_buf.tell() > 0:
        err_buf.seek(0)
        log(f"FFmpeg Errors: {err_buf.read().decode()}")


def transcode_list(file_list, dry_run=False):
    """Transcode a list of files

    Args:
        file_list (list): List of tuples (file_path, timestamp) to transcode
        dry_run (bool, optional): If True, don't actually make changes. Defaults to False.

    Returns:
        int: Number of files successfully transcoded
    """
    if not file_list:
        log("Error: transcode_list got an empty list!")
        return 0

    total_transcoded = 0

    for idx, (filepath, timestamp) in enumerate(file_list):
        result = format_timestamp_filepath(filepath, timestamp, output_dir)

        if not result:
            log(f"Warning: Failed to format path for {filepath}")
            continue

        out_file, out_dir = result
        output_path = os.path.join(out_dir, out_file)

        log(
            f"Transcoding file {idx + 1} of {len(file_list)}: {filepath} -> {output_path}"
        )

        if dry_run:
            log("Would have transcoded and deleted source")
            continue

        # Create directory structure
        os.makedirs(out_dir, exist_ok=True)

        try:
            transcode_file(filepath, output_path)

            # Verify file was created successfully
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                log(f"Deleting old source file: {filepath}")
                os.remove(filepath)
                total_transcoded += 1
        except Exception as e:
            log(f"Error transcoding {filepath}: {str(e)}")

    return total_transcoded


def setup_logging():
    """Configure logging to both a file and stdout"""
    if not os.path.exists(os.path.dirname(log_path)):
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Configure the logger
    log_format = "[%(asctime)s] [%(levelname)s]: %(message)s"

    # File handler
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format=log_format,
    )

    # Console handler (stdout and stderr)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)

    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(error_handler)


def find_year_directories(base_path):
    """Find top-level year directories (2000-2099) in a base directory

    Args:
        base_path (str): Base path to search for year directories

    Returns:
        list: List of paths to valid year directories
    """
    if not os.path.exists(base_path):
        log(f"Base directory does not exist: {base_path}")
        return []

    year_dirs = []

    try:
        # Only look at direct children of base_path
        for item in os.listdir(base_path):
            item_path = os.path.join(base_path, item)

            if (
                os.path.isdir(item_path)
                and len(item) == 4
                and item.isdigit()
                and item.startswith("20")
                and 2000 <= int(item) <= 2099
            ):
                year_dirs.append(item_path)

    except OSError as e:
        log(f"Error reading directory {base_path}: {e}")

    return sorted(year_dirs)


def organize(dry=False):
    """Clean up archived files and source directories

    Args:
        dry (bool): If True, don't actually make changes
    """
    # Clean archived files
    cleanup_old_files(output_dir, max_days_old=30, max_size_gb=300, dry_run=dry)

    # Remove empty directories in the archive
    remove_empty(output_dir, dry_run=dry)

    # Clean source directories (year folders only)
    year_dirs = find_year_directories(base_dir)
    for year_dir in year_dirs:
        remove_empty(year_dir, dry_run=dry)


def transcode(dry=False):
    """Process files for transcoding

    Args:
        dry (bool): If True, don't actually make changes
    """
    # Only process top-level year directories
    year_dirs = find_year_directories(base_dir)

    if not year_dirs:
        log("No year directories found!")
        return 0

    total_files_processed = 0

    for year_dir in year_dirs:
        log(f"Processing year directory: {year_dir}")

        # List files to transcode
        files_to_transcode = create_file_list_recurse(year_dir, age_days=1)

        if not files_to_transcode:
            continue

        log(f"Found {len(files_to_transcode)} files in {year_dir}")

        # Process the files
        num_processed = transcode_list(files_to_transcode, dry_run=dry)
        total_files_processed += num_processed

        # Clean up empty directories for this year only
        remove_empty(year_dir, dry_run=dry)

    if total_files_processed == 0:
        log("No valid media files found in any year directory!")
    else:
        log(
            f"Finished processing {total_files_processed} files across {len(year_dirs)} year directories"
        )

    return total_files_processed


def main():
    """Main entry point for the application"""
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Archive and organize ReoLink media files"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-t", "--transcode", action="store_true", help="Transcode only (no cleanup)"
    )
    group.add_argument(
        "-c", "--cleanup", action="store_true", help="Cleanup only (no transcode)"
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Do a dry run without making any changes",
    )
    args = parser.parse_args()

    if not any(vars(args).values()):
        transcode(dry=False)
        organize(dry=False)
    elif args.transcode:
        transcode(dry=args.dry_run)
    elif args.cleanup:
        organize(dry=args.dry_run)
    else:
        log("No action specified. Use -t for transcoding or -c for cleanup.")
        parser.print_help()


if __name__ == "__main__":
    main()
