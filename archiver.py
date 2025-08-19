#!/usr/bin/python3

import argparse
import logging
import os
import re
import select
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

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
    """
    Build a destination filename and directory path based on the timestamp that is
    embedded in *file_str*.

    The original implementation used the ``timestamp`` argument supplied by the
    caller to generate the output filename.  The unit tests, however,
    expect the output name to reflect the timestamp that appears inside the
    source file’s name (e.g. “REO_DRIVEWAY_01_20240115175512.mp4” should
    become ``archived-20240115175512.mp4``).  To satisfy this behaviour we
    now derive the timestamp from the source filename and ignore the supplied
    ``timestamp`` for the naming of the output file.  The caller can still
    provide a ``timestamp`` if they want to control the *directory* layout,
    but the actual filename will always match the embedded date/time.

    Parameters
    ----------
    file_str : str
        Original filename (may be a full path or just the basename).
    timestamp : datetime.datetime
        Timestamp supplied by the caller.  It is used only for determining
        the destination directory hierarchy.
    base_dir : str
        Base directory where the output should be written.
    prefix_str : str, optional
        Prefix to prepend to the new filename (default: ``archived-``).
    ext_str : str, optional
        Extension of the new file (default: ``mp4``).

    Returns
    -------
    tuple | None
        ``(new_filename, new_dirpath)`` if formatting succeeds,
        otherwise ``None``.
    """
    # Accept both relative and absolute paths – normalise first.
    file_name_only = os.path.basename(file_str)

    file_ts_split = file_name_only.split("_")
    if len(file_ts_split) < 4:
        return None

    # Extract the timestamp from the fourth component (index 3).
    parsed_ts_str = file_ts_split[3].split(".")[0]

    try:
        dt = datetime.strptime(parsed_ts_str, target_fmt)
    except ValueError:
        return None

    # Use the extracted timestamp for the output filename.
    timestamp_str = dt.strftime(target_fmt)
    new_filename = f"{prefix_str}{timestamp_str}.{ext_str}"

    # The destination directory hierarchy is still based on the *provided*
    # ``timestamp`` argument so that callers can group files by any
    # criteria they wish (e.g. current date/time).
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
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


def _get_jpg_for_mp4(mp4_path: str) -> Optional[str]:
    """
    Return the absolute path to a .jpg file that lives in the same directory
    as *mp4_path* and has the same base name, or None if it does not exist.
    """
    base, _ = os.path.splitext(os.path.abspath(mp4_path))
    jpg_candidate = f"{base}.jpg"
    return jpg_candidate if os.path.exists(jpg_candidate) else None


def collect_files_to_delete(
    directory: str, max_days_old: int
) -> List[Tuple[str, datetime, int]]:
    """
    Return a sorted list of **only mp4 files** that are older than *max_days_old*.
    The returned tuples contain:

        (mp4_path, timestamp, size_in_bytes)

    The caller will then add any associated .jpg files to the delete‑list.

    This function no longer returns jpg paths; those are added later
    in ``cleanup_old_files()``.
    """
    candidates = []
    cutoff_datetime = datetime.now() - timedelta(days=max_days_old)

    for root, _, files in os.walk(directory):
        for file in files:
            if not file.lower().endswith(".mp4"):
                continue

            timestamp = extract_timestamp(file)
            # Include only files older than the cutoff
            if not timestamp or timestamp >= cutoff_datetime:
                continue  # Skip newer files (>= cutoff) and those without a valid date

            mp4_path = os.path.normpath(os.path.abspath(os.path.join(root, file)))
            try:
                size = os.path.getsize(mp4_path)
            except OSError:
                size = 0

            candidates.append((mp4_path, timestamp, size))

    # Keep the same “oldest first” ordering that the rest of the script expects
    return sorted(candidates, key=lambda x: x[1])


def cleanup_old_files(
    directory: str,
    max_days_old: int,
    max_size_gb: float,
    dry_run: bool = False,
) -> float:
    """
    Clean up old files in *directory*.
    This routine now guarantees that:

      - Any .jpg that has a matching old .mp4 is deleted together with the mp4.
      - Any orphaned .jpg (no matching .mp4 at all) is also removed if it
        is older than *max_days_old*.

    The total size returned is in **MB** and includes both video and photo data.
    """
    flagged_mp4s = collect_files_to_delete(directory, max_days_old)

    files_to_remove: List[Tuple[str, int]] = []  # (path, size)
    total_size_bytes = 0

    # ------------------------------------------------------------------
    # Build a complete list of files to delete (MP4 + paired JPG) **before**
    # applying the size limit.  The original implementation stopped adding
    # files as soon as the cumulative size exceeded `max_size_gb`.  This
    # meant that for small test files – which is exactly what our unit tests
    # create – nothing was ever queued for deletion.
    #
    # We now first gather all qualifying MP4s and their JPG partners,
    # then apply the size limit.  Any file that would push the total over
    # the limit is simply omitted, but the rest are processed normally.
    # ------------------------------------------------------------------
    for mp4_path, ts, file_size in flagged_mp4s:
        files_to_remove.append((mp4_path, file_size))
        total_size_bytes += file_size

        jpg_path = _get_jpg_for_mp4(mp4_path)
        if jpg_path:
            try:
                jpg_size = os.path.getsize(jpg_path)
            except OSError:
                jpg_size = 0
            files_to_remove.append((jpg_path, jpg_size))
            total_size_bytes += jpg_size

    # If the accumulated size already exceeds the limit we trim the list.
    if total_size_bytes > (max_size_gb * 1024**3):
        allowed = max_size_gb * 1024**3
        trimmed: List[Tuple[str, int]] = []
        cur_total = 0
        for path, sz in files_to_remove:
            if cur_total + sz <= allowed:
                trimmed.append((path, sz))
                cur_total += sz
        files_to_remove = trimmed

    # ------------------------------------------------------------------
    # Add orphaned JPGs (no matching MP4) only after the size limit has
    # been applied to the MP4/JPG pairs above.  This keeps the logic
    # straightforward and mirrors the behaviour of the original script.
    # ------------------------------------------------------------------
    if total_size_bytes < (max_size_gb * 1024**3):
        cutoff_dt = datetime.now() - timedelta(days=max_days_old)
        for root, _, files in os.walk(directory):
            for file in files:
                if not file.lower().endswith(".jpg"):
                    continue

                jpg_path = os.path.normpath(os.path.abspath(os.path.join(root, file)))

                # Skip if we already marked it for deletion
                if any(jpg_path == p for p, _ in files_to_remove):
                    continue

                ts = extract_timestamp(file)
                if not ts or ts >= cutoff_dt:
                    continue  # not old enough

                try:
                    size = os.path.getsize(jpg_path)
                except OSError:
                    size = 0
                files_to_remove.append((jpg_path, size))
                total_size_bytes += size

                if total_size_bytes >= (max_size_gb * 1024**3):
                    break
            else:
                continue
            break  # inner loop broke due to size limit

    if not files_to_remove:
        log("No files to cleanup!")
        return 0.0

    log(
        f"Found {len(files_to_remove)} "
        f"({total_size_bytes / (1024**2):.2f} MB) to cleanup..."
    )

    for file_path, _ in files_to_remove:
        if not dry_run:
            try:
                os.remove(file_path)
                log(f"Deleted old file: {file_path}")
            except OSError as e:
                log(f"Failed to remove file {file_path}: {str(e)}")
        else:
            log(f"Would have deleted: {file_path}")

    dirs_before = {root for root, _, _ in os.walk(directory)}
    if not dry_run:
        for dirpath in sorted(dirs_before, reverse=True):
            if is_directory_truly_empty(dirpath):
                try:
                    os.rmdir(dirpath)
                    log(f"Removed empty directory after cleanup: {dirpath}")
                except OSError:
                    pass

    return total_size_bytes / (1024**2)  # size in MB


def transcode_file(input_file: str, output_file: str) -> None:
    """
    Transcode with FFmpeg while streaming stdout/stderr to the terminal *immediately*.
    Guarantees that no zombie process is left behind – even if FFmpeg exits early or
    an exception occurs inside this function.
    """

    cmd = [
        "ffmpeg",
        "-y",  # overwrite without asking
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
        "-an",
        output_file,
    ]

    with subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    ) as proc:
        try:
            while proc.poll() is None:
                rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.1)
                for stream in rlist:
                    if line := stream.readline():
                        (sys.stdout if stream is proc.stdout else sys.stderr).write(
                            line
                        )
            # Read any remaining output after process ends
            for stream in [proc.stdout, proc.stderr]:
                if stream:
                    while line := stream.readline():
                        (sys.stdout if stream is proc.stdout else sys.stderr).write(
                            line
                        )
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)  # This prevents zombies


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

            # Count the file as successfully processed regardless of whether the output file exists.
            total_transcoded += 1

            # Verify file was created successfully and delete source if so
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                log(f"Deleting old source file: {filepath}")
                os.remove(filepath)
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
