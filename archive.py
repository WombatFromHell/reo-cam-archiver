#!/usr/bin/python3

import argparse
import io
import logging
import operator
import os
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timedelta

base_dir = "/camera"
# Base directory where the output files will be stored
output_dir = f"{base_dir}/archived"
# Timestamp format for parsing and output
target_fmt = "%Y%m%d%H%M%S"
log_path = f"{base_dir}/transcoding.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")


def log(message):
    sys.stdout.write(message + "\n")
    sys.stdout.flush()


def extract_timestamp(filename):
    # Find the timestamp in the filename
    match = re.search(r"\d{14}", filename)
    if match:
        timestamp_str = match.group()
        timestamp = datetime.strptime(timestamp_str, target_fmt)
        return timestamp
    else:
        return None


def format_timestamp_filepath(
    file_str, timestamp, base_dir, prefix_str="archived-", ext_str="mp4"
):
    # takes a filename, timestamp, and directory path to output a tuple with a formatted file/directory path
    # using a structure like: /<year>/<month>/<day>/archived-<timestamp>.mp4
    file_ts_split = file_str.split("_")
    if file_ts_split is None and len(file_ts_split) != 4:
        return None

    parsed_ts_str = file_ts_split[3].split(".")[0]
    dt = datetime.strptime(parsed_ts_str, target_fmt)
    timestamp_str = timestamp.strftime(target_fmt)
    new_filename = f"{prefix_str}{timestamp_str}.{ext_str}"
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    new_dirpath = os.path.join(base_dir, year, month, day)
    return (new_filename, new_dirpath)


def test_extract_timestamp():
    file1 = "/input/2024/01/15/REO_DRIVEWAY_01_20240115175512.mp4"
    file2 = "/output/2024/01/15/archived-20240115175512.mp4"
    file3 = "archived-20240115175512.mp4"
    real_timestamp = datetime(2024, 1, 15, 17, 55, 12)
    parsed_dt1 = extract_timestamp(file1)
    parsed_dt2 = extract_timestamp(file2)
    parsed_dt3 = extract_timestamp(file3)
    assert (
        parsed_dt1 == real_timestamp
        and parsed_dt2 == real_timestamp
        and parsed_dt3 == real_timestamp
    )


def test_format_timestamp_filepath():
    file1 = "/input/2024/01/15/REO_DRIVEWAY_01_20240115175512.mp4"
    file1_timestamp = extract_timestamp(file1)
    file_tuple = format_timestamp_filepath(file1, file1_timestamp, output_dir)
    if file_tuple is None:
        assert False
    filename, dir_path = file_tuple
    assert (
        filename == "archived-20240115175512.mp4"
        and dir_path == "/camera/archived/2024/01/15"
    )


def create_file_list_recurse(directory, age_days=0, ext_str="mp4", excluded=None):
    file_tuples = []
    min_age_ms = age_days * 86400 * 1000000
    today = datetime.now() - timedelta(days=age_days)
    today_ms = today.timestamp() * 1000000
    for root, _, files in os.walk(directory):
        if excluded is not None and excluded in str(root):
            log(f"list_recurse skipping: {root}")
            continue  # skip this (sub)directory
        for file in files:
            if file.endswith(f".{ext_str}"):
                file_path = os.path.join(root, file)

                timestamp = extract_timestamp(file)
                if timestamp is None:
                    continue

                timestamp_ms = timestamp.timestamp() * 1000000
                delta_ms = int(today_ms - timestamp_ms)
                if age_days == 0 or age_days > 0 and delta_ms > min_age_ms:
                    # log(f"list_recurse appended filtered item: {file}")
                    file_tuples.append((file_path, timestamp))

    # ensure file_tuples does not contain None
    filtered_file_tuples = [item for item in file_tuples if item is not None]
    return filtered_file_tuples


def create_file_list(directory, age_days=0, ext_str="mp4"):
    file_tuples = []
    today = datetime.now() - timedelta(days=age_days)
    for entry in os.scandir(directory):
        if entry.is_file() and entry.name.endswith(f".{ext_str}"):
            file_path = entry.path
            timestamp = extract_timestamp(entry.name)
            if timestamp is None:
                return file_tuples
            day = int(timestamp.strftime("%d"))
            if age_days == 0 or age_days > 0 and day <= today.day:
                file_tuples.append((file_path, timestamp))
    return file_tuples


def remove_empty(path, dry_run=False):
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.splitext(filename)[1] == ".jpg":
                try:
                    if not dry_run:
                        log(f"Removed .jpg file: {filepath}")
                        os.remove(filepath)
                    else:
                        log(f"Would have removed .jpg file: {filepath}")
                except OSError as e:
                    log(f"Failed to remove .jpg file {filepath}: {str(e)}")
        if not dirnames and not filenames:
            try:
                if not dry_run:
                    log(f"Removed empty directory {dirpath}")
                    os.rmdir(dirpath)
                else:
                    log(f"Would have removed empty directory: {dirpath}")
            except OSError as e:
                log(f"Failed to remove empty directory {dirpath}: {str(e)}")


def collect_files_to_delete(directory, max_days_old):
    files_to_delete = []

    max_timestamp = datetime.now() - timedelta(days=max_days_old)

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_timestamp = extract_timestamp(file_path)

            if file_timestamp is not None and file_timestamp < max_timestamp:
                file_size = os.path.getsize(file_path)
                files_to_delete.append((file_path, file_timestamp, file_size))

    return files_to_delete


def cleanup_old_files(directory, max_days_old, max_size_gb, dry_run=False):
    max_size_bytes = max_size_gb * (1024**3)  # Convert max_size_gb to bytes
    total_size = 0
    flagged_files = collect_files_to_delete(directory, max_days_old)
    # Sort files for removal by timestamp (oldest to newest)
    files_sorted = sorted(
        flagged_files,
        key=operator.itemgetter(1),
    )
    files_to_remove = []
    for file_path, _, file_size in files_sorted:
        if total_size >= max_size_bytes:
            break
        files_to_remove.append((file_path, file_size))
        total_size += file_size

    if len(files_to_remove) == 0:
        log("No files to cleanup!")
        return

    log(
        f"Found {len(files_to_remove)} ({total_size / 1024 / 1024:.2f} MB) to cleanup..."
    )
    for file_path, _ in files_to_remove:
        parent_dir = os.path.dirname(file_path)
        try:
            if not dry_run:
                if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                    log(f"Removing empty directory: {parent_dir}")
                    os.rmdir(parent_dir)
            else:
                if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                    log(f"Would have removed empty directory: {parent_dir}")
        except OSError as e:
            log(f"Failed to remove directory {parent_dir}: {str(e)}")

    return total_size


def transcode_file(input_file, output_file):
    # tailored for an Intel® Celeron® J3455 (Apollo Lake) found in a QNAP TS-253be NAS
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
        "-an",
        output_file,
    ]
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE
    )
    stdin = process.stdin
    stdout = process.stdout
    if stdin is None:
        log("Error: stdin is None!")
        return
    if stdout is None:
        log("Error: stdout is None!")
        return
    stdin.close()
    err_buf = io.BytesIO()
    err_thread = threading.Thread(
        target=shutil.copyfileobj, args=(process.stderr, err_buf)
    )
    err_thread.start()
    for line in stdout:
        line = line.decode()  # defaulting to system encoding
        log(line)
    process.wait()
    err_thread.join()
    err_buf.seek(0)
    log(f"Errors: {err_buf.read().decode()}")


def transcode_list(file_list, dry_run=False):
    file_list_len = len(file_list)
    if file_list_len == 0:
        log("Error: transcode_list got an empty list!")
        return

    for idx, (filepath, timestamp) in enumerate(file_list):
        result = format_timestamp_filepath(filepath, timestamp, output_dir)
        if result is None:
            log("Error: format_timestamp_filepath returned None!")
            return
        out_file, out_dir = result
        output = os.path.join(out_dir, out_file)
        log(f"Transcoding file {idx + 1} of {file_list_len}: {filepath} -> {output}")
        if not dry_run:
            os.makedirs(out_dir, exist_ok=True)
            transcode_file(filepath, output)
            if os.path.exists(output) and os.path.getsize(output) > 0:
                log(f"Deleting old file: {filepath}")
                os.remove(filepath)
            if idx == file_list_len - 1:
                log("Finished transcoding!")
        else:
            log(f"Would have transcoded: {filepath} -> {output}")
            log(f"Would have deleted: {filepath}")
            if idx == file_list_len - 1:
                log("Finished transcoding dry-run!")


def test():
    test_format_timestamp_filepath()
    test_extract_timestamp()


def setup_logging():
    log_format = "[%(asctime)s] [%(levelname)s]: %(message)s"
    if not os.path.exists(log_path):
        open(log_path, "a").close()
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format=log_format,
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)

    console_handler_err = logging.StreamHandler(sys.stderr)
    console_handler_err.setLevel(logging.ERROR)
    console_handler_err.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler_err)


def find_year_directories(base_path):
    """Find only top-level year directories (e.g., /camera/2023, /camera/2024)"""
    year_dirs = []

    if not os.path.exists(base_path):
        log(f"Base directory does not exist: {base_path}")
        return year_dirs

    try:
        # Only look at direct children of base_path
        for item in os.listdir(base_path):
            item_path = os.path.join(base_path, item)

            # Check if it's a directory and looks like a 4-digit year (2000-2099)
            if (
                os.path.isdir(item_path)
                and len(item) == 4
                and item.isdigit()
                and item.startswith("20")
            ):
                try:
                    year = int(item)
                    if 2000 <= year <= 2099:
                        year_dirs.append(item_path)
                except ValueError:
                    continue
    except OSError as e:
        log(f"Error reading directory {base_path}: {e}")

    return sorted(year_dirs)


def organize(dry=False):
    # Clean archived files
    cleanup_old_files(output_dir, max_days_old=30, max_size_gb=300, dry_run=dry)
    remove_empty(output_dir, dry_run=dry)
    # Clean source directories
    year_dirs = find_year_directories(base_dir)
    for year_dir in year_dirs:
        remove_empty(year_dir, dry_run=dry)


def transcode(dry=False):
    # Only process top-level year directories
    year_dirs = find_year_directories(base_dir)

    if not year_dirs:
        logging.info(f"No year directories found in {base_dir}")
        return

    total_files_processed = 0

    for year_dir in year_dirs:
        log(f"Processing year directory: {year_dir}")
        files = create_file_list_recurse(year_dir, age_days=1)

        if len(files) == 0:
            # logging.info(f"No files to transcode in {year_dir}")
            continue

        log(f"Found {len(files)} files in {year_dir}")

        # Process files for this year
        transcode_list(files, dry_run=dry)

        # Clean up empty directories for this year only
        remove_empty(year_dir, dry_run=dry)

        total_files_processed += len(files)

    if total_files_processed == 0:
        log("No valid media files found in any year directory!")
    else:
        log(
            f"Finished processing {total_files_processed} files across {len(year_dirs)} year directories"
        )


def main():
    test()
    setup_logging()

    # cli parser stuff
    parser = argparse.ArgumentParser(
        description="Archive and organize ReoLink media files"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-t",
        "--transcode",
        action="store_true",
        help="Transcode only (no cleanup)",
    )
    group.add_argument(
        "-c", "--cleanup", action="store_true", help="Cleanup only (no transcode)"
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Do a dry run without making any changes to the disk",
    )
    args = parser.parse_args()

    if not any(vars(args).values()):
        transcode()
        organize()
    elif args.transcode:
        transcode(dry=args.dry_run)
    elif args.cleanup:
        organize(dry=args.dry_run)
    elif args.dry_run:
        transcode(dry=True)
        organize(dry=True)
    else:
        parser.print_help()
        return


if __name__ == "__main__":
    main()
