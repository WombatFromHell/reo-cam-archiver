#!/usr/bin/env python3
import argparse
import logging
import re
import subprocess
import sys
import time
import shlex
from datetime import datetime, timedelta
from pathlib import Path
import threading

TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)


class ConsoleOrchestrator:
    """Thread-safe lock for console output."""

    def __init__(self):
        self._lock = threading.RLock()

    def guard(self):
        return self._lock


class GuardedStreamHandler(logging.StreamHandler):
    """Log handler that preserves the progress bar line."""

    def __init__(self, orchestrator, stream=None, progress_bar=None):
        super().__init__(stream)
        self.orchestrator = orchestrator
        self.progress_bar = progress_bar

    def emit(self, record):
        msg = self.format(record) + self.terminator
        with self.orchestrator.guard():
            if self.progress_bar and self.progress_bar.has_progress:
                self.progress_bar._clear_area()
            self.stream.write(msg)
            self.flush()
            if self.progress_bar and self.progress_bar.has_progress:
                self.progress_bar.redraw()


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

    @property
    def has_progress(self):
        return bool(self._progress_line)

    def _is_tty(self):
        return hasattr(self.out, "isatty") and self.out.isatty()

    def start_processing(self):
        """Initialize the total processing start time."""
        if self.start_time is None:
            self.start_time = time.time()

    def finish(self):
        if not self.silent:
            self.out.write("\x1b[999B\x1b[2K\r\n")
            self.out.flush()

    def start_file(self):
        self.file_start = time.time()
        # Ensure start_time is set when first file starts
        if self.start_time is None:
            self.start_time = time.time()

    def update_progress(self, idx, pct=0.0):
        if self.silent:
            return
        line = self._format_line(idx, pct)
        if line == self._progress_line:
            return
        self._progress_line = line
        with self.orchestrator.guard():
            self._display(line)

    def finish_file(self, idx):
        self.update_progress(idx, 100.0)

    def _format_line(self, idx, pct):
        now = time.time()
        bar = f"[{'|' * int(pct / 100 * self.blocks)}{'-' * (self.blocks - int(pct / 100 * self.blocks))}]"
        elapsed_file = datetime.fromtimestamp(now - (self.file_start or now)).strftime(
            "%M:%S"
        )
        elapsed_total = datetime.fromtimestamp(now - (self.start_time or now)).strftime(
            "%M:%S"
        )
        return f"Progress [{idx}/{self.total}]: {pct:.0f}% {bar} {elapsed_file} ({elapsed_total})"

    def _clear_area(self):
        if not self._is_tty():
            return
        # Move cursor down one line, clear it, then move back up
        self.out.write("\x1b[1E\x1b[2K\x1b[1A")
        self.out.flush()

    def redraw(self):
        # Fixed logic: redraw when NOT silent and we have a progress line
        if self.silent or not self._progress_line:
            return
        with self.orchestrator.guard():
            self._display(self._progress_line)

    def _display(self, line):
        if not self._is_tty():
            now = time.time()
            if getattr(self, "_last_print_time", 0) < now - 5:
                self.out.write(f"\r{line}\n")
                self.out.flush()
                self._last_print_time = now
            return
        # For TTY: save cursor, clear line, write progress, restore cursor
        self.out.write(f"\x1b[s\x1b[2K\r{line}\x1b[u")
        self.out.flush()


def setup_logging(log_file, progress_bar=None):
    logger = logging.getLogger("camera_archiver")
    logger.setLevel(logging.INFO)

    for h in list(logger.handlers):
        try:
            h.close()
        except Exception:
            pass
        logger.removeHandler(h)

    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh)

    # Use the same stream as the progress bar to avoid conflicts
    stream = progress_bar.out if progress_bar else sys.stdout
    # Use the progress bar's orchestrator for proper coordination
    orch = progress_bar.orchestrator if progress_bar else ConsoleOrchestrator()
    sh = GuardedStreamHandler(orch, stream=stream, progress_bar=progress_bar)
    sh.setFormatter(logging.Formatter(fmt))
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    logger.propagate = False
    return logger


def parse_timestamp_from_filename(name):
    m = TIMESTAMP_RE.search(name)
    if not m:
        return None
    try:
        ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
        return ts if 2000 <= ts.year <= 2099 else None
    except ValueError:
        return None


def safe_remove(p, logger, dry_run):
    if dry_run:
        logger.info(f"[DRY RUN] Would remove {p}")
    else:
        try:
            p.unlink()
            logger.info(f"Removed: {p}")
        except Exception as e:
            logger.error(f"Failed to remove {p}: {e}")


def get_video_duration(inp):
    import shutil

    if not shutil.which("ffprobe"):
        return None
    try:
        cmd_str = (
            "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "
            f"{shlex.quote(str(inp))}"
        )
        cmd = shlex.split(cmd_str)
        out = subprocess.check_output(cmd, text=True)
        return float(out.strip())
    except Exception:
        return None


def transcode_file(inp, outp, logger, progress_cb=None):
    cmd_str = (
        f"ffmpeg -hide_banner -hwaccel qsv -hwaccel_output_format qsv -y "
        f"-i {shlex.quote(str(inp))} "
        f"-vf scale_qsv=w=1024:h=768:mode=hq "
        f"-global_quality 26 -c:v h264_qsv -an "
        f"{shlex.quote(str(outp))}"
    )
    cmd = shlex.split(cmd_str)
    # logger.info(f"Using the following transcode command: {cmd_str}")
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
    # Log ffmpeg output on failure
    if rc != 0:
        msg = f"FFmpeg failed (code {rc}) for {inp} -> {outp}\n" + "".join(log_lines)
        logger.error(msg)
    return rc == 0


def scan_files(base_dir):
    mp4s = []
    mapping = {}
    for p in base_dir.rglob("*.*"):
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
    return mp4s, mapping


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


def process_files(old_list, out_dir, logger, dry_run, no_skip, mapping, bar):
    logger.info(f"Found {len(old_list)} files to process")
    if not old_list:
        logger.info("No files to process")
        return set()

    # Initialize the progress bar's total timer
    bar.start_processing()

    processed = set()
    for idx, (fp, ts) in enumerate(old_list, 1):
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
            safe_remove(fp, logger, dry_run)
            if jpg and jpg.exists():
                safe_remove(jpg, logger, dry_run)
                processed.add(jpg)
            continue
        bar.start_file()
        logger.info(f"Transcoding {fp}->{outp}")
        ok = transcode_file(fp, outp, logger, lambda pct: bar.update_progress(idx, pct))
        if ok:
            bar.finish_file(idx)
            safe_remove(fp, logger, dry_run)
            if jpg and jpg.exists():
                safe_remove(jpg, logger, dry_run)
                processed.add(jpg)
        else:
            logger.error(f"Transcoding failed: {fp}")
            if jpg:
                logger.info(f"Keeping paired JPG due to transcoding failure: {jpg}")
            break
    bar.finish()
    return processed


def remove_orphaned_jpgs(base_dir, mapping, processed, logger, dry_run):
    count = 0
    for key, files in mapping.items():
        jpg = files.get(".jpg")
        mp4 = files.get(".mp4")
        if not jpg or jpg in processed:
            continue
        if not mp4:
            if dry_run:
                logger.info(f"[DRY RUN] Found orphaned JPG (no MP4 pair): {jpg}")
            else:
                logger.info(f"Found orphaned JPG (no MP4 pair): {jpg}")
            safe_remove(jpg, logger, dry_run)
            count += 1
    if dry_run:
        logger.info(f"[DRY RUN] Would remove {count} orphaned JPG files")
    else:
        logger.info(f"Removed {count} orphaned JPG files")


def main():
    parser = argparse.ArgumentParser(
        description="Archive and transcode camera files based on timestamp parsing"
    )
    parser.add_argument(
        "--directory", "-d", type=Path, default=Path.cwd(), help="Input directory"
    )
    parser.add_argument(
        "--output", "-o", type=Path, help="Destination for archived MP4s"
    )
    parser.add_argument("--age", "-a", type=int, default=30, help="Age in days")
    parser.add_argument(
        "--dry-run", "-n", action="store_true", help="Show actions only"
    )
    parser.add_argument(
        "--max-size",
        "-m",
        type=int,
        default=500,
        help="Maximum archive size in GB (used only when not dry-run)",
    )
    parser.add_argument(
        "--no-skip",
        "-s",
        action="store_true",
        help="Do not skip transcoding when archived copy exists",
    )
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    base_dir = args.directory if args.directory.exists() else Path("/camera")
    if not base_dir.exists():
        print(
            f"Error: Directory {args.directory} does not exist and /camera is missing"
        )
        sys.exit(1)
    out_dir = args.output or (base_dir / "archived")
    if base_dir == Path("/camera"):
        out_dir = Path("/camera/archived")

    mp4s, mapping = scan_files(base_dir)
    cutoff = datetime.now() - timedelta(days=args.age)
    old_list = [(p, t) for p, t in mp4s if t < cutoff]
    bar = ProgressBar(total_files=len(old_list), silent=args.dry_run, out=sys.stderr)
    logger = setup_logging(base_dir / "transcoding.log", progress_bar=bar)

    for msg in [
        "Starting camera archive process...",
        f"Input: {base_dir}",
        f"Output: {out_dir}",
        f"Age threshold: {args.age} days",
        f"Size limit: {args.max_size} GB",
        f"Dry run: {args.dry_run}",
    ]:
        logger.info(msg)

    processed = process_files(
        old_list, out_dir, logger, args.dry_run, args.no_skip, mapping, bar
    )
    remove_orphaned_jpgs(base_dir, mapping, processed, logger, args.dry_run)

    if not args.dry_run:
        archive_files = list(out_dir.rglob("archived-*.mp4"))
        if archive_files:
            cur_size = sum(p.stat().st_size for p in archive_files if p.is_file())
            limit = args.max_size * (1024**3)
            logger.info(f"Current archive size: {cur_size / (1024**3):.1f} GB")
            if cur_size > limit:
                logger.info(
                    f"Archive size exceeds limit ({args.max_size} GB), removing oldest files..."
                )
                for f in sorted(archive_files, key=lambda p: p.stat().st_mtime):
                    sz = f.stat().st_size
                    f.unlink()
                    cur_size -= sz
                    logger.info(f"Removed old archive: {f}")
                    if cur_size <= limit:
                        break
                logger.info(f"Final archive size: {cur_size / (1024**3):.1f} GB")
    else:
        logger.info(
            f"[DRY RUN] Would check archive size limit ({args.max_size} GB) and remove old files if needed"
        )

    if args.dry_run:
        logger.info("[DRY RUN] Done - no files were actually modified")
    else:
        logger.info("Archive process completed successfully")


if __name__ == "__main__":
    main()
