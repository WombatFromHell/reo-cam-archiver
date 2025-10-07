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
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Protocol

# Constants
MIN_ARCHIVE_SIZE_BYTES = 1_048_576  # 1MB
DEFAULT_PROGRESS_WIDTH = 30
PROGRESS_UPDATE_INTERVAL = 5  # seconds for non-TTY output


# 1. State Enum
class State(Enum):
    INITIALIZATION = "initialization"
    DISCOVERY = "discovery"
    PLANNING = "planning"
    EXECUTION = "execution"
    CLEANUP = "cleanup"
    ARCHIVE_CLEANUP = "archive_cleanup"
    TERMINATION = "termination"


# 2. StateHandler Protocol
class StateHandler(Protocol):
    def enter(self, context: "Context") -> None: ...
    def execute(self, context: "Context") -> State: ...
    def exit(self, context: "Context") -> None: ...


# 3. GracefulExit Helper
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


# 4. FileInfo Class
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


# 5. Context Class
class Context:
    """Manages application state and transitions"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.current_state = State.INITIALIZATION
        self.state_data: Dict[str, Any] = {}
        self.graceful_exit = GracefulExit()
        self.orchestrator = (
            ConsoleOrchestrator()
        )  # Shared orchestrator for coordination
        self.services = self._initialize_services()
        self.logger: Optional[logging.Logger] = None
        self.progress_bar: Optional["ProgressReporter"] = None

    def _initialize_services(self) -> Dict[str, Any]:
        """Initialize all services"""
        return {
            "camera_service": CameraService(self.config),
            "storage_service": StorageService(self.config),
            "logging_service": LoggingService(self.config),
            "transcoder_service": TranscoderService(self.config),
            "file_service": FileService(self.config),
        }

    def transition_to(self, new_state: State) -> None:
        """Handle state transitions"""
        if self.current_state != new_state:
            old_state = self.current_state
            self.current_state = new_state
            if self.logger:
                self.logger.info(
                    f"State transition: {old_state.value} -> {new_state.value}"
                )


# 6. Service Classes
class CameraService:
    """Handles camera-related operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def discover_files(
        self, directory: Path
    ) -> Tuple[List[Tuple[Path, datetime]], Dict[str, Dict[str, Path]], Set[Path]]:
        """Discover camera files with valid timestamps in the correct directory structure"""
        mp4s: List[Tuple[Path, datetime]] = []
        mapping: Dict[str, Dict[str, Path]] = {}
        trash_files: Set[Path] = set()
        trash_root = self.config.get("trash_root")

        # Scan base directory
        for p in directory.rglob("*.*"):
            if not p.is_file():
                continue

            # Skip files in trash directory when scanning base directory
            if trash_root and trash_root in p.parents:
                continue

            # Check if the file is in the correct directory structure: /camera/<YYYY>/<MM>/<DD>/*.*
            # We need at least 4 parts: YYYY/MM/DD/filename from the base directory
            try:
                # Get the path relative to the base directory
                rel_parts = p.relative_to(directory).parts
                if len(rel_parts) >= 4:
                    y, m, d = (
                        rel_parts[-4],
                        rel_parts[-3],
                        rel_parts[-2],
                    )  # YYYY/MM/DD are the 4th, 3rd, and 2nd to last parts
                    # Check if the parent directories match YYYY/MM/DD pattern
                    int(y)
                    int(m)
                    int(d)
                    # Validate these are valid date components
                    y_int = int(y)
                    m_int = int(m)
                    d_int = int(d)
                    if (
                        y_int < 1000
                        or y_int > 9999
                        or m_int < 1
                        or m_int > 12
                        or d_int < 1
                        or d_int > 31
                    ):
                        continue  # Not a valid date structure
                else:
                    continue  # Not deep enough in the directory structure
            except (ValueError, AttributeError):
                continue  # Not a valid date structure

            try:
                ts = self._parse_timestamp(p.name)
                if not ts:
                    continue

                key = ts.strftime("%Y%m%d%H%M%S")
                ext = p.suffix.lower()
                mapping.setdefault(key, {})[ext] = p
                if ext == ".mp4":
                    mp4s.append((p, ts))
            except Exception:
                # Skip files that cause errors during processing
                continue

        # Scan trash directory if enabled
        if trash_root and trash_root.exists():
            for trash_type in ["input", "output"]:
                trash_dir = trash_root / trash_type
                if trash_dir.exists():
                    for p in trash_dir.rglob("*.*"):
                        if not p.is_file():
                            continue

                        ts = self._parse_timestamp(p.name)
                        if not ts:
                            continue

                        key = ts.strftime("%Y%m%d%H%M%S")
                        ext = p.suffix.lower()
                        mapping.setdefault(key, {})[ext] = p
                        trash_files.add(p)
                        if ext == ".mp4":
                            mp4s.append((p, ts))

        return mp4s, mapping, trash_files

    def _parse_timestamp(self, filename: str) -> Optional[datetime]:
        """Extract timestamp from filename"""
        TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = TIMESTAMP_RE.search(filename)
        if not m:
            return None
        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None


class StorageService:
    """Handles storage-related operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def check_storage(self) -> Dict[str, Any]:
        """Check storage availability and status"""
        directory = self.config.get("directory", Path("/camera"))
        output = self.config.get("output", directory / "archived")

        return {
            "input_dir_exists": directory.exists(),
            "output_dir_exists": output.exists(),
            "input_space": self._get_free_space(directory),
            "output_space": self._get_free_space(output),
        }

    def _get_free_space(self, path: Path) -> int:
        """Get free space in bytes"""
        try:
            stat = shutil.disk_usage(path)
            return stat.free
        except Exception:
            return 0


class LoggingService:
    """Handles logging setup and operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def setup_logging(
        self,
        progress_bar: Optional["ProgressReporter"] = None,
        orchestrator: Optional["ConsoleOrchestrator"] = None,
    ) -> logging.Logger:
        """Setup logging with optional progress bar"""
        logger = logging.getLogger("camera_archiver")
        logger.setLevel(logging.INFO)

        # Clear existing handlers
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

        fmt = "%(asctime)s - %(levelname)s - %(message)s"

        # File handler
        log_file = self.config.get("log_file")
        if log_file:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter(fmt))
            logger.addHandler(fh)

        # Stream handler
        stream = sys.stderr  # Use stderr consistently for logging and progress
        # Use provided orchestrator, or the one from progress bar, or create a new one
        orch = orchestrator or (
            progress_bar.orchestrator if progress_bar else ConsoleOrchestrator()
        )
        sh = GuardedStreamHandler(orch, stream=stream, progress_bar=progress_bar)
        sh.setFormatter(logging.Formatter(fmt))
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)

        logger.propagate = False
        return logger


class TranscoderService:
    """Handles video transcoding operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def get_video_duration(self, file_path: Path) -> Optional[float]:
        """Get video duration using ffprobe"""
        if not shutil.which("ffprobe"):
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
        self,
        input_path: Path,
        output_path: Path,
        logger: logging.Logger,
        progress_cb: Optional[Callable[[float], None]] = None,
        graceful_exit: Optional[GracefulExit] = None,
    ) -> bool:
        """Transcode a video file using ffmpeg with QSV hardware acceleration."""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return False

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

        # Get video duration
        total_duration = self.get_video_duration(input_path)
        debug_ffmpeg = logger.isEnabledFor(logging.DEBUG)

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

                        if debug_ffmpeg:
                            logger.debug(f"FFmpeg output: {line.strip()}")

                        log_lines.append(line)

                        if total_duration and total_duration > 0:
                            time_match = re.search(r"time=([0-9:.]+)", line)
                            if time_match:
                                time_str = time_match.group(1)
                                if ":" in time_str:
                                    # HH:MM:SS format
                                    h, mn, s = map(float, time_str.split(":")[:3])
                                    elapsed_seconds = h * 3600 + mn * 60 + s
                                else:
                                    # Seconds format
                                    elapsed_seconds = float(time_str)
                                cur_pct = min(
                                    elapsed_seconds / total_duration * 100, 100.0
                                )
                        else:
                            cur_pct = min(cur_pct + 0.5, 99.0)

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


class FileService:
    """Handles file operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def remove_file(
        self,
        file_path: Path,
        logger: logging.Logger,
        dry_run: bool = False,
        use_trash: bool = False,
        trash_root: Optional[Path] = None,
        is_output: bool = False,
        source_root: Optional[Path] = None,
    ) -> None:
        """Remove a file, optionally moving to trash"""
        if dry_run:
            logger.info(f"[DRY RUN] Would remove {file_path}")
            return

        try:
            if source_root is None:
                source_root = file_path.parent

            # Use config trash_root if not provided explicitly
            if trash_root is None:
                trash_root = self.config.get("trash_root")

            if use_trash and trash_root:
                new_dest = self._calculate_trash_destination(
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

    def _calculate_trash_destination(
        self,
        file_path: Path,
        source_root: Path,
        trash_root: Path,
        is_output: bool = False,
    ) -> Path:
        """Calculate the destination path in trash for a given file"""
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


# 7. Progress Reporting
class ConsoleOrchestrator:
    """Thread-safe lock for console output"""

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
                # Wait for any ongoing progress updates to complete to avoid clobbering
                import time

                max_wait = 0.1  # 100ms max wait
                start_time = time.time()
                while self.progress_bar._is_updating and (
                    time.time() - start_time < max_wait
                ):
                    time.sleep(0.001)  # 1ms sleep

                # Clear the current progress line, write the log message, and don't redraw immediately
                try:
                    # Clear the current progress line
                    self.stream.write("\r\x1b[2K")  # Clear current line
                    # Write the log message
                    self.stream.write(msg)
                    # Add a newline to separate from progress bar if the message didn't end with one
                    if not msg.endswith("\n"):
                        self.stream.write("\n")
                    self.stream.flush()
                    # The progress bar will update naturally via progress updates,
                    # so we don't redraw it here to avoid interference
                except Exception:
                    # Fallback: just write the message
                    self.stream.write(msg)
                    self.stream.flush()
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
        self.start_time: Optional[float] = None
        self.file_start: Optional[float] = None
        self._progress_line = ""
        self._last_print_time = time.time()
        self._finished = False
        self._is_updating = (
            False  # Flag to indicate if we're currently updating the progress
        )
        self._original_signal_handlers: Dict[int, Any] = {}
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
        self._finished = True

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

        # Set the updating flag to coordinate with logging
        self._is_updating = True
        try:
            line = self._format_line(idx, pct)
            if line == self._progress_line:
                return

            self._progress_line = line
            self._display(line)
        finally:
            self._is_updating = False

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

    def finish_current_line(self):
        """Finish the current progress line with a newline so logs can appear cleanly"""
        if self._progress_line and not self.silent:
            try:
                # Add a newline after clearing the line to move to the next line
                self.out.write("\r\x1b[2K\n")
                self.out.flush()
            except Exception:
                pass

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
            # Clear the current line and write the new progress line
            self.out.write(f"\r\x1b[2K{line}")
            self.out.flush()
        except Exception:
            try:
                # Fallback for terminals that don't support ANSI escape codes
                self.out.write(f"\r{line}")
                self.out.flush()
            except Exception:
                # If both write attempts fail, silently ignore to prevent cascading errors
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


# 8. State Handlers
class InitializationHandler:
    """Handles initialization state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering initialization state")

    def execute(self, context: Context) -> State:
        # Setup logging with shared orchestrator but no progress bar initially
        context.progress_bar = None
        context.logger = context.services["logging_service"].setup_logging(
            context.progress_bar, context.orchestrator
        )

        # Check storage
        storage_status = context.services["storage_service"].check_storage()
        if not storage_status["input_dir_exists"]:
            if context.logger:
                context.logger.error(
                    f"Input directory does not exist: {context.config.get('directory')}"
                )
            return State.TERMINATION

        # Create output directory if needed
        output_dir = context.config.get("output")
        if output_dir and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            if context.logger:
                context.logger.info(f"Created output directory: {output_dir}")

        return State.DISCOVERY

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting initialization state")


class DiscoveryHandler:
    """Handles discovery state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering discovery state")

    def execute(self, context: Context) -> State:
        directory = context.config.get("directory")
        if not isinstance(directory, Path):
            if context.logger:
                context.logger.error("Invalid directory configuration")
            return State.TERMINATION

        mp4s, mapping, trash_files = context.services["camera_service"].discover_files(
            directory
        )

        context.state_data["mp4s"] = mp4s
        context.state_data["mapping"] = mapping
        context.state_data["trash_files"] = trash_files

        if context.logger:
            context.logger.info(f"Discovered {len(mp4s)} MP4 files")

        return State.PLANNING

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting discovery state")


class PlanningHandler:
    """Handles planning state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering planning state")

    def execute(self, context: Context) -> State:
        mp4s = context.state_data.get("mp4s", [])
        mapping = context.state_data.get("mapping", {})

        # Generate action plan
        plan = self._generate_action_plan(context, mp4s, mapping)
        context.state_data["plan"] = plan

        # Display plan
        self._display_plan(context, plan)

        # If in dry-run mode, exit immediately after displaying the plan
        if context.config.get("dry_run", False):
            if context.logger:
                context.logger.info(
                    "Dry run completed - no transcoding or removals performed"
                )
            return State.TERMINATION

        # Check if we should go directly to archive cleanup
        if context.config.get("cleanup") and not plan["transcoding"]:
            return State.ARCHIVE_CLEANUP

        # Ask for confirmation if needed
        if not context.config.get("no_confirm", False):
            confirm = self._ask_confirmation(
                "Proceed with transcoding and file removals?", default=False
            )
            if not confirm:
                if context.logger:
                    context.logger.info("Operation cancelled by user")
                return State.TERMINATION

        return State.EXECUTION

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting planning state")

    def _generate_action_plan(
        self,
        context: Context,
        mp4s: List[Tuple[Path, datetime]],
        mapping: Dict[str, Dict[str, Path]],
    ) -> Dict[str, List]:
        """Generate a plan of all actions to be performed"""
        transcoding_actions: List[Dict[str, Any]] = []
        removal_actions: List[Dict[str, Any]] = []

        # Calculate age cutoff if cleanup is enabled
        age_cutoff = None
        # Calculate age cutoff - apply to all files being processed, not just cleanup
        age_cutoff = None
        if context.config.get("age", 30) > 0:
            age_cutoff = datetime.now() - timedelta(days=context.config.get("age", 30))

        for fp, ts in mp4s:
            if fp in context.state_data.get("trash_files", set()):
                continue

            outp = self._output_path(context, fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            # Skip files newer than age cutoff (files must be at least age_days old to be processed)
            if age_cutoff and ts >= age_cutoff:
                if context.logger:
                    context.logger.debug(
                        f"Skipping {fp}: timestamp {ts} is newer than age cutoff {age_cutoff}"
                    )
                continue

            # Check skip logic
            if (
                not context.config.get("no_skip", False)
                and outp.exists()
                and outp.stat().st_size > MIN_ARCHIVE_SIZE_BYTES
            ):
                # Archive exists, skip transcoding but mark for removal
                removal_actions.append(
                    {
                        "type": "source_removal_after_skip",
                        "file": fp,
                        "reason": f"Skipping transcoding: archive exists at {outp}",
                    }
                )
                if jpg:
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_skip",
                            "file": jpg,
                            "reason": "Skipping transcoding: archive exists for paired MP4",
                        }
                    )
            else:
                # Will transcode the file
                transcoding_actions.append(
                    {
                        "type": "transcode",
                        "input": fp,
                        "output": outp,
                        "jpg_to_remove": jpg,
                    }
                )
                # Add source MP4 for removal after successful transcoding
                removal_actions.append(
                    {
                        "type": "source_removal_after_transcode",
                        "file": fp,
                        "reason": f"Source file for transcoded archive at {outp}",
                    }
                )
                if jpg:
                    removal_actions.append(
                        {
                            "type": "jpg_removal_after_transcode",
                            "file": jpg,
                            "reason": "Paired with transcoded MP4",
                        }
                    )

        return {
            "transcoding": transcoding_actions,
            "removals": removal_actions,
        }

    def _display_plan(self, context: Context, plan: Dict[str, List]) -> None:
        """Display the action plan to the user"""
        if not context.logger:
            return

        logger = context.logger
        logger.info("=== ACTION PLAN ===")
        logger.info(f"Transcoding {len(plan['transcoding'])} files:")
        for i, action in enumerate(plan["transcoding"], 1):
            logger.info(f"  {i}. {action['input']} -> {action['output']}")
            if action["jpg_to_remove"]:
                logger.info(f"      + Removing paired JPG: {action['jpg_to_remove']}")

        logger.info(f"Removing {len(plan['removals'])} files:")
        for i, action in enumerate(plan["removals"], 1):
            logger.info(f"  {i}. {action['file']} - {action['reason']}")

        # Display age cutoff information if cleanup is enabled
        if context.config.get("cleanup", False):
            age_days = context.config.get("age", 30)
            age_cutoff = datetime.now() - timedelta(days=age_days)
            logger.info(
                f"Cleanup enabled: Files older than {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')} will be removed based on age threshold of {age_days} days"
            )
            if context.config.get("clean_output", False):
                logger.info(
                    "Cleanup scope: Source files, archive files, and trash files"
                )
            else:
                logger.info(
                    "Cleanup scope: Source files and trash files (archive files excluded)"
                )

        logger.info("=== END PLAN ===")

    def _ask_confirmation(self, message: str, default: bool = False) -> bool:
        """Ask for user confirmation"""
        suffix = " [Y/n]" if default else " [y/N]"
        try:
            response = input(f"{message}{suffix}: ").strip().lower()
            if not response:
                return default
            return response in ("y", "yes")
        except KeyboardInterrupt:
            return False

    def _output_path(
        self, context: Context, input_file: Path, timestamp: datetime
    ) -> Path:
        """Generate output path for archived file"""
        out_dir = context.config.get("output")
        if not isinstance(out_dir, Path):
            raise ValueError("Output directory is not properly configured")

        # Use consistent timestamp-based directory structure regardless of input path
        return (
            out_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )


class ExecutionHandler:
    """Handles execution state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering execution state")
        plan = context.state_data.get("plan", {})
        transcoding_actions = plan.get("transcoding", [])

        # Setup progress bar with the shared orchestrator
        context.progress_bar = ProgressReporter(
            total_files=len(transcoding_actions),
            graceful_exit=context.graceful_exit,
            silent=context.config.get("dry_run", False),
            out=sys.stderr,
        )
        # Set the progress bar's orchestrator to the shared one
        context.progress_bar.orchestrator = context.orchestrator

        # Update the logger with the progress bar for coordination
        context.logger = context.services["logging_service"].setup_logging(
            context.progress_bar, context.orchestrator
        )

        context.progress_bar.start_processing()

    def execute(self, context: Context) -> State:
        plan = context.state_data.get("plan", {})
        transcoding_actions = plan.get("transcoding", [])
        removal_actions = plan.get("removals", [])

        # Execute transcoding actions and immediate removals
        for i, action in enumerate(transcoding_actions, 1):
            if context.graceful_exit.should_exit():
                break

            input_path = action["input"]
            output_path = action["output"]

            # Log before starting file to avoid disrupting progress updates
            if context.logger:
                context.logger.info(f"Processing {input_path}")

            if context.progress_bar:
                context.progress_bar.start_file()

            # Create a progress callback that updates the progress bar
            def progress_callback(pct):
                if context.progress_bar and not context.graceful_exit.should_exit():
                    context.progress_bar.update_progress(i, pct)

            # Transcode file with the progress callback
            if context.logger:
                success = context.services["transcoder_service"].transcode_file(
                    input_path,
                    output_path,
                    context.logger,
                    progress_callback,
                    graceful_exit=context.graceful_exit,
                )
            else:
                success = False

            if success:
                if context.progress_bar:
                    context.progress_bar.finish_file(i)
                if context.logger:
                    context.logger.info(
                        f"Successfully transcoded {input_path} -> {output_path}"
                    )

                # Remove paired JPG if exists
                jpg = action.get("jpg_to_remove")
                if jpg and context.logger:
                    context.services["file_service"].remove_file(
                        jpg,
                        context.logger,
                        dry_run=context.config.get("dry_run", False),
                        use_trash=context.config.get("use_trash", False),
                        trash_root=context.config.get("trash_root"),
                        is_output=False,
                        source_root=jpg.parent,
                    )

                # Remove source file after successful transcoding
                # Find and execute the corresponding source removal action
                source_removal_action = None
                for removal_action in removal_actions:
                    if (
                        removal_action.get("type") == "source_removal_after_transcode"
                        and removal_action["file"] == input_path
                    ):
                        source_removal_action = removal_action
                        break

                if source_removal_action:
                    context.services["file_service"].remove_file(
                        source_removal_action["file"],
                        context.logger,
                        dry_run=context.config.get("dry_run", False),
                        use_trash=context.config.get("use_trash", False),
                        trash_root=context.config.get("trash_root"),
                        is_output=False,
                        source_root=source_removal_action["file"].parent,
                    )
                    # Remove this action from the list so it's not processed again later
                    removal_actions.remove(source_removal_action)
            else:
                if context.logger:
                    context.logger.error(f"Failed to transcode {input_path}")
                continue

        # Execute any remaining removal actions that weren't handled above
        for action in removal_actions:
            if context.graceful_exit.should_exit():
                break

            file_path = action["file"]
            if context.logger:
                context.services["file_service"].remove_file(
                    file_path,
                    context.logger,
                    dry_run=context.config.get("dry_run", False),
                    use_trash=context.config.get("use_trash", False),
                    trash_root=context.config.get("trash_root"),
                    is_output=False,
                    source_root=file_path.parent,
                )

        # Check if cleanup is requested
        if context.config.get("cleanup"):
            return State.ARCHIVE_CLEANUP

        return State.CLEANUP

    def exit(self, context: Context) -> None:
        if context.progress_bar:
            context.progress_bar.finish()
        if context.logger:
            context.logger.info("Exiting execution state")


class CleanupHandler:
    """Handles cleanup state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering cleanup state")

    def execute(self, context: Context) -> State:
        # Remove orphaned JPGs
        mapping = context.state_data.get("mapping", {})
        processed: Set[Path] = set()

        for action in context.state_data.get("plan", {}).get("transcoding", []):
            processed.add(action["input"])

        self._remove_orphaned_jpgs(context, mapping, processed)

        # Clean empty directories
        self._clean_empty_directories(context)

        return State.TERMINATION

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting cleanup state")

    def _remove_orphaned_jpgs(
        self,
        context: Context,
        mapping: Dict[str, Dict[str, Path]],
        processed: Set[Path],
    ) -> None:
        """Remove JPG files without corresponding MP4 files"""
        count = 0
        for key, files in mapping.items():
            if context.graceful_exit.should_exit():
                break

            jpg = files.get(".jpg")
            mp4 = files.get(".mp4")
            if not jpg or jpg in processed:
                continue
            if mp4:
                continue

            if context.logger:
                context.logger.info(f"Found orphaned JPG (no MP4 pair): {jpg}")
                context.services["file_service"].remove_file(
                    jpg,
                    context.logger,
                    dry_run=context.config.get("dry_run", False),
                    use_trash=context.config.get("use_trash", False),
                    trash_root=context.config.get("trash_root"),
                    is_output=False,
                    source_root=jpg.parent,
                )
            count += 1

        if not context.graceful_exit.should_exit() and context.logger:
            context.logger.info(f"Removed {count} orphaned JPG files")

    def _clean_empty_directories(self, context: Context) -> None:
        """Remove empty date-structured directories"""
        directory = context.config.get("directory")
        trash_root = context.config.get("trash_root")

        if not isinstance(directory, Path):
            if context.logger:
                context.logger.error("Invalid directory configuration")
            return

        for dirpath, dirs, files in os.walk(directory, topdown=False):
            if context.graceful_exit.should_exit():
                break

            p = Path(dirpath)
            if p == directory:
                continue

            # Only clean directories with exactly 3 parts (year/month/day structure)
            try:
                rel_parts = p.relative_to(directory).parts
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

            # Only remove if directory is actually empty
            if not files and not dirs:
                try:
                    if context.config.get("use_trash", False) and trash_root:
                        new_dest = context.services[
                            "file_service"
                        ]._calculate_trash_destination(
                            p, directory, trash_root, is_output=False
                        )
                        new_dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(p), str(new_dest))
                        if context.logger:
                            context.logger.info(
                                f"Moved empty directory to trash: {p} -> {new_dest}"
                            )
                    else:
                        p.rmdir()
                        if context.logger:
                            context.logger.info(f"Removed empty directory: {p}")
                except Exception as e:
                    if context.logger:
                        context.logger.error(
                            f"Failed to remove empty directory {p}: {e}"
                        )


class ArchiveCleanupHandler:
    """Handles intelligent cleanup of the entire archive"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering archive cleanup state")

    def execute(self, context: Context) -> State:
        # Collect all file information
        all_file_infos = self._collect_all_archive_files(context)

        if not all_file_infos:
            if context.logger:
                context.logger.info("No files found for cleanup")
            return State.TERMINATION

        # Run intelligent cleanup
        files_to_remove = self._intelligent_cleanup(context, all_file_infos)

        # Remove the files
        for file_info in files_to_remove:
            if context.graceful_exit.should_exit():
                break

            if context.logger:
                context.services["file_service"].remove_file(
                    file_info.path,
                    context.logger,
                    dry_run=context.config.get("dry_run", False),
                    use_trash=context.config.get("use_trash", False),
                    trash_root=context.config.get("trash_root"),
                    is_output=file_info.is_archive,
                    source_root=context.config.get("output")
                    if file_info.is_archive
                    else context.config.get("directory"),
                )

        return State.CLEANUP

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting archive cleanup state")

    def _collect_all_archive_files(self, context: Context) -> List[FileInfo]:
        """Collect file information for all relevant files"""
        all_files: List[FileInfo] = []
        seen_paths: Set[Path] = set()
        trash_root = context.config.get("trash_root")

        # Get all archive files
        out_dir = context.config.get("output")
        if isinstance(out_dir, Path) and out_dir.exists():
            try:
                archive_files = list(out_dir.rglob("archived-*.mp4"))
            except (OSError, IOError) as e:
                archive_files = []
                if context.logger:
                    context.logger.error(
                        f"Failed to scan archive directory {out_dir}: {e}"
                    )
        else:
            archive_files = []

        # Process archive files
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
                        if trash_root
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

        # Process source files
        mp4s = context.state_data.get("mp4s", [])
        for fp, ts in mp4s:
            if fp in seen_paths:
                continue

            try:
                if not fp.is_file():
                    continue
                size = fp.stat().st_size
            except (OSError, IOError):
                continue

            is_trash = trash_root is not None and any(
                p in fp.parents
                for p in [trash_root, trash_root / "input"]
                if trash_root
            )
            all_files.append(
                FileInfo(fp, ts, size, is_archive=False, is_trash=is_trash)
            )
            seen_paths.add(fp)

        return all_files

    def _intelligent_cleanup(
        self, context: Context, all_files: List[FileInfo]
    ) -> List[FileInfo]:
        """Select files to remove based on location priority and size/age constraints"""
        if not all_files:
            return []

        # Calculate totals
        total_size = sum(f.size for f in all_files)
        size_limit = context.config.get("max_size", 500) * (1024**3)
        age_cutoff = datetime.now() - timedelta(days=context.config.get("age", 30))

        if context.logger:
            context.logger.info(f"Current total size: {total_size / (1024**3):.1f} GB")
            context.logger.info(f"Size limit: {context.config.get('max_size', 500)} GB")
            context.logger.info(
                f"Age cutoff: {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            if not context.config.get("clean_output", False):
                context.logger.info("Output files excluded from age-based cleanup")

        # Categorize files by location priority
        categorized_files = self._categorize_files(all_files)

        files_to_remove: List[FileInfo] = []
        remaining_size = total_size

        # PHASE 1: Enforce size limit (if over limit)
        if remaining_size > size_limit:
            files_to_remove, remaining_size = self._apply_size_cleanup(
                context, categorized_files, total_size, size_limit
            )
        else:
            # PHASE 2: Enforce age limit (only if under size limit AND age_days > 0)
            if context.config.get("age", 30) > 0:
                files_to_remove, remaining_size = self._apply_age_cleanup(
                    context, categorized_files, age_cutoff, total_size
                )
            else:
                if context.logger:
                    context.logger.info("Age-based cleanup disabled (age_days <= 0)")

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

        if context.logger:
            context.logger.info(
                f"Final removal plan: {len(files_to_remove)} files, "
                f"final size: {remaining_size / (1024**3):.1f} GB"
            )

        return files_to_remove

    def _categorize_files(self, all_files: List[FileInfo]) -> Dict[int, List[FileInfo]]:
        """Categorize files by location priority (0 = trash, 1 = archive, 2 = source)"""
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
        context: Context,
        categorized_files: Dict[int, List[FileInfo]],
        total_size: int,
        size_limit: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply size-based cleanup based on priority"""
        files_to_remove: List[FileInfo] = []
        remaining_size = total_size

        if context.logger:
            context.logger.info("Archive size exceeds limit")
            context.logger.info(
                f"Size threshold exceeded, removing files by priority to reach {context.config.get('max_size', 500)} GB..."
            )
            context.logger.info(
                "Priority order: Trash > Archive > Source (oldest first within each)"
            )

        # Remove files starting from highest priority (0) to lowest (2)
        for priority in range(3):
            if remaining_size <= size_limit:
                break

            category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
            category_files = categorized_files[priority]

            if category_files:
                if context.logger:
                    context.logger.info(
                        f"Processing {category_name} files for size cleanup..."
                    )

                for file_info in category_files:
                    if remaining_size <= size_limit:
                        break

                    files_to_remove.append(file_info)
                    remaining_size -= file_info.size

                    if context.config.get("dry_run", False) and context.logger:
                        context.logger.info(
                            f"[DRY RUN] Would remove {category_name} file for size: {file_info.path} "
                            f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                        )

        if context.logger:
            context.logger.info(
                f"After size cleanup: {remaining_size / (1024**3):.1f} GB "
                f"({len(files_to_remove)} files marked for removal)"
            )

        return files_to_remove, remaining_size

    def _apply_age_cleanup(
        self,
        context: Context,
        categorized_files: Dict[int, List[FileInfo]],
        age_cutoff: datetime,
        total_size: int,
    ) -> Tuple[List[FileInfo], int]:
        """Apply age-based cleanup respecting clean_output setting"""
        files_to_remove: List[FileInfo] = []
        remaining_size = total_size

        files_over_age_by_priority: Dict[int, List[FileInfo]] = {0: [], 1: [], 2: []}

        for priority in range(3):
            # Skip archive files (priority 1) if clean_output is False
            if priority == 1 and not context.config.get("clean_output", False):
                continue

            files_over_age_by_priority[priority] = [
                f for f in categorized_files[priority] if f.timestamp < age_cutoff
            ]

        total_over_age = sum(
            len(files) for files in files_over_age_by_priority.values()
        )

        if total_over_age > 0:
            if context.logger:
                context.logger.info(
                    f"Found {total_over_age} files older than {context.config.get('age', 30)} days"
                )

            # Remove age-eligible files by priority order
            for priority in range(3):
                # Skip archive files (priority 1) if clean_output is False
                if priority == 1 and not context.config.get("clean_output", False):
                    continue

                category_name = {0: "Trash", 1: "Archive", 2: "Source"}[priority]
                age_files = files_over_age_by_priority[priority]

                if age_files:
                    if context.logger:
                        context.logger.info(
                            f"Processing {category_name} files for age cleanup..."
                        )

                    for file_info in age_files:
                        files_to_remove.append(file_info)
                        remaining_size -= file_info.size

                        if context.config.get("dry_run", False) and context.logger:
                            context.logger.info(
                                f"[DRY RUN] Would remove {category_name} file for age: {file_info.path} "
                                f"({file_info.size / (1024**2):.1f} MB, {file_info.timestamp})"
                            )

            if context.logger:
                context.logger.info(
                    f"Added {total_over_age} files for age-based removal"
                )
        else:
            if context.logger:
                context.logger.info(
                    f"No files older than {context.config.get('age', 30)} days found"
                )

        return files_to_remove, remaining_size


class TerminationHandler:
    """Handles termination state"""

    def enter(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Entering termination state")

    def execute(self, context: Context) -> State:
        if context.logger:
            context.logger.info("Camera archiver completed")
        return State.TERMINATION  # Stay in termination state

    def exit(self, context: Context) -> None:
        if context.logger:
            context.logger.info("Exiting termination state")


# 9. StateHandlerFactory
class StateHandlerFactory:
    """Creates appropriate state handlers"""

    @staticmethod
    def create_handler(state: Any) -> StateHandler:
        handlers = {
            State.INITIALIZATION: InitializationHandler(),
            State.DISCOVERY: DiscoveryHandler(),
            State.PLANNING: PlanningHandler(),
            State.EXECUTION: ExecutionHandler(),
            State.CLEANUP: CleanupHandler(),
            State.ARCHIVE_CLEANUP: ArchiveCleanupHandler(),
            State.TERMINATION: TerminationHandler(),
        }
        return handlers.get(state, TerminationHandler())


# 10. Helper Functions
def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Camera Archiver")
    parser.add_argument(
        "-d",
        "--directory",
        type=Path,
        default=Path("/camera"),
        help="Input directory containing camera files",
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="Output directory for archived files"
    )
    parser.add_argument(
        "--trashdir", type=Path, help="Directory to move deleted files to"
    )
    parser.add_argument(
        "--no-trash",
        action="store_true",
        help="Don't use trash directory, delete files permanently",
    )
    parser.add_argument("--age", type=int, default=30, help="Age in days for cleanup")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--max-size", type=int, default=500, help="Maximum size in GB for archive"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip files that already have archives",
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Run cleanup after processing"
    )
    parser.add_argument(
        "--clean-output", action="store_true", help="Include output files in cleanup"
    )
    parser.add_argument(
        "-y",
        "--no-confirm",
        action="store_true",
        help="Don't ask for confirmation before processing",
    )
    parser.add_argument("--log-file", type=Path, help="Log file path")
    return parser.parse_args()


def setup_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Setup configuration from arguments"""
    directory = args.directory if args.directory.exists() else Path("/camera")
    output = args.output or (directory / "archived")

    config = {
        "directory": directory,
        "output": output,
        "trashdir": args.trashdir,
        "use_trash": not args.no_trash,
        "age": args.age,
        "dry_run": args.dry_run,
        "max_size": args.max_size,
        "no_skip": args.no_skip,
        "cleanup": args.cleanup,
        "clean_output": args.clean_output,
        "no_confirm": args.no_confirm,
        "log_file": args.log_file or (directory / "transcoding.log"),
    }

    # Calculate trash root - always defined
    if config["use_trash"]:
        config["trash_root"] = config["trashdir"] or directory / ".deleted"
    else:
        config["trash_root"] = None

    return config


def run_state_machine(context: Context) -> int:
    """Run the state machine until termination"""
    initial_state = context.current_state
    error_occurred = False
    states_visited = set()

    while (
        context.current_state != State.TERMINATION
        and not context.graceful_exit.should_exit()
    ):
        # Track the states we visit to know if we're completing the workflow
        states_visited.add(context.current_state)

        handler = StateHandlerFactory.create_handler(context.current_state)

        try:
            handler.enter(context)
            next_state = handler.execute(context)
            handler.exit(context)

            context.transition_to(next_state)
        except Exception as e:
            error_occurred = True
            if context.logger:
                context.logger.error(
                    f"Error in state {context.current_state.value}: {e}"
                )
            context.transition_to(State.TERMINATION)

    # Return 1 if an exception occurred during execution
    if error_occurred:
        return 1

    # Return 1 if we terminated early in the workflow (e.g., from INITIALIZATION due to missing directory)
    # If we only visited INITIALIZATION and then TERMINATION, that's an error condition
    if (
        initial_state == State.INITIALIZATION
        and len(states_visited) == 1
        and context.current_state == State.TERMINATION
    ):
        # We started at INITIALIZATION and immediately went to TERMINATION without visiting other states
        return 1

    # Return success code otherwise
    return 0


def run_state_machine_with_config(config: Dict[str, Any]) -> int:
    """Run the state machine with a configuration dictionary.

    Args:
        config: Configuration dictionary

    Returns:
        int: 0 for success, non-zero for error
    """
    context = Context(config)
    return run_state_machine(context)


def cleanup_resources(context: Optional[Context]) -> None:
    """Clean up resources before exit"""
    if context and context.progress_bar:
        context.progress_bar.finish()


# 11. Clean Main Function
def main() -> int:
    """Main entry point"""
    context: Optional[Context] = None
    try:
        args = parse_arguments()
        config = setup_config(args)
        context = Context(config)
        result = run_state_machine(context)
        return result
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1
    finally:
        cleanup_resources(context)


if __name__ == "__main__":
    sys.exit(main())
