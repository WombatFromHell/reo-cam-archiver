"""
Video transcoding operations for the Camera Archiver application.
"""

import logging
import re
import shutil
import subprocess
from typing import List, Optional

from .graceful_exit import GracefulExit
from .utils import FilePath, ProgressCallback


class Transcoder:
    """Handles video transcoding operations with strict typing"""

    @staticmethod
    def get_video_duration(file_path: FilePath) -> Optional[float]:
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

    @staticmethod
    def _build_ffmpeg_command(input_path: FilePath, output_path: FilePath) -> List[str]:
        """Build the ffmpeg command with appropriate parameters.

        Args:
            input_path: Path to the input video file
            output_path: Path for the output transcoded file

        Returns:
            List of command arguments for ffmpeg subprocess
        """
        return [
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

    @staticmethod
    def _parse_ffmpeg_time(time_str: str) -> float:
        """Parse time string from ffmpeg output and convert to seconds.

        Args:
            time_str: Time string in format 'HH:MM:SS' or just seconds

        Returns:
            Time in seconds
        """
        if ":" in time_str:
            h, mn, s = map(float, time_str.split(":")[:3])
            return h * 3600 + mn * 60 + s
        else:
            return float(time_str)

    @staticmethod
    def _calculate_progress(
        line: str, total_duration: Optional[float], current_pct: float
    ) -> float:
        """Calculate the current progress percentage based on ffmpeg output.

        Args:
            line: Line of output from ffmpeg process
            total_duration: Total duration of the video if known, None otherwise
            current_pct: Current progress percentage

        Returns:
            Updated progress percentage
        """
        if total_duration and total_duration > 0:
            time_match = re.search(r"time=([0-9:.]+)", line)
            if time_match:
                time_str = time_match.group(1)
                elapsed_seconds = Transcoder._parse_ffmpeg_time(time_str)
                return min(elapsed_seconds / total_duration * 100, 100.0)
        else:
            return min(current_pct + 0.5, 99.0)
        return current_pct

    @staticmethod
    def _handle_ffmpeg_stdout_error(proc, logger) -> bool:
        """Handle ffmpeg stdout capture error."""
        logger.error("Failed to capture ffmpeg output")
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        return False

    @staticmethod
    def _handle_graceful_exit(proc, logger) -> bool:
        """Handle graceful exit by terminating ffmpeg process."""
        logger.info("Cancellation requested, terminating ffmpeg process...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        return False

    @staticmethod
    def _log_ffmpeg_output(
        line: str, log_lines: List[str], debug_ffmpeg: bool, logger
    ) -> None:
        """Log ffmpeg output if debug mode is enabled."""
        if debug_ffmpeg:
            logger.debug(f"FFmpeg output: {line.strip()}")
        log_lines.append(line)

    @staticmethod
    def _handle_ffmpeg_completion(
        rc: int, log_lines: List[str], graceful_exit: GracefulExit, logger
    ) -> bool:
        """Handle ffmpeg process completion and return success status."""
        if rc != 0 and not graceful_exit.should_exit():
            msg = f"FFmpeg failed (code {rc})\n" + "".join(log_lines)
            logger.error(msg)
        return rc == 0 and not graceful_exit.should_exit()

    @staticmethod
    def _process_ffmpeg_output(
        proc,
        total_duration: Optional[float],
        progress_cb: Optional[ProgressCallback],
        graceful_exit: GracefulExit,
        logger,
    ) -> bool:
        """Process ffmpeg output stream and report progress."""
        return Transcoder._process_ffmpeg_output_with_state(
            proc, total_duration, progress_cb, graceful_exit, logger
        )

    @staticmethod
    def _process_ffmpeg_output_with_state(
        proc,
        total_duration: Optional[float],
        progress_cb: Optional[ProgressCallback],
        graceful_exit: GracefulExit,
        logger,
    ) -> bool:
        """Process ffmpeg output stream with state management."""
        log_lines: List[str] = []
        prev_pct = -1.0
        cur_pct = 0.0
        debug_ffmpeg = logger.isEnabledFor(logging.DEBUG)

        if proc.stdout is None:
            return Transcoder._handle_ffmpeg_stdout_error(proc, logger)

        stdout_iter = iter(proc.stdout.readline, "")

        for line in stdout_iter:
            if Transcoder._should_exit_gracefully(graceful_exit):
                return Transcoder._handle_graceful_exit(proc, logger)

            if not line:
                break

            Transcoder._log_ffmpeg_output(line, log_lines, debug_ffmpeg, logger)
            cur_pct = Transcoder._calculate_progress(line, total_duration, cur_pct)
            prev_pct = Transcoder._update_progress_callback(
                progress_cb, cur_pct, prev_pct
            )

        rc = proc.wait()
        return Transcoder._handle_ffmpeg_completion(
            rc, log_lines, graceful_exit, logger
        )

    @staticmethod
    def _should_exit_gracefully(graceful_exit: GracefulExit) -> bool:
        """Check if graceful exit has been requested."""
        return graceful_exit.should_exit()

    @staticmethod
    def _update_progress_callback(
        progress_cb: Optional[ProgressCallback], cur_pct: float, prev_pct: float
    ) -> float:
        """Update progress callback if conditions are met and return updated previous percentage."""
        if progress_cb and cur_pct != prev_pct:
            progress_cb(cur_pct)
            return cur_pct
        return prev_pct

    @staticmethod
    def _handle_dry_run(input_path: FilePath, output_path: FilePath, logger) -> bool:
        """Handle dry run mode for transcoding."""
        logger.info(f"[DRY RUN] Would transcode {input_path} -> {output_path}")
        return True  # Pretend transcoding succeeded in dry run

    @staticmethod
    def _setup_output_directory(output_path: FilePath) -> None:
        """Create output directory if it doesn't exist."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _start_ffmpeg_process(cmd: List[str], logger) -> Optional[subprocess.Popen]:
        """Start ffmpeg subprocess and handle potential errors."""
        try:
            return subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError as e:
            logger.error(f"Failed to start ffmpeg process: {e}")
            return None

    @staticmethod
    def _cleanup_ffmpeg_process(proc: Optional[subprocess.Popen]) -> None:
        """Clean up ffmpeg process resources."""
        if proc:
            Transcoder._close_process_stdout(proc)
            Transcoder._wait_for_process_completion(proc)

    @staticmethod
    def _close_process_stdout(proc: subprocess.Popen) -> None:
        """Close process stdout if it exists."""
        if proc and proc.stdout:
            try:
                proc.stdout.close()
            except Exception:
                pass

    @staticmethod
    def _wait_for_process_completion(proc: subprocess.Popen) -> None:
        """Wait for process completion with proper timeout handling."""
        if not proc:
            return

        try:
            proc.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            Transcoder._terminate_process_with_timeout(proc)
        except Exception:
            pass

    @staticmethod
    def _terminate_process_with_timeout(proc: subprocess.Popen) -> None:
        """Terminate process with timeout and fallback to kill if needed."""
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    @staticmethod
    def transcode_file(
        input_path: FilePath,
        output_path: FilePath,
        logger,
        progress_cb: Optional[ProgressCallback] = None,
        graceful_exit: Optional[GracefulExit] = None,
        dry_run: bool = False,
    ) -> bool:
        """Transcode a video file using ffmpeg with QSV hardware acceleration"""
        if graceful_exit is None:
            graceful_exit = GracefulExit()
        if graceful_exit.should_exit():
            return False

        # Validate input path
        if not input_path or str(input_path).strip() == "" or str(input_path) == ".":
            logger.error("Empty input path provided for transcoding")
            return False

        if dry_run:
            return Transcoder._handle_dry_run(input_path, output_path, logger)

        Transcoder._setup_output_directory(output_path)

        cmd = Transcoder._build_ffmpeg_command(input_path, output_path)

        # Get video duration
        total_duration = Transcoder.get_video_duration(input_path)

        proc = Transcoder._start_ffmpeg_process(cmd, logger)
        if proc is None:
            return False

        try:
            success = Transcoder._process_ffmpeg_output(
                proc, total_duration, progress_cb, graceful_exit, logger
            )
            return success
        finally:
            Transcoder._cleanup_ffmpeg_process(proc)
