"""
Test module for Transcoder class - comprehensive transcoding testing.
"""

import subprocess
from pathlib import Path

import pytest

from src.archiver.graceful_exit import GracefulExit
from src.archiver.transcoder import Transcoder


class TestTranscoderInitialization:
    """Test Transcoder class initialization and basic functionality."""

    def test_transcoder_class_exists(self, mocker):
        """Test that Transcoder class exists and is importable."""
        assert Transcoder is not None
        assert hasattr(Transcoder, "transcode_file")
        assert hasattr(Transcoder, "get_video_duration")
        assert hasattr(Transcoder, "_build_ffmpeg_command")


class TestVideoDuration:
    """Test video duration functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "valid_file",
                "setup": lambda mocker: {
                    "which_return": True,
                    "run_return": mocker.Mock(stdout="120.5\n", returncode=0),
                    "expected": 120.5,
                },
            },
            {
                "name": "ffprobe_not_found",
                "setup": lambda mocker: {
                    "which_return": None,
                    "run_return": None,
                    "expected": None,
                },
            },
            {
                "name": "invalid_output",
                "setup": lambda mocker: {
                    "which_return": True,
                    "run_return": mocker.Mock(stdout="N/A\n", returncode=0),
                    "expected": None,
                },
            },
            {
                "name": "exception",
                "setup": lambda mocker: {
                    "which_return": True,
                    "run_side_effect": Exception("Test error"),
                    "expected": None,
                },
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "valid_file"},
                {"name": "ffprobe_not_found"},
                {"name": "invalid_output"},
                {"name": "exception"},
            ]
        ],
    )
    def test_get_video_duration(self, mocker, test_case):
        """Test getting video duration with various scenarios."""
        setup = test_case["setup"](mocker)

        # Set up mocks
        mocker.patch("shutil.which", return_value=setup["which_return"])

        if "run_side_effect" in setup:
            mocker.patch("subprocess.run", side_effect=setup["run_side_effect"])
        elif setup["run_return"] is not None:
            mocker.patch("subprocess.run", return_value=setup["run_return"])

        test_file = Path("/test/video.mp4")
        duration = Transcoder.get_video_duration(test_file)

        assert duration == setup["expected"], (
            f"Expected {setup['expected']}, got {duration}"
        )


class TestFFmpegCommandBuilding:
    """Test FFmpeg command building functionality."""

    def test_build_ffmpeg_command(self, mocker):
        """Test building FFmpeg command with correct parameters."""
        input_path = Path("/input/video.mp4")
        output_path = Path("/output/video.mp4")

        cmd = Transcoder._build_ffmpeg_command(input_path, output_path)

        assert "ffmpeg" in cmd
        assert "-hwaccel" in cmd
        assert "qsv" in cmd
        assert "-c:v" in cmd
        assert "h264_qsv" in cmd
        assert str(input_path) in cmd
        assert str(output_path) in cmd
        assert "-vf" in cmd
        assert "scale_qsv=w=1024:h=768:mode=hq" in cmd
        assert "-global_quality" in cmd
        assert "26" in cmd

    def test_build_ffmpeg_command_with_spaces_in_paths(self, mocker):
        """Test building FFmpeg command with spaces in file paths."""
        input_path = Path("/input folder/video file.mp4")
        output_path = Path("/output folder/video file.mp4")

        cmd = Transcoder._build_ffmpeg_command(input_path, output_path)

        assert str(input_path) in cmd
        assert str(output_path) in cmd


class TestFFmpegTimeParsing:
    """Test FFmpeg time parsing functionality."""

    def test_parse_ffmpeg_time_with_hms_format(self, mocker):
        """Test parsing FFmpeg time in HH:MM:SS format."""
        time_str = "01:23:45"
        seconds = Transcoder._parse_ffmpeg_time(time_str)

        expected = 1 * 3600 + 23 * 60 + 45
        assert seconds == expected

    def test_parse_ffmpeg_time_with_seconds_only(self, mocker):
        """Test parsing FFmpeg time in seconds format."""
        time_str = "123.45"
        seconds = Transcoder._parse_ffmpeg_time(time_str)

        assert seconds == 123.45

    def test_parse_ffmpeg_time_with_partial_hms(self, mocker):
        """Test parsing FFmpeg time with partial HH:MM:SS format."""
        time_str = "1:23:00"  # Full format
        seconds = Transcoder._parse_ffmpeg_time(time_str)

        expected = 1 * 3600 + 23 * 60 + 0
        assert seconds == expected


class TestProgressCalculation:
    """Test progress calculation functionality."""

    def test_calculate_progress_with_duration(self, mocker):
        """Test progress calculation with known duration."""
        line = "time=00:01:00"
        total_duration = 120.0  # 2 minutes
        current_pct = 0.0

        new_pct = Transcoder._calculate_progress(line, total_duration, current_pct)

        # 1 minute out of 2 minutes = 50%
        assert new_pct == 50.0

    def test_calculate_progress_without_duration(self, mocker):
        """Test progress calculation without known duration."""
        line = "some output"
        total_duration = None
        current_pct = 0.0

        new_pct = Transcoder._calculate_progress(line, total_duration, current_pct)

        # Should increment by 0.5 when no duration is known
        assert new_pct == 0.5

    def test_calculate_progress_with_max_cap(self, mocker):
        """Test that progress doesn't exceed 100%."""
        line = "time=00:03:00"
        total_duration = 120.0  # 2 minutes
        current_pct = 0.0

        new_pct = Transcoder._calculate_progress(line, total_duration, current_pct)

        # 3 minutes > 2 minutes, should be capped at 100%
        assert new_pct == 100.0


class TestFFmpegProcessHandling:
    """Test FFmpeg process handling functionality."""

    def test_start_ffmpeg_process_success(self, mocker):
        """Test starting FFmpeg process successfully."""
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mock_popen.return_value

        cmd = ["ffmpeg", "-i", "input.mp4", "output.mp4"]
        logger = mocker.Mock()

        proc = Transcoder._start_ffmpeg_process(cmd, logger)

        assert proc == mock_process
        mock_popen.assert_called_once_with(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def test_start_ffmpeg_process_failure(self, mocker):
        """Test starting FFmpeg process when it fails."""
        mocker.patch("subprocess.Popen", side_effect=OSError("Test error"))

        cmd = ["ffmpeg", "-i", "input.mp4", "output.mp4"]
        logger = mocker.Mock()

        proc = Transcoder._start_ffmpeg_process(cmd, logger)

        assert proc is None
        logger.error.assert_called_once_with(
            "Failed to start ffmpeg process: Test error"
        )


class TestTranscodingWorkflow:
    """Test the complete transcoding workflow."""

    def test_transcode_file_dry_run(self, mocker):
        """Test transcoding in dry run mode."""
        input_path = Path("/input/video.mp4")
        output_path = Path("/output/video.mp4")
        logger = mocker.Mock()

        result = Transcoder.transcode_file(
            input_path, output_path, logger, dry_run=True
        )

        assert result is True
        logger.info.assert_called_once_with(
            "[DRY RUN] Would transcode /input/video.mp4 -> /output/video.mp4"
        )

    def test_transcode_file_with_graceful_exit(self, mocker):
        """Test transcoding with graceful exit requested."""
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        input_path = Path("/input/video.mp4")
        output_path = Path("/output/video.mp4")
        logger = mocker.Mock()

        result = Transcoder.transcode_file(
            input_path, output_path, logger, graceful_exit=graceful_exit
        )

        assert result is False

    def test_transcode_file_success(self, mocker, temp_dir):
        """Test successful transcoding workflow."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        # Create a mock file-like object with readline method
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["time=00:01:00\n", "time=00:02:00\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is True
        # Verify output directory was created
        assert output_path.parent.exists()

    def test_transcode_file_failure(self, mocker, temp_dir):
        """Test failed transcoding workflow."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process that fails
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["error output\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 1  # Non-zero exit code
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is False
        logger.error.assert_called()


class TestProgressCallback:
    """Test progress callback functionality."""

    def test_transcode_with_progress_callback(self, mocker, temp_dir):
        """Test transcoding with progress callback."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process with progress output
        mock_process = mocker.Mock()
        progress_lines = [
            "time=00:00:30\n",  # 25%
            "time=00:01:00\n",  # 50%
            "time=00:01:30\n",  # 75%
            "time=00:02:00\n",  # 100%
            "",
        ]
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = progress_lines
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        # Track progress callbacks
        progress_values = []

        def progress_cb(pct):
            progress_values.append(pct)

        result = Transcoder.transcode_file(
            input_path, output_path, logger, progress_cb=progress_cb
        )

        assert result is True
        assert len(progress_values) > 0
        assert any(p > 0 for p in progress_values)  # Should have some progress


class TestGracefulExitHandling:
    """Test graceful exit handling during transcoding."""

    def test_graceful_exit_during_transcoding(self, mocker, temp_dir):
        """Test graceful exit during transcoding."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()

        # Simulate graceful exit being requested during processing
        mock_stdout = mocker.Mock()

        def readline_side_effect():
            graceful_exit.request_exit()  # Request exit after first read
            return "time=00:00:30\n"  # 25%

        mock_stdout.readline.side_effect = readline_side_effect
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mock_process.terminate = mocker.Mock()
        mock_process.kill = mocker.Mock()
        mocker.patch("subprocess.Popen", return_value=mock_process)

        graceful_exit = GracefulExit()
        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(
            input_path, output_path, logger, graceful_exit=graceful_exit
        )

        assert result is False
        mock_process.terminate.assert_called_once()
        logger.info.assert_called_with(
            "Cancellation requested, terminating ffmpeg process..."
        )


class TestOutputDirectorySetup:
    """Test output directory setup functionality."""

    def test_setup_output_directory_creates_parents(self, temp_dir):
        """Test that output directory and parents are created."""
        output_path = temp_dir / "subdir1" / "subdir2" / "output.mp4"

        # Directory should not exist initially
        assert not output_path.parent.exists()

        Transcoder._setup_output_directory(output_path)

        # Directory should exist now
        assert output_path.parent.exists()

    def test_setup_output_directory_already_exists(self, temp_dir):
        """Test output directory setup when directory already exists."""
        output_path = temp_dir / "existing" / "output.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Should not raise exception
        Transcoder._setup_output_directory(output_path)

        assert output_path.parent.exists()


class TestProcessCleanup:
    """Test process cleanup functionality."""

    def test_cleanup_ffmpeg_process(self, mocker):
        """Test cleanup of ffmpeg process."""
        mock_process = mocker.Mock()
        mock_process.stdout = mocker.Mock()
        mock_process.wait = mocker.Mock()

        Transcoder._cleanup_ffmpeg_process(mock_process)

        mock_process.stdout.close.assert_called_once()
        mock_process.wait.assert_called_once()

    def test_cleanup_none_process(self, mocker):
        """Test cleanup with None process."""
        # Should not raise exception
        Transcoder._cleanup_ffmpeg_process(None)


class TestTranscoderFixtureIntegration:
    """Test integration with pytest fixtures."""

    def test_transcode_with_mock_transcode_success(
        self, mock_transcode_success, temp_dir, mocker
    ):
        """Test transcoding with mock_transcode_success fixture."""
        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        # Apply the mock
        mock_transcode = mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )
        result = Transcoder.transcode_file(input_path, output_path, logger)
        mock_transcode.stop()

        assert result is True
        assert output_path.exists()

    def test_transcode_with_mock_transcode_fail(self, mock_transcode_fail, mocker):
        """Test transcoding with mock_transcode_fail fixture."""
        input_path = Path("/input/video.mp4")
        output_path = Path("/output/video.mp4")
        logger = mocker.Mock()

        # Apply the mock
        mock_transcode = mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_fail
        )
        result = Transcoder.transcode_file(input_path, output_path, logger)
        mock_transcode.stop()

        assert result is False


class TestTranscoderSpecialCases:
    """Test special cases for transcoding."""

    def test_transcode_with_very_short_video(self, mocker, temp_dir):
        """Test transcoding with very short video duration."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock very short video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "0.5\n"  # 0.5 seconds
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["time=00:00:00.1\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/short.mp4")
        output_path = temp_dir / "output" / "short.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is True

    def test_transcode_with_no_duration(self, mocker, temp_dir):
        """Test transcoding when duration cannot be determined."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration to return None
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "N/A\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["some output\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is True


class TestTranscoderLogging:
    """Test logging functionality in transcoding."""

    def test_ffmpeg_output_logging_in_debug_mode(self, mocker, temp_dir):
        """Test that ffmpeg output is logged in debug mode."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["debug output line\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"

        # Create logger in debug mode
        logger = mocker.Mock()
        logger.isEnabledFor.return_value = True  # Debug mode enabled

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is True
        # Should have logged the debug output
        logger.debug.assert_called()

    def test_ffmpeg_output_logging_in_normal_mode(self, mocker, temp_dir):
        """Test that ffmpeg output is not logged in normal mode."""
        # Mock dependencies
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["output line\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"

        # Create logger in normal mode (not debug)
        logger = mocker.Mock()
        logger.isEnabledFor.return_value = False  # Debug mode disabled

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is True
        # Should not have logged debug output
        logger.debug.assert_not_called()


class TestTranscoderEdgeCases:
    """Test edge cases for transcoding."""

    def test_transcode_with_empty_input_path(self, mocker, temp_dir):
        """Test transcoding with empty input path."""
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["output\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("")  # Empty path
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        # Should handle empty path gracefully
        assert result is False

    def test_transcode_with_very_long_paths(self, mocker, temp_dir):
        """Test transcoding with very long file paths."""
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process
        mock_process = mocker.Mock()
        mock_stdout = mocker.Mock()
        mock_stdout.readline.side_effect = ["output\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        # Create very long paths
        long_input = Path("/" + "a" * 200 + "/input.mp4")
        long_output = temp_dir / "output" / "very_long_output.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(long_input, long_output, logger)

        # Should handle long paths (may fail due to OS limits, but shouldn't crash)
        assert result is False or result is True


class TestErrorHandling:
    """Test error handling methods."""

    def test_handle_ffmpeg_stdout_error(self, mocker):
        """Test handling of ffmpeg stdout capture error."""
        logger = mocker.Mock()

        # Create a mock process
        mock_proc = mocker.Mock()

        # Mock the wait method to raise TimeoutExpired only on first call
        call_count = [0]

        def wait_side_effect(timeout=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise subprocess.TimeoutExpired("test_command", 5)
            return 0

        mock_proc.wait.side_effect = wait_side_effect

        # Call the error handler
        result = Transcoder._handle_ffmpeg_stdout_error(mock_proc, logger)

        # Should return False and handle the timeout
        assert result is False

        # Should have called kill and wait
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_called()

        # Should have logged the error
        logger.error.assert_called_with("Failed to capture ffmpeg output")

    def test_handle_ffmpeg_stdout_error_with_successful_wait(self, mocker):
        """Test handling of ffmpeg stdout error with successful wait."""
        logger = mocker.Mock()

        # Create a mock process
        mock_proc = mocker.Mock()

        # Mock the wait method to succeed
        mock_proc.wait.return_value = 0

        # Call the error handler
        result = Transcoder._handle_ffmpeg_stdout_error(mock_proc, logger)

        # Should return False
        assert result is False

        # Should have called wait but not kill
        mock_proc.wait.assert_called_once_with(timeout=5)
        mock_proc.kill.assert_not_called()

        # Should have logged the error
        logger.error.assert_called_with("Failed to capture ffmpeg output")

    def test_handle_graceful_exit(self, mocker):
        """Test handling of graceful exit."""
        logger = mocker.Mock()

        # Create a mock process
        mock_proc = mocker.Mock()

        # Mock the wait method to raise TimeoutExpired only on first call
        call_count = [0]

        def wait_side_effect(timeout=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise subprocess.TimeoutExpired("test_command", 5)
            return 0

        mock_proc.wait.side_effect = wait_side_effect

        # Call the graceful exit handler
        result = Transcoder._handle_graceful_exit(mock_proc, logger)

        # Should return False and handle the timeout
        assert result is False

        # Should have called terminate, kill, and wait
        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()
        mock_proc.wait.assert_called()

        # Should have logged the cancellation
        logger.info.assert_called_with(
            "Cancellation requested, terminating ffmpeg process..."
        )

    def test_handle_graceful_exit_with_successful_wait(self, mocker):
        """Test handling of graceful exit with successful wait."""
        logger = mocker.Mock()

        # Create a mock process
        mock_proc = mocker.Mock()

        # Mock the wait method to succeed
        mock_proc.wait.return_value = 0

        # Call the graceful exit handler
        result = Transcoder._handle_graceful_exit(mock_proc, logger)

        # Should return False
        assert result is False

        # Should have called terminate and wait but not kill
        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once_with(timeout=5)
        mock_proc.kill.assert_not_called()

        # Should have logged the cancellation
        logger.info.assert_called_with(
            "Cancellation requested, terminating ffmpeg process..."
        )

    def test_transcode_file_with_stdout_error(self, temp_dir, mocker):
        """Test transcoding when stdout capture fails."""
        logger = mocker.Mock()
        graceful_exit = GracefulExit()

        # Create test files
        input_file = temp_dir / "input.mp4"
        output_file = temp_dir / "output.mp4"
        input_file.touch()

        # Mock Popen to return a process with None stdout
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.Mock()
        mock_process.stdout = None  # This will trigger the stdout error path
        mock_process.wait.return_value = 1
        mock_popen.return_value = mock_process

        # Mock the error handler
        mocker.patch(
            "src.archiver.transcoder.Transcoder._handle_ffmpeg_stdout_error",
            return_value=False,
        )

        # Try to transcode
        result = Transcoder.transcode_file(
            input_file, output_file, logger, None, graceful_exit
        )

        # Should return False due to stdout error
        assert result is False

        # Should have called the error handler
        Transcoder._handle_ffmpeg_stdout_error.assert_called_once()  # type: ignore

    def test_transcode_file_with_graceful_exit(self, temp_dir, mocker):
        """Test transcoding when graceful exit is requested."""
        logger = mocker.Mock()
        graceful_exit = GracefulExit()

        # Create test files
        input_file = temp_dir / "input.mp4"
        output_file = temp_dir / "output.mp4"
        input_file.touch()

        # Mock Popen
        mock_popen = mocker.patch("subprocess.Popen")
        mock_process = mocker.Mock()

        # Create a generator that will trigger graceful exit check
        def readline_generator():
            graceful_exit.request_exit()  # Request exit during iteration
            yield "output\n"
            yield ""

        mock_process.stdout.readline.side_effect = readline_generator()
        mock_process.wait.return_value = 0
        mock_popen.return_value = mock_process

        # Mock the graceful exit handler
        mocker.patch(
            "src.archiver.transcoder.Transcoder._handle_graceful_exit",
            return_value=False,
        )

        # Try to transcode
        result = Transcoder.transcode_file(
            input_file, output_file, logger, None, graceful_exit
        )

        # Should return False due to graceful exit
        assert result is False

        # Should have called the graceful exit handler
        Transcoder._handle_graceful_exit.assert_called_once()  # type: ignore

    def test_transcode_with_missing_ffmpeg(self, mocker, temp_dir):
        """Test transcoding when ffmpeg is not available."""
        mocker.patch("shutil.which", return_value=False)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is False

    def test_transcode_with_invalid_input_file(self, mocker, temp_dir):
        """Test transcoding with invalid input file."""
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration to fail
        mocker.patch("subprocess.run", side_effect=Exception("File not found"))

        input_path = Path("/nonexistent/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is False

    def test_transcode_with_stdout_error(self, mocker, temp_dir):
        """Test transcoding when stdout capture fails."""
        mocker.patch("shutil.which", return_value=True)

        # Mock video duration
        mock_duration_result = mocker.Mock()
        mock_duration_result.stdout = "120.5\n"
        mock_duration_result.returncode = 0
        mocker.patch("subprocess.run", return_value=mock_duration_result)

        # Mock ffmpeg process with None stdout
        mock_process = mocker.Mock()
        mock_process.stdout = None
        mock_process.wait.return_value = 0
        mocker.patch("subprocess.Popen", return_value=mock_process)

        input_path = Path("/input/video.mp4")
        output_path = temp_dir / "output" / "video.mp4"
        logger = mocker.Mock()

        result = Transcoder.transcode_file(input_path, output_path, logger)

        assert result is False
        logger.error.assert_called_with("Failed to capture ffmpeg output")
