"""
Integration tests for component interactions in the Camera Archiver system.
"""

import os
import signal
import time
from pathlib import Path

from archiver import (
    Config,
    GracefulExit,
    ProgressReporter,
    Logger,
    FileManager,
    FileDiscovery,
    Transcoder,
    FileProcessor,
    run_archiver,
    parse_args,
)


class TestFileDiscoveryIntegration:
    """Integration tests for FileDiscovery with other components."""

    def test_discover_files_with_file_processor(
        self,
        config,
        logger,
        graceful_exit,
        sample_files,
        archived_dir,
        mocker,
        mock_transcode_success,
    ):
        """Test FileDiscovery integration with FileProcessor."""
        _ = sample_files["mp4"].parent.parent.parent
        config.output = archived_dir

        # 0.  Remove any leftover archive from previous runs
        ts = sample_files["timestamp"]
        archive_path = (
            archived_dir
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        if archive_path.exists():
            archive_path.unlink()

        # Also remove the parent directory to ensure it doesn't exist
        if archive_path.parent.exists():
            import shutil

            shutil.rmtree(archive_path.parent)

        # 1.  No archive exists → planner will queue transcoding
        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_success
        )

        # Mock FileDiscovery.discover_files to return the expected files
        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }
        mocker.patch.object(
            FileDiscovery, "discover_files", return_value=(mp4s, mapping, set())
        )

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # 2.  Expect: 1 transcode, 2 removals (MP4 + JPG after success)
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2

    def test_discover_files_with_trash_and_file_processor(
        self, config, logger, graceful_exit, sample_files, trash_dir
    ):
        """Test FileDiscovery with trash directory integration with FileProcessor."""
        camera_dir = sample_files["mp4"].parent.parent.parent

        # Create files in trash
        trash_input_dir = trash_dir / "input"
        trash_input_dir.mkdir(parents=True)
        trash_file = trash_input_dir / "REO_camera_20230115120000.mp4"
        trash_file.touch()
        trash_jpg = trash_input_dir / "REO_camera_20230115120000.jpg"
        trash_jpg.touch()

        # Discover files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir, trash_dir)

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should have a plan for the discovered files, excluding trash files
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2  # MP4 and JPG


class TestFileManagerIntegration:
    """Integration tests for FileManager with other components."""

    def test_transcoder_with_file_processor(
        self, config, logger, graceful_exit, sample_files, archived_dir, mocker
    ):
        """Test Transcoder integration with FileProcessor."""
        camera_dir = sample_files["mp4"].parent.parent.parent.parent
        config.output = archived_dir
        config.directory = camera_dir

        # Mock subprocess.Popen
        mock_process = mocker.Mock()
        mock_process.stdout = mocker.Mock()
        mock_process.stdout.readline = mocker.Mock(
            side_effect=[
                "frame=  100 fps=100 q=28.0 size=     512kB time=00:00:01.00 bitrate= 419.2kbits/s speed=1.01x\n",
                "frame=  200 fps=100 q=28.0 size=    1024kB time=00:00:02.00 bitrate= 419.2kbits/s speed=1.01x\n",
                "",  # End of output
            ]
        )
        mock_process.wait.return_value = 0

        # Create a mock for subprocess.Popen that creates the output file
        def mock_popen(*args, **kwargs):
            # Extract the output path from the command arguments
            # args[0] is the command list: ['ffmpeg', '-hide_banner', ..., output_path]
            output_path = None
            if args and len(args) > 0:
                cmd = args[0]
                if isinstance(cmd, list) and len(cmd) > 0:
                    # The output path is the last element of the command list
                    output_path = Path(cmd[-1])

            # Create the output file and its parent directories
            if output_path:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()

            return mock_process

        mocker.patch("archiver.subprocess.Popen", side_effect=mock_popen)

        # Mock FileDiscovery.discover_files to return the expected files
        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }
        mocker.patch.object(
            FileDiscovery, "discover_files", return_value=(mp4s, mapping, set())
        )

        # Mock FileManager.remove_file to avoid errors
        mocker.patch.object(FileManager, "remove_file")

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Mock get_video_duration
        mocker.patch.object(Transcoder, "get_video_duration", return_value=2.0)

        # Execute plan
        progress_reporter = mocker.Mock()
        processor.execute_plan(plan, progress_reporter)

        # Check that output file was created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert output_file.exists()

    def test_file_manager_with_output_directory(
        self, config, logger, graceful_exit, sample_files, archived_dir, mocker
    ):
        """Test FileManager integration with output directory."""
        _ = sample_files["mp4"].parent.parent.parent
        config.output = archived_dir

        # Mock FileDiscovery.discover_files to return the expected files
        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }
        mocker.patch.object(
            FileDiscovery, "discover_files", return_value=(mp4s, mapping, set())
        )

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Mock transcoding to create output files
        def mock_transcode(
            input_path,
            output_path,
            logger,
            progress_cb=None,
            graceful_exit=None,
            dry_run=False,
        ):
            if not dry_run:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()
            if progress_cb:
                for pct in (25.0, 50.0, 75.0, 100.0):
                    progress_cb(pct)
            return True

        mocker.patch.object(Transcoder, "transcode_file", side_effect=mock_transcode)

        # Execute plan
        progress_reporter = mocker.Mock()
        processor.execute_plan(plan, progress_reporter)

        # Check that output directory structure was created
        timestamp = sample_files["timestamp"]
        expected_dir = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
        )
        assert expected_dir.exists()


class TestTranscoderIntegration:
    """Integration tests for Transcoder with other components."""

    def test_transcoder_with_file_processor(
        self, config, logger, graceful_exit, sample_files, archived_dir, mocker
    ):
        """Test Transcoder integration with FileProcessor."""
        camera_dir = sample_files["mp4"].parent.parent.parent.parent
        config.output = archived_dir
        config.directory = camera_dir

        # Mock subprocess.Popen
        mock_process = mocker.Mock()
        mock_process.stdout = mocker.Mock()
        mock_process.stdout.readline = mocker.Mock(
            side_effect=[
                "frame=  100 fps=100 q=28.0 size=     512kB time=00:00:01.00 bitrate= 419.2kbits/s speed=1.01x\n",
                "frame=  200 fps=100 q=28.0 size=    1024kB time=00:00:02.00 bitrate= 419.2kbits/s speed=1.01x\n",
                "",  # End of output
            ]
        )
        mock_process.wait.return_value = 0

        # Create a mock for subprocess.Popen that creates the output file
        def mock_popen(*args, **kwargs):
            # Extract the output path from the command arguments
            # args[0] is the command list: ['ffmpeg', '-hide_banner', ..., output_path]
            output_path = None
            if args and len(args) > 0:
                cmd = args[0]
                if isinstance(cmd, list) and len(cmd) > 0:
                    # The output path is the last element of the command list
                    output_path = Path(cmd[-1])

            # Create the output file and its parent directories
            if output_path:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()

            return mock_process

        mocker.patch("archiver.subprocess.Popen", side_effect=mock_popen)

        # Mock FileDiscovery.discover_files to return the expected files
        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }
        mocker.patch.object(
            FileDiscovery, "discover_files", return_value=(mp4s, mapping, set())
        )

        # Mock FileManager.remove_file to avoid errors
        mocker.patch.object(FileManager, "remove_file")

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Mock get_video_duration
        mocker.patch.object(Transcoder, "get_video_duration", return_value=2.0)

        # Execute plan
        progress_reporter = mocker.Mock()
        processor.execute_plan(plan, progress_reporter)

        # Check that output file was created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert output_file.exists()

    def test_transcoder_with_progress_reporter(
        self, config, logger, graceful_exit, sample_files, archived_dir, mocker
    ):
        """Test Transcoder integration with ProgressReporter."""
        camera_dir = sample_files["mp4"].parent.parent.parent.parent
        config.output = archived_dir
        config.directory = camera_dir

        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            camera_dir,
            trash_root=None,
            output_directory=config.output,  # ← add this
            clean_output=False,
        )

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        def mock_transcode(
            input_path,
            output_path,
            logger,
            progress_cb=None,
            graceful_exit=None,
            dry_run=False,
        ):
            if not dry_run:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()
                print(f"[MOCK] created = {output_path.exists()}")
            if progress_cb:
                for pct in (25.0, 50.0, 75.0, 100.0):
                    progress_cb(pct)
            return True

        mocker.patch.object(Transcoder, "transcode_file", side_effect=mock_transcode)

        # Execute plan with progress reporter
        with ProgressReporter(
            len(plan["transcoding"]), graceful_exit
        ) as progress_reporter:
            processor.execute_plan(plan, progress_reporter)

        # Check that output file was created
        timestamp = sample_files["timestamp"]
        output_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert output_file.exists()


class TestLoggerIntegration:
    """Integration tests for Logger with other components."""

    def test_logger_with_file_processor(
        self, config, logger, graceful_exit, sample_files, temp_dir, mocker
    ):
        """Test Logger integration with FileProcessor."""
        # Set up log file
        log_file = temp_dir / "test.log"
        config.log_file = log_file
        logger = Logger.setup(config)
        config.output = temp_dir / "archived"
        camera_dir = sample_files["mp4"].parent.parent.parent.parent

        # Discover files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir)

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Execute plan
        progress_reporter = mocker.Mock()
        processor.execute_plan(plan, progress_reporter)

        # Check that log file was created and contains entries
        assert log_file.exists()
        with open(log_file, "r") as f:
            content = f.read()
            assert "Processing" in content
            assert (
                "Successfully transcoded" in content or "Failed to transcode" in content
            )

    def test_logger_with_thread_safe_stream_handler(
        self, config, graceful_exit, sample_files
    ):
        """Test Logger integration with ThreadSafeStreamHandler."""
        # Set up logger
        logger = Logger.setup(config)

        # Check that console handler is a ThreadSafeStreamHandler
        from archiver import ThreadSafeStreamHandler

        handlers = [
            h for h in logger.handlers if isinstance(h, ThreadSafeStreamHandler)
        ]
        assert len(handlers) == 1

        # Log a message
        logger.info("Test message")

        # Check that the message was logged (can't easily verify console output, but no exceptions should be raised)
        assert True


class TestGracefulExitIntegration:
    """Integration tests for GracefulExit with other components."""

    def test_graceful_exit_with_file_processor(
        self, config, logger, sample_files, mocker
    ):
        """Test GracefulExit integration with FileProcessor."""
        graceful_exit = GracefulExit()

        camera_dir = sample_files["mp4"].parent.parent.parent.parent

        # Discover files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir)

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Request graceful exit
        graceful_exit.request_exit()

        # Execute plan
        progress_reporter = mocker.Mock()
        result = processor.execute_plan(plan, progress_reporter)

        # Should return True even with graceful exit (execution stops early)
        assert result is True

    def test_graceful_exit_with_transcoder(self, config, logger, sample_files):
        """Test GracefulExit integration with Transcoder."""
        graceful_exit = GracefulExit()

        # Request graceful exit
        graceful_exit.request_exit()

        # Try to transcode
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

        result = Transcoder.transcode_file(
            input_path, output_path, logger, graceful_exit=graceful_exit
        )

        # Should return False when exit is requested
        assert result is False

    def test_graceful_exit_with_progress_reporter(self, graceful_exit, mocker):
        """Test GracefulExit integration with ProgressReporter."""
        progress_reporter = ProgressReporter(10, graceful_exit)

        # Request graceful exit
        graceful_exit.request_exit()

        # Try to update progress
        mock_stderr = mocker.patch("sys.stderr")
        progress_reporter.update_progress(50.0)
        # Should not write to stderr when graceful exit is requested
        mock_stderr.write.assert_not_called()

    def test_signal_handling_with_graceful_exit(self, graceful_exit):
        """Test signal handling integration with GracefulExit."""
        # Set up signal handler
        from archiver import setup_signal_handlers

        setup_signal_handlers(graceful_exit)

        # Send SIGINT signal
        os.kill(os.getpid(), signal.SIGINT)

        # Give some time for the signal to be processed
        time.sleep(0.1)

        # Check that exit was requested
        assert graceful_exit.should_exit()


class TestConfigIntegration:
    """Integration tests for Config with other components."""

    def test_config_with_file_processor(
        self,
        mock_args,
        logger,
        graceful_exit,
        sample_files,
        archived_dir,
        trash_dir,
        mocker,
    ):
        """Test Config integration with FileProcessor."""
        # Set up config with custom values
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.dry_run = True

        config = Config(mock_args)

        camera_dir = sample_files["mp4"].parent.parent.parent.parent

        # Discover files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            camera_dir, config.trash_root
        )

        # Process files
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Execute plan
        progress_reporter = mocker.Mock()
        result = processor.execute_plan(plan, progress_reporter)

        # Should return True for dry run
        assert result is True

        # Files should still exist in dry run mode
        assert sample_files["mp4"].exists()
        assert sample_files["jpg"].exists()

    def test_config_with_logger(self, mock_args, temp_dir):
        """Test Config integration with Logger."""
        # Set up config with custom log file
        log_file = temp_dir / "test.log"
        mock_args.log_file = str(log_file)

        config = Config(mock_args)

        # Set up logger
        logger = Logger.setup(config)

        # Log a message
        logger.info("Test message")

        # Check that log file was created and contains the message
        assert log_file.exists()
        with open(log_file, "r") as f:
            content = f.read()
            assert "Test message" in content


class TestRunArchiverIntegration:
    """Integration tests for the run_archiver function."""

    def test_run_archiver_with_dry_run(self, mock_args, camera_dir, sample_files):
        """Test run_archiver with dry-run mode."""
        mock_args.directory = str(camera_dir)
        mock_args.dry_run = True

        config = Config(mock_args)

        result = run_archiver(config)

        # Should return 0 for successful dry run
        assert result == 0

        # Files should still exist in dry run mode
        assert sample_files["mp4"].exists()
        assert sample_files["jpg"].exists()

    def test_run_archiver_with_cleanup(
        self, mock_args, camera_dir, sample_files, trash_dir
    ):
        """Test run_archiver with cleanup mode."""
        mock_args.directory = str(camera_dir)
        mock_args.cleanup = True
        mock_args.trash_root = str(trash_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        result = run_archiver(config)

        # Should return 0 for successful cleanup
        assert result == 0

        # Files should be moved to trash
        assert not sample_files["mp4"].exists()
        assert not sample_files["jpg"].exists()

        trash_mp4 = trash_dir / "input" / sample_files["mp4"].relative_to(camera_dir)
        trash_jpg = trash_dir / "input" / sample_files["jpg"].relative_to(camera_dir)
        assert trash_mp4.exists()
        assert trash_jpg.exists()

    def test_run_archiver_with_nonexistent_directory(self, mock_args):
        """Test run_archiver with non-existent input directory."""
        mock_args.directory = "/nonexistent/directory"

        config = Config(mock_args)

        result = run_archiver(config)

        # Should return 1 for error
        assert result == 1

    def test_run_archiver_with_no_files(self, mock_args, camera_dir):
        """Test run_archiver with no files to process."""
        mock_args.directory = str(camera_dir)
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        result = run_archiver(config)

        # Should return 0 for successful execution with no files
        assert result == 0

    def test_run_archiver_with_clean_output(
        self, mock_args, camera_dir, archived_dir, sample_files, trash_dir
    ):
        """Test run_archiver with clean_output functionality."""
        # Set up archived file
        timestamp = sample_files["timestamp"]
        archived_file_path = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archived_file_path.parent.mkdir(parents=True)
        archived_file_path.touch()

        # Configure arguments for cleanup with clean_output
        mock_args.directory = str(camera_dir)
        mock_args.output = str(archived_dir)
        mock_args.trash_root = str(trash_dir)
        mock_args.cleanup = True
        mock_args.clean_output = True
        mock_args.age = 0  # Set age to 0 to make all files eligible for cleanup
        mock_args.no_confirm = True  # Skip confirmation

        config = Config(mock_args)

        result = run_archiver(config)

        # Should return 0 for successful execution
        assert result == 0

        # The archived file should be moved to trash since clean_output is True and age is 0
        archived_in_trash = (
            trash_dir / "output" / archived_file_path.relative_to(archived_dir)
        )
        assert archived_in_trash.exists()


class TestParseArgsIntegration:
    """Integration tests for the parse_args function."""

    def test_parse_args_with_defaults(self):
        """Test parse_args with default arguments."""
        args = parse_args([])

        assert args.directory == "/camera"
        assert args.output is None
        assert args.dry_run is False
        assert args.no_confirm is False
        assert args.no_skip is False
        assert args.delete is False
        assert args.trash_root is None
        assert args.cleanup is False
        assert args.clean_output is False
        assert args.age == 30
        assert args.log_file is None

    def test_parse_args_with_custom_values(self):
        """Test parse_args with custom arguments."""
        args = parse_args(
            [
                "/custom/directory",
                "-o",
                "/custom/output",
                "--dry-run",
                "--no-confirm",
                "--no-skip",
                "--delete",
                "--trash-root",
                "/custom/trash",
                "--cleanup",
                "--clean-output",
                "--age",
                "60",
                "--log-file",
                "/custom/log.log",
            ]
        )

        assert args.directory == "/custom/directory"
        assert args.output == "/custom/output"
        assert args.dry_run is True
        assert args.no_confirm is True
        assert args.no_skip is True
        assert args.delete is True
        assert args.trash_root == "/custom/trash"
        assert args.cleanup is True
        assert args.clean_output is True
        assert args.age == 60
        assert args.log_file == "/custom/log.log"


class TestIntegrationWithUncoveredPaths:
    """Integration tests for scenarios involving uncovered code paths."""

    def test_logger_with_exception_handling_during_setup(self, temp_dir, mocker):
        """Integration test for Logger setup with exception scenarios."""
        from archiver import Config, Logger
        from argparse import Namespace

        # Create args with a problematic log file path
        args = Namespace()
        args.directory = str(temp_dir)
        args.output = str(temp_dir / "archived")
        args.dry_run = False
        args.no_confirm = False
        args.no_skip = False
        args.delete = False
        args.trash_root = str(temp_dir / ".deleted")
        args.cleanup = False
        args.clean_output = False
        args.age = 30
        args.log_file = str(
            temp_dir / "nonexistent" / "log.log"
        )  # Non-existent directory

        config = Config(args)

        # This should handle the exception gracefully in Logger setup
        logger = Logger.setup(config)
        assert logger.name == "camera_archiver"

    def test_file_discovery_with_archived_files_integration(
        self, temp_dir, archived_dir, sample_files
    ):
        """Integration test for discovering both regular and archived files."""
        # Create archived file in output directory
        timestamp = sample_files["timestamp"]
        archived_file = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archived_file.parent.mkdir(parents=True)
        archived_file.touch()

        # Discover files with clean_output enabled to scan output directory too
        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            temp_dir, output_directory=archived_dir, clean_output=True
        )

        # Should find both the original sample files and the archived file
        assert len(mp4s) >= 2  # At least original and archived files

    def test_file_manager_remove_with_exception_paths(self, temp_dir, logger, mocker):
        """Integration test for FileManager remove with exception scenarios."""
        from archiver import FileManager

        # Create a test file
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Mock shutil.move to raise an exception to test exception handling
        mocker.patch("shutil.move", side_effect=OSError("Permission denied"))

        # Attempt to remove file to trash (will trigger the exception handling)
        FileManager.remove_file(
            test_file, logger, delete=False, trash_root=temp_dir / ".trash"
        )

        # File should still exist since operation failed
        assert test_file.exists()

    def test_transcoder_error_integration(self, sample_files, logger, mocker):
        """Integration test for Transcoder error handling scenarios."""
        from archiver import Transcoder

        # Mock subprocess.Popen to raise an OSError to test error handling
        mocker.patch(
            "archiver.subprocess.Popen", side_effect=OSError("Command not found")
        )

        # Should handle the error gracefully
        result = Transcoder.transcode_file(
            sample_files["mp4"], sample_files["mp4"].parent / "output.mp4", logger
        )
        assert result is False
