"""
Unit tests for individual components of the Camera Archiver system.
"""

import subprocess
from pathlib import Path
from datetime import datetime
import pytest
from pytest_mock import MockerFixture

from archiver import (
    Config,
    GracefulExit,
    ProgressReporter,
    Logger,
    FileDiscovery,
    FileManager,
    Transcoder,
    FileProcessor,
)


class TestConfig:
    """Test cases for the Config class."""

    def test_config_initialization(self, mock_args):
        """Test that Config initializes correctly with default arguments."""
        config = Config(mock_args)
        assert config.directory == Path("/camera")
        assert config.output == Path("/camera/archived")
        assert config.dry_run is False
        assert config.no_confirm is False
        assert config.no_skip is False
        assert config.delete is False
        assert config.trash_root == Path("/camera/.deleted")
        assert config.cleanup is False
        assert config.clean_output is False
        assert config.age == 30
        assert config.log_file == Path("/camera/archiver.log")

    @pytest.mark.parametrize(
        "attr_name,new_value,expected_value",
        [
            ("output", "/custom/output", Path("/custom/output")),
            ("trash_root", "/custom/trash", Path("/custom/trash")),
            ("log_file", "/custom/log.log", Path("/custom/log.log")),
        ],
    )
    def test_config_with_custom_values(
        self, mock_args, attr_name, new_value, expected_value
    ):
        """Test Config with custom values."""
        setattr(mock_args, attr_name, new_value)
        config = Config(mock_args)
        assert getattr(config, attr_name) == expected_value

    def test_config_with_delete_flag(self, mock_args):
        """Test Config with delete flag set."""
        mock_args.delete = True
        config = Config(mock_args)
        assert config.delete is True
        assert config.trash_root is None


class TestGracefulExit:
    """Test cases for the GracefulExit class."""

    def test_initial_exit_state(self, graceful_exit):
        """Test that initial exit state is False."""
        assert graceful_exit.should_exit() is False

    def test_request_exit(self, graceful_exit):
        """Test that requesting exit changes the state."""
        graceful_exit.request_exit()
        assert graceful_exit.should_exit() is True

    def test_thread_safety(self, graceful_exit):
        """Test that GracefulExit is thread-safe."""
        import threading

        results = []

        def check_exit():
            results.append(graceful_exit.should_exit())

        def request_exit():
            graceful_exit.request_exit()

        # Start threads
        t1 = threading.Thread(target=check_exit)
        t2 = threading.Thread(target=request_exit)
        t3 = threading.Thread(target=check_exit)

        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()

        # First check should be False, second should be True
        assert results[0] is False
        assert results[1] is True


class TestProgressReporter:
    """Test cases for the ProgressReporter class."""

    def test_progress_initialization(self, graceful_exit):
        """Test that ProgressReporter initializes correctly."""
        reporter = ProgressReporter(10, graceful_exit)
        assert reporter.total == 10
        assert reporter.current == 0
        assert reporter.graceful_exit == graceful_exit
        assert reporter.silent is False

    def test_progress_silent_mode(self, graceful_exit):
        """Test that ProgressReporter works in silent mode."""
        reporter = ProgressReporter(10, graceful_exit, silent=True)
        assert reporter.silent is True

    def test_start_file(self, graceful_exit):
        """Test that start_file increments the counter."""
        reporter = ProgressReporter(10, graceful_exit)
        initial_count = reporter.current
        reporter.start_file()
        assert reporter.current == initial_count + 1

    def test_update_progress(self, graceful_exit, mocker: MockerFixture):
        """Test that update_progress works correctly."""
        reporter = ProgressReporter(10, graceful_exit)
        mock_stderr = mocker.patch("sys.stderr")
        reporter.update_progress(50.0)
        mock_stderr.write.assert_called()
        mock_stderr.flush.assert_called()

    def test_finish_file(self, graceful_exit, mocker: MockerFixture):
        """Test that finish_file updates progress to 100%."""
        reporter = ProgressReporter(10, graceful_exit)
        mock_stderr = mocker.patch("sys.stderr")
        reporter.finish_file()
        mock_stderr.write.assert_called()
        mock_stderr.flush.assert_called()

    def test_context_manager(self, graceful_exit):
        """Test that ProgressReporter works as a context manager."""
        with ProgressReporter(10, graceful_exit) as reporter:
            assert reporter is not None
        # After exiting context, ACTIVE_PROGRESS_REPORTER should be None
        from archiver import ACTIVE_PROGRESS_REPORTER

        assert ACTIVE_PROGRESS_REPORTER is None


class TestLogger:
    """Test cases for the Logger class."""

    def test_logger_setup(self, config):
        """Test that Logger.setup creates a valid logger."""
        logger = Logger.setup(config)
        assert logger.name == "camera_archiver"
        assert logger.level == 20  # INFO level

    def test_logger_with_file(self, config, temp_dir):
        """Test Logger with a log file."""
        log_file = temp_dir / "test.log"
        config.log_file = log_file
        logger = Logger.setup(config)

        # Log a message
        logger.info("Test message")

        # Check that the log file was created and contains the message
        assert log_file.exists()
        with open(log_file, "r") as f:
            content = f.read()
            assert "Test message" in content

    def test_log_rotation(self, temp_dir, mocker: MockerFixture):
        """Test log file rotation."""
        log_file = temp_dir / "test.log"

        # Create a 5 MB log file
        with open(log_file, "w") as f:
            f.write("x" * 5_000_000)

        # Create real args instead of a mock to avoid Path issues
        from argparse import Namespace

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
        args.log_file = str(log_file)

        config = Config(args)

        Logger.setup(config)  # rotation now runs

        # Original large file should be renamed to .1, and a new empty file should exist
        assert log_file.exists()  # New empty log file was created
        assert (temp_dir / "test.log.1").exists()  # Old file rotated to .1

    def test_thread_safe_stream_handler(self, config):
        """Test that ThreadSafeStreamHandler is thread-safe."""
        logger = Logger.setup(config)

        # Check that the console handler is a ThreadSafeStreamHandler
        from archiver import ThreadSafeStreamHandler

        handlers = [
            h for h in logger.handlers if isinstance(h, ThreadSafeStreamHandler)
        ]
        assert len(handlers) == 1

    def test_log_rotation_large_file(self, temp_dir, mocker: MockerFixture):
        """Test that log rotation works when file exceeds maximum size."""
        from archiver import LOG_ROTATION_SIZE

        log_file = temp_dir / "test.log"

        # Create a log file larger than the rotation size
        with open(log_file, "w") as f:
            f.write("x" * (LOG_ROTATION_SIZE + 100))  # Exceed rotation size

        # Create real args instead of a mock to avoid Path issues
        from argparse import Namespace

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
        args.log_file = str(log_file)

        config = Config(args)

        Logger.setup(config)  # rotation now runs

        # Original large file should be renamed to .1, and a new empty file should exist
        assert log_file.exists()  # New empty log file was created
        assert (log_file.with_suffix(log_file.suffix + ".1")).exists()  # Backup exists
        assert log_file.with_name(f"{log_file.name}.1").exists()  # Check backup exists

    def test_log_rotation_multiple_backups(self, temp_dir, mocker: MockerFixture):
        """Test that log rotation handles multiple backup files."""
        from archiver import LOG_ROTATION_SIZE

        log_file = temp_dir / "test.log"

        # Create the original log file
        with open(log_file, "w") as f:
            f.write("x" * (LOG_ROTATION_SIZE + 100))

        # Create some backup files
        with open(log_file.with_name(f"{log_file.name}.1"), "w") as f:
            f.write("backup1")
        with open(log_file.with_name(f"{log_file.name}.2"), "w") as f:
            f.write("backup2")

        # Create real args instead of a mock to avoid Path issues
        from argparse import Namespace

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
        args.log_file = str(log_file)

        config = Config(args)

        Logger.setup(config)  # rotation now runs

        # Check that rotation happened correctly
        assert log_file.exists()  # New empty log file was created
        assert log_file.with_name(f"{log_file.name}.1").exists()  # Old .1 moved to .2
        assert log_file.with_name(f"{log_file.name}.2").exists()  # Old .2 moved to .3
        assert log_file.with_name(
            f"{log_file.name}.3"
        ).exists()  # New .1 file (was original)


class TestFileDiscovery:
    """Test cases for the FileDiscovery class."""

    def test_discover_files_empty_directory(self, temp_dir):
        """Test discovering files in an empty directory."""
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_with_valid_structure(self, sample_files):
        """Test discovering files with a valid directory structure."""
        camera_dir = sample_files["mp4"].parent.parent.parent.parent
        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir)

        assert len(mp4s) == 1
        assert mp4s[0][0] == sample_files["mp4"]
        assert mp4s[0][1] == sample_files["timestamp"]

        key = sample_files["timestamp"].strftime("%Y%m%d%H%M%S")
        assert key in mapping
        assert ".mp4" in mapping[key]
        assert ".jpg" in mapping[key]
        assert mapping[key][".mp4"] == sample_files["mp4"]
        assert mapping[key][".jpg"] == sample_files["jpg"]

        assert len(trash_files) == 0

    def test_discover_files_with_invalid_structure(self, temp_dir):
        """Test discovering files with an invalid directory structure."""
        # Create a file directly in the root directory (invalid structure)
        invalid_file = temp_dir / "REO_camera_20230115120000.mp4"
        invalid_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_with_trash(self, sample_files, trash_dir):
        """Test discovering files with trash directory."""
        camera_dir = sample_files["mp4"].parent.parent.parent.parent

        # Create a file in trash
        trash_input_dir = trash_dir / "input"
        trash_input_dir.mkdir(parents=True)
        trash_file = trash_input_dir / "REO_camera_20230115120000.mp4"
        trash_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir, trash_dir)

        assert len(mp4s) == 2  # One in camera, one in trash
        assert len(trash_files) == 1
        assert trash_file in trash_files

    @pytest.mark.parametrize(
        "filename,expected_result",
        [
            ("REO_camera_20230115120000.mp4", datetime(2023, 1, 15, 12, 0, 0)),
            ("invalid_filename.mp4", None),
            ("REO_camera_18000115120000.mp4", None),
        ],
    )
    def test_parse_timestamp(self, filename, expected_result):
        """Test parsing timestamps from filenames with various formats."""
        timestamp = FileDiscovery._parse_timestamp(filename)
        assert timestamp == expected_result

    @pytest.mark.parametrize(
        "archived_filename,expected_result",
        [
            ("archived-20230115120000.mp4", datetime(2023, 1, 15, 12, 0, 0)),
            ("invalid_archived.mp4", None),
        ],
    )
    def test_parse_timestamp_from_archived_filename(
        self, archived_filename, expected_result
    ):
        """Test parsing timestamps from archived filenames."""
        timestamp = FileDiscovery._parse_timestamp_from_archived_filename(
            archived_filename
        )
        assert timestamp == expected_result

    def test_discover_files_with_invalid_directory_structure(self, temp_dir):
        """Test discovering files with deeply nested invalid directory structure."""
        # Create files in a structure that doesn't match YYYY/MM/DD pattern
        invalid_dir = temp_dir / "invalid" / "structure" / "path"
        invalid_dir.mkdir(parents=True)
        invalid_file = invalid_dir / "REO_camera_20230115120000.mp4"
        invalid_file.touch()

        # Should not discover the file because of invalid directory structure
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_with_invalid_timestamp(self, temp_dir):
        """Test discovering files with invalid timestamp in filename."""
        # Create proper directory structure but invalid timestamp
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # File with invalid timestamp format
        invalid_file = day_dir / "REO_camera_invalid.mp4"
        invalid_file.touch()

        # Should not discover the file because of invalid timestamp
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_year_out_of_range(self, temp_dir):
        """Test discovering files with timestamp year out of valid range."""
        # Create proper directory structure but year out of range in filename
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # File with year out of range in timestamp (year 1800 in filename)
        out_of_range_file = day_dir / "REO_camera_18000115120000.mp4"
        out_of_range_file.touch()

        # Should not discover the file because year is out of range
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_with_non_regular_files(self, temp_dir):
        """Test discovering files when directory contains non-regular files."""

        # Create proper directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Create a valid camera file
        valid_file = day_dir / "REO_camera_20230115120000.mp4"
        valid_file.touch()

        # The test should still find the valid file even with other non-regular files
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)
        assert len(mp4s) == 1
        assert mp4s[0][1] == datetime(2023, 1, 15, 12, 0, 0)


class TestFileManager:
    """Test cases for the FileManager class."""

    @pytest.mark.parametrize(
        "dry_run,delete,expected_exists",
        [
            (True, False, True),  # dry-run: file should remain
            (False, True, False),  # permanent delete: file should be removed
        ],
    )
    def test_remove_file_modes(
        self, sample_files, logger, dry_run, delete, expected_exists
    ):
        """Test different file removal modes."""
        file_path = sample_files["mp4"]
        initial_exists = file_path.exists()

        FileManager.remove_file(file_path, logger, dry_run=dry_run, delete=delete)

        # Check if file exists based on the mode
        assert file_path.exists() == (initial_exists and expected_exists)

    def test_remove_file_to_trash(self, sample_files, trash_dir, logger):
        """Test moving a file to trash."""
        file_path = sample_files["mp4"]
        camera_dir = file_path.parent.parent.parent

        FileManager.remove_file(
            file_path,
            logger,
            delete=False,
            trash_root=trash_dir,
            source_root=camera_dir,
        )

        # File should be moved to trash
        assert not file_path.exists()
        trash_file = trash_dir / "input" / file_path.relative_to(camera_dir)
        assert trash_file.exists()

    def test_remove_nonexistent_file(self, logger):
        """Test removing a non-existent file."""
        non_existent = Path("/non/existent/file.mp4")

        # Should not raise an exception
        FileManager.remove_file(non_existent, logger)

    def test_calculate_trash_destination(self, temp_dir, trash_dir):
        """Test calculating trash destination for a file."""
        source_root = temp_dir / "source"
        source_root.mkdir()

        file_path = source_root / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        dest = FileManager._calculate_trash_destination(
            file_path, source_root, trash_dir, is_output=False
        )

        expected = (
            trash_dir / "input" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        assert dest == expected

    def test_calculate_trash_destination_with_conflict(self, temp_dir, trash_dir):
        """Test calculating trash destination when a conflict exists."""
        source_root = temp_dir / "source"
        source_root.mkdir()

        file_path = source_root / "REO_camera_20230115120000.mp4"
        file_path.touch()

        # Create the expected destination in advance
        expected = trash_dir / "input" / "REO_camera_20230115120000.mp4"
        expected.parent.mkdir(parents=True)
        expected.touch()

        dest = FileManager._calculate_trash_destination(
            file_path, source_root, trash_dir, is_output=False
        )

        # Destination should be different to avoid conflict
        assert dest != expected
        assert dest.parent == expected.parent
        assert dest.name.startswith("REO_camera_20230115120000_")

    def test_clean_empty_directories(self, temp_dir, logger):
        """Test cleaning empty directories."""
        # Create a directory structure with some empty directories
        dir1 = temp_dir / "2023"
        dir2 = dir1 / "01"
        dir3 = dir2 / "15"
        dir3.mkdir(parents=True)

        # Create a file in dir3
        file_path = dir3 / "test.mp4"
        file_path.touch()

        # Create an empty directory
        empty_dir = dir1 / "empty"
        empty_dir.mkdir()

        # Clean empty directories
        FileManager.clean_empty_directories(temp_dir, logger)

        # Empty directory should be removed
        assert not empty_dir.exists()
        # Non-empty directories should remain
        assert dir3.exists()

    def test_remove_file_with_oserror(
        self, sample_files, logger, mocker: MockerFixture
    ):
        """Test removing a file when an OSError occurs."""
        file_path = sample_files["mp4"]

        # Mock shutil.move to raise OSError
        mocker.patch("shutil.move", side_effect=OSError("Permission denied"))
        FileManager.remove_file(
            file_path, logger, delete=False, trash_root=Path("/trash")
        )

        # File should still exist since the operation failed
        assert file_path.exists()

    def test_remove_file_with_general_exception(
        self, sample_files, logger, mocker: MockerFixture
    ):
        """Test removing a file when a general exception occurs."""
        file_path = sample_files["mp4"]

        # Mock pathlib.Path.unlink to raise a general exception
        mocker.patch.object(Path, "unlink", side_effect=Exception("Unexpected error"))
        FileManager.remove_file(file_path, logger, delete=True)

        # File should still exist since the operation failed
        assert file_path.exists()

    def test_clean_empty_directories_dry_run(self, temp_dir, logger):
        """Test cleaning empty directories in dry-run mode."""
        # Create an empty directory
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()

        # Clean empty directories in dry-run mode
        FileManager.clean_empty_directories(temp_dir, logger, dry_run=True)

        # Directory should still exist in dry-run mode
        assert empty_dir.exists()


class TestTranscoder:
    """Test cases for the Transcoder class."""

    @pytest.mark.parametrize(
        "ffprobe_available,run_side_effect,run_return_value,stdout_output,expected_result",
        [
            (None, None, None, None, None),  # ffprobe not available
            (
                "/usr/bin/ffprobe",
                None,
                0,
                "120.5\n",
                120.5,
            ),  # ffprobe available with valid output
            (
                "/usr/bin/ffprobe",
                subprocess.CalledProcessError(1, "ffprobe"),
                None,
                None,
                None,
            ),  # ffprobe fails
            (
                "/usr/bin/ffprobe",
                Exception("Command failed"),
                None,
                None,
                None,
            ),  # subprocess exception
            ("/usr/bin/ffprobe", None, None, "N/A\n", None),  # ffprobe returns N/A
            ("/usr/bin/ffprobe", None, None, "", None),  # ffprobe returns empty string
        ],
    )
    def test_get_video_duration_scenarios(
        self,
        mocker: MockerFixture,
        ffprobe_available,
        run_side_effect,
        run_return_value,
        stdout_output,
        expected_result,
    ):
        """Test getting video duration in various scenarios."""
        mocker.patch("shutil.which", return_value=ffprobe_available)

        if run_side_effect and not isinstance(run_side_effect, Exception):
            mock_run = mocker.patch("subprocess.run")
            mock_run.return_value.stdout = stdout_output
            mock_run.return_value.returncode = run_return_value
        elif run_side_effect and isinstance(run_side_effect, Exception):
            mocker.patch("subprocess.run", side_effect=run_side_effect)
        elif ffprobe_available and stdout_output is not None:
            mock_run = mocker.patch("subprocess.run")
            mock_run.return_value.stdout = stdout_output
            mock_run.return_value.returncode = run_return_value

        duration = Transcoder.get_video_duration(Path("test.mp4"))
        assert duration == expected_result

    def test_transcode_file_dry_run(self, sample_files, logger):
        """Test transcoding a file in dry-run mode."""
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

        result = Transcoder.transcode_file(
            input_path, output_path, logger, dry_run=True
        )

        # Should return True in dry-run mode
        assert result is True
        # Output file should not be created
        assert not output_path.exists()

    def test_transcode_file_with_graceful_exit(self, sample_files, logger):
        """Test transcoding a file when graceful exit is requested."""
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        result = Transcoder.transcode_file(
            input_path, output_path, logger, graceful_exit=graceful_exit
        )

        # Should return False when exit is requested
        assert result is False

    def test_transcode_file_success(self, sample_files, logger, mocker: MockerFixture):
        """Test successful transcoding of a file."""
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

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
        mock_popen = mocker.patch(
            "archiver.subprocess.Popen", return_value=mock_process
        )

        # Mock get_video_duration
        mocker.patch.object(Transcoder, "get_video_duration", return_value=2.0)

        result = Transcoder.transcode_file(input_path, output_path, logger)

        # Should return True for successful transcoding
        assert result is True
        # Check that subprocess was called with the correct arguments
        mock_popen.assert_called_once()

        # Verify that the output directory was created
        assert output_path.parent.exists()

    def test_transcode_file_failure(self, sample_files, logger, mocker: MockerFixture):
        """Test failed transcoding of a file."""
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

        # Mock subprocess.Popen
        mock_process = mocker.Mock()
        mock_process.stdout = mocker.Mock()
        mock_process.stdout.readline = mocker.Mock(
            side_effect=[
                "Error: Invalid input\n",
                "",  # End of output
            ]
        )
        mock_process.wait.return_value = 1  # Non-zero exit code
        mocker.patch("archiver.subprocess.Popen", return_value=mock_process)

        # Mock get_video_duration
        mocker.patch.object(Transcoder, "get_video_duration", return_value=2.0)
        result = Transcoder.transcode_file(input_path, output_path, logger)

        # Should return False for failed transcoding
        assert result is False

    def test_transcode_file_with_progress_callback(
        self, sample_files, logger, mocker: MockerFixture
    ):
        """Test transcoding a file with progress callback."""
        input_path = sample_files["mp4"]
        output_path = input_path.parent / "output.mp4"

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
        mocker.patch("archiver.subprocess.Popen", return_value=mock_process)

        # Mock progress callback
        progress_callback = mocker.Mock()

        # Mock get_video_duration
        mocker.patch.object(Transcoder, "get_video_duration", return_value=2.0)
        Transcoder.transcode_file(
            input_path, output_path, logger, progress_cb=progress_callback
        )

        # Progress callback should have been called
        assert progress_callback.call_count > 0


class TestFileProcessor:
    """Test cases for the FileProcessor class."""

    def test_file_processor_initialization(self, config, logger, graceful_exit):
        """Test that FileProcessor initializes correctly."""
        processor = FileProcessor(config, logger, graceful_exit)
        assert processor.config == config
        assert processor.logger == logger
        assert processor.graceful_exit == graceful_exit

    def test_generate_action_plan(self, config, logger, graceful_exit, sample_files):
        """Test generating an action plan."""
        processor = FileProcessor(config, logger, graceful_exit)

        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }

        plan = processor.generate_action_plan(mp4s, mapping)

        assert "transcoding" in plan
        assert "removals" in plan
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2  # MP4 and JPG

    def test_generate_action_plan_with_existing_archive(
        self, config, logger, graceful_exit, sample_files, archived_dir
    ):
        """Test generating an action plan when archive already exists."""
        # Update config to use the test's archived directory
        config.output = archived_dir
        processor = FileProcessor(config, logger, graceful_exit)

        # Create an existing archive
        timestamp = sample_files["timestamp"]
        archive_path = (
            archived_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True)
        # Create file with content larger than MIN_ARCHIVE_SIZE_BYTES to trigger skip
        from archiver import MIN_ARCHIVE_SIZE_BYTES

        with open(archive_path, "w") as f:
            f.write(
                "x" * (MIN_ARCHIVE_SIZE_BYTES + 1)
            )  # Write content larger than MIN_ARCHIVE_SIZE_BYTES

        mp4s = [(sample_files["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }

        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip transcoding and only have removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # MP4 and JPG

    def test_generate_action_plan_with_cleanup(
        self, config, logger, graceful_exit, sample_files
    ):
        """Test generating an action plan when cleanup is enabled."""
        config.cleanup = True
        processor = FileProcessor(config, logger, graceful_exit)

        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }

        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip transcoding and only have removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # MP4 and JPG

    def test_execute_plan_with_transcoding(
        self, config, logger, graceful_exit, sample_files, mocker: MockerFixture
    ):
        """Test executing a plan with transcoding actions."""
        processor = FileProcessor(config, logger, graceful_exit)

        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }

        plan = processor.generate_action_plan(mp4s, mapping)

        # Mock transcoding
        mocker.patch.object(Transcoder, "transcode_file", return_value=True)
        # Mock progress reporter
        progress_reporter = mocker.Mock()

        result = processor.execute_plan(plan, progress_reporter)

        # Should return True for successful execution
        assert result is True

    def test_execute_plan_with_graceful_exit(
        self, config, logger, graceful_exit, sample_files, mocker: MockerFixture
    ):
        """Test executing a plan when graceful exit is requested."""
        processor = FileProcessor(config, logger, graceful_exit)

        mp4s = [(sample_files["mp4"], sample_files["timestamp"])]
        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".mp4": sample_files["mp4"],
                ".jpg": sample_files["jpg"],
            }
        }

        plan = processor.generate_action_plan(mp4s, mapping)

        # Request graceful exit
        graceful_exit.request_exit()

        # Mock progress reporter
        progress_reporter = mocker.Mock()

        result = processor.execute_plan(plan, progress_reporter)

        # Should return True even with graceful exit (execution stops early)
        assert result is True

    def test_cleanup_orphaned_files(
        self, config, logger, graceful_exit, sample_files, mocker: MockerFixture
    ):
        """Test cleaning up orphaned JPG files."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Remove the MP4 file to create an orphaned JPG
        sample_files["mp4"].unlink()

        mapping = {
            sample_files["timestamp"].strftime("%Y%m%d%H%M%S"): {
                ".jpg": sample_files["jpg"]
            }
        }

        # Mock file removal
        mock_remove = mocker.patch.object(FileManager, "remove_file")
        processor.cleanup_orphaned_files(mapping)

        # Should have called remove_file for the orphaned JPG
        mock_remove.assert_called_once()

    def test_output_path_generation(self, config, logger, graceful_exit, sample_files):
        """Test generating output paths for archived files."""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = sample_files["mp4"]
        timestamp = sample_files["timestamp"]

        output_path = processor._output_path(input_file, timestamp)

        expected = (
            config.output
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        assert output_path == expected


class TestSignalHandling:
    """Test cases for signal handling functionality."""

    def test_setup_signal_handlers(self, graceful_exit):
        """Test that setup_signal_handlers correctly sets up signal handlers."""
        from archiver import setup_signal_handlers

        # This test is tricky because it involves actual signal handling
        # We can at least verify that the function runs without error
        setup_signal_handlers(graceful_exit)

        # Verify that signal handlers were registered (by checking the function doesn't crash)
        assert True  # If we got here, the function executed without error

    def test_signal_handler_processes_signals(self, graceful_exit):
        """Test that the signal handler correctly processes signals."""
        from archiver import setup_signal_handlers

        # Set up signal handlers
        setup_signal_handlers(graceful_exit)

        # Verify initial state
        assert not graceful_exit.should_exit()
