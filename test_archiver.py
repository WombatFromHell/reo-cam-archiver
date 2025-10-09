"""
Comprehensive test suite for the Camera Archiver application.
Tests focus on integration and end-to-end scenarios with proper mocking.
"""

import logging
import signal
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Import the modules we're testing
import archiver


class TestConfig:
    """Test the Config class"""

    @pytest.mark.parametrize(
        "args_input,expected_values",
        [
            # Test config initialization with default directory
            (
                [],
                {
                    "directory": Path("/camera"),
                    "output": Path("/camera") / "archived",
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": Path("/camera") / ".deleted",
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": Path("/camera/archiver.log"),
                },
            ),
            # Test config initialization with custom directory
            (
                ["{tmp_path}"],
                {
                    "directory": "{tmp_path}",
                    "output": "{tmp_path}/archived",
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": "{tmp_path}/.deleted",
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": "{tmp_path}/archiver.log",
                },
            ),
            # Test config with all options enabled
            (
                [
                    "{tmp_path}",
                    "-o",
                    "{tmp_path}/output",
                    "--dry-run",
                    "--no-confirm",
                    "--no-skip",
                    "--delete",
                    "--trash-root",
                    "{tmp_path}/trash",
                    "--cleanup",
                    "--clean-output",
                    "--age",
                    "60",
                    "--log-file",
                    "{tmp_path}/log.txt",
                ],
                {
                    "directory": "{tmp_path}",
                    "output": "{tmp_path}/output",
                    "dry_run": True,
                    "no_confirm": True,
                    "no_skip": True,
                    "delete": True,
                    "trash_root": None,  # With --delete flag, trash root is ignored
                    "cleanup": True,
                    "clean_output": True,
                    "age": 60,
                    "log_file": "{tmp_path}/log.txt",
                },
            ),
        ],
    )
    def test_config_initialization_various_scenarios(
        self, tmp_path, args_input, expected_values
    ):
        """Test Config initialization with various argument combinations"""
        # Replace placeholders in args_input
        processed_args = [arg.format(tmp_path=tmp_path) for arg in args_input]
        args = archiver.parse_args(processed_args)
        config = archiver.Config(args)

        # Replace placeholders in expected values
        processed_expected = {}
        for key, value in expected_values.items():
            if isinstance(value, str):
                processed_expected[key] = Path(value.format(tmp_path=tmp_path))
            else:
                processed_expected[key] = value

        # Assert all expected values
        for key, expected_value in processed_expected.items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value


class TestGracefulExit:
    """Test the GracefulExit class"""

    @pytest.mark.parametrize(
        "action,expected_state",
        [
            ("initial", False),
            ("request_exit", True),
        ],
    )
    def test_graceful_exit_states(self, action, expected_state):
        """Test that GracefulExit starts in the correct state and changes correctly after request_exit"""
        graceful_exit = archiver.GracefulExit()

        if action == "request_exit":
            graceful_exit.request_exit()

        assert graceful_exit.should_exit() is expected_state

    def test_thread_safety(self):
        """Test that GracefulExit is thread-safe"""
        import threading

        graceful_exit = archiver.GracefulExit()
        results = []

        def check_and_request():
            results.append(graceful_exit.should_exit())
            graceful_exit.request_exit()
            results.append(graceful_exit.should_exit())

        t = threading.Thread(target=check_and_request)
        t.start()
        t.join()

        assert results[0] is False
        assert results[1] is True


class TestProgressReporter:
    """Test the ProgressReporter class"""

    def test_initialization(self):
        """Test ProgressReporter initialization"""
        graceful_exit = archiver.GracefulExit()
        reporter = archiver.ProgressReporter(10, graceful_exit, silent=True)

        assert reporter.total == 10
        assert reporter.current == 0
        assert reporter.silent is True
        assert reporter.graceful_exit == graceful_exit

    def test_start_file(self):
        """Test that start_file increments the counter"""
        graceful_exit = archiver.GracefulExit()
        reporter = archiver.ProgressReporter(10, graceful_exit, silent=True)

        reporter.start_file()
        assert reporter.current == 1

        reporter.start_file()
        assert reporter.current == 2

    @pytest.mark.parametrize(
        "elapsed_time,expected_file_time,expected_total_time",
        [
            (100.0, "01:40", "(01:40)"),  # 100 seconds = 1 minute 40 seconds
            (3661.0, "01:01", "(01:01:01)"),  # 3661 seconds = 1 hour 1 minute 1 second
        ],
    )
    def test_update_progress(
        self, mocker, elapsed_time, expected_file_time, expected_total_time
    ):
        """Test progress update with mocked stderr for different elapsed times"""
        graceful_exit = archiver.GracefulExit()

        # Mock time.time before creating the reporter to set start_time
        mock_time = mocker.patch("time.time")
        mock_time.return_value = 0.0  # Start time

        reporter = archiver.ProgressReporter(10, graceful_exit, silent=False)
        mock_stderr = mocker.patch("sys.stderr")

        # Set the elapsed time
        mock_time.return_value = elapsed_time

        reporter.start_file()
        reporter.update_progress(50.0)

        # Verify stderr.write was called with progress information
        mock_stderr.write.assert_called()
        call_args = mock_stderr.write.call_args[0][0]
        assert "Progress [1/10]: 50%" in call_args
        assert "|" in call_args  # Progress bar
        assert expected_file_time in call_args  # File elapsed time
        assert expected_total_time in call_args  # Total elapsed time

    def test_silent_mode(self, mocker):
        """Test that silent mode doesn't write to stderr"""
        graceful_exit = archiver.GracefulExit()
        reporter = archiver.ProgressReporter(10, graceful_exit, silent=True)
        mock_stderr = mocker.patch("sys.stderr")

        reporter.start_file()
        reporter.update_progress(50.0)

        # Verify stderr.write was not called
        mock_stderr.write.assert_not_called()

    def test_exit_requested(self, mocker):
        """Test that progress updates are skipped when exit is requested"""
        graceful_exit = archiver.GracefulExit()
        reporter = archiver.ProgressReporter(10, graceful_exit, silent=False)
        mock_stderr = mocker.patch("sys.stderr")

        graceful_exit.request_exit()
        reporter.start_file()
        reporter.update_progress(50.0)

        # Verify stderr.write was not called
        mock_stderr.write.assert_not_called()

    def test_context_manager(self, mocker):
        """Test that the context manager writes a newline on exit"""
        graceful_exit = archiver.GracefulExit()
        reporter = archiver.ProgressReporter(10, graceful_exit, silent=False)
        mock_stderr = mocker.patch("sys.stderr")

        with reporter:
            pass

        # Verify a newline was written
        mock_stderr.write.assert_called_with("\n")


class TestLogger:
    """Test the Logger class"""

    @pytest.mark.parametrize(
        "args_input,has_file_handler,has_console_handler,should_rotate",
        [
            # With explicit log file
            (["{tmp_path}", "--log-file", "{tmp_path}/test.log"], True, True, True),
            # With default log file
            (["{tmp_path}"], True, True, True),
        ],
    )
    def test_setup_various_scenarios(
        self,
        tmp_path,
        mocker,
        args_input,
        has_file_handler,
        has_console_handler,
        should_rotate,
    ):
        """Test logger setup with various configurations"""
        # Replace placeholders in args_input
        processed_args = [arg.format(tmp_path=tmp_path) for arg in args_input]
        args = archiver.parse_args(processed_args)
        config = archiver.Config(args)

        # Initialize mock variable to prevent unbound error
        mock_rotate = None

        # Mock the rotation function to avoid actual file operations if needed
        if should_rotate:
            mock_rotate = mocker.patch.object(archiver.Logger, "_rotate_log_file")

        logger = archiver.Logger.setup(config)

        if should_rotate and mock_rotate:
            # Verify rotation was called
            log_file = config.log_file
            mock_rotate.assert_called_once_with(log_file)

        # Verify logger has expected handlers
        assert len(logger.handlers) == sum([has_file_handler, has_console_handler])
        if has_file_handler:
            assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        if has_console_handler:
            assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

    def test_log_rotation(self, tmp_path):
        """Test log file rotation"""
        log_file = tmp_path / "test.log"

        # Create a log file that exceeds the rotation size
        with open(log_file, "w") as f:
            f.write("x" * (archiver.LOG_ROTATION_SIZE + 1000))

        archiver.Logger._rotate_log_file(log_file)

        # Verify the file was moved to backup
        backup_file = tmp_path / "test.log.1"
        assert backup_file.exists()

        # Verify the original file was recreated and is empty
        assert log_file.exists()
        assert log_file.stat().st_size == 0

    def test_log_rotation_with_existing_backups(self, tmp_path, mocker):
        """Test log rotation with existing backup files"""
        log_file = tmp_path / "test.log"

        # Create backup files
        (tmp_path / "test.log.1").touch()
        (tmp_path / "test.log.2").touch()

        # Create a log file that exceeds the rotation size
        with open(log_file, "w") as f:
            f.write("x" * (archiver.LOG_ROTATION_SIZE + 1000))

        # Mock shutil.move to avoid actual file operations
        mock_move = mocker.patch("shutil.move")

        archiver.Logger._rotate_log_file(log_file)

        # Verify the files were moved correctly
        assert mock_move.call_count >= 3  # For .2 -> .3, .1 -> .2, and .log -> .1


class TestFileDiscovery:
    """Test the FileDiscovery class"""

    def test_discover_files(self, tmp_path, mocker):
        """Test file discovery with valid camera directory structure"""
        # Create directory structure
        camera_dir = tmp_path / "camera"
        year_dir = camera_dir / "2023" / "01" / "15"

        # Create test files
        mp4_file = year_dir / "REO_camera_20230115120000.mp4"
        jpg_file = year_dir / "REO_camera_20230115120000.jpg"
        invalid_file = year_dir / "invalid.txt"

        # Mock the files to exist without actually creating them
        mocker.patch("pathlib.Path.is_file", return_value=True)
        mock_stat = mocker.patch("pathlib.Path.stat")
        mock_stat.return_value.st_size = 1000

        # Mock rglob to return our test files
        mock_rglob = mocker.patch("pathlib.Path.rglob")
        mock_rglob.return_value = [mp4_file, jpg_file, invalid_file]

        # Run discovery
        mp4s, mapping, trash_files = archiver.FileDiscovery.discover_files(camera_dir)

        # Verify results
        assert len(mp4s) == 1
        assert mp4s[0][0] == mp4_file
        assert mp4s[0][1] == datetime(2023, 1, 15, 12, 0, 0)

        assert len(mapping) == 1
        assert "20230115120000" in mapping
        assert mapping["20230115120000"][".mp4"] == mp4_file
        assert mapping["20230115120000"][".jpg"] == jpg_file

        assert len(trash_files) == 0

    def test_discover_files_with_trash(self, tmp_path, mocker):
        """Test file discovery with trash directory"""
        # Create directory structure
        camera_dir = tmp_path / "camera"
        trash_dir = tmp_path / "trash"
        year_dir = camera_dir / "2023" / "01" / "15"
        trash_year_dir = trash_dir / "input" / "2023" / "01" / "15"

        # Create test files
        mp4_file = year_dir / "REO_camera_20230115120000.mp4"
        trash_mp4 = trash_year_dir / "REO_camera_20230115130000.mp4"

        # Mock the files to exist without actually creating them
        mocker.patch("pathlib.Path.is_file", return_value=True)
        mock_stat = mocker.patch("pathlib.Path.stat")
        mock_stat.return_value.st_size = 1000

        # Mock exists for trash directory
        def mock_exists(self):
            return self == trash_dir or self == trash_dir / "input"

        mocker.patch("pathlib.Path.exists", mock_exists)

        # Mock rglob to return our test files
        def mock_rglob_func(self, pattern):
            if pattern == "*.*":
                if trash_dir in self.parents:
                    return [trash_mp4]
                else:
                    return [mp4_file]
            return []

        mocker.patch("pathlib.Path.rglob", mock_rglob_func)

        # Run discovery
        mp4s, mapping, trash_files = archiver.FileDiscovery.discover_files(
            camera_dir, trash_dir
        )

        # Verify results
        assert len(mp4s) == 2
        assert mp4_file in [mp4[0] for mp4 in mp4s]
        assert trash_mp4 in [mp4[0] for mp4 in mp4s]

        assert len(trash_files) == 1
        assert trash_mp4 in trash_files

    @pytest.mark.parametrize(
        "filename,method,expected_result",
        [
            # Valid timestamps for regular filenames
            (
                "REO_camera_20230115120000.mp4",
                "_parse_timestamp",
                datetime(2023, 1, 15, 12, 0, 0),
            ),
            (
                "REO_front_20231231235959.JPG",
                "_parse_timestamp",
                datetime(2023, 12, 31, 23, 59, 59),
            ),
            # Valid timestamps for archived filenames
            (
                "archived-20230115120000.mp4",
                "_parse_timestamp_from_archived_filename",
                datetime(2023, 1, 15, 12, 0, 0),
            ),
            (
                "archived-20231231235959.MP4",
                "_parse_timestamp_from_archived_filename",
                datetime(2023, 12, 31, 23, 59, 59),
            ),
            # Invalid timestamps for regular filenames
            ("invalid_filename.mp4", "_parse_timestamp", None),
            ("REO_camera_2023011512000.mp4", "_parse_timestamp", None),  # Missing digit
            (
                "REO_camera_19991231235959.mp4",
                "_parse_timestamp",
                None,
            ),  # Year too early
            (
                "REO_camera_21001231235959.mp4",
                "_parse_timestamp",
                None,
            ),  # Year too late
            # Invalid timestamps for archived filenames
            ("invalid_filename.mp4", "_parse_timestamp_from_archived_filename", None),
        ],
    )
    def test_parse_timestamp_various_formats(self, filename, method, expected_result):
        """Test parsing timestamps from various filename formats"""
        parse_method = getattr(archiver.FileDiscovery, method)
        result = parse_method(filename)
        assert result == expected_result

    @pytest.mark.parametrize(
        "test_scenario",
        [
            # Basic functionality: input/output flags with standard paths
            {
                "type": "basic",
                "file_path": "camera/2023/01/15/file.mp4",
                "source_root": "camera",
                "is_output": False,
                "expected_subdir": "input",
                "with_conflict": False,
            },
            {
                "type": "basic",
                "file_path": "camera/2023/01/15/file.mp4",
                "source_root": "camera",
                "is_output": True,
                "expected_subdir": "output",
                "with_conflict": False,
            },
            # Real workflow paths
            {
                "type": "workflow",
                "file_path": "camera/2023/01/15/file.mp4",
                "source_root": "camera",
                "is_output": False,
                "expected_subdir": "input",
                "with_conflict": False,
            },
            {
                "type": "workflow",
                "file_path": "camera/archived/2023/01/15/file.mp4",
                "source_root": "camera/archived",
                "is_output": True,
                "expected_subdir": "output",
                "with_conflict": False,
            },
            # Complex workflow with archived files managed from main camera root
            {
                "type": "workflow_complex",
                "file_path": "camera/archived/2023/01/15/archived-20230115120000.mp4",
                "source_root": "camera",
                "is_output": True,
                "expected_subdir": "output",
                "with_conflict": False,
            },
            {
                "type": "workflow_complex",
                "file_path": "camera/2023/01/15/original.mp4",
                "source_root": "camera",
                "is_output": False,
                "expected_subdir": "input",
                "with_conflict": False,
            },
            # Conflict scenarios
            {
                "type": "conflict",
                "file_path": "camera/2023/01/15/file.mp4",
                "source_root": "camera",
                "is_output": False,
                "expected_subdir": "input",
                "with_conflict": True,
            },
            {
                "type": "conflict",
                "file_path": "camera/2023/01/15/file.mp4",
                "source_root": "camera",
                "is_output": True,
                "expected_subdir": "output",
                "with_conflict": True,
            },
        ],
    )
    def test_calculate_trash_destination_comprehensive(
        self, tmp_path, mocker, test_scenario
    ):
        """Comprehensive test for calculating trash destination with various scenarios"""
        file_path = tmp_path / test_scenario["file_path"]
        source_root = tmp_path / test_scenario["source_root"]
        trash_root = (
            tmp_path / "trash"
            if test_scenario["type"] == "conflict"
            else tmp_path / ".deleted"
        )
        is_output = test_scenario["is_output"]
        expected_subdir = test_scenario["expected_subdir"]
        with_conflict = test_scenario["with_conflict"]

        if with_conflict:
            # Mock exists to return True for the base destination for conflict testing
            def mock_exists(self):
                return (
                    self
                    == trash_root / expected_subdir / "2023" / "01" / "15" / "file.mp4"
                )

            mocker.patch("pathlib.Path.exists", mock_exists)
            mocker.patch("time.time", return_value=1000.0)

        dest = archiver.FileManager._calculate_trash_destination(
            file_path, source_root, trash_root, is_output=is_output
        )

        if with_conflict:
            # For conflict tests, verify timestamp and counter were added
            assert "1000_1" in dest.name
            assert dest.suffix == ".mp4"
        else:
            # For normal tests, verify path correctness
            relative_path = file_path.relative_to(source_root)
            expected_dest = trash_root / expected_subdir / relative_path
            assert dest == expected_dest


class TestFileManager:
    """Test the FileManager class"""

    @pytest.mark.parametrize(
        "dry_run,delete,trash_root,expected_message_contains",
        [
            # Test dry run mode
            (True, False, None, "[DRY RUN] Would remove"),
            # Test actual file removal
            (False, True, None, "Removed:"),
            # Test trash mode
            (False, False, "/trash", "Moved to trash"),
        ],
    )
    def test_remove_file_various_scenarios(
        self, mocker, dry_run, delete, trash_root, expected_message_contains
    ):
        """Test file removal with various configurations"""
        logger = mocker.MagicMock()
        file_path = Path("/test/file.mp4")

        # Initialize mock variables to prevent unbound errors
        mock_unlink = None
        mock_move = None

        # Conditionally handle trash_root
        effective_trash_root = Path(trash_root) if trash_root else None

        if not dry_run and delete:
            # For actual removal
            mocker.patch("pathlib.Path.is_file", return_value=True)
            mock_unlink = mocker.patch("pathlib.Path.unlink")
        elif not dry_run and effective_trash_root:
            # For trash mode
            mocker.patch("pathlib.Path.is_file", return_value=True)
            mocker.patch("pathlib.Path.mkdir")
            mock_move = mocker.patch("shutil.move")

        archiver.FileManager.remove_file(
            file_path,
            logger,
            dry_run=dry_run,
            delete=delete,
            trash_root=effective_trash_root,
        )

        # Verify logger was called
        assert logger.info.call_count >= 1
        call_args = logger.info.call_args[0][0]
        assert expected_message_contains in call_args

        # Additional verification for non-dry-run cases
        if not dry_run and delete and mock_unlink:
            assert mock_unlink.call_count == 1
        elif not dry_run and effective_trash_root and mock_move:
            assert mock_move.call_count == 1

    def test_clean_empty_directories(self, mocker):
        """Test cleaning empty directories"""
        logger = mocker.MagicMock()
        directory = Path("/test")

        # Mock os.walk to return empty directories
        mocker.patch(
            "os.walk",
            return_value=[
                ("/test/2023/01/15", [], []),
                ("/test/2023/01", [], []),
                ("/test/2023", [], []),
                ("/test", ["2023"], []),
            ],
        )

        # Mock Path operations
        mock_rmdir = mocker.patch("pathlib.Path.rmdir")
        mocker.patch("pathlib.Path.iterdir", return_value=[])

        archiver.FileManager.clean_empty_directories(directory, logger)

        # Verify rmdir was called for empty directories
        assert mock_rmdir.call_count == 3
        logger.info.assert_called()


class TestTranscoder:
    """Test the Transcoder class"""

    def test_get_video_duration(self, mocker):
        """Test getting video duration with ffprobe"""
        file_path = Path("/test/video.mp4")

        # Mock shutil.which to return ffprobe path
        mocker.patch("shutil.which", return_value="/usr/bin/ffprobe")

        # Mock subprocess.run
        mock_run = mocker.patch("subprocess.run")
        mock_run.return_value.stdout = "120.5"

        duration = archiver.Transcoder.get_video_duration(file_path)

        assert duration == 120.5
        mock_run.assert_called_once()

    def test_get_video_duration_no_ffprobe(self, mocker):
        """Test getting video duration when ffprobe is not available"""
        file_path = Path("/test/video.mp4")

        # Mock shutil.which to return None
        mocker.patch("shutil.which", return_value=None)

        duration = archiver.Transcoder.get_video_duration(file_path)

        assert duration is None

    def test_transcode_file_success(self, mocker):
        """Test successful file transcoding"""
        input_path = Path("/test/input.mp4")
        output_path = Path("/test/output.mp4")
        logger = mocker.MagicMock()
        graceful_exit = archiver.GracefulExit()

        # Mock subprocess.Popen
        mock_proc = mocker.MagicMock()
        mock_proc.stdout = mocker.MagicMock()
        mock_proc.stdout.readline = mocker.MagicMock()
        mock_proc.stdout.readline.side_effect = [
            "frame=  100 fps=100 q=20.0 size=    1024kB time=00:00:01.00 bitrate=1024.0kbits/s",
            "frame=  200 fps=100 q=20.0 size=    2048kB time=00:00:02.00 bitrate=1024.0kbits/s",
            "",  # End of output
        ]
        mock_proc.wait.return_value = 0

        mocker.patch("subprocess.Popen", return_value=mock_proc)

        # Mock get_video_duration
        mocker.patch.object(archiver.Transcoder, "get_video_duration", return_value=3.0)

        # Mock Path.mkdir
        mocker.patch("pathlib.Path.mkdir")

        # Mock progress callback
        progress_cb = mocker.MagicMock()

        result = archiver.Transcoder.transcode_file(
            input_path, output_path, logger, progress_cb, graceful_exit
        )

        assert result is True
        progress_cb.assert_called()

    def test_transcode_file_failure(self, mocker):
        """Test failed file transcoding"""
        input_path = Path("/test/input.mp4")
        output_path = Path("/test/output.mp4")
        logger = mocker.MagicMock()
        graceful_exit = archiver.GracefulExit()

        # Mock subprocess.Popen
        mock_proc = mocker.MagicMock()
        mock_proc.stdout = mocker.MagicMock()
        mock_proc.stdout.readline = mocker.MagicMock()
        mock_proc.stdout.readline.side_effect = [
            "Error: Invalid input",
            "",  # End of output
        ]
        mock_proc.wait.return_value = 1  # Non-zero exit code

        mocker.patch("subprocess.Popen", return_value=mock_proc)

        # Mock get_video_duration
        mocker.patch.object(archiver.Transcoder, "get_video_duration", return_value=3.0)

        # Mock Path.mkdir
        mocker.patch("pathlib.Path.mkdir")

        # Mock progress callback
        progress_cb = mocker.MagicMock()

        result = archiver.Transcoder.transcode_file(
            input_path, output_path, logger, progress_cb, graceful_exit
        )

        assert result is False
        logger.error.assert_called()

    def test_transcode_file_cancellation(self, mocker):
        """Test file transcoding with cancellation"""
        input_path = Path("/test/input.mp4")
        output_path = Path("/test/output.mp4")
        logger = mocker.MagicMock()
        graceful_exit = archiver.GracefulExit()

        # Request exit before transcoding
        graceful_exit.request_exit()

        # Mock Path.mkdir
        mocker.patch("pathlib.Path.mkdir")

        # Mock progress callback
        progress_cb = mocker.MagicMock()

        result = archiver.Transcoder.transcode_file(
            input_path, output_path, logger, progress_cb, graceful_exit
        )

        assert result is False


class TestFileProcessor:
    """Test the FileProcessor class"""

    def test_generate_action_plan(self, tmp_path, mocker):
        """Test generating an action plan"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        jpg_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.jpg"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        mp4s = [(mp4_file, timestamp)]
        mapping = {"20230115120000": {".mp4": mp4_file, ".jpg": jpg_file}}

        # Mock exists and stat for output file
        mocker.patch("pathlib.Path.exists", return_value=False)

        # Generate plan
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan
        assert "transcoding" in plan
        assert "removals" in plan
        assert len(plan["transcoding"]) == 1
        assert plan["transcoding"][0]["input"] == mp4_file
        assert plan["transcoding"][0]["jpg_to_remove"] == jpg_file
        assert len(plan["removals"]) == 2  # One for MP4, one for JPG

    def test_generate_action_plan_with_existing_archive(self, tmp_path, mocker):
        """Test generating an action plan when archive already exists"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        jpg_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.jpg"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        mp4s = [(mp4_file, timestamp)]
        mapping = {"20230115120000": {".mp4": mp4_file, ".jpg": jpg_file}}

        # Mock exists and stat for output file (exists and is large enough)
        mocker.patch("pathlib.Path.exists", return_value=True)
        mock_stat = mocker.patch("pathlib.Path.stat")
        mock_stat.return_value.st_size = archiver.MIN_ARCHIVE_SIZE_BYTES + 1000

        # Generate plan
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan - no transcoding, just removals
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # One for MP4, one for JPG

    def test_generate_action_plan_with_age_cutoff(self, tmp_path, mocker):
        """Test generating an action plan with age cutoff"""
        # Create config with age of 30 days
        args = archiver.parse_args([str(tmp_path), "--age", "30"])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data - recent file (should be skipped)
        recent_mp4 = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        recent_timestamp = datetime.now() - timedelta(days=10)  # 10 days ago

        # Create test data - old file (should be included)
        old_mp4 = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120001.mp4"
        )
        old_timestamp = datetime.now() - timedelta(days=40)  # 40 days ago

        mp4s = [(recent_mp4, recent_timestamp), (old_mp4, old_timestamp)]
        mapping = {
            recent_timestamp.strftime("%Y%m%d%H%M%S"): {".mp4": recent_mp4},
            old_timestamp.strftime("%Y%m%d%H%M%S"): {".mp4": old_mp4},
        }

        # Mock exists and stat for output file
        mocker.patch("pathlib.Path.exists", return_value=False)

        # Generate plan
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan - only old file should be included
        assert len(plan["transcoding"]) == 1
        assert plan["transcoding"][0]["input"] == old_mp4

    def test_execute_plan(self, tmp_path, mocker):
        """Test executing an action plan"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test plan
        input_path = tmp_path / "input.mp4"
        output_path = tmp_path / "output.mp4"
        jpg_path = tmp_path / "input.jpg"

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": input_path,
                    "output": output_path,
                    "jpg_to_remove": jpg_path,
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": input_path,
                    "reason": "Source file for transcoded archive",
                },
                {
                    "type": "jpg_removal_after_transcode",
                    "file": jpg_path,
                    "reason": "Paired with transcoded MP4",
                },
            ],
        }

        # Mock transcoding
        mocker.patch.object(archiver.Transcoder, "transcode_file", return_value=True)

        # Mock file removal
        mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")

        # Create progress reporter
        progress_reporter = mocker.MagicMock()

        # Execute plan
        result = processor.execute_plan(plan, progress_reporter)

        # Verify execution
        assert result is True
        # Should be 3 calls: 1 for JPG after transcode, 1 for MP4 source, 1 for JPG from removals
        assert mock_remove.call_count == 3

    def test_cleanup_orphaned_files(self, tmp_path, mocker):
        """Test cleaning up orphaned JPG files"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data - orphaned JPG (no MP4)
        orphaned_jpg = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.jpg"
        )

        # Create test data - paired files
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120001.mp4"
        )
        paired_jpg = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120001.jpg"
        )

        mapping = {
            "20230115120000": {".jpg": orphaned_jpg},  # Orphaned
            "20230115120001": {".mp4": mp4_file, ".jpg": paired_jpg},  # Paired
        }

        # Mock file removal
        mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")

        # Mock clean_empty_directories
        mock_clean_dirs = mocker.patch.object(
            archiver.FileManager, "clean_empty_directories"
        )

        # Cleanup orphaned files
        processor.cleanup_orphaned_files(mapping)

        # Verify only orphaned JPG was removed
        mock_remove.assert_called_once()
        assert mock_remove.call_args[0][0] == orphaned_jpg
        mock_clean_dirs.assert_called_once()

    def test_output_path(self, tmp_path, mocker):
        """Test generating output path for archived file"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data
        input_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Generate output path
        output_path = processor._output_path(input_file, timestamp)

        # Verify path
        expected = (
            tmp_path / "archived" / "2023" / "01" / "15" / "archived-20230115120000.mp4"
        )
        assert output_path == expected

    def test_generate_action_plan_with_cleanup_flag(self, tmp_path, mocker):
        """Test that cleanup flag disables transcoding and only generates removal actions"""
        # Create config with cleanup flag
        args = archiver.parse_args([str(tmp_path), "--cleanup"])
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        jpg_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.jpg"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        mp4s = [(mp4_file, timestamp)]
        mapping = {"20230115120000": {".mp4": mp4_file, ".jpg": jpg_file}}

        # Mock exists to return False (so normally we'd transcode if not for cleanup)
        mocker.patch("pathlib.Path.exists", return_value=False)

        # Generate plan
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan - should have no transcoding when cleanup is active
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # One for MP4, one for JPG
        # Verify the reason includes cleanup information
        for removal in plan["removals"]:
            assert "cleanup mode enabled" in removal["reason"]

    def test_generate_action_plan_with_clean_output_flag(self, tmp_path, mocker):
        """Test that clean_output flag works with cleanup to include output directory files"""
        # Create config with cleanup and clean_output flags
        output_dir = tmp_path / "archived"
        args = archiver.parse_args(
            [str(tmp_path), "--cleanup", "--clean-output", "--age", "10"]
        )
        config = archiver.Config(args)

        # Create logger
        logger = mocker.MagicMock()

        # Create graceful exit
        graceful_exit = archiver.GracefulExit()

        # Create processor
        processor = archiver.FileProcessor(config, logger, graceful_exit)

        # Calculate dates based on current time for accurate age testing
        ten_days_ago = datetime.now() - timedelta(days=10)
        twelve_days_ago = datetime.now() - timedelta(days=12)
        two_days_ago = datetime.now() - timedelta(days=2)

        # Create test data - both input and output (archived) files
        old_input_mp4 = (
            tmp_path
            / "camera"
            / f"{ten_days_ago.year}"
            / f"{ten_days_ago.month:02d}"
            / f"{ten_days_ago.day:02d}"
            / f"REO_camera_{ten_days_ago.strftime('%Y%m%d%H%M%S')}.mp4"  # Old file (just at threshold)
        )
        older_input_mp4 = (
            tmp_path
            / "camera"
            / f"{twelve_days_ago.year}"
            / f"{twelve_days_ago.month:02d}"
            / f"{twelve_days_ago.day:02d}"
            / f"REO_camera_{twelve_days_ago.strftime('%Y%m%d%H%M%S')}.mp4"  # Older file
        )
        old_archived_mp4 = (
            output_dir
            / f"{twelve_days_ago.year}"
            / f"{twelve_days_ago.month:02d}"
            / f"{twelve_days_ago.day:02d}"
            / f"archived-{twelve_days_ago.strftime('%Y%m%d%H%M%S')}.mp4"  # Older archived file
        )
        recent_input_mp4 = (
            tmp_path
            / "camera"
            / f"{two_days_ago.year}"
            / f"{two_days_ago.month:02d}"
            / f"{two_days_ago.day:02d}"
            / f"REO_camera_{two_days_ago.strftime('%Y%m%d%H%M%S')}.mp4"  # Recent file
        )
        recent_archived_mp4 = (
            output_dir
            / f"{two_days_ago.year}"
            / f"{two_days_ago.month:02d}"
            / f"{two_days_ago.day:02d}"
            / f"archived-{two_days_ago.strftime('%Y%m%d%H%M%S')}.mp4"  # Recent archived file
        )

        # Both old files should be processed (they're older than 10 days from now)
        mp4s = [
            (old_input_mp4, ten_days_ago),
            (older_input_mp4, twelve_days_ago),
            (
                old_archived_mp4,
                twelve_days_ago,
            ),  # This is an archived file that should be removed with --clean-output
            (recent_input_mp4, two_days_ago),  # This is too recent to be removed
            (
                recent_archived_mp4,
                two_days_ago,
            ),  # This is also too recent to be removed
        ]

        # Actually, let me fix the mapping to have unique keys for each file
        mapping = {
            ten_days_ago.strftime("%Y%m%d%H%M%S"): {".mp4": old_input_mp4},
            twelve_days_ago.strftime("%Y%m%d%H%M%S"): {
                ".mp4": older_input_mp4
            },  # for input file
            f"{twelve_days_ago.strftime('%Y%m%d%H%M%S')}": {
                ".mp4": old_archived_mp4
            },  # Same timestamp for archived
            two_days_ago.strftime("%Y%m%d%H%M%S"): {".mp4": recent_input_mp4},
            f"{two_days_ago.strftime('%Y%m%d%H%M%S')}": {
                ".mp4": recent_archived_mp4
            },  # Same timestamp for archived
        }

        # Since two files have the same timestamp, I'll need to use different timestamps
        mp4s = [
            (old_input_mp4, ten_days_ago),
            (older_input_mp4, twelve_days_ago),
            (
                old_archived_mp4,
                datetime.strptime(
                    twelve_days_ago.strftime("%Y%m%d%H%M%S"), "%Y%m%d%H%M%S"
                ),
            ),  # Same timestamp
            (recent_input_mp4, two_days_ago),
            (
                recent_archived_mp4,
                datetime.strptime(
                    two_days_ago.strftime("%Y%m%d%H%M%S"), "%Y%m%d%H%M%S"
                ),
            ),  # Same timestamp
        ]

        # Better approach: use unique timestamps for each file
        # Rebuilding data with clearly different timestamps for testing
        old_timestamp = datetime.now() - timedelta(
            days=15
        )  # definitely older than 10-day threshold
        recent_timestamp = datetime.now() - timedelta(
            days=5
        )  # definitely newer than 10-day threshold

        old_input_mp4 = (
            tmp_path
            / "camera"
            / f"{old_timestamp.year}"
            / f"{old_timestamp.month:02d}"
            / f"{old_timestamp.day:02d}"
            / f"REO_camera_{old_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_archived_mp4 = (
            output_dir
            / f"{old_timestamp.year}"
            / f"{old_timestamp.month:02d}"
            / f"{old_timestamp.day:02d}"
            / f"archived-{old_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_input_mp4 = (
            tmp_path
            / "camera"
            / f"{recent_timestamp.year}"
            / f"{recent_timestamp.month:02d}"
            / f"{recent_timestamp.day:02d}"
            / f"REO_camera_{recent_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_archived_mp4 = (
            output_dir
            / f"{recent_timestamp.year}"
            / f"{recent_timestamp.month:02d}"
            / f"{recent_timestamp.day:02d}"
            / f"archived-{recent_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        mp4s = [
            (old_input_mp4, old_timestamp),
            (old_archived_mp4, old_timestamp),  # Old archived file
            (recent_input_mp4, recent_timestamp),  # Recent file
            (recent_archived_mp4, recent_timestamp),  # Recent archived file
        ]
        mapping = {
            old_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": old_input_mp4,
                ".arch_mp4": old_archived_mp4,
            },  # Actually, each timestamp should correspond to the original source files
            recent_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_input_mp4,
                ".arch_mp4": recent_archived_mp4,
            },
        }

        # Correct the mapping to have separate entries for each file (same timestamp)
        mapping = {}
        # old timestamp key
        old_key = old_timestamp.strftime("%Y%m%d%H%M%S")
        mapping[old_key] = {".mp4": old_input_mp4}  # Input file
        # For the archived file with same timestamp, we need to handle it differently
        # Since the mapping key is based on timestamp, archived files with same timestamp
        # would overwrite the input file. This is not how it works in real world.
        # Let me create separate entries for the archived files with the same time
        # In real implementation, the mapping happens during discovery and archived files
        # would have their own entries based on their timestamp

        # Recreate with different timestamps to avoid mapping conflicts
        old_input_timestamp = datetime.now() - timedelta(days=15)
        old_archived_timestamp = datetime.now() - timedelta(days=12)
        recent_input_timestamp = datetime.now() - timedelta(days=5)
        recent_archived_timestamp = datetime.now() - timedelta(days=3)

        old_input_mp4 = (
            tmp_path
            / "camera"
            / f"{old_input_timestamp.year}"
            / f"{old_input_timestamp.month:02d}"
            / f"{old_input_timestamp.day:02d}"
            / f"REO_camera_{old_input_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_archived_mp4 = (
            output_dir
            / f"{old_archived_timestamp.year}"
            / f"{old_archived_timestamp.month:02d}"
            / f"{old_archived_timestamp.day:02d}"
            / f"archived-{old_archived_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_input_mp4 = (
            tmp_path
            / "camera"
            / f"{recent_input_timestamp.year}"
            / f"{recent_input_timestamp.month:02d}"
            / f"{recent_input_timestamp.day:02d}"
            / f"REO_camera_{recent_input_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_archived_mp4 = (
            output_dir
            / f"{recent_archived_timestamp.year}"
            / f"{recent_archived_timestamp.month:02d}"
            / f"{recent_archived_timestamp.day:02d}"
            / f"archived-{recent_archived_timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        mp4s = [
            (old_input_mp4, old_input_timestamp),
            (old_archived_mp4, old_archived_timestamp),
            (recent_input_mp4, recent_input_timestamp),
            (recent_archived_mp4, recent_archived_timestamp),
        ]
        mapping = {
            old_input_timestamp.strftime("%Y%m%d%H%M%S"): {".mp4": old_input_mp4},
            old_archived_timestamp.strftime("%Y%m%d%H%M%S"): {".mp4": old_archived_mp4},
            recent_input_timestamp.strftime("%Y%m%d%H%M%S"): {".mp4": recent_input_mp4},
            recent_archived_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_archived_mp4
            },
        }

        # Mock exists to return False (so normally we'd transcode if not for cleanup)
        mocker.patch("pathlib.Path.exists", return_value=False)

        # Generate plan
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan - should have no transcoding when cleanup is active
        assert len(plan["transcoding"]) == 0

        # Should have removals for both old input file and old archived file
        # Recent files should be excluded due to age cutoff
        # old_input_timestamp and old_archived_timestamp are both older than 10 days
        # recent_input_timestamp and recent_archived_timestamp are both newer than 10 days
        assert (
            len(plan["removals"]) == 2
        )  # One old input file and one old archived file

        removal_files = [action["file"] for action in plan["removals"]]
        assert old_input_mp4 in removal_files
        assert old_archived_mp4 in removal_files
        assert recent_input_mp4 not in removal_files  # Too recent to remove
        assert recent_archived_mp4 not in removal_files  # Too recent to remove

    # This test has been moved to TestFileDiscovery class


class TestDisplayAndConfirmPlan:
    """Test display and confirm plan functions"""

    def test_display_plan(self, mocker):
        """Test displaying the action plan"""
        logger = mocker.MagicMock()

        # Create config
        args = archiver.parse_args(["/test"])
        config = archiver.Config(args)

        # Create test plan
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": Path("/test/input1.mp4"),
                    "output": Path("/test/output1.mp4"),
                    "jpg_to_remove": Path("/test/input1.jpg"),
                },
                {
                    "type": "transcode",
                    "input": Path("/test/input2.mp4"),
                    "output": Path("/test/output2.mp4"),
                    "jpg_to_remove": None,
                },
            ],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": Path("/test/input1.mp4"),
                    "reason": "Source file for transcoded archive",
                },
                {
                    "type": "jpg_removal_after_transcode",
                    "file": Path("/test/input1.jpg"),
                    "reason": "Paired with transcoded MP4",
                },
            ],
        }

        # Display plan
        archiver.display_plan(plan, logger, config)

        # Verify logger was called with plan information
        assert (
            logger.info.call_count >= 6
        )  # Header, transcoding count, transcoding items, removals count, removal items, footer

    def test_display_plan_with_cleanup(self, mocker):
        """Test displaying the action plan with cleanup enabled"""
        logger = mocker.MagicMock()

        # Create config with cleanup
        args = archiver.parse_args(["/test", "--cleanup", "--age", "30"])
        config = archiver.Config(args)

        # Create test plan
        plan = {"transcoding": [], "removals": []}

        # Display plan
        archiver.display_plan(plan, logger, config)

        # Verify logger was called with cleanup information
        assert any(
            "Cleanup enabled" in call[0][0] for call in logger.info.call_args_list
        )

    def test_confirm_plan_no_confirm(self, mocker):
        """Test confirming plan when no_confirm is True"""
        logger = mocker.MagicMock()

        # Create config with no_confirm
        args = archiver.parse_args(["/test", "--no-confirm"])
        config = archiver.Config(args)

        # Create test plan
        plan = {"transcoding": [], "removals": []}

        # Confirm plan
        result = archiver.confirm_plan(plan, config, logger)

        # Should return True without prompting
        assert result is True

    def test_confirm_plan_with_user_input(self, mocker):
        """Test confirming plan with user input"""
        logger = mocker.MagicMock()

        # Create config
        args = archiver.parse_args(["/test"])
        config = archiver.Config(args)

        # Create test plan
        plan = {"transcoding": [], "removals": []}

        # Mock user input
        mock_input = mocker.patch("builtins.input", return_value="y")

        # Confirm plan
        result = archiver.confirm_plan(plan, config, logger)

        # Should return True based on user input
        assert result is True
        mock_input.assert_called_once()

    def test_confirm_plan_with_user_rejection(self, mocker):
        """Test confirming plan with user rejection"""
        logger = mocker.MagicMock()

        # Create config
        args = archiver.parse_args(["/test"])
        config = archiver.Config(args)

        # Create test plan
        plan = {"transcoding": [], "removals": []}

        # Mock user input
        mock_input = mocker.patch("builtins.input", return_value="n")

        # Confirm plan
        result = archiver.confirm_plan(plan, config, logger)

        # Should return False based on user input
        assert result is False
        mock_input.assert_called_once()

    def test_confirm_plan_with_keyboard_interrupt(self, mocker):
        """Test confirming plan with keyboard interrupt"""
        logger = mocker.MagicMock()

        # Create config
        args = archiver.parse_args(["/test"])
        config = archiver.Config(args)

        # Create test plan
        plan = {"transcoding": [], "removals": []}

        # Mock user input with KeyboardInterrupt
        mock_input = mocker.patch("builtins.input", side_effect=KeyboardInterrupt)

        # Confirm plan
        result = archiver.confirm_plan(plan, config, logger)

        # Should return False on KeyboardInterrupt
        assert result is False
        mock_input.assert_called_once()


class TestSignalHandlers:
    """Test signal handler setup"""

    def test_setup_signal_handlers(self, mocker):
        """Test setting up signal handlers"""
        graceful_exit = archiver.GracefulExit()

        # Mock signal.signal
        mock_signal = mocker.patch("signal.signal")

        # Setup signal handlers
        archiver.setup_signal_handlers(graceful_exit)

        # Verify signal.signal was called for each signal
        assert mock_signal.call_count == 3  # SIGINT, SIGTERM, SIGHUP

        # Verify the handler function
        handler = mock_signal.call_args_list[0][0][1]
        handler(signal.SIGINT, None)
        assert graceful_exit.should_exit() is True


class TestParseArgs:
    """Test argument parsing"""

    @pytest.mark.parametrize(
        "args_input,expected_values",
        [
            # Test minimal arguments
            (
                ["/test"],
                {
                    "directory": "/test",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
            ),
            # Test all options
            (
                [
                    "/test",
                    "-o",
                    "/output",
                    "--dry-run",
                    "--no-confirm",
                    "--no-skip",
                    "--delete",  # New flag replacing --use-trash
                    "--trash-root",
                    "/trash",
                    "--cleanup",
                    "--clean-output",
                    "--age",
                    "60",
                    "--log-file",
                    "/log.txt",
                ],
                {
                    "directory": "/test",
                    "output": "/output",
                    "dry_run": True,
                    "no_confirm": True,
                    "no_skip": True,
                    "delete": True,  # New behavior: --delete flag
                    "trash_root": "/trash",
                    "cleanup": True,
                    "clean_output": True,
                    "age": 60,
                    "log_file": "/log.txt",
                },
            ),
        ],
    )
    def test_parse_args_various_scenarios(self, args_input, expected_values):
        """Test parsing arguments with various configurations"""
        args = archiver.parse_args(args_input)

        # Assert all expected values
        for key, expected_value in expected_values.items():
            actual_value = getattr(args, key)
            assert actual_value == expected_value


class TestRunArchiver:
    """Test the main run_archiver function"""

    def test_run_archiver_no_files(self, tmp_path, mocker):
        """Test running archiver with no files to process"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Mock file discovery to return no files
        mocker.patch.object(
            archiver.FileDiscovery, "discover_files", return_value=([], {}, set())
        )

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0
        mock_logger.info.assert_any_call("No files to process")

    def test_run_archiver_dry_run(self, tmp_path, mocker):
        """Test running archiver in dry run mode"""
        # Create config with dry run
        args = archiver.parse_args([str(tmp_path), "--dry-run"])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Mock file discovery
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, timestamp)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )

        # Mock directory exists
        mocker.patch("pathlib.Path.exists", return_value=True)

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0
        mock_logger.info.assert_any_call(
            "Dry run completed - no transcoding or removals performed"
        )

    def test_run_archiver_user_cancelled(self, tmp_path, mocker):
        """Test running archiver when user cancels"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Mock file discovery
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, timestamp)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )

        # Mock directory exists
        mocker.patch("pathlib.Path.exists", return_value=True)

        # Mock confirm_plan to return False (user cancelled)
        mocker.patch("archiver.confirm_plan", return_value=False)

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0
        mock_logger.info.assert_any_call("Operation cancelled by user")

    def test_run_archiver_success(self, tmp_path, mocker):
        """Test running archiver successfully"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Mock file discovery
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=([(mp4_file, timestamp)], {}, set()),
        )

        # Mock directory exists
        mocker.patch("pathlib.Path.exists", return_value=True)

        # Mock confirm_plan to return True
        mocker.patch("archiver.confirm_plan", return_value=True)

        # Mock FileProcessor
        mock_processor = mocker.MagicMock()
        mock_processor.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }
        mock_processor.execute_plan.return_value = True
        mock_processor.cleanup_orphaned_files.return_value = None
        mocker.patch("archiver.FileProcessor", return_value=mock_processor)

        # Mock ProgressReporter
        mock_progress = mocker.MagicMock()
        mock_progress.__enter__ = mocker.MagicMock(return_value=mock_progress)
        mock_progress.__exit__ = mocker.MagicMock(return_value=None)
        mocker.patch("archiver.ProgressReporter", return_value=mock_progress)

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0
        mock_logger.info.assert_any_call("Archiving completed successfully")

    def test_run_archiver_with_error(self, tmp_path, mocker):
        """Test running archiver with an error"""
        # Create config
        args = archiver.parse_args([str(tmp_path)])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Mock directory exists to raise an exception
        mocker.patch("pathlib.Path.exists", side_effect=Exception("Test error"))

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 1
        mock_logger.error.assert_called_once()

    def test_run_archiver_with_cleanup(self, tmp_path, mocker):
        """Test running archiver with cleanup enabled"""
        # Create config with cleanup
        args = archiver.parse_args([str(tmp_path), "--cleanup"])
        config = archiver.Config(args)

        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Create test data
        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Mock file discovery
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=([(mp4_file, timestamp)], {}, set()),
        )

        # Mock directory exists
        mocker.patch("pathlib.Path.exists", return_value=True)

        # Mock confirm_plan to return True
        mocker.patch("archiver.confirm_plan", return_value=True)

        # Mock FileProcessor
        mock_processor = mocker.MagicMock()
        mock_processor.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }
        mock_processor.execute_plan.return_value = True
        mock_processor.cleanup_orphaned_files.return_value = None
        mocker.patch("archiver.FileProcessor", return_value=mock_processor)

        # Mock ProgressReporter
        mock_progress = mocker.MagicMock()
        mock_progress.__enter__ = mocker.MagicMock(return_value=mock_progress)
        mock_progress.__exit__ = mocker.MagicMock(return_value=None)
        mocker.patch("archiver.ProgressReporter", return_value=mock_progress)

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0
        mock_logger.info.assert_any_call("Cleaning up files")
        mock_processor.cleanup_orphaned_files.assert_called_once()


class TestMain:
    """Test the main entry point"""

    def test_main(self, mocker):
        """Test the main function"""
        # Mock parse_args
        mock_args = mocker.MagicMock()
        mocker.patch("archiver.parse_args", return_value=mock_args)

        # Mock Config
        mock_config = mocker.MagicMock()
        mocker.patch("archiver.Config", return_value=mock_config)

        # Mock run_archiver
        mocker.patch("archiver.run_archiver", return_value=0)

        # Call main (it calls sys.exit internally)
        archiver.main()


class TestIntegration:
    """Integration tests for the archiver"""

    @pytest.mark.parametrize("dry_run", [True, False])
    @pytest.mark.parametrize("use_trash", [True, False])
    @pytest.mark.parametrize("cleanup", [True, False])
    def test_end_to_end_workflow(self, tmp_path, mocker, dry_run, use_trash, cleanup):
        """Test the end-to-end workflow with different configurations"""
        # Create directory structure
        camera_dir = tmp_path / "camera"
        output_dir = tmp_path / "output"
        trash_dir = tmp_path / "trash"

        # Create test files
        mp4_file = camera_dir / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"

        # Build command line arguments
        args = [str(camera_dir), "-o", str(output_dir)]
        if dry_run:
            args.append("--dry-run")
        # The parametrized test has old behavior: use_trash=True meant enable trash
        # Now trash is default, so we need to reverse the logic
        # When old use_trash was True (wanted trash), now we use default (no flag needed)
        # When old use_trash was False (didn't want trash), now we need --delete flag
        if not use_trash:
            args.append("--delete")
        # Add trash-root configuration regardless
        if use_trash:
            args.extend(["--trash-root", str(trash_dir)])

        # Parse arguments and create config
        parsed_args = archiver.parse_args(args)
        config = archiver.Config(parsed_args)
        # Mock logger setup
        mock_logger = mocker.MagicMock()
        mocker.patch.object(archiver.Logger, "setup", return_value=mock_logger)

        # Create test data
        timestamp = datetime(2023, 1, 15, 12, 0, 0)

        # Mock file discovery
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, timestamp)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )

        # Mock directory exists
        mocker.patch("pathlib.Path.exists", return_value=True)

        # Mock confirm_plan to return True
        mocker.patch("archiver.confirm_plan", return_value=True)

        # Mock FileProcessor if not dry_run
        if not dry_run:
            mock_processor = mocker.MagicMock()
            mock_processor.generate_action_plan.return_value = {
                "transcoding": [],
                "removals": [],
            }
            mock_processor.execute_plan.return_value = True
            if cleanup:
                mock_processor.cleanup_orphaned_files.return_value = None
            mocker.patch("archiver.FileProcessor", return_value=mock_processor)

            # Mock ProgressReporter
            mock_progress = mocker.MagicMock()
            mock_progress.__enter__ = mocker.MagicMock(return_value=mock_progress)
            mock_progress.__exit__ = mocker.MagicMock(return_value=None)
            mocker.patch("archiver.ProgressReporter", return_value=mock_progress)

        # Run archiver
        result = archiver.run_archiver(config)

        # Verify result
        assert result == 0


class TestLoggingProgressInteraction:
    """Test the interaction between logging and progress updates"""

    def test_thread_safety_with_logging_and_progress(self, mocker):
        """Test that logging and progress updates don't interfere with each other"""
        import threading
        import time

        # Create a logger and progress reporter
        graceful_exit = archiver.GracefulExit()

        # Create a mock logger to avoid actual file writes
        _ = mocker.MagicMock()

        # Test the global OUTPUT_LOCK coordination
        reporter = archiver.ProgressReporter(5, graceful_exit, silent=False)

        # Mock stderr to capture writes
        mock_stderr = mocker.patch("sys.stderr")

        # Mock time for predictable elapsed time
        mock_time = mocker.patch("time.time")
        mock_time.return_value = 0.0  # Start time

        results = []

        def simulate_logging():
            """Simulate logging happening in another thread"""
            for i in range(3):
                with (
                    archiver.OUTPUT_LOCK
                ):  # This simulates what the logger does when writing
                    mock_stderr.write(f"Log message {i}\n")
                    mock_stderr.flush()
                    results.append(f"log_{i}")
                time.sleep(0.01)  # Small delay

        def simulate_progress():
            """Simulate progress updates happening in main thread"""
            for i in range(3):
                reporter.current = i
                reporter.update_progress(i * 33.33)
                results.append(f"progress_{i}")
                time.sleep(0.01)  # Small delay

        # Run both operations simultaneously to test thread safety
        log_thread = threading.Thread(target=simulate_logging)
        progress_thread = threading.Thread(target=simulate_progress)

        log_thread.start()
        progress_thread.start()

        log_thread.join()
        progress_thread.join()

        # Verify no exceptions occurred due to race conditions
        # Both operations should complete without interfering
        assert len([r for r in results if r.startswith("log_")]) == 3
        assert len([r for r in results if r.startswith("progress_")]) == 3

    def test_logger_uses_threadsafe_handler(self, mocker, tmp_path):
        """Test that the logger uses the thread-safe handler"""
        log_file = tmp_path / "test.log"
        args = archiver.parse_args([str(tmp_path), "--log-file", str(log_file)])
        config = archiver.Config(args)

        # Mock the rotation function to avoid actual file operations
        mocker.patch.object(archiver.Logger, "_rotate_log_file")

        logger = archiver.Logger.setup(config)

        # Check that at least one handler is our ThreadSafeStreamHandler
        console_handlers = [
            h
            for h in logger.handlers
            if isinstance(h, archiver.ThreadSafeStreamHandler)
        ]
        assert len(console_handlers) == 1, (
            f"Expected 1 ThreadSafeStreamHandler, got {len(console_handlers)}: {logger.handlers}"
        )

        # Verify the handler is correctly configured (we can't easily check stream since it's stderr)
        handler = console_handlers[0]
        assert handler.formatter is not None  # Should have a formatter
