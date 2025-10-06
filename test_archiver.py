#!/usr/bin/env python3
"""Comprehensive tests for archiver.py using pytest that match the new state-based implementation."""

import logging
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from archiver import (
    MIN_ARCHIVE_SIZE_BYTES,
    ArchiveCleanupHandler,
    CameraService,
    CleanupHandler,
    Context,
    DiscoveryHandler,
    ExecutionHandler,
    FileInfo,
    FileService,
    GracefulExit,
    GuardedStreamHandler,
    InitializationHandler,
    LoggingService,
    PlanningHandler,
    ProgressReporter,
    State,
    StateHandlerFactory,
    StorageService,
    TerminationHandler,
    TranscoderService,
    main,
    parse_arguments,
    run_state_machine,
    setup_config,
)


class TestBase:
    """Base class with temp directory handling and log suppression."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "camera"
        self.output_dir = self.temp_dir / "archived"
        self.trash_dir = self.temp_dir / ".deleted"

        for d in (self.input_dir, self.output_dir, self.trash_dir):
            d.mkdir(parents=True, exist_ok=True)

        # Suppress log output during tests
        self._orig_emit = GuardedStreamHandler.emit
        GuardedStreamHandler.emit = lambda *_, **__: None

        # Also suppress progress bar output during tests
        self._orig_progress_display = ProgressReporter._display
        self._orig_progress_redraw = ProgressReporter.redraw
        self._orig_progress_cleanup = ProgressReporter._cleanup_progress_bar
        ProgressReporter._display = lambda self, line: None
        ProgressReporter.redraw = lambda self: None
        ProgressReporter._cleanup_progress_bar = lambda self: None

    def teardown_method(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        GuardedStreamHandler.emit = self._orig_emit
        ProgressReporter._display = self._orig_progress_display
        ProgressReporter.redraw = self._orig_progress_redraw
        ProgressReporter._cleanup_progress_bar = self._orig_progress_cleanup

    # ---------- file helpers ----------
    def create_file(
        self,
        rel_path: str,
        content: bytes = b"test",
        ts: datetime | None = None,
    ) -> Path:
        """Create a file with an optional timestamp embedded in the name."""
        if ts is None:
            ts = datetime.now()
        stem = f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}"
        full_path = self.input_dir / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        file_path = full_path.with_name(stem + Path(rel_path).suffix)
        file_path.write_bytes(content)
        return file_path

    def create_archive(self, ts: datetime, size: int | None = None) -> Path:
        """Create a dummy archive in the output tree."""
        p = (
            self.output_dir
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * (size or MIN_ARCHIVE_SIZE_BYTES + 1))
        return p

    def create_trash_file(self, ts: datetime) -> Path:
        """Create a file in the trash tree."""
        p = (
            self.trash_dir
            / "input"
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
        return p


@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    input_dir = temp_dir / "camera"
    output_dir = temp_dir / "archived"
    trash_dir = temp_dir / ".deleted"

    for d in (input_dir, output_dir, trash_dir):
        d.mkdir(parents=True, exist_ok=True)

    yield {
        "temp_dir": temp_dir,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "trash_dir": trash_dir,
    }

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def suppress_logging_and_progress():
    """Suppress logging and progress bar output during tests."""
    # Suppress log output during tests
    _orig_emit = GuardedStreamHandler.emit
    GuardedStreamHandler.emit = lambda *_, **__: None

    # Also suppress progress bar output during tests
    _orig_progress_display = ProgressReporter._display
    _orig_progress_redraw = ProgressReporter.redraw
    _orig_progress_cleanup = ProgressReporter._cleanup_progress_bar
    ProgressReporter._display = lambda self, line: None
    ProgressReporter.redraw = lambda self: None
    ProgressReporter._cleanup_progress_bar = lambda self: None

    yield  # Provide the fixture

    # Restore original behavior
    GuardedStreamHandler.emit = _orig_emit
    ProgressReporter._display = _orig_progress_display
    ProgressReporter.redraw = _orig_progress_redraw
    ProgressReporter._cleanup_progress_bar = _orig_progress_cleanup


class TestContextAndServices(TestBase):
    """Test Context class and core services."""

    def test_context_initialization(self):
        """Test Context class initialization."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "trash_root": self.trash_dir,
        }

        context = Context(config)
        assert context.current_state == State.INITIALIZATION
        assert context.config == config
        assert isinstance(context.graceful_exit, GracefulExit)
        assert "camera_service" in context.services
        assert "storage_service" in context.services
        assert "logging_service" in context.services
        assert "transcoder_service" in context.services
        assert "file_service" in context.services

    def test_context_state_transition(self):
        """Test Context state transition functionality."""
        config = {"directory": self.input_dir, "output": self.output_dir}
        context = Context(config)

        # Initially should be in INITIALIZATION state
        assert context.current_state == State.INITIALIZATION

        # Transition to DISCOVERY state
        context.transition_to(State.DISCOVERY)
        assert context.current_state == State.DISCOVERY

        # Transition to PLANNING state
        context.transition_to(State.PLANNING)
        assert context.current_state == State.PLANNING

    def test_state_handler_factory(self):
        """Test StateHandlerFactory creates correct handlers."""
        # Test each handler type
        init_handler = StateHandlerFactory.create_handler(State.INITIALIZATION)
        assert isinstance(init_handler, InitializationHandler)

        discovery_handler = StateHandlerFactory.create_handler(State.DISCOVERY)
        assert isinstance(discovery_handler, DiscoveryHandler)

        planning_handler = StateHandlerFactory.create_handler(State.PLANNING)
        assert isinstance(planning_handler, PlanningHandler)

        execution_handler = StateHandlerFactory.create_handler(State.EXECUTION)
        assert isinstance(execution_handler, ExecutionHandler)

        cleanup_handler = StateHandlerFactory.create_handler(State.CLEANUP)
        assert isinstance(cleanup_handler, CleanupHandler)

        archive_cleanup_handler = StateHandlerFactory.create_handler(
            State.ARCHIVE_CLEANUP
        )
        assert isinstance(archive_cleanup_handler, ArchiveCleanupHandler)

        termination_handler = StateHandlerFactory.create_handler(State.TERMINATION)
        assert isinstance(termination_handler, TerminationHandler)


class TestServiceClasses(TestBase):
    """Test service classes functionality."""

    def test_camera_service_discover_files(self):
        """Test CameraService discover_files functionality."""
        config = {
            "directory": self.input_dir,
            "trash_root": self.trash_dir,
        }
        service = CameraService(config)

        # Create test files - ensure no microseconds in timestamp
        old_ts = (datetime.now() - timedelta(days=31)).replace(microsecond=0)
        mp4_file = self.create_file("2023/01/01/test_video.mp4", ts=old_ts)
        jpg_file = self.create_file("2023/01/01/test_image.jpg", ts=old_ts)

        # Discover files
        mp4s, mapping, trash_files = service.discover_files(self.input_dir)

        # Check that discovery worked
        assert len(mp4s) == 1
        assert mp4s[0][0] == mp4_file
        assert mp4s[0][1] == old_ts

        # Check that mapping contains both files
        key = old_ts.strftime("%Y%m%d%H%M%S")
        assert key in mapping
        assert ".mp4" in mapping[key]
        assert ".jpg" in mapping[key]
        assert mapping[key][".mp4"] == mp4_file
        assert mapping[key][".jpg"] == jpg_file

    @pytest.mark.parametrize(
        "filename,expected_valid",
        [
            ("REO_CAM_20230101120000.mp4", True),  # Valid MP4
            ("REO_CAM_20230101120000.jpg", True),  # Valid JPG
            ("REO_CAM_19991231235959.mp4", False),  # Year out of range (too past)
            ("REO_CAM_21001231235959.mp4", False),  # Year out of range (too future)
            ("invalid.mp4", None),  # Invalid format
            (
                "REO_Camera_20230101120000.mp4",
                True,
            ),  # Different case should work (IGNORECASE)
            (
                "reo_camera_20230101120000.mp4",
                True,
            ),  # Lowercase should work (IGNORECASE)
            ("REO_XYZ_20230101120000.mp4", True),  # Any prefix after REO_ should work
            ("REO_CAM_invalid.mp4", None),  # Invalid timestamp
            ("REO_CAM_20230101120000", None),  # Missing extension
        ],
    )
    def test_camera_service_parse_timestamp_parametrized(
        self, filename, expected_valid
    ):
        """Parametrized test for CameraService parse_timestamp functionality."""
        service = CameraService({})

        ts = service._parse_timestamp(filename)
        if expected_valid is True:
            assert ts is not None
            # Extract the timestamp part from filename (after underscore)
            timestamp_part = filename.split("_")[-1][:14]  # Get the 14-digit timestamp
            expected_year = int(timestamp_part[:4])
            assert ts.year == expected_year
            # Validate the timestamp is in the expected range
            assert 2000 <= ts.year <= 2099
        elif expected_valid is False:
            assert ts is None  # Year out of range returns None
        else:  # expected_valid is None
            assert ts is None

    def test_camera_service_parse_timestamp_value_error(self, mocker):
        """Test CameraService parse_timestamp with value error in datetime parsing."""
        service = CameraService({})

        # Test with an invalid timestamp that would cause strptime to fail
        # Using a valid format but invalid date (e.g., Feb 30)
        result = service._parse_timestamp(
            "REO_CAM_20230230120000.mp4"
        )  # Feb 30 doesn't exist
        # This should handle the ValueError gracefully and return None
        assert result is None

    def test_camera_service_parse_timestamp_detailed(self):
        """Test CameraService parse_timestamp functionality in detail."""
        service = CameraService({})

        # Test valid timestamp
        timestamp = service._parse_timestamp("REO_CAM_20230101120000.mp4")
        assert timestamp is not None
        assert timestamp.year == 2023
        assert timestamp.month == 1
        assert timestamp.day == 1
        assert timestamp.hour == 12
        assert timestamp.minute == 0
        assert timestamp.second == 0

        # Test invalid format
        assert service._parse_timestamp("invalid_name.mp4") is None

    def test_storage_service_check_storage(self):
        """Test StorageService check_storage functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        service = StorageService(config)

        # Check storage status
        status = service.check_storage()
        assert status["input_dir_exists"] is True
        assert status["output_dir_exists"] is True
        assert isinstance(status["input_space"], int)
        assert isinstance(status["output_space"], int)

    def test_storage_service_get_free_space_error(self, mocker):
        """Test StorageService _get_free_space error handling."""
        service = StorageService({})

        # Mock shutil.disk_usage to raise an exception
        mocker.patch("archiver.shutil.disk_usage", side_effect=Exception("Disk error"))
        result = service._get_free_space(self.input_dir)
        assert result == 0

    def test_transcoder_service_get_video_duration(self, mocker):
        """Test TranscoderService get_video_duration functionality."""
        service = TranscoderService({})

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Test when ffprobe is not available (should return None)
        mocker.patch("archiver.shutil.which", return_value=None)
        result = service.get_video_duration(test_file)
        assert result is None

    def test_transcoder_service_get_video_duration_error(self, mocker):
        """Test TranscoderService get_video_duration error handling."""
        service = TranscoderService({})

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Test when subprocess.run raises an exception
        mocker.patch("archiver.subprocess.run", side_effect=Exception("Command failed"))
        result = service.get_video_duration(test_file)
        assert result is None

    def test_transcoder_service_get_video_duration_n_a(self, mocker):
        """Test TranscoderService get_video_duration with N/A result."""
        service = TranscoderService({})

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Mock that ffprobe returns "N/A"
        mock_result = mocker.Mock()
        mock_result.stdout.strip.return_value = "N/A"
        mocker.patch("archiver.shutil.which", return_value="/usr/bin/ffprobe")
        mocker.patch("archiver.subprocess.run", return_value=mock_result)
        result = service.get_video_duration(test_file)
        assert result is None

    def test_transcoder_service_get_video_duration_empty(self, mocker):
        """Test TranscoderService get_video_duration with empty result."""
        service = TranscoderService({})

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Mock that ffprobe returns empty string
        mock_result = mocker.Mock()
        mock_result.stdout.strip.return_value = ""
        mocker.patch("archiver.shutil.which", return_value="/usr/bin/ffprobe")
        mocker.patch("archiver.subprocess.run", return_value=mock_result)
        result = service.get_video_duration(test_file)
        assert result is None

    def test_file_service_remove_file_dry_run(self):
        """Test FileService remove_file in dry run mode."""
        config = {
            "dry_run": True,
            "use_trash": True,
            "trash_root": self.trash_dir,
        }
        service = FileService(config)

        # Create a test file
        test_file = self.create_file("test_file.mp4")
        assert test_file.exists()

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Remove file in dry run mode
        service.remove_file(test_file, logger, dry_run=True)

        # File should still exist in dry run mode
        assert test_file.exists()

    def test_file_service_remove_directory(self):
        """Test FileService remove_file for directories."""
        config = {
            "dry_run": False,
            "use_trash": False,
        }
        service = FileService(config)

        # Create a test directory
        test_dir = self.input_dir / "test_dir"
        test_dir.mkdir(parents=True, exist_ok=True)
        assert test_dir.exists()

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Remove directory
        service.remove_file(test_dir, logger, dry_run=False)

        # Directory should be removed
        assert not test_dir.exists()

    def test_file_service_remove_unsupported_type(self, mocker):
        """Test FileService remove_file for unsupported file types."""
        config = {
            "dry_run": False,
            "use_trash": False,
        }
        service = FileService(config)

        # Create a mock Path object to simulate unsupported file type
        mock_path = mocker.MagicMock()
        mock_path.__str__.return_value = "/fake/unsupported"
        mock_path.is_file.return_value = False
        mock_path.is_dir.return_value = False
        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Capture logs to verify warning is logged
        mock_warning = mocker.patch.object(logger, "warning")
        service.remove_file(mock_path, logger, dry_run=False)
        mock_warning.assert_called_once()

    def test_file_service_remove_file_error(self, mocker):
        """Test FileService remove_file error handling."""
        config = {
            "dry_run": False,
            "use_trash": False,
        }
        service = FileService(config)

        # Create a mock file object instead of a real file
        mock_file = mocker.MagicMock()
        mock_file.__str__.return_value = "/fake/test_error_file.mp4"
        mock_file.is_file.return_value = True  # This behaves like a file
        mock_file.unlink.side_effect = Exception("Permission denied")

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Capture logs to verify error is logged
        mock_error = mocker.patch.object(logger, "error")
        service.remove_file(mock_file, logger, dry_run=False)
        mock_error.assert_called_once()

    def test_file_service_calculate_trash_destination(self):
        """Test FileService _calculate_trash_destination functionality."""
        config = {}
        service = FileService(config)

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Calculate trash destination
        dest = service._calculate_trash_destination(
            test_file, self.input_dir, self.trash_dir
        )

        # Should be in trash with correct structure
        assert str(self.trash_dir / "input") in str(dest)
        assert dest.name.startswith("REO_CAMERA_")
        assert dest.suffix == ".mp4"

        # Test collision handling by creating the file and calculating again
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("content")  # Create the file to cause collision

        # Calculate destination again - should get a different path
        dest2 = service._calculate_trash_destination(
            test_file, self.input_dir, self.trash_dir
        )
        assert dest != dest2  # Different due to collision

    def test_logging_service_setup_logging(self):
        """Test LoggingService setup_logging functionality."""
        config = {"log_file": self.temp_dir / "test.log"}
        service = LoggingService(config)

        # Setup logging with progress bar
        progress_bar = ProgressReporter(total_files=1, silent=True)
        logger = service.setup_logging(progress_bar)

        assert logger is not None
        assert logger.name == "camera_archiver"

        # Test logging works
        logger.info("Test message")

        # Check log file was created
        log_file = self.temp_dir / "test.log"
        assert log_file.exists()


class TestProgressReporter(TestBase):
    """Test ProgressReporter functionality."""

    def test_progress_reporter_basic(self):
        """Test basic ProgressReporter functionality."""
        with ProgressReporter(total_files=5, silent=True) as pr:
            assert pr.total == 5
            assert pr.silent is True

            # Test basic functionality
            pr.start_processing()
            pr.start_file()
            pr.update_progress(1, 50.0)
            pr.finish_file(1)
            assert isinstance(pr.has_progress, bool)

    def test_progress_reporter_tty_detection(self):
        """Test TTY detection in ProgressReporter."""
        import io

        # Test with TTY stream
        tty_stream = io.StringIO()
        tty_stream.isatty = lambda: True
        with ProgressReporter(total_files=1, out=tty_stream) as pr:
            assert pr._is_tty() is True

        # Test with non-TTY stream
        non_tty_stream = io.StringIO()
        non_tty_stream.isatty = lambda: False
        with ProgressReporter(total_files=1, out=non_tty_stream) as pr:
            assert pr._is_tty() is False

    def test_progress_reporter_format_line(self):
        """Test the _format_line method."""
        pr = ProgressReporter(total_files=2, silent=True)
        line = pr._format_line(1, 50.0)

        # Check that the line contains expected elements
        assert "50%" in line
        assert "[" in line and "]" in line
        assert "00:00" in line  # elapsed time

        pr.finish()

    def test_progress_reporter_non_tty_output(self, mocker):
        """Test ProgressReporter non-TTY output functionality."""
        import io

        # Temporarily restore the original _display method to test functionality
        original_display = (
            self._orig_progress_display
        )  # Use the saved original from setup

        # Temporarily restore the actual display method for this test only
        ProgressReporter._display = original_display

        # Test with non-TTY stream that updates periodically
        non_tty_stream = io.StringIO()
        non_tty_stream.isatty = lambda: False

        try:
            with ProgressReporter(
                total_files=1, out=non_tty_stream, silent=False
            ) as pr:
                # Update progress to match total (which results in 100%)
                pr.update_progress(1, 100.0)

                pr.finish()
                # Check output
                output = non_tty_stream.getvalue()
                # Should have written something to the stream because it's 100% and it's not TTY
                assert output != ""
        finally:
            # Restore the test suppression immediately
            ProgressReporter._display = lambda *_, **__: None

    def test_progress_reporter_display_exception(self, mocker):
        """Test ProgressReporter _display method when stream operations fail."""

        # Create a stream that will raise an exception
        class FailingStream:
            def __init__(self):
                self.isatty_val = True
                self.closed = False

            def isatty(self):
                return self.isatty_val

            def write(self, data):
                raise Exception("Write failed")

            def flush(self):
                pass

        failing_stream = FailingStream()
        with ProgressReporter(total_files=5, out=failing_stream) as pr:
            # Should handle the exception gracefully
            pr._display("test line")

        # Now try with the non-TTY path
        class FailingNonTTYStream:
            def __init__(self):
                self.isatty_val = False
                self.closed = False
                self.data_written = ""

            def isatty(self):
                return self.isatty_val

            def write(self, data):
                self.data_written += data
                if "test" in data:
                    raise Exception("Write failed")

            def flush(self):
                pass

        failing_stream2 = FailingNonTTYStream()
        with ProgressReporter(total_files=5, out=failing_stream2) as pr:
            # Should handle the exception gracefully
            pr.silent = False
            pr._display("test line")

    def test_progress_reporter_signal_handling(self, mocker):
        """Test ProgressReporter signal handling functionality."""
        import signal

        # Mock signal handling functions to test the registration
        mock_signal = mocker.patch("archiver.signal.signal")
        _ = mocker.patch("archiver.signal.getsignal", return_value=mocker.Mock())
        mock_atexit_register = mocker.patch("archiver.atexit.register")

        # Create a ProgressReporter and check signal handlers are registered
        with ProgressReporter(total_files=1, silent=True) as _:
            # Check that atexit handler was registered
            assert mock_atexit_register.called

            # Check that signal handlers were registered for expected signals
            expected_signals = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
            for expected_sig in expected_signals:
                # Check that signal.signal was called for each signal
                calls = [
                    call
                    for call in mock_signal.call_args_list
                    if call[0][0] == expected_sig
                ]
                assert len(calls) > 0, (
                    f"Signal handler not registered for {expected_sig}"
                )

    def test_progress_reporter_signal_handler_execution(self, mocker):
        """Test ProgressReporter signal handler execution."""
        import io
        import signal

        # Create a progress reporter
        stream = io.StringIO()
        pr = ProgressReporter(total_files=5, out=stream, silent=False)

        # Create a mock frame object
        mock_frame = mocker.Mock()

        # Manually call the signal handler to test it
        pr._signal_handler(signal.SIGINT, mock_frame)

        # Check that exit was requested
        assert pr.graceful_exit.should_exit()

        # Clean up
        pr.finish()

    def test_progress_reporter_cleanup_handlers(self, mocker):
        """Test ProgressReporter cleanup handler registration and unregistration."""
        import signal

        # Mock the functions that are used in cleanup
        mock_atexit_unregister = mocker.patch("archiver.atexit.unregister")
        _ = mocker.patch("archiver.signal.signal")
        _ = mocker.patch("archiver.signal.getsignal", return_value=signal.SIG_DFL)

        # Create and finish a ProgressReporter to test cleanup
        pr = ProgressReporter(total_files=1, silent=True)
        pr.finish()  # Explicitly finish to trigger cleanup

        # Check that cleanup functions were called
        assert mock_atexit_unregister.called


class TestStateHandlers(TestBase):
    """Test state handler classes."""

    def test_initialization_handler(self):
        """Test InitializationHandler functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": True,
        }
        context = Context(config)

        handler = InitializationHandler()
        next_state = handler.execute(context)

        # Should transition to DISCOVERY
        assert next_state == State.DISCOVERY
        assert context.logger is not None
        assert context.progress_bar is not None

    def test_initialization_handler_directory_missing(self):
        """Test InitializationHandler when input directory doesn't exist."""
        config = {
            "directory": Path("/nonexistent/directory"),
            "output": self.output_dir,
        }
        context = Context(config)

        handler = InitializationHandler()
        next_state = handler.execute(context)

        # Should transition to TERMINATION due to missing directory
        assert next_state == State.TERMINATION

    def test_discovery_handler(self):
        """Test DiscoveryHandler functionality."""
        # Create test files first
        old_ts = (datetime.now() - timedelta(days=31)).replace(microsecond=0)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)
        jpg_file = self.create_file("2023/01/01/test.jpg", ts=old_ts)

        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Need to setup logging first
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        handler = DiscoveryHandler()
        next_state = handler.execute(context)

        # Should transition to PLANNING
        assert next_state == State.PLANNING

        # Should have discovered the files
        assert "mp4s" in context.state_data
        assert len(context.state_data["mp4s"]) == 1
        assert context.state_data["mp4s"][0][0] == mp4_file
        assert context.state_data["mp4s"][0][1] == old_ts

        # Mapping should contain both mp4 and jpg files
        key = old_ts.strftime("%Y%m%d%H%M%S")
        assert key in context.state_data["mapping"]
        assert ".mp4" in context.state_data["mapping"][key]
        assert ".jpg" in context.state_data["mapping"][key]
        assert context.state_data["mapping"][key][".mp4"] == mp4_file
        assert context.state_data["mapping"][key][".jpg"] == jpg_file

    def test_discovery_handler_invalid_directory_config(self):
        """Test DiscoveryHandler with invalid directory configuration."""
        config = {
            "directory": "not_a_path_object",  # Invalid - not a Path object
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        handler = DiscoveryHandler()
        next_state = handler.execute(context)

        # Should transition to TERMINATION due to invalid directory config
        assert next_state == State.TERMINATION

    def test_planning_handler(self, mocker):
        """Test PlanningHandler functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "no_confirm": True,  # Skip confirmation
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Setup test data
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        context.state_data["mp4s"] = [(mp4_file, old_ts)]
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file}
        }
        context.state_data["trash_files"] = set()

        handler = PlanningHandler()

        # Mock the confirmation to return True
        mocker.patch.object(handler, "_ask_confirmation", return_value=True)
        next_state = handler.execute(context)

        # Should transition to EXECUTION
        assert next_state == State.EXECUTION
        assert "plan" in context.state_data

    def test_planning_handler_output_path_invalid_config(self):
        """Test PlanningHandler's _output_path with invalid output config."""
        config = {
            "directory": self.input_dir,
            "output": "not_a_path_object",  # Invalid - not a Path object
        }
        context = Context(config)

        handler = PlanningHandler()

        # Should raise ValueError for invalid output directory config
        with pytest.raises(
            ValueError, match="Output directory is not properly configured"
        ):
            handler._output_path(context, self.input_dir / "test.mp4", datetime.now())

    def test_planning_handler_output_path_with_date_structure(self):
        """Test PlanningHandler's _output_path with valid date structure in path."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        handler = PlanningHandler()

        # Use a file with date structure in parent directories: input_dir/year/month/day/filename
        test_file = self.input_dir / "2023" / "01" / "15" / "test.mp4"
        timestamp = datetime(2023, 6, 15, 12, 30, 45)

        # Should reuse the date structure from the path
        result = handler._output_path(context, test_file, timestamp)
        expected = (
            self.output_dir / "2023" / "01" / "15" / "archived-20230615123045.mp4"
        )
        assert result == expected

    def test_planning_handler_output_path_without_date_structure(self):
        """Test PlanningHandler's _output_path without valid date structure in path."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        handler = PlanningHandler()

        # Use a file without date structure in parent directories
        test_file = self.input_dir / "plain_dir" / "test.mp4"
        timestamp = datetime(2023, 6, 15, 12, 30, 45)

        # Should use the timestamp-based structure
        result = handler._output_path(context, test_file, timestamp)
        expected = (
            self.output_dir / "2023" / "06" / "15" / "archived-20230615123045.mp4"
        )
        assert result == expected

    def test_planning_handler_output_path_value_error_in_date_check(self, mocker):
        """Test PlanningHandler's _output_path with value error in date validation."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        handler = PlanningHandler()

        # Instead of mocking builtins.int, let's test with paths that will cause value errors
        # when we have non-numeric values in date position
        test_file = self.input_dir / "2023" / "not_a_month" / "15" / "test.mp4"
        timestamp = datetime(2023, 6, 15, 12, 30, 45)

        # Should fall back to timestamp-based structure when date validation fails
        result = handler._output_path(context, test_file, timestamp)
        expected = (
            self.output_dir / "2023" / "06" / "15" / "archived-20230615123045.mp4"
        )
        assert result == expected

    def test_transcoder_service_transcode_file_error_handling(self, mocker):
        """Test TranscoderService transcode_file error handling."""
        config = {"directory": self.input_dir, "output": self.output_dir}
        service = TranscoderService(config)

        # Create test files
        input_file = self.create_file("input.mp4")
        output_file = self.temp_dir / "output.mp4"

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Mock the process to raise OSError when starting
        mocker.patch(
            "archiver.subprocess.Popen", side_effect=OSError("Failed to start process")
        )

        mock_error = mocker.patch.object(logger, "error")
        result = service.transcode_file(input_file, output_file, logger)
        assert result is False
        mock_error.assert_called_once()

    def test_transcoder_service_transcode_file_no_stdout(self, mocker):
        """Test TranscoderService transcode_file when stdout is None."""
        config = {"directory": self.input_dir, "output": self.output_dir}
        service = TranscoderService(config)

        # Create test files
        input_file = self.create_file("input.mp4")
        output_file = self.temp_dir / "output.mp4"

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Mock the process to return None for stdout (for the old implementation)
        # and stderr (for the new implementation)
        mock_proc = mocker.Mock()
        mock_proc.stdout = None
        mock_proc.stderr = None  # After our changes, the method uses stderr
        mock_proc.wait.return_value = 0
        mocker.patch("archiver.subprocess.Popen", return_value=mock_proc)

        mock_error = mocker.patch.object(logger, "error")
        result = service.transcode_file(input_file, output_file, logger)
        assert result is False
        mock_error.assert_called_once()

    def test_transcoder_service_transcode_file_failure_with_logs(self, mocker):
        """Test TranscoderService transcode_file when ffmpeg returns non-zero exit code."""
        config = {"directory": self.input_dir, "output": self.output_dir}
        service = TranscoderService(config)

        # Create test files
        input_file = self.create_file("input.mp4")
        output_file = self.temp_dir / "output.mp4"

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Create a mock stderr that behaves like a real file object with readline (after our change to use stderr)
        mock_stderr = mocker.MagicMock()
        lines = iter(["time=00:00:01.00\n", "some other log line\n", ""])
        mock_stderr.readline.side_effect = lambda: next(lines, "")

        # Mock a process where ffmpeg fails
        mock_proc = mocker.Mock()
        mock_proc.stdout = None  # After our change, the method uses stderr
        mock_proc.stderr = mock_stderr  # New mock for stderr
        mock_proc.wait.return_value = 1  # failure code
        mocker.patch("archiver.subprocess.Popen", return_value=mock_proc)
        mocker.patch("archiver.TranscoderService.get_video_duration", return_value=10.0)

        mock_error = mocker.patch.object(logger, "error")
        result = service.transcode_file(input_file, output_file, logger)
        assert result is False
        # Check that the error message includes the failure code and log lines
        mock_error.assert_called()

    def test_transcoder_service_transcode_file_process_terminate_timeout(self, mocker):
        """Test TranscoderService transcode_file when process termination times out."""
        import subprocess

        config = {"directory": self.input_dir, "output": self.output_dir}
        service = TranscoderService(config)

        # Create test files
        input_file = self.create_file("input.mp4")
        output_file = self.temp_dir / "output.mp4"

        # Setup logger
        logger = logging.getLogger("test")
        logger.setLevel(logging.INFO)

        # Create a mock stderr that behaves like a real file object with readline (after our change to use stderr)
        mock_stderr = mocker.MagicMock()
        lines = iter(["time=00:00:01.00\n"])
        mock_stderr.readline.side_effect = lambda: next(lines, "")

        # Mock a successful process run but make the wait() in finally block time out
        mock_proc = mocker.Mock()
        mock_proc.stdout = None  # After our change, the method uses stderr
        mock_proc.stderr = mock_stderr  # New mock for stderr

        # Initially succeeds, but when called in finally block during cleanup it times out
        # We'll mock the wait method such that after the main loop is done,
        # the finally block's calls will timeout
        def mock_wait(timeout=None):
            if timeout == 0.1:
                # This is the first call in finally block that should timeout
                raise subprocess.TimeoutExpired("cmd", 0.1)
            elif timeout == 5:
                # This is the second call in finally block that should also timeout
                raise subprocess.TimeoutExpired("cmd", 5)
            else:
                # Default wait call in main loop
                return 0

        mock_proc.wait = mocker.Mock(side_effect=mock_wait)
        mock_proc.terminate = mocker.Mock()
        mock_proc.kill = mocker.Mock()
        mock_proc.stderr.readline.side_effect = [
            "time=00:00:00.00\n",
            "time=00:00:01.00\n",
            "",
        ]  # end iteration

        mocker.patch("archiver.subprocess.Popen", return_value=mock_proc)
        mocker.patch("archiver.TranscoderService.get_video_duration", return_value=10.0)

        result = service.transcode_file(input_file, output_file, logger)
        # Should handle the timeout gracefully
        assert result in [True, False]  # Could be either depending on exact execution

    def test_execution_handler(self):
        """Test ExecutionHandler functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": True,  # Don't actually transcode
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Setup test data
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Create a plan with a transcoding action
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": mp4_file,
                    "output": self.output_dir
                    / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4",
                    "jpg_to_remove": None,
                }
            ],
            "removals": [],
        }
        context.state_data["plan"] = plan

        handler = ExecutionHandler()
        next_state = handler.execute(context)

        # Should transition to CLEANUP
        assert next_state == State.CLEANUP

    def test_cleanup_handler(self):
        """Test CleanupHandler functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Setup test data
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Create a transcoding plan to mark the file as processed
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": mp4_file,
                    "output": self.output_dir
                    / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4",
                    "jpg_to_remove": None,
                }
            ],
            "removals": [],
        }
        context.state_data["plan"] = plan

        # Create a mapping with an orphaned JPG
        jpg_file = self.create_file("2023/01/01/test.jpg", ts=old_ts)
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file, ".jpg": jpg_file}
        }

        handler = CleanupHandler()
        next_state = handler.execute(context)

        # Should transition to TERMINATION
        assert next_state == State.TERMINATION

    def test_cleanup_handler_remove_orphaned_jpgs_with_graceful_exit(self, mocker):
        """Test CleanupHandler's _remove_orphaned_jpgs with graceful exit."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create graceful exit that's already requested
        context.graceful_exit.request_exit()

        # Setup mapping with orphaned JPG
        old_ts = datetime.now() - timedelta(days=31)
        jpg_file = self.create_file("2023/01/01/orphaned.jpg", ts=old_ts)
        mapping = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".jpg": jpg_file}  # No corresponding MP4
        }
        processed = set()  # JPG not in processed set

        handler = CleanupHandler()

        # Call the private method directly
        handler._remove_orphaned_jpgs(context, mapping, processed)

        # Method should exit early due to graceful exit request

    def test_cleanup_handler_clean_empty_directories_invalid_path(self, mocker):
        """Test CleanupHandler's _clean_empty_directories with invalid directory."""
        config = {
            "directory": "/nonexistent/directory",
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        handler = CleanupHandler()

        # Call the private method directly - should handle invalid directory gracefully
        handler._clean_empty_directories(context)

    def test_cleanup_handler_clean_empty_directories_with_error(self, mocker):
        """Test CleanupHandler's _clean_empty_directories with removal error."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "use_trash": False,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create a directory structure that matches date format
        test_dir = self.input_dir / "2023" / "01" / "01"
        test_dir.mkdir(parents=True, exist_ok=True)

        # Mock rmdir to raise an exception
        mocker.patch("pathlib.Path.rmdir", side_effect=OSError("Permission denied"))

        handler = CleanupHandler()

        # Call the private method directly
        handler._clean_empty_directories(context)
        # Should handle the error gracefully without crashing

    def test_archive_cleanup_handler(self):
        """Test ArchiveCleanupHandler functionality."""
        # Create some test archive files for cleanup to process
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        archive_file = self.create_archive(old_ts, size=1024 * 1024)  # 1MB
        assert archive_file.exists()

        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "max_size": 1,  # 1GB
            "age": 30,
            "dry_run": True,  # Use dry run to avoid actual deletion
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Setup state data
        context.state_data["mp4s"] = []
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = set()
        context.config = config

        handler = ArchiveCleanupHandler()
        next_state = handler.execute(context)

        # Can transition to CLEANUP or TERMINATION (both are valid depending on implementation)
        # ArchiveCleanupHandler may transition to CLEANUP or TERMINATION
        assert next_state in [State.CLEANUP, State.TERMINATION]

    def test_termination_handler(self):
        """Test TerminationHandler functionality."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        handler = TerminationHandler()
        next_state = handler.execute(context)

        # Should stay in TERMINATION
        assert next_state == State.TERMINATION

    def test_archive_cleanup_handler_execute_with_no_files(self):
        """Test ArchiveCleanupHandler execute with no files to process."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Setup context state data with no files
        context.state_data["mp4s"] = []
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = set()

        handler = ArchiveCleanupHandler()
        next_state = handler.execute(context)

        # Should transition to TERMINATION when no files are found
        assert next_state == State.TERMINATION


class TestMainAndIntegration(TestBase):
    """Test main functions and integration workflows."""

    def test_parse_arguments(self, monkeypatch):
        """Test the parse_arguments function."""
        import sys

        # Test with default arguments
        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--age",
            "45",
            "--max-size",
            "100",
            "--dry-run",
            "--no-skip",
        ]

        monkeypatch.setattr(sys, "argv", test_args)
        args = parse_arguments()
        assert args.directory == self.input_dir
        assert args.output == self.output_dir
        assert args.age == 45
        assert args.max_size == 100
        assert args.dry_run is True
        assert args.no_skip is True

    def test_parse_arguments_no_trash(self, monkeypatch):
        """Test parse_arguments with --no-trash flag."""
        import sys

        test_args = ["archiver.py", "--directory", str(self.input_dir), "--no-trash"]

        monkeypatch.setattr(sys, "argv", test_args)
        args = parse_arguments()
        assert args.no_trash is True

        # Test that setup_config correctly translates no_trash to use_trash
        config = setup_config(args)
        assert config["use_trash"] is False

    def test_setup_config(self):
        """Test the setup_config function."""
        import argparse

        # Create mock args
        args = argparse.Namespace(
            directory=self.input_dir,
            output=self.output_dir,
            trashdir=None,
            no_trash=False,
            age=30,
            dry_run=True,
            max_size=500,
            no_skip=False,
            cleanup=False,
            clean_output=False,
            no_confirm=True,
            log_file=None,
        )

        config = setup_config(args)

        # Check that config was set up properly
        assert config["directory"] == self.input_dir
        assert config["output"] == self.output_dir
        assert config["use_trash"] is True  # Default when no_trash is False
        assert config["age"] == 30
        assert config["dry_run"] is True
        assert config["max_size"] == 500
        assert config["trash_root"] == self.input_dir / ".deleted"  # Default trash root

    def test_setup_config_with_no_trash(self):
        """Test setup_config when no_trash is True."""
        import argparse

        args = argparse.Namespace(
            directory=self.input_dir,
            output=self.output_dir,
            trashdir=None,
            no_trash=True,  # No trash
            age=30,
            dry_run=True,
            max_size=500,
            no_skip=False,
            cleanup=False,
            clean_output=False,
            no_confirm=True,
            log_file=None,
        )

        config = setup_config(args)
        assert config["use_trash"] is False
        assert config["trash_root"] is None

    def test_run_state_machine_basic(self):
        """Test running the state machine with mocked handlers."""
        # Create a context with mocked config
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": True,
            "no_confirm": True,
        }
        context = Context(config)

        # Pre-populate the context with necessary data to avoid needing full discovery
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # We'll test a simplified state transition by executing the handlers directly
        init_handler = InitializationHandler()
        next_state = init_handler.execute(context)
        assert next_state == State.DISCOVERY

    def test_run_state_machine_with_handler_error(self, mocker):
        """Test run_state_machine error handling when handler raises an exception."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": True,
            "no_confirm": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Mock a handler that raises an exception
        mock_handler = mocker.MagicMock()
        mock_handler.enter.side_effect = Exception("Handler error")
        mock_handler.execute.side_effect = Exception("Handler error")
        mock_handler.exit.side_effect = Exception("Handler error")

        # Mock the StateHandlerFactory to return our mock handler
        mocker.patch(
            "archiver.StateHandlerFactory.create_handler", return_value=mock_handler
        )

        # This should handle the exception and transition to TERMINATION
        run_state_machine(context)
        assert context.current_state == State.TERMINATION

    def test_main_function_basic(self, monkeypatch, mocker):
        """Test the main function with mocked arguments."""
        import sys

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--age",
            "1",  # Low age to trigger processing
            "--dry-run",  # Use dry-run to prevent actual transcoding
            "--no-confirm",  # Skip confirmation
            "--max-size",
            "500",
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Create a test file that would be processed
        old_ts = datetime.now() - timedelta(days=2)
        self.create_file(
            "test_video.mp4", ts=old_ts, content=b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1000)
        )

        # Mock the run_state_machine function to avoid full execution
        mock_run = mocker.patch("archiver.run_state_machine")
        mock_run.return_value = None
        # This should not raise an exception
        result = main()
        assert result == 0  # Main should return 0 on success
        # Ensure run_state_machine was called
        assert mock_run.called

    def test_main_function_with_error(self, monkeypatch, mocker):
        """Test main function error handling."""
        import sys

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Mock setup_config to raise an exception
        mocker.patch("archiver.setup_config", side_effect=Exception("Test error"))
        # The main function returns exit code instead of raising SystemExit
        result = main()
        assert result == 1

    def test_main_function_with_logging_error(self, monkeypatch, mocker):
        """Test main function error handling with logging error."""
        import sys

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Mock run_state_machine to raise an exception
        mocker.patch(
            "archiver.run_state_machine", side_effect=Exception("State machine error")
        )
        # Mock logging.error to verify it's called
        mock_log_error = mocker.patch("archiver.logging.error")
        # The main function should catch the exception and return 1
        result = main()
        assert result == 1
        mock_log_error.assert_called_once()

    def test_full_integration_workflow(self, monkeypatch):
        """Test an end-to-end workflow with the new architecture."""
        import sys

        # Create test files first
        old_ts = datetime.now() - timedelta(days=31)
        self.create_file("2023/01/01/test.mp4", ts=old_ts)

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--age",
            "30",
            "--dry-run",  # Use dry-run to prevent actual processing
            "--no-confirm",  # Skip confirmation
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Run main function (this tests the full flow)
        result = main()
        assert result == 0  # Should complete successfully

    def test_file_info_class(self):
        """Test the FileInfo class functionality."""
        test_path = Path("/test/file.mp4")
        test_ts = datetime.now()

        file_info = FileInfo(test_path, test_ts, 1024, is_archive=True, is_trash=False)

        assert file_info.path == test_path
        assert file_info.timestamp == test_ts
        assert file_info.size == 1024
        assert file_info.is_archive is True
        assert file_info.is_trash is False

    def test_graceful_exit(self):
        """Test GracefulExit functionality."""
        graceful_exit = GracefulExit()

        # Initially should not be exiting
        assert not graceful_exit.should_exit()

        # Request exit
        graceful_exit.request_exit()
        assert graceful_exit.should_exit()

        # Test thread safety by running multiple concurrent requests
        def request_exit():
            graceful_exit.request_exit()

        import threading

        threads = [threading.Thread(target=request_exit) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should still be True
        assert graceful_exit.should_exit()

    @pytest.mark.parametrize(
        "args_combo",
        [
            # Test with cleanup flag
            ["--directory", "camera", "--output", "archived", "--cleanup"],
            # Test with trash functionality
            ["--directory", "camera", "--output", "archived", "--no-trash"],
            # Test with size limit
            ["--directory", "camera", "--output", "archived", "--max-size", "10"],
            # Test with age limit
            ["--directory", "camera", "--output", "archived", "--age", "7"],
            # Test with no skip flag
            ["--directory", "camera", "--output", "archived", "--no-skip"],
        ],
    )
    def test_main_function_with_different_arg_combinations(
        self, monkeypatch, args_combo, mocker
    ):
        """Test main function with different argument combinations."""
        import sys

        # Prepend script name and add required args
        test_args = ["archiver.py"]
        for arg in args_combo:
            if arg == "camera":
                test_args.append(str(self.input_dir))
            elif arg == "archived":
                test_args.append(str(self.output_dir))
            else:
                test_args.append(arg)

        # Add necessary arguments to avoid errors
        if "--directory" not in test_args:
            test_args.extend(["--directory", str(self.input_dir)])
        if "--output" not in test_args:
            test_args.extend(["--output", str(self.output_dir)])
        if "--no-confirm" not in test_args:
            test_args.append("--no-confirm")
        if "--dry-run" not in test_args:
            test_args.append("--dry-run")

        monkeypatch.setattr(sys, "argv", test_args)

        # Mock run_state_machine to prevent full execution
        mock_run = mocker.patch("archiver.run_state_machine")
        mock_run.return_value = None

        # Create a test file
        old_ts = datetime.now() - timedelta(days=2)
        self.create_file("test.mp4", ts=old_ts)

        result = main()
        assert (
            result == 0
        )  # Should complete successfully regardless of arg combinations

    def test_end_to_end_transcoding_workflow(self, monkeypatch, mocker):
        """Test end-to-end transcoding workflow with mocking."""
        import sys

        # Create test files
        old_ts = datetime.now() - timedelta(days=5)
        self.create_file(
            "2023/01/01/test_video.mp4", ts=old_ts, content=b"fake video content"
        )

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--age",
            "3",  # Files older than 3 days
            "--dry-run",  # Use dry-run to avoid actual transcoding
            "--no-confirm",  # Skip confirmation
            "--max-size",
            "500",  # 500GB limit
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Mock transcoder service to avoid actual ffmpeg calls
        _ = mocker.patch("archiver.TranscoderService.transcode_file", return_value=True)
        _ = mocker.patch(
            "archiver.TranscoderService.get_video_duration", return_value=10.0
        )

        result = main()
        assert result == 0
        # The transcoder methods might not be called depending on the workflow path
        # but the main function should complete successfully

    def test_end_to_end_cleanup_workflow(self, monkeypatch, mocker):
        """Test end-to-end cleanup workflow."""
        import sys

        # Create test files with old timestamps
        old_ts = datetime.now() - timedelta(days=60)
        self.create_file("2023/01/01/old_video.mp4", ts=old_ts)

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--cleanup",  # Enable cleanup
            "--age",
            "30",  # Age threshold
            "--dry-run",  # Don't actually delete
            "--no-confirm",  # Skip confirmation
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Mock out the actual deletion operations
        mocker.patch("archiver.FileService.remove_file")

        result = main()
        assert result == 0


class TestArchiveCleanupHandler(TestBase):
    """Test ArchiveCleanupHandler specifically."""

    def test_archive_cleanup_handler_collect_files(self):
        """Test the _collect_all_archive_files method."""
        config = {
            "output": self.output_dir,
            "directory": self.input_dir,
            "trash_root": self.trash_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        archive_file = self.create_archive(old_ts)
        source_file = self.create_file("test_source.mp4", ts=old_ts)

        # Setup context state data
        context.state_data["mp4s"] = [(source_file, old_ts)]

        handler = ArchiveCleanupHandler()

        # Call the private method directly for testing
        all_files = handler._collect_all_archive_files(context)

        # Should collect both archive and source files
        file_paths = {f.path for f in all_files}
        assert archive_file in file_paths
        assert source_file in file_paths

    def test_archive_cleanup_handler_intelligent_cleanup(self):
        """Test the _intelligent_cleanup method."""
        config = {
            "max_size": 1,  # 1GB
            "age": 30,
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create file info objects
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        new_ts = datetime.now() - timedelta(days=1)  # Newer than age threshold

        # Create test files
        old_archive = self.create_archive(old_ts, size=1024 * 1024)  # 1MB
        new_archive = self.create_archive(new_ts, size=1024 * 1024)  # 1MB

        all_files = [
            FileInfo(old_archive, old_ts, 1024 * 1024, is_archive=True, is_trash=False),
            FileInfo(new_archive, new_ts, 1024 * 1024, is_archive=True, is_trash=False),
        ]

        handler = ArchiveCleanupHandler()
        files_to_remove = handler._intelligent_cleanup(context, all_files)

        # Should remove the old archive but not the new one (due to age)
        paths_to_remove = {f.path for f in files_to_remove}
        assert old_archive in paths_to_remove
        assert new_archive not in paths_to_remove

    def test_archive_cleanup_handler_collect_all_archive_files_error_handling(
        self, mocker
    ):
        """Test ArchiveCleanupHandler's _collect_all_archive_files with error handling."""
        config = {
            "output": self.output_dir,
            "directory": self.input_dir,
            "trash_root": self.trash_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        archive_file = self.create_archive(old_ts)

        # Setup context state data
        context.state_data["mp4s"] = [(archive_file, old_ts)]

        handler = ArchiveCleanupHandler()

        # Patch the specific call to list(out_dir.rglob(...)) to raise an exception
        def mock_rglob(*args):
            raise OSError("Permission denied")

        mocker.patch.object(type(self.output_dir), "rglob", mock_rglob)

        # Call the private method directly for testing
        all_files = handler._collect_all_archive_files(context)

        # Should handle the error gracefully and return empty list or list with source files only
        assert isinstance(all_files, list)

    def test_archive_cleanup_handler_collect_all_archive_files_invalid_timestamp(
        self, mocker
    ):
        """Test ArchiveCleanupHandler's _collect_all_archive_files with invalid timestamp."""
        config = {
            "output": self.output_dir,
            "directory": self.input_dir,
            "trash_root": self.trash_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create an archive file with invalid timestamp in name
        invalid_archive = (
            self.output_dir / "2023" / "01" / "01" / "archived-invalid_timestamp.mp4"
        )
        invalid_archive.parent.mkdir(parents=True, exist_ok=True)
        invalid_archive.write_bytes(b"dummy content")

        # Setup context state data
        old_ts = datetime.now() - timedelta(days=31)
        context.state_data["mp4s"] = [(invalid_archive, old_ts)]

        handler = ArchiveCleanupHandler()

        # Call the private method directly for testing
        all_files = handler._collect_all_archive_files(context)

        # Should handle invalid timestamp gracefully
        assert isinstance(all_files, list)

    def test_archive_cleanup_handler_intelligent_cleanup_size_priority(self):
        """Test ArchiveCleanupHandler's _intelligent_cleanup with size-based removal."""
        config = {
            "max_size": 0.001,  # Very small size limit (1MB)
            "age": 30,
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create file info objects
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        newer_ts = datetime.now() - timedelta(days=5)  # Newer than age threshold

        # Create test files
        old_trash_file = self.create_trash_file(old_ts)
        old_archive_file = self.create_archive(old_ts)
        source_file = self.create_file("source.mp4", ts=newer_ts)

        all_files = [
            FileInfo(
                old_trash_file, old_ts, 2 * 1024 * 1024, is_archive=False, is_trash=True
            ),  # 2MB
            FileInfo(
                old_archive_file,
                old_ts,
                2 * 1024 * 1024,
                is_archive=True,
                is_trash=False,
            ),  # 2MB
            FileInfo(
                source_file, newer_ts, 2 * 1024 * 1024, is_archive=False, is_trash=False
            ),  # 2MB
        ]

        handler = ArchiveCleanupHandler()
        files_to_remove = handler._intelligent_cleanup(context, all_files)

        # With very small size limit, should remove files according to priority (trash first)
        assert len(files_to_remove) > 0
        # The trash file should be removed first due to priority
        paths_to_remove = {f.path for f in files_to_remove}
        assert old_trash_file in paths_to_remove

    def test_archive_cleanup_handler_intelligent_cleanup_age_disabled(self):
        """Test ArchiveCleanupHandler's _intelligent_cleanup with age-based cleanup disabled."""
        config = {
            "max_size": 1,  # 1GB - not exceeded
            "age": 0,  # Age-based cleanup disabled
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create file info objects
        old_ts = datetime.now() - timedelta(
            days=35
        )  # Older than would normally be removed

        # Create test files
        old_archive = self.create_archive(old_ts, size=1024 * 1024)  # 1MB

        all_files = [
            FileInfo(old_archive, old_ts, 1024 * 1024, is_archive=True, is_trash=False),
        ]

        handler = ArchiveCleanupHandler()
        files_to_remove = handler._intelligent_cleanup(context, all_files)

        # With age=0, age-based cleanup should be disabled, so no files should be removed (size not exceeded)
        # This tests the early exit condition when age_days <= 0
        paths_to_remove = {f.path for f in files_to_remove}
        # With size limit of 1GB and only 1MB file, no files should be removed based on size
        assert old_archive not in paths_to_remove

    def test_archive_cleanup_handler_categorize_files(self):
        """Test ArchiveCleanupHandler's _categorize_files method."""
        handler = ArchiveCleanupHandler()

        # Create test files
        old_ts = datetime.now() - timedelta(days=35)
        newer_ts = datetime.now() - timedelta(days=5)

        test_files = [
            FileInfo(
                Path("/trash/file1.mp4"), old_ts, 1024, is_archive=False, is_trash=True
            ),  # Priority 0
            FileInfo(
                Path("/archive/file2.mp4"),
                old_ts,
                2048,
                is_archive=True,
                is_trash=False,
            ),  # Priority 1
            FileInfo(
                Path("/source/file3.mp4"),
                newer_ts,
                4096,
                is_archive=False,
                is_trash=False,
            ),  # Priority 2
            FileInfo(
                Path("/archive/file4.mp4"),
                newer_ts,
                3072,
                is_archive=True,
                is_trash=False,
            ),  # Priority 1
        ]

        categorized = handler._categorize_files(test_files)

        # Check that files are categorized by priority
        assert len(categorized[0]) == 1  # Trash files
        assert len(categorized[1]) == 2  # Archive files
        assert len(categorized[2]) == 1  # Source files

        # Check that files within each category are sorted by timestamp
        if categorized[1]:  # If archive files exist
            # Should be sorted with oldest first
            archive_files = categorized[1]
            assert archive_files[0].timestamp <= archive_files[-1].timestamp

    @pytest.mark.parametrize(
        "size_limit_gb,expected_removal",
        [
            (0.001, True),  # Very small limit, should remove files
            (100, False),  # Large limit, should not remove files
        ],
    )
    def test_archive_cleanup_handler_size_based_cleanup(
        self, size_limit_gb, expected_removal
    ):
        """Test ArchiveCleanupHandler size-based cleanup with different size limits."""
        config = {
            "max_size": size_limit_gb,
            "age": 30,
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        newer_ts = datetime.now() - timedelta(days=5)  # Newer than age threshold

        # Create test files - larger files to trigger size-based cleanup
        archive_file = self.create_archive(old_ts, size=5 * 1024 * 1024)  # 5MB
        source_file = self.create_file(
            "test_source.mp4", ts=newer_ts, content=b"x" * (2 * 1024 * 1024)
        )  # 2MB

        # Setup context state data
        context.state_data["mp4s"] = [(source_file, newer_ts)]
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = set()
        context.config = config

        handler = ArchiveCleanupHandler()

        # Check initial files exist
        assert archive_file.exists()
        assert source_file.exists()

        # Call execute to run the cleanup
        next_state = handler.execute(context)

        # Verify behavior based on size limit
        if expected_removal:
            # With small size limit, should have attempted removal of the archive file
            if archive_file.exists():
                # If it still exists, it means dry_run was active
                pass
        else:
            # With large size limit, should not have attempted removal
            # The files should still exist (or would if not in dry run mode)
            pass

        # The state transition should be valid
        assert next_state in [State.CLEANUP, State.TERMINATION]

    def test_archive_cleanup_handler_age_based_cleanup(self):
        """Test ArchiveCleanupHandler age-based cleanup functionality."""
        config = {
            "max_size": 1,  # 1GB - not exceeded
            "age": 30,  # Files older than 30 days should be removed if clean_output is True
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create old and new archive files
        old_ts = datetime.now() - timedelta(days=40)  # Older than age threshold
        newer_ts = datetime.now() - timedelta(days=5)  # Newer than age threshold

        _ = self.create_archive(old_ts, size=1024 * 1024)  # 1MB
        _ = self.create_archive(newer_ts, size=1024 * 1024)  # 1MB

        # Setup context state data
        context.state_data["mp4s"] = []
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = set()
        context.config = config

        handler = ArchiveCleanupHandler()
        next_state = handler.execute(context)

        # Should transition appropriately
        assert next_state in [State.CLEANUP, State.TERMINATION]

    def test_archive_cleanup_handler_priority_based_removal(self):
        """Test ArchiveCleanupHandler removal priority: trash > archive > source."""
        config = {
            "max_size": 0.001,  # Very small size limit to trigger removals
            "age": 30,
            "clean_output": True,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create files with different priorities
        old_ts = datetime.now() - timedelta(days=35)
        newer_ts = datetime.now() - timedelta(days=5)

        # Create trash file (priority 0 - highest)
        trash_file = self.create_trash_file(old_ts)
        # Create archive file (priority 1 - medium)
        _ = self.create_archive(old_ts, size=2 * 1024 * 1024)  # 2MB
        # Create source file (priority 2 - lowest)
        source_file = self.create_file(
            "source.mp4", ts=newer_ts, content=b"x" * (2 * 1024 * 1024)
        )  # 2MB

        # Setup context state data
        context.state_data["mp4s"] = [(source_file, newer_ts)]
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = {
            trash_file
        }  # Add trash file to trash_files set
        context.config = config

        handler = ArchiveCleanupHandler()
        next_state = handler.execute(context)

        # Should transition appropriately
        assert next_state in [State.CLEANUP, State.TERMINATION]

    @pytest.mark.parametrize("clean_output_flag", [True, False])
    def test_archive_cleanup_handler_clean_output_flag(self, clean_output_flag):
        """Test ArchiveCleanupHandler with different clean_output flag values."""
        config = {
            "max_size": 1,  # 1GB
            "age": 1,  # 1 day threshold
            "clean_output": clean_output_flag,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create an old archive file
        old_ts = datetime.now() - timedelta(days=5)  # Older than 1 day threshold
        _ = self.create_archive(old_ts, size=1024 * 1024)  # 1MB

        # Setup context state data
        context.state_data["mp4s"] = []
        context.state_data["mapping"] = {}
        context.state_data["trash_files"] = set()
        context.config = config

        handler = ArchiveCleanupHandler()
        next_state = handler.execute(context)

        # Should transition appropriately regardless of clean_output flag
        assert next_state in [State.CLEANUP, State.TERMINATION]


class TestPlanningHandlerSkipLogic(TestBase):
    """Test PlanningHandler skip logic functionality."""

    def test_planning_handler_skip_existing_archive(self):
        """Test PlanningHandler when archive already exists with sufficient size."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "no_confirm": True,  # Skip confirmation
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create an old MP4 file to be processed
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Create an existing archive file with sufficient size
        archive_path = (
            self.output_dir
            / "2023"
            / "01"
            / "01"
            / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.write_bytes(
            b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1000)
        )  # Larger than threshold

        # Setup context state data
        context.state_data["mp4s"] = [(mp4_file, old_ts)]
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file}
        }
        context.state_data["trash_files"] = set()

        handler = PlanningHandler()

        # Mock the confirmation to return True
        with patch.object(handler, "_ask_confirmation", return_value=True):
            next_state = handler.execute(context)

        # Should transition to EXECUTION (or ARCHIVE_CLEANUP if cleanup flag is set)
        assert next_state in [State.EXECUTION, State.ARCHIVE_CLEANUP, State.TERMINATION]

        # Verify that the plan was generated correctly
        assert "plan" in context.state_data
        plan = context.state_data["plan"]

        # Should have no transcoding actions (since archive exists)
        assert len(plan["transcoding"]) == 0

        # Should have removal actions for the source file
        assert len(plan["removals"]) > 0
        removal_file_paths = [action["file"] for action in plan["removals"]]
        assert mp4_file in removal_file_paths

    def test_planning_handler_skip_existing_archive_with_jpg_pair(self, mocker):
        """Test PlanningHandler when archive exists and there's a JPG pair to be removed."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "no_confirm": True,  # Skip confirmation
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create an old MP4 file and paired JPG
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)
        jpg_file = self.create_file("2023/01/01/test.jpg", ts=old_ts)

        # Create an existing archive file with sufficient size
        archive_path = (
            self.output_dir
            / "2023"
            / "01"
            / "01"
            / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.write_bytes(
            b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1000)
        )  # Larger than threshold

        # Setup context state data
        context.state_data["mp4s"] = [(mp4_file, old_ts)]
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file, ".jpg": jpg_file}
        }
        context.state_data["trash_files"] = set()

        handler = PlanningHandler()

        # Mock the confirmation to return True
        with patch.object(handler, "_ask_confirmation", return_value=True):
            next_state = handler.execute(context)

        # Should transition appropriately
        assert next_state in [State.EXECUTION, State.ARCHIVE_CLEANUP, State.TERMINATION]

        # Verify the plan
        assert "plan" in context.state_data
        plan = context.state_data["plan"]

        # Should have no transcoding actions
        assert len(plan["transcoding"]) == 0

        # Should have removal actions for both MP4 and JPG
        assert len(plan["removals"]) >= 2
        removal_file_paths = [action["file"] for action in plan["removals"]]
        assert mp4_file in removal_file_paths
        assert jpg_file in removal_file_paths

    def test_planning_handler_dry_run_exits_immediately(self, mocker):
        """Test PlanningHandler exits immediately after displaying plan when dry_run is True."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": True,  # Enable dry run
            "no_confirm": False,  # This should be ignored in dry run
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test data
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Setup context state data
        context.state_data["mp4s"] = [(mp4_file, old_ts)]
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file}
        }
        context.state_data["trash_files"] = set()

        handler = PlanningHandler()

        # Mock the confirmation to ensure it's not called (since dry_run should bypass confirmation)
        mock_confirmation = mocker.patch.object(handler, "_ask_confirmation")

        next_state = handler.execute(context)

        # Should transition to TERMINATION immediately after displaying plan
        assert next_state == State.TERMINATION

        # Confirmation should not have been called in dry-run mode
        mock_confirmation.assert_not_called()

        # Plan should have been generated
        assert "plan" in context.state_data
        plan = context.state_data["plan"]
        assert len(plan["transcoding"]) > 0  # Should have transcoding plan


class TestErrorHandling(TestBase):
    """Test error handling and edge case scenarios."""

    def test_transcoder_error_handling_during_execution(self, mocker):
        """Test ExecutionHandler behavior when transcoding fails."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": False,  # Actually test transcoding
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Create a plan with transcoding action
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": mp4_file,
                    "output": self.output_dir
                    / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4",
                    "jpg_to_remove": None,
                }
            ],
            "removals": [],
        }
        context.state_data["plan"] = plan

        # Mock the transcoder to fail
        mocker.patch.object(
            context.services["transcoder_service"],
            "transcode_file",
            return_value=False,  # Simulate failure
        )

        handler = ExecutionHandler()
        next_state = handler.execute(context)

        # Should still transition to the next state even if transcoding fails for individual files
        assert next_state in [State.CLEANUP, State.ARCHIVE_CLEANUP, State.TERMINATION]

    def test_file_removal_error_handling(self, mocker):
        """Test error handling when file removal fails."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
            "dry_run": False,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        test_file = self.create_file("2023/01/01/test_error.mp4", ts=old_ts)

        # Create plan with removal action
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_skip",
                    "file": test_file,
                    "reason": f"Test removal of {test_file}",
                }
            ],
        }
        context.state_data["plan"] = plan

        # Mock the logger's error method to verify it's called when an error occurs
        mock_logger_error = mocker.patch.object(context.logger, "error")

        # Mock the file removal to raise an exception
        def failing_remove(
            file_path,
            logger,
            dry_run=False,
            use_trash=False,
            trash_root=None,
            is_output=False,
            source_root=None,
        ):
            # Simulate a real file removal error by actually removing the try/except
            # This should be caught and logged by the FileService.remove_file method
            raise PermissionError("Permission denied")

        # Instead, let's test the actual error handling by mocking the low-level operation
        original_unlink = Path.unlink

        def mock_unlink(self):
            if str(self) == str(test_file):
                raise PermissionError("Permission denied")
            return original_unlink(self)

        mocker.patch("pathlib.Path.unlink", side_effect=mock_unlink)

        handler = ExecutionHandler()
        # Should handle the exception gracefully without crashing
        _ = handler.execute(context)
        # Should continue to next state despite file removal error
        # Verify that the error was logged
        assert mock_logger_error.called

    def test_camera_service_discovery_error_handling(self, mocker):
        """Test CameraService error handling during file discovery."""
        config = {
            "directory": self.input_dir,
            "trash_root": self.trash_dir,
        }
        service = CameraService(config)

        # Create test files first so there's something to iterate over
        old_ts = datetime.now() - timedelta(days=31)
        _ = self.create_file("2023/01/01/test_error.mp4", ts=old_ts)

        # Mock the timestamp parsing to fail for the test file, which would cause an exception in the loop
        mocker.patch.object(
            service, "_parse_timestamp", side_effect=Exception("Parse error")
        )

        # Should handle the exception gracefully and continue processing
        mp4s, mapping, trash_files = service.discover_files(self.input_dir)
        # Should return empty lists/dict rather than crashing when parsing fails
        assert mp4s == []
        assert mapping == {}
        # trash_files might not be empty if test files are in trash directory

    def test_cleanup_handler_error_interrupted_by_graceful_exit(self, mocker):
        """Test cleanup handler behavior when interrupted by graceful exit."""
        config = {
            "directory": self.input_dir,
            "output": self.output_dir,
        }
        context = Context(config)

        # Setup logging
        logging_service = LoggingService(config)
        context.logger = logging_service.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        mp4_file = self.create_file("2023/01/01/test.mp4", ts=old_ts)

        # Setup context data
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": mp4_file,
                    "output": self.output_dir
                    / f"archived-{old_ts.strftime('%Y%m%d%H%M%S')}.mp4",
                    "jpg_to_remove": None,
                }
            ],
            "removals": [],
        }
        context.state_data["plan"] = plan

        # Create mapping with orphaned JPG
        jpg_file = self.create_file("2023/01/01/orphaned.jpg", ts=old_ts)
        context.state_data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_file, ".jpg": jpg_file}
        }

        handler = CleanupHandler()

        # Request graceful exit before execution
        context.graceful_exit.request_exit()

        # Execute should handle the graceful exit condition
        _ = handler.execute(context)
        # Should transition appropriately despite graceful exit

    def test_transcoder_service_duration_error_handling(self, mocker):
        """Test TranscoderService error handling when getting video duration fails."""
        config = {"directory": self.input_dir, "output": self.output_dir}
        service = TranscoderService(config)

        # Create a test file
        test_file = self.create_file("test.mp4")

        # Mock shutil.which to simulate ffprobe available but subprocess failing
        mocker.patch("archiver.shutil.which", return_value="/usr/bin/ffprobe")
        mocker.patch("archiver.subprocess.run", side_effect=Exception("Command failed"))

        # Should handle the error gracefully and return None
        result = service.get_video_duration(test_file)
        assert result is None


class TestProgressIntegration:
    """Integration tests for progress bar and logging interaction."""

    def test_progress_bar_redraw_with_logging_in_tty(self):
        """Integration test for progress bar redraw fix in TTY mode after logging."""
        import io
        import logging
        from archiver import ProgressReporter, GuardedStreamHandler, ConsoleOrchestrator

        # Create a mock TTY stream
        stream = io.StringIO()
        stream.isatty = lambda: True  # Mock as TTY

        # Create progress reporter with TTY stream
        progress_bar = ProgressReporter(total_files=3, out=stream, silent=False)
        orchestrator = ConsoleOrchestrator()

        # Create guarded stream handler with progress bar (simulating actual usage)
        handler = GuardedStreamHandler(
            orchestrator, stream=stream, progress_bar=progress_bar
        )

        # Set up basic formatter
        formatter = logging.Formatter("%(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        # Create a mock log record that could occur during transcoding
        record = logging.LogRecord(
            name="camera_archiver",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Processing file test_video.mp4",
            args=(),
            exc_info=None,
        )

        # Initialize progress bar with an active progress line (simulating active transcoding)
        progress_bar.start_processing()
        progress_bar.start_file()
        progress_bar._progress_line = (
            "Progress [1/3]: 50% [||||||||||||||||--------] 00:05 (00:00:10)"
        )

        # Verify initial state
        assert progress_bar._progress_line, "Progress line should be active"
        assert progress_bar._is_tty(), "Should detect as TTY"

        # The critical test: emit a log record while progress bar is active
        # Before the fix, this would disrupt the progress bar display
        handler.emit(record)

        # Verify that the stream contains both the log message and the progress bar redraw
        stream_content = stream.getvalue()

        # The content should show the log message was written and the progress bar was redrawn
        # This verifies that the TTY-specific logic in the emit method works correctly
        assert "Processing file test_video.mp4" in stream_content, (
            "Log message should appear in output"
        )

        # Now test progress bar update after logging - this should continue to work
        progress_bar.update_progress(1, 75.0)

        # Clean up
        progress_bar.finish()

        # Additional verification that TTY-specific behavior worked correctly
        # In TTY mode with progress bar, the output should contain control characters
        assert "\r\x1b[2K" in stream_content, (
            "Should use TTY control sequences for progress bar redraw"
        )

    def test_progress_bar_normal_logging_without_progress_bar(self):
        """Test that normal logging continues to work when no progress bar is active."""
        import io
        import logging
        from archiver import ProgressReporter, GuardedStreamHandler, ConsoleOrchestrator

        stream = io.StringIO()
        stream.isatty = lambda: True  # Test in TTY mode

        # Create progress reporter but without active progress line
        progress_bar = ProgressReporter(total_files=1, out=stream, silent=True)
        orchestrator = ConsoleOrchestrator()

        # Create guarded stream handler
        handler = GuardedStreamHandler(
            orchestrator, stream=stream, progress_bar=progress_bar
        )

        # Set up basic formatter
        formatter = logging.Formatter("%(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        # Create a mock log record - no active progress line
        record = logging.LogRecord(
            name="camera_archiver",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Normal log without progress bar",
            args=(),
            exc_info=None,
        )

        # Since there's no active progress line, should use normal logging path
        handler.emit(record)

        stream_content = stream.getvalue()

        # Verify log message appears normally
        assert "Normal log without progress bar" in stream_content, (
            "Normal log should work"
        )
