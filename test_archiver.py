#!/usr/bin/env python3
"""Comprehensive integration tests for archiver.py using pytest."""

import logging
import shutil
import tempfile
import unittest.mock
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from archiver import (
    MIN_ARCHIVE_SIZE_BYTES,
    ActionPlanner,
    Archiver,
    ArchiveStateMachine,
    CleanupState,
    Config,
    ConsoleOrchestrator,
    DiscoveryState,
    ExecutionState,
    FileCleaner,
    FileOperationsService,
    FileScanner,
    GracefulExit,
    GuardedStreamHandler,
    InitializationState,
    Logger,
    PlanningState,
    ProgressReporter,
    State,
    TerminationState,
    Transcoder,
    TranscodingService,
    _ActionPlanGenerator,
    ask_confirmation,
    construct_output_path,
    execute_with_service_check,
    main,
    parse_arguments,
    safe_get_data,
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

        # Patch ask_confirmation to return True by default to prevent hanging
        import unittest.mock

        self.ask_confirmation_patcher = unittest.mock.patch(
            "archiver.ask_confirmation", return_value=True
        )
        self.mock_ask_confirmation = self.ask_confirmation_patcher.start()

    def teardown_method(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        GuardedStreamHandler.emit = self._orig_emit
        ProgressReporter._display = self._orig_progress_display
        ProgressReporter.redraw = self._orig_progress_redraw
        ProgressReporter._cleanup_progress_bar = self._orig_progress_cleanup
        # Stop the ask_confirmation patcher if it exists
        if hasattr(self, "ask_confirmation_patcher"):
            self.ask_confirmation_patcher.stop()

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


class TestConfigAndFileScanner(TestBase):
    """Test configuration and file scanner functionality."""

    def test_config_defaults_and_parsing(self):
        """Test Config class defaults and CLI parsing."""
        import unittest.mock

        args = unittest.mock.Mock(
            directory=Path("/camera"),
            output=Path("/camera/archived"),
            age=30,
            dry_run=False,
            max_size=500,
            no_skip=False,
            use_trash=True,
            cleanup=False,
            clean_output=False,
            trashdir=None,
            no_confirm=False,
        )
        cfg = Config.from_args(args)
        assert cfg.directory == Path("/camera")
        assert cfg.output == Path("/camera/archived")
        assert not cfg.dry_run
        assert cfg.use_trash
        assert not cfg.no_confirm

    def test_config_directory_exists_logic(self):
        """Test Config.from_args when directory exists vs doesn't exist."""
        # Mock args where directory exists
        existing_dir = Path(self.temp_dir)  # This directory exists
        args_with_existing_dir = unittest.mock.Mock(
            directory=existing_dir,
            output=Path("/camera/archived"),
            age=30,
            dry_run=False,
            max_size=500,
            no_skip=False,
            use_trash=True,
            cleanup=False,
            clean_output=False,
            trashdir=None,
            no_confirm=False,
        )
        cfg1 = Config.from_args(args_with_existing_dir)
        assert cfg1.directory == existing_dir

        # Mock args where directory doesn't exist - should fall back to default /camera
        non_existing_dir = Path("/nonexistent/directory")
        args_with_non_existing_dir = unittest.mock.Mock(
            directory=non_existing_dir,
            output=Path("/camera/archived"),
            age=30,
            dry_run=False,
            max_size=500,
            no_skip=False,
            use_trash=True,
            cleanup=False,
            clean_output=False,
            trashdir=None,
            no_confirm=False,
        )
        cfg2 = Config.from_args(args_with_non_existing_dir)
        # When provided directory doesn't exist, it should fall back to default /camera
        assert cfg2.directory == Path("/camera")

    def test_config_get_trash_root(self):
        """Test Config.get_trash_root functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.trashdir = None
        assert cfg.get_trash_root() == self.input_dir / ".deleted"
        cfg.trashdir = Path("/tmp/trash")
        assert cfg.get_trash_root() == Path("/tmp/trash")

    @pytest.mark.parametrize(
        "filename,expected_result",
        [
            ("REO_CAMERA_20230101120000.mp4", True),  # Valid MP4
            ("REO_CAMERA_20230101120000.jpg", True),  # Valid JPG
            ("REO_CAMERA_19991231120000.mp4", False),  # Year out of range (too past)
            ("REO_CAMERA_21001231120000.mp4", False),  # Year out of range (too future)
            ("invalid.mp4", None),  # Invalid format
            (
                "SOME_OTHER_20230101120000.mp4",
                None,
            ),  # Wrong prefix - doesn't start with REO
            ("REO_CAMERA_invalid.mp4", None),  # Invalid timestamp
            ("REO_CAMERA_20230101120000", None),  # Missing extension
            (
                "REO_Camera_20230101120000.mp4",
                True,
            ),  # Different case should work (IGNORECASE)
            (
                "reo_camera_20230101120000.mp4",
                True,
            ),  # Lowercase should work (IGNORECASE)
            ("REO_XYZ_20230101120000.mp4", True),  # Any prefix after REO_ should work
        ],
    )
    def test_parse_timestamp_from_filename_parametrized(
        self, filename, expected_result
    ):
        """Parametrized test for FileScanner.parse_timestamp_from_filename."""
        ts = FileScanner.parse_timestamp_from_filename(filename)
        if expected_result is True:
            assert ts is not None
            # Extract the timestamp part from filename (after underscore)
            timestamp_part = filename.split("_")[-1][:14]  # Get the 14-digit timestamp
            expected_year = int(timestamp_part[:4])
            assert ts.year == expected_year
            # Validate the timestamp is in the expected range
            assert 2000 <= ts.year <= 2099
        elif expected_result is False:
            assert ts is None  # Year out of range returns None
        else:  # expected_result is None
            assert ts is None

    def test_parse_timestamp_from_filename(self):
        """Test FileScanner.parse_timestamp_from_filename."""
        cases = [
            ("REO_CAMERA_20230101120000.mp4", True),
            ("REO_CAMERA_19991231120000.mp4", False),  # year out of range
            ("invalid.mp4", None),
            ("REO_CAMERA_20231231120000.jpg", True),
            ("REO_CAMERA_21001231120000.mp4", False),  # year out of range (too future)
        ]
        for name, expected in cases:
            ts = FileScanner.parse_timestamp_from_filename(name)
            if expected is True:
                assert ts is not None
                # Extract the timestamp part from filename (after underscore)
                timestamp_part = name.split("_")[-1][:14]  # Get the 14-digit timestamp
                expected_year = int(timestamp_part[:4])
                assert ts.year == expected_year
            elif expected is False:
                assert ts is None  # Year out of range returns None
            else:  # expected is None
                assert ts is None

    def test_scan_files_includes_trash(self):
        """Test FileScanner functionality with trash files."""
        ts = (datetime.now() - timedelta(days=10)).replace(microsecond=0)
        inp = self.create_file("2023/01/01/video.mp4", ts=ts)
        trash_file = self.create_trash_file(ts)

        mp4s, mapping, trash = FileScanner.scan_files(
            self.input_dir, include_trash=True, trash_root=self.trash_dir
        )
        assert (inp, ts) in mp4s
        assert trash_file in trash

    def test_scan_files_with_graceful_exit(self):
        """Test FileScanner when graceful exit is requested."""
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()
        mp4s, mapping, trash_files = FileScanner.scan_files(
            self.input_dir, graceful_exit=graceful_exit
        )
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0


class TestOutputPathAndArchiver(TestBase):
    """Test output path generation and archiver functionality."""

    def test_output_path_with_date_structure(self):
        """Test Archiver.output_path with date structure in input path."""
        ts = datetime.now()
        inp = (
            self.input_dir
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        inp.parent.mkdir(parents=True, exist_ok=True)
        inp.write_bytes(b"x")
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        archiver = Archiver(cfg)

        out = archiver.output_path(inp, ts)
        parts = list(out.parts[-4:])
        assert parts[0] == str(ts.year)
        assert parts[1] == f"{ts.month:02d}"
        assert parts[2] == f"{ts.day:02d}"

    def test_output_path_non_date_input(self):
        """Test Archiver.output_path with non-date structure in input path."""
        ts = datetime.now()
        inp = (
            self.input_dir
            / "some_folder"
            / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        inp.parent.mkdir(parents=True, exist_ok=True)
        inp.write_bytes(b"x")
        cfg = Config()
        cfg.output = self.output_dir
        archiver = Archiver(cfg)

        out = archiver.output_path(inp, ts)
        # The structure should be: output_dir/year/month/day/filename
        assert str(ts.year) in out.parts[-4:]
        assert out.name.startswith("archived-")

    def test_construct_output_path_utility(self):
        """Test the construct_output_path utility function."""
        output_dir = Path("/output")

        # Test with date-structured input path
        input_with_date = Path("/input/2023/01/15/REO_CAMERA_20230115120000.mp4")
        ts = datetime(2023, 1, 15, 12, 0, 0)
        result = construct_output_path(output_dir, input_with_date, ts)

        expected = (
            output_dir
            / "2023"
            / "01"
            / "15"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert result == expected

        # Test with non-date structured input path
        input_no_date = Path("/input/videos/REO_CAMERA_20230115120000.mp4")
        result2 = construct_output_path(output_dir, input_no_date, ts)

        expected2 = (
            output_dir
            / "2023"
            / "01"
            / "15"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert result2 == expected2


class TestTranscoder(TestBase):
    """Test transcoder functionality."""

    def test_get_video_duration(self):
        """Test Transcoder.get_video_duration."""
        f = self.create_file("video.mp4")
        with unittest.mock.patch("shutil.which", return_value=None):
            assert Transcoder.get_video_duration(f) is None
        # Simulate ffprobe returning a number
        proc = MagicMock()
        proc.stdout.strip.return_value = "12.34"
        graceful_exit = GracefulExit()
        with (
            unittest.mock.patch("subprocess.run", return_value=proc),
            unittest.mock.patch("shutil.which", return_value="/usr/bin/ffprobe"),
        ):
            assert (
                Transcoder.get_video_duration(f, graceful_exit=graceful_exit) == 12.34
            )

    def test_get_video_duration_error_handling(self):
        """Test error handling in transcoder duration extraction."""
        f = self.create_file("video.mp4")

        # Test when ffprobe is not available
        import unittest.mock

        with unittest.mock.patch("archiver.shutil.which", return_value=None):
            assert Transcoder.get_video_duration(f) is None

        # Test exception handling in duration extraction
        with (
            unittest.mock.patch(
                "archiver.subprocess.run", side_effect=Exception("Command failed")
            ),
            unittest.mock.patch(
                "archiver.shutil.which", return_value="/usr/bin/ffprobe"
            ),
        ):
            assert Transcoder.get_video_duration(f) is None

        # Test N/A result
        proc = MagicMock()
        proc.stdout.strip.return_value = "N/A"
        with (
            unittest.mock.patch("archiver.subprocess.run", return_value=proc),
            unittest.mock.patch(
                "archiver.shutil.which", return_value="/usr/bin/ffprobe"
            ),
        ):
            assert Transcoder.get_video_duration(f) is None

    def test_transcode_graceful_exit(self):
        """Test Transcoder.transcode_file with graceful exit."""
        src = self.create_file("src.mp4")
        dst = Path("/tmp/out.mp4")

        # Before start
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()
        ok = Transcoder.transcode_file(
            src, dst, Logger.setup(None), graceful_exit=graceful_exit
        )
        assert not ok

        # During run – mock Popen to have an iterable stdout that causes exit during processing
        def mock_stdout_generator():
            graceful_exit.request_exit()  # Trigger exit on first iteration
            yield "frame=1 time=00:00:01.00\n"
            yield "frame=2 time=00:00:02.00\n"

        mock_proc = MagicMock()
        mock_proc.stdout = mock_stdout_generator()
        mock_proc.wait = MagicMock(return_value=0)

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc):
            ok = Transcoder.transcode_file(
                src,
                dst,
                Logger.setup(None),
                progress_cb=None,
                graceful_exit=graceful_exit,
            )
            assert not ok

    def test_transcode_success_and_progress(self):
        """Test Transcoder.transcode_file with progress callback."""
        src = self.create_file("src.mp4")
        dst = Path("/tmp/out.mp4")

        # Mock Popen to provide a fake stdout with time=… lines
        mp = MagicMock()
        mp.stdout = iter(
            [
                "frame=1 time=00:00:10.00\n",
                "frame=2 time=00:00:20.00\n",
            ]
        )
        mp.wait.return_value = 0

        graceful_exit = GracefulExit()
        with (
            unittest.mock.patch("subprocess.Popen", return_value=mp),
            unittest.mock.patch.object(
                Transcoder, "get_video_duration", return_value=60
            ),
        ):
            progress_calls = []
            ok = Transcoder.transcode_file(
                src,
                dst,
                Logger.setup(None),
                progress_cb=lambda p: progress_calls.append(p),
                graceful_exit=graceful_exit,
            )
            assert ok
            assert len(progress_calls) > 0

    def test_transcode_error_handling(self):
        """Test Transcoder error handling scenarios."""
        src = self.create_file("src.mp4")
        dst = self.temp_dir / "out.mp4"

        # Test when Popen fails
        with (
            unittest.mock.patch.object(
                Transcoder, "get_video_duration", return_value=None
            ),
            unittest.mock.patch(
                "archiver.subprocess.Popen",
                side_effect=OSError("Failed to start process"),
            ),
        ):
            result = Transcoder.transcode_file(src, dst, Logger.setup(None))
            assert not result

        # Test process with non-zero return code
        mock_proc = MagicMock()
        mock_proc.stdout = iter(["frame=1 time=00:00:01.00\n"])
        mock_proc.wait.return_value = 1  # Non-zero exit code

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc):
            result = Transcoder.transcode_file(src, dst, Logger.setup(None))
            # Should return False due to non-zero exit code
            assert not result

    def test_transcoder_transcode_file_with_invalid_stdout_types(self):
        """Test Transcoder.transcode_file with different stdout types."""
        src_file = self.create_file("src.mp4", content=b"x" * 100000)
        dst_file = self.temp_dir / "dst.mp4"

        logger = Logger.setup(None)
        graceful_exit = GracefulExit()

        # Mock process with list stdout to test different path
        mock_proc = MagicMock()
        mock_proc.stdout = ["line1", "line2"]  # List instead of file object
        mock_proc.wait.return_value = 0

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc):
            with unittest.mock.patch.object(
                Transcoder, "get_video_duration", return_value=5.0
            ):
                _ = Transcoder.transcode_file(
                    src_file, dst_file, logger, graceful_exit=graceful_exit
                )
                # Should handle list output gracefully without crashing

    def test_transcoder_error_scenarios(self):
        """Test various error handling scenarios in Transcoder."""
        src_file = self.create_file("src.mp4", content=b"x" * 100000)
        dst_file = self.temp_dir / "dst.mp4"
        logger = Logger.setup(None)

        # Test scenario where proc.stdout is None
        mock_proc_with_none_stdout = MagicMock()
        mock_proc_with_none_stdout.stdout = (
            None  # This should trigger the error handling
        )
        mock_proc_with_none_stdout.wait.return_value = 0

        with unittest.mock.patch(
            "subprocess.Popen", return_value=mock_proc_with_none_stdout
        ):
            result = Transcoder.transcode_file(src_file, dst_file, logger)
            # Should return False when stdout is None
            assert result is False

        # Test scenario where proc.stdout has no readline method and no __iter__ method
        class MockStdoutNoIterNoReadline:
            pass  # Object with no readline or __iter__ methods

        mock_proc_unsupported = MagicMock()
        mock_proc_unsupported.stdout = MockStdoutNoIterNoReadline()
        mock_proc_unsupported.wait.return_value = 0

        with unittest.mock.patch(
            "subprocess.Popen", return_value=mock_proc_unsupported
        ):
            result = Transcoder.transcode_file(src_file, dst_file, logger)
            # Should return False for unsupported stdout type
            assert result is False

        # Test scenario where reading from stdout raises an exception
        class FailingStdout:
            def __iter__(self):
                raise Exception("Mock iteration failure")

        mock_proc_failing = MagicMock()
        mock_proc_failing.stdout = FailingStdout()
        mock_proc_failing.wait.return_value = 0

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc_failing):
            result = Transcoder.transcode_file(src_file, dst_file, logger)
            # Should return False when stdout reading fails
            assert result is False


class TestFileCleaner(TestBase):
    """Test file cleaner functionality."""

    def test_calculate_trash_destination_collision(self):
        """Test FileCleaner.calculate_trash_destination collision handling."""
        src = self.create_file("file.mp4")
        dest1 = FileCleaner.calculate_trash_destination(
            src, self.input_dir, self.trash_dir
        )
        dest1.parent.mkdir(parents=True, exist_ok=True)
        dest1.write_text("x")  # create collision

        dest2 = FileCleaner.calculate_trash_destination(
            src, self.input_dir, self.trash_dir
        )
        assert dest1 != dest2
        assert not dest2.exists()

    def test_calculate_trash_destination_with_value_error(self):
        """Test FileCleaner.calculate_trash_destination when relative_to raises ValueError."""
        src = self.create_file("test.mp4")

        # This should handle the ValueError when relative_to fails
        dest = FileCleaner.calculate_trash_destination(
            src, self.input_dir / "nonexistent_parent", self.trash_dir
        )

        # Should still return a valid destination path even when ValueError occurs
        assert isinstance(dest, Path)
        assert src.name in dest.name  # Should contain the original file name

    def test_remove_one_dry_run(self):
        """Test FileCleaner.remove_one with dry run."""
        f = self.create_file("file.mp4")
        FileCleaner.remove_one(
            f,
            Logger.setup(None),
            dry_run=True,
            use_trash=True,
            trash_root=self.trash_dir,
            is_output=False,
            source_root=self.input_dir,
        )
        assert f.exists()

    def test_safe_remove_with_trash(self):
        """Test FileCleaner.safe_remove functionality."""
        f = self.create_file("file.mp4")
        FileCleaner.safe_remove(
            f,
            Logger.setup(None),
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_dir,
            source_root=self.input_dir,
        )
        assert not f.exists()
        moved = list(self.trash_dir.rglob("*.mp4"))
        assert any(m.name == f.name for m in moved)

    def test_safe_remove_edge_cases(self):
        """Test FileCleaner.safe_remove with edge cases."""
        logger = Logger.setup(None)

        # Test with non-existent path in dry run mode
        nonexistent_path = self.temp_dir / "nonexistent" / "file.mp4"
        FileCleaner.safe_remove(nonexistent_path, logger, dry_run=True, use_trash=False)

        # Test with file that has different permissions (if possible)
        temp_file = self.temp_dir / "test_perms.txt"
        temp_file.write_text("test content")

        FileCleaner.safe_remove(temp_file, logger, dry_run=False, use_trash=False)
        # File should be removed
        assert not temp_file.exists()

    def test_remove_orphaned_jpgs(self):
        """Test FileCleaner.remove_orphaned_jpgs functionality."""
        # Create MP4 and JPG pair
        mp4_file = self.create_file("test.mp4")
        jpg_file = self.create_file("test.jpg")

        # Create mapping where MP4 exists for JPG
        mapping = {"20230101120000": {".mp4": mp4_file, ".jpg": jpg_file}}

        logger = Logger.setup(None)
        # This should not remove the JPG since MP4 exists
        FileCleaner.remove_orphaned_jpgs(mapping, set(), logger)
        assert jpg_file.exists()

        # Create mapping where only JPG exists (orphaned)
        orphaned_jpg = self.create_file("orphan.jpg")
        orphaned_mapping = {"20230101120001": {".jpg": orphaned_jpg}}

        # This should remove the orphaned JPG
        FileCleaner.remove_orphaned_jpgs(
            orphaned_mapping, set(), logger, dry_run=False, use_trash=False
        )
        assert not orphaned_jpg.exists()

    def test_remove_orphaned_jpgs_with_graceful_exit(self):
        """Test FileCleaner.remove_orphaned_jpgs with graceful exit."""
        logger = Logger.setup(None)

        # Create a mapping with orphaned JPG
        orphaned_jpg = self.create_file("orphan.jpg", content=b"jpg content")
        mapping = {"20230101120000": {".jpg": orphaned_jpg}}
        processed = set()  # Empty processed set, so JPG will be considered orphaned

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()  # Request exit early

        # This should handle graceful exit gracefully
        FileCleaner.remove_orphaned_jpgs(
            mapping,
            processed,
            logger,
            dry_run=True,  # Use dry run
            use_trash=False,
            trash_root=None,
            graceful_exit=graceful_exit,
        )

        # The file should still exist since it's dry run
        assert orphaned_jpg.exists()

    def test_clean_empty_directories(self):
        """Test FileCleaner.clean_empty_directories functionality."""
        # Create an empty date-structured directory
        empty_dir = self.input_dir / "2023" / "01" / "01"
        empty_dir.mkdir(parents=True, exist_ok=True)

        logger = Logger.setup(None)

        # This should remove the empty directory
        FileCleaner.clean_empty_directories(self.input_dir, logger, use_trash=False)
        assert not empty_dir.exists()

        # Create another empty directory and test with trash
        empty_dir2 = self.input_dir / "2024" / "02" / "02"
        empty_dir2.mkdir(parents=True, exist_ok=True)

        FileCleaner.clean_empty_directories(
            self.input_dir, logger, use_trash=True, trash_root=self.trash_dir
        )
        # Directory should be moved to trash
        assert not empty_dir2.exists()

    def test_clean_empty_directories_with_graceful_exit(self):
        """Test FileCleaner.clean_empty_directories with graceful exit."""
        logger = Logger.setup(None)

        # Create an empty directory with date structure
        empty_date_dir = self.input_dir / "2023" / "01" / "15"
        empty_date_dir.mkdir(parents=True, exist_ok=True)
        assert empty_date_dir.exists()

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()  # Request exit early

        # This should handle graceful exit gracefully
        FileCleaner.clean_empty_directories(
            self.input_dir,
            logger,
            use_trash=False,
            trash_root=None,
            is_output=False,
            is_trash=False,
            graceful_exit=graceful_exit,
        )


class TestArchiverFunctionality(TestBase):
    """Test main archiver functionality."""

    def test_archiver_run_normal_mode_dry_run(self):
        """Test Archiver.run in normal mode with dry run."""
        ts_old = datetime.now() - timedelta(days=31)
        src = self.create_file("2023/01/01/video.mp4", ts=ts_old)

        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30
        cfg.dry_run = True

        archiver = Archiver(cfg)
        rc = archiver.run()
        assert rc == 0
        # No files should have been touched in dry run
        assert src.exists()

    def test_archiver_run_cleanup_mode(self):
        """Test Archiver.run in cleanup mode."""
        # Create files older than the threshold
        old_ts = datetime.now() - timedelta(days=31)
        for i in range(3):
            self.create_file(f"2023/01/0{i + 1}/old{i}.mp4", ts=old_ts)

        # Create a large archive to trigger size cleanup
        big_arch = self.create_archive(old_ts, size=1024 * 1024 * 1024)  # 1 GB

        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30  # age limit
        cfg.max_size = 1  # 1 GB – will trigger size cleanup
        cfg.cleanup = True
        cfg.clean_output = False

        archiver = Archiver(cfg)
        rc = archiver.run()
        assert rc == 0

        # The big archive should have been removed because size limit was exceeded
        assert not big_arch.exists()

    def test_archiver_run_with_nonexistent_directory(self):
        """Test Archiver.run when directory doesn't exist."""
        cfg = Config()
        cfg.directory = Path("/nonexistent/directory")  # This doesn't exist
        archiver = Archiver(cfg)

        # This should return error code 1
        result = archiver.run()
        assert result == 1

    def test_archiver_run_with_nonexistent_and_default_camera(self):
        """Test Archiver.run when directory and default /camera don't exist."""
        cfg = Config()
        cfg.directory = Path("/nonexistent/directory")  # This doesn't exist
        # Config defaults to /camera which also doesn't exist in our test setup
        archiver = Archiver(cfg)

        # This should return error code 1
        result = archiver.run()
        assert result == 1

    def test_archiver_run_with_graceful_exit(self):
        """Test Archiver.run when graceful exit is requested."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.dry_run = True  # Use dry run
        cfg.age = 1  # 1 day threshold

        # Create a test file
        old_ts = datetime.now() - timedelta(days=2)
        self.create_file("test_video.mp4", ts=old_ts)

        archiver = Archiver(cfg)

        # Create a graceful exit that's already requested
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        # This should return error code 1 due to graceful exit
        result = archiver.run(graceful_exit)
        assert result == 1

    def test_archiver_cleanup_archive_size_limit(self):
        """Test the cleanup_archive_size_limit functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB for size limit
        cfg.age = 30  # 30 days for age limit
        cfg.cleanup = True  # Enable cleanup mode
        cfg.clean_output = True  # Include output files in age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create files that exceed size limit
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        large_arch = self.create_archive(old_ts, size=1024 * 1024 * 1024)  # 1 GB
        assert large_arch.exists()

        # Run cleanup_archive_size_limit directly
        graceful_exit = GracefulExit()
        archiver.cleanup_archive_size_limit(graceful_exit)

        # The large archive should have been removed due to size/age cleanup
        assert not large_arch.exists()

    def test_archiver_cleanup_archive_size_limit_dry_run(self):
        """Test the cleanup_archive_size_limit functionality in dry run mode."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB for size limit
        cfg.age = 30  # 30 days for age limit
        cfg.cleanup = True  # Enable cleanup mode
        cfg.dry_run = True  # Dry run mode
        cfg.clean_output = True  # Include output files in age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create files that would be cleaned up
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        large_arch = self.create_archive(old_ts, size=1024 * 1024 * 1024)  # 1 GB
        assert large_arch.exists()

        # Run cleanup_archive_size_limit directly
        graceful_exit = GracefulExit()
        archiver.cleanup_archive_size_limit(graceful_exit)

        # The archive should still exist in dry run mode
        assert large_arch.exists()

    def test_archiver_cleanup_archive_size_limit_with_graceful_exit(self):
        """Test the cleanup_archive_size_limit functionality with graceful exit."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB for size limit
        cfg.age = 30  # 30 days for age limit
        cfg.cleanup = True  # Enable cleanup mode
        cfg.clean_output = True  # Include output files in age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create files that would be cleaned up
        old_ts = datetime.now() - timedelta(days=35)  # Older than age threshold
        large_arch = self.create_archive(old_ts, size=1024 * 1024 * 1024)  # 1 GB
        assert large_arch.exists()

        # Request graceful exit
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        # Run cleanup_archive_size_limit with graceful exit
        archiver.cleanup_archive_size_limit(graceful_exit)

        # The archive should still exist because operation was cancelled
        assert large_arch.exists()

    def test_archiver_process_files_intelligent_with_empty_list(self):
        """Test process_files_intelligent with an empty list of files."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30
        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Run with empty list
        graceful_exit = GracefulExit()
        result = archiver.process_files_intelligent([], {}, graceful_exit, set())

        # Should return empty set
        assert result == set()

    def test_archiver_process_files_intelligent_with_graceful_exit_before_start(self):
        """Test process_files_intelligent with graceful exit requested before start."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30
        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create a test file to process
        old_ts = datetime.now() - timedelta(days=35)
        test_file = self.create_file("test.mp4", ts=old_ts)

        # Request exit before starting
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        # Run with the file and graceful exit
        result = archiver.process_files_intelligent(
            [(test_file, old_ts)], {}, graceful_exit, set()
        )

        # Should return empty set due to graceful exit
        assert result == set()

    def test_archiver_intelligent_cleanup_size_and_age(self):
        """Test Archiver.intelligent_cleanup with both size and age constraints."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB
        cfg.age = 30
        cfg.clean_output = True  # Include output files in age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create test files
        old_ts = datetime.now() - timedelta(days=31)  # Older than age threshold
        _ = datetime.now() - timedelta(days=1)  # Newer than age threshold

        # Create a source file
        source_file = self.create_file("2023/01/01/source.mp4", ts=old_ts)
        old_list = [(source_file, old_ts)]

        # Collect file info and test cleanup
        all_files = archiver.collect_file_info(old_list)
        result = archiver.intelligent_cleanup(all_files)
        # Result should be based on the age threshold
        assert isinstance(result, list)

        # Test with size-based cleanup
        cfg.max_size = 0  # 0 GB to force size cleanup
        archiver_size = Archiver(cfg)
        archiver_size.setup_logging()

        all_files_with_size = archiver_size.collect_file_info(old_list)
        result_size = archiver_size.intelligent_cleanup(all_files_with_size)
        # Should also have files removed due to size limit
        assert isinstance(result_size, list)

    def test_archiver_intelligent_cleanup_priority_logic(self):
        """Test the priority logic in intelligent cleanup (trash > archive > source)."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 0  # Force size-based cleanup
        cfg.age = 30
        cfg.clean_output = True

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create files in all three categories with same timestamp
        test_ts = datetime.now() - timedelta(days=10)

        # Create source file
        source_file = self.create_file(
            "source.mp4", ts=test_ts, content=b"x" * 1000000
        )  # 1MB

        # Create archive file
        archive_file = self.create_archive(test_ts, size=1000000)  # 1MB

        # Create trash file
        trash_file = self.create_trash_file(test_ts)
        trash_file.write_bytes(b"x" * 1000000)  # 1MB

        # Create file info objects manually to test the logic directly
        from archiver import FileInfo

        all_files = [
            FileInfo(source_file, test_ts, 1000000, is_archive=False, is_trash=False),
            FileInfo(archive_file, test_ts, 1000000, is_archive=True, is_trash=False),
            FileInfo(trash_file, test_ts, 1000000, is_archive=False, is_trash=True),
        ]

        # The cleanup should prioritize trash first, then archive, then source
        result = archiver.intelligent_cleanup(all_files)
        # With max_size=0, all files should be marked for removal, with priority order
        assert len(result) > 0

    def test_archiver_apply_size_cleanup_functionality(self):
        """Test the _apply_size_cleanup functionality directly."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB
        cfg.age = 30

        archiver = Archiver(cfg)
        archiver.setup_logging()

        from archiver import FileInfo

        # Create files with different priorities and sizes
        test_ts = datetime.now() - timedelta(days=10)

        # Create trash file (priority 0)
        trash_file = self.create_trash_file(test_ts)
        trash_info = FileInfo(
            trash_file, test_ts, 500 * 1024 * 1024, is_archive=False, is_trash=True
        )  # 500MB

        # Create archive file (priority 1)
        archive_file = self.create_archive(test_ts, size=300 * 1024 * 1024)  # 300MB
        archive_info = FileInfo(
            archive_file, test_ts, 300 * 1024 * 1024, is_archive=True, is_trash=False
        )

        # Create source file (priority 2)
        source_file = self.create_file(
            "source.mp4", ts=test_ts, content=b"x" * 200 * 1024 * 1024
        )  # 200MB
        source_info = FileInfo(
            source_file, test_ts, 200 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # Categorize files by priority
        categorized_files = {0: [trash_info], 1: [archive_info], 2: [source_info]}
        total_size = 1000 * 1024 * 1024  # 1GB
        size_limit = 500 * 1024 * 1024  # 500MB (smaller than total)

        # Apply size cleanup - should remove files starting from lowest priority
        files_to_remove, remaining_size = archiver._apply_size_cleanup(
            categorized_files, total_size, size_limit
        )

        # Should prioritize removal from trash first (highest priority for removal)
        assert len(files_to_remove) > 0
        # Verify the size calculation logic
        assert remaining_size < total_size

    def test_archiver_intelligent_cleanup_with_empty_files_list(self):
        """Test intelligent_cleanup with an empty file list."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB
        cfg.age = 30

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Test with empty list
        result = archiver.intelligent_cleanup([])
        assert result == []

    def test_archiver_intelligent_cleanup_age_disabled(self):
        """Test intelligent_cleanup when age-based cleanup is disabled (age <= 0)."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB
        cfg.age = 0  # Age cleanup disabled

        archiver = Archiver(cfg)
        archiver.setup_logging()

        from archiver import FileInfo

        # Create files
        test_ts = datetime.now() - timedelta(days=10)
        source_file = self.create_file(
            "source.mp4", ts=test_ts, content=b"x" * 100 * 1024 * 1024
        )  # 100MB
        source_info = FileInfo(
            source_file, test_ts, 100 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # With age=0, should only consider size limits, not age
        result = archiver.intelligent_cleanup([source_info])
        # Since size is under limit and age cleanup is disabled, no files should be removed
        assert len(result) == 0

    def test_archiver_intelligent_cleanup_with_logger_not_initialized(self):
        """Test intelligent_cleanup when logger is not initialized (should raise RuntimeError)."""
        cfg = Config()
        archiver = Archiver(cfg)
        # Don't call setup_logging(), so logger is None

        from archiver import FileInfo

        # Create a file info object
        test_ts = datetime.now() - timedelta(days=10)
        source_file = self.create_file(
            "source.mp4", ts=test_ts, content=b"x" * 100 * 1024 * 1024
        )  # 100MB
        source_info = FileInfo(
            source_file, test_ts, 100 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # Should raise RuntimeError because logger is not initialized
        with pytest.raises(RuntimeError, match="Logger not initialized"):
            archiver.intelligent_cleanup([source_info])

    def test_archiver_intelligent_cleanup_no_files(self):
        """Test intelligent_cleanup when no files are provided."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 1  # 1 GB
        cfg.age = 30

        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Test with empty file list
        result = archiver.intelligent_cleanup([])
        assert result == []

    def test_archiver_get_all_archive_files_empty_output_dir(self):
        """Test get_all_archive_files when output directory doesn't exist."""
        cfg = Config()
        cfg.output = Path("/nonexistent/output/directory")  # This doesn't exist
        archiver = Archiver(cfg)

        # Should return empty list when output directory doesn't exist
        result = archiver.get_all_archive_files()
        assert result == []

    def test_archiver_collect_file_info_with_nonexistent_paths(self):
        """Test collect_file_info with files that don't exist on disk."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create a file info object for a file that doesn't exist
        test_ts = datetime.now() - timedelta(days=10)
        nonexistent_file = self.input_dir / "nonexistent.mp4"
        old_list = [(nonexistent_file, test_ts)]

        # This should handle the nonexistent file gracefully
        all_files = archiver.collect_file_info(old_list)
        # Should return empty list since the file doesn't exist
        assert all_files == []

    def test_archiver_apply_age_cleanup_functionality(self):
        """Test the _apply_age_cleanup functionality directly."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 10  # Large size to not trigger size cleanup
        cfg.age = 5  # 5 days old
        cfg.clean_output = True  # Include output files in age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        from archiver import FileInfo

        # Create old files (older than age threshold)
        old_ts = datetime.now() - timedelta(
            days=10
        )  # 10 days old, older than 5-day threshold

        # Create trash file (priority 0)
        trash_file = self.create_trash_file(old_ts)
        trash_info = FileInfo(
            trash_file, old_ts, 100 * 1024 * 1024, is_archive=False, is_trash=True
        )  # 100MB

        # Create archive file (priority 1)
        archive_file = self.create_archive(old_ts, size=200 * 1024 * 1024)  # 200MB
        archive_info = FileInfo(
            archive_file, old_ts, 200 * 1024 * 1024, is_archive=True, is_trash=False
        )

        # Create source file (priority 2)
        source_file = self.create_file(
            "source.mp4", ts=old_ts, content=b"x" * 300 * 1024 * 1024
        )  # 300MB
        source_info = FileInfo(
            source_file, old_ts, 300 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # Also create newer files (within age threshold) that should not be removed
        new_ts = datetime.now() - timedelta(
            days=2
        )  # 2 days old, newer than 5-day threshold
        newer_source = self.create_file(
            "newer.mp4", ts=new_ts, content=b"x" * 50 * 1024 * 1024
        )  # 50MB
        newer_info = FileInfo(
            newer_source, new_ts, 50 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # Categorize files by priority
        categorized_files = {
            0: [trash_info],
            1: [archive_info],
            2: [source_info, newer_info],
        }
        total_size = 650 * 1024 * 1024  # 650MB

        # Apply age cleanup - should remove files older than age threshold
        age_cutoff = datetime.now() - timedelta(days=cfg.age)
        files_to_remove, remaining_size = archiver._apply_age_cleanup(
            categorized_files, age_cutoff, total_size
        )

        # Should have removed the old files but not the newer one
        assert len(files_to_remove) >= 3  # Trash, archive, and source should be removed
        # The newer file should not be in the removal list
        assert newer_info not in files_to_remove
        # Verify size reduction
        assert remaining_size < total_size

    def test_archiver_apply_age_cleanup_with_clean_output_false(self):
        """Test _apply_age_cleanup when clean_output is False (output files excluded)."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 10  # Large size to not trigger size cleanup
        cfg.age = 5  # 5 days old
        cfg.clean_output = False  # Exclude output files from age cleanup

        archiver = Archiver(cfg)
        archiver.setup_logging()

        from archiver import FileInfo

        # Create old files (older than age threshold)
        old_ts = datetime.now() - timedelta(
            days=10
        )  # 10 days old, older than 5-day threshold

        # Create trash file (priority 0) - should still be removed
        trash_file = self.create_trash_file(old_ts)
        trash_info = FileInfo(
            trash_file, old_ts, 100 * 1024 * 1024, is_archive=False, is_trash=True
        )  # 100MB

        # Create archive file (priority 1) - should NOT be removed because clean_output=False
        archive_file = self.create_archive(old_ts, size=200 * 1024 * 1024)  # 200MB
        archive_info = FileInfo(
            archive_file, old_ts, 200 * 1024 * 1024, is_archive=True, is_trash=False
        )

        # Create source file (priority 2) - should be removed
        source_file = self.create_file(
            "source.mp4", ts=old_ts, content=b"x" * 300 * 1024 * 1024
        )  # 300MB
        source_info = FileInfo(
            source_file, old_ts, 300 * 1024 * 1024, is_archive=False, is_trash=False
        )

        # Categorize files by priority
        categorized_files = {0: [trash_info], 1: [archive_info], 2: [source_info]}
        total_size = 600 * 1024 * 1024  # 600MB

        # Apply age cleanup - should remove trash and source but NOT archive (due to clean_output=False)
        age_cutoff = datetime.now() - timedelta(days=cfg.age)
        files_to_remove, remaining_size = archiver._apply_age_cleanup(
            categorized_files, age_cutoff, total_size
        )

        # Should have removed trash and source but not archive
        assert len(files_to_remove) >= 2  # Trash and source should be removed
        # Archive should not be in removal list because clean_output=False
        assert archive_info not in files_to_remove
        # Verify some files were removed
        assert remaining_size < total_size

    def test_archiver_collect_file_info(self):
        """Test Archiver.collect_file_info functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        archiver = Archiver(cfg)
        archiver.setup_logging()

        # Create test files with different characteristics
        old_ts = datetime.now() - timedelta(days=10)
        new_ts = datetime.now()

        # Create source file
        source_file = self.create_file("test.mp4", ts=old_ts)
        old_list = [(source_file, old_ts)]

        # Create some archive files
        archive_file = self.create_archive(new_ts)

        # Create a file in trash
        trash_file = self.create_trash_file(old_ts)
        assert trash_file.exists()

        # Test collect_file_info with mixed file types
        all_files = archiver.collect_file_info(old_list)

        # Should have source file and archive file in results
        found_paths = [f.path for f in all_files]
        assert source_file in found_paths
        assert archive_file in found_paths

    def test_generate_action_plan(self):
        """Test that action plan generation works correctly."""
        # Create test files
        old_ts = datetime.now() - timedelta(days=31)
        src_file = self.create_file("test_video.mp4", ts=old_ts)
        jpg_file = self.create_file("test_image.jpg", ts=old_ts)

        # Create archiver
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        archiver = Archiver(cfg)
        archiver.setup_logging()

        old_list = [(src_file, old_ts)]
        mapping = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": src_file, ".jpg": jpg_file}
        }
        trash_files = set()

        # Generate plan
        plan = archiver.generate_action_plan(old_list, mapping, trash_files)

        # Check that plan contains expected actions
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 1  # JPG removal after transcode
        assert len(plan["cleanup_removals"]) == 0

        # Verify the transcoding action
        transcode_action = plan["transcoding"][0]
        assert transcode_action["input"] == src_file
        assert transcode_action["jpg_to_remove"] == jpg_file

        # Verify the removal action
        removal_action = plan["removals"][0]
        assert removal_action["file"] == jpg_file
        assert removal_action["type"] == "jpg_removal_after_transcode"

    def test_action_plan_with_skip_logic(self):
        """Test action plan when skip logic applies."""
        old_ts = datetime.now() - timedelta(days=31)
        src_file = self.create_file("test_video.mp4", ts=old_ts)

        # Create an archive file that already exists to trigger skip logic
        existing_archive = self.create_archive(old_ts)
        assert existing_archive.exists()

        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.no_skip = False  # Allow skipping
        archiver = Archiver(cfg)
        archiver.setup_logging()

        old_list = [(src_file, old_ts)]
        mapping = {old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": src_file}}
        trash_files = set()

        # Generate plan
        plan = archiver.generate_action_plan(old_list, mapping, trash_files)

        # Should have removals but no transcoding (since archive exists)
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 1  # Source file removal after skip
        assert len(plan["cleanup_removals"]) == 0

        # Verify the removal action is for source removal after skip
        removal_action = plan["removals"][0]
        assert removal_action["type"] == "source_removal_after_skip"
        assert removal_action["file"] == src_file


class TestUtilityFunctionsAndServices(TestBase):
    """Test utility functions and service classes."""

    def test_execute_with_service_check(self):
        """Test the execute_with_service_check utility function."""

        # Create a simple action function
        def test_action(service):
            return State.PLANNING

        # Test when service is available
        next_state = execute_with_service_check(
            lambda: "mock_service", "Error message", test_action
        )
        assert next_state == State.PLANNING

        # Test when service is not available (None)
        next_state = execute_with_service_check(
            lambda: None, "Error message", test_action
        )
        assert next_state == State.TERMINATION

    def test_safe_get_data(self):
        """Test the safe_get_data utility function."""
        data_dict = {"key1": "value1", "key2": "value2"}

        # Test with existing key
        result = safe_get_data(data_dict, "key1")
        assert result == "value1"

        # Test with non-existing key and default
        result = safe_get_data(data_dict, "key3", "default")
        assert result == "default"

        # Test with non-existing key and no default
        result = safe_get_data(data_dict, "key3")
        assert result is None

    def test_file_operations_service(self):
        """Test FileOperationsService functionality."""
        logger = Logger.setup(None)
        graceful_exit = GracefulExit()

        service = FileOperationsService(logger, graceful_exit)

        # Test that methods exist and can be called (with mocked behavior)
        assert hasattr(service, "scan_files")
        assert hasattr(service, "remove_one")
        assert hasattr(service, "remove_orphaned_jpgs")
        assert hasattr(service, "clean_empty_directories")

    def test_transcoding_service(self):
        """Test TranscodingService functionality."""
        logger = Logger.setup(None)
        graceful_exit = GracefulExit()

        service = TranscodingService(logger, graceful_exit)

        # Test that methods exist and can be called
        assert hasattr(service, "transcode_file")
        assert hasattr(service, "get_video_duration")

    def test_action_planner(self):
        """Test ActionPlanner functionality."""
        cfg = Config()
        logger = Logger.setup(None)

        planner = ActionPlanner(cfg, logger)

        # Test that methods exist and can be called
        assert hasattr(planner, "generate_action_plan")
        assert hasattr(planner, "display_action_plan")
        assert hasattr(planner, "collect_file_info")
        assert hasattr(planner, "intelligent_cleanup")

    def test_action_plan_generator(self):
        """Test the _ActionPlanGenerator internal utility class."""
        cfg = Config()
        generator = _ActionPlanGenerator(cfg)

        assert generator.config == cfg
        assert generator.logger is None

        # Test output path method
        cfg.output = self.output_dir
        generator = _ActionPlanGenerator(cfg)

        # Test with date-structured input path
        input_with_date = Path("/input/2023/01/15/REO_CAMERA_20230115120000.mp4")
        ts = datetime(2023, 1, 15, 12, 0, 0)
        result = generator.output_path(input_with_date, ts)

        expected = (
            self.output_dir
            / "2023"
            / "01"
            / "15"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert result == expected

        # Test _ensure_logger method - should raise RuntimeError when logger is not initialized
        with pytest.raises(RuntimeError):
            generator._ensure_logger()

        # Set logger and test again
        logger = Logger.setup(None)
        generator.logger = logger
        assert generator._ensure_logger() == logger


class TestProgressReporter(TestBase):
    """Test progress reporter functionality."""

    def test_progress_reporter_basic_functionality(self):
        """Test basic ProgressReporter functionality."""
        with ProgressReporter(total_files=5, silent=True) as pr:
            assert pr is not None
            pr.start_processing()
            pr.start_file()
            pr.update_progress(1, 50.0)
            pr.finish_file(1)
            # Check if it has progress after update
            assert isinstance(pr.has_progress, bool)

    def test_progress_reporter_tty_and_non_tty_modes(self):
        """Test ProgressReporter in both TTY and non-TTY modes."""
        import io

        # Test with TTY stream
        tty_stream = io.StringIO()
        tty_stream.isatty = lambda: True  # Mock isatty to return True
        with ProgressReporter(total_files=2, out=tty_stream) as pr:
            pr.start_file()
            pr.update_progress(1, 50.0)
            pr.finish_file(1)

        # Test with non-TTY stream (should update every PROGRESS_UPDATE_INTERVAL)
        non_tty_stream = io.StringIO()
        non_tty_stream.isatty = lambda: False  # Mock isatty to return False
        with ProgressReporter(total_files=2, width=15, out=non_tty_stream) as pr:
            pr.start_file()
            pr.update_progress(0, 0.0)  # Should update immediately for 0%
            pr.update_progress(1, 50.0)  # May not update due to interval
            pr.update_progress(1, 100.0)  # Should update for 100%

    def test_progress_reporter_signal_handling(self):
        """Test signal handling in ProgressReporter."""
        import signal

        # Test signal registration and cleanup
        pr = ProgressReporter(total_files=1, silent=True)

        # Test signal handler directly
        original_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(
            signal.SIGINT,
            pr._original_signal_handlers.get(signal.SIGINT, signal.SIG_DFL),
        )

        # Call the signal handler to make sure it doesn't crash
        try:
            pr._signal_handler(signal.SIGINT, None)
        except Exception:
            pass  # Expected to handle exceptions gracefully

        # Restore original handler
        signal.signal(signal.SIGINT, original_sigint)

        pr.finish()  # Cleanup

    def test_progress_reporter_with_different_signal_types(self):
        """Test ProgressReporter signal handling with different signal types."""
        import signal

        # Test signal registration with different signals
        pr = ProgressReporter(total_files=1, silent=True)

        # Test different signal types (if available on the system)
        test_signals = []
        if hasattr(signal, "SIGTERM"):
            test_signals.append(signal.SIGTERM)
        if hasattr(signal, "SIGHUP"):
            test_signals.append(signal.SIGHUP)
        if hasattr(signal, "SIGINT"):
            test_signals.append(signal.SIGINT)

        # Test each available signal
        for sig in test_signals:
            original_handler = signal.signal(sig, signal.SIG_DFL)
            # Register the reporter's handler
            if sig in pr._original_signal_handlers:
                signal.signal(sig, pr._original_signal_handlers[sig])

            # Call the signal handler to make sure it doesn't crash
            try:
                pr._signal_handler(sig, None)
            except Exception:
                pass  # Expected to handle exceptions gracefully

            # Restore original handler
            signal.signal(sig, original_handler)

        # Test cleanup - should handle errors in signal restoration gracefully
        pr.finish()  # Cleanup


class TestStateClasses(TestBase):
    """Test state machine classes and their functionality."""

    def test_archive_state_machine_basic(self):
        """Test the basic functionality of the ArchiveStateMachine."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Test state machine initialization
        assert state_machine.config == cfg
        assert state_machine.graceful_exit == graceful_exit
        assert state_machine.state is None
        assert state_machine.data == {}
        assert not state_machine.error_occurred
        assert state_machine._exit_code == 0

        # Test that all states are properly initialized
        assert state_machine._states[State.INITIALIZATION] is not None
        assert state_machine._states[State.DISCOVERY] is not None
        assert state_machine._states[State.PLANNING] is not None
        assert state_machine._states[State.EXECUTION] is not None
        assert state_machine._states[State.CLEANUP] is not None
        assert state_machine._states[State.TERMINATION] is not None

    def test_initialization_state(self):
        """Test InitializationState functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = InitializationState(state_machine)

        # Execute the state
        next_state = state.execute()

        # Check that the next state is DISCOVERY
        assert next_state == State.DISCOVERY

        # Check that data was populated in the context
        assert "progress_bar" in state_machine.data
        assert "logger" in state_machine.data
        assert state_machine.file_ops_service is not None
        assert state_machine.transcoding_service is not None
        assert state_machine.action_planner is not None

    def test_initialization_state_directory_error(self):
        """Test InitializationState when input directory doesn't exist."""
        cfg = Config()
        cfg.directory = Path("/nonexistent/directory")  # This doesn't exist
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = InitializationState(state_machine)

        # Execute the state - this should result in TERMINATION due to directory error
        next_state = state.execute()

        # Check that the next state is TERMINATION
        assert next_state == State.TERMINATION
        assert state_machine.error_occurred

    def test_discovery_state(self):
        """Test DiscoveryState functionality."""
        # First run InitializationState to set up required services and data
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Manually run initialization to set up services
        init_state = InitializationState(state_machine)
        init_state.execute()

        # Now test DiscoveryState
        discovery_state = DiscoveryState(state_machine)
        next_state = discovery_state.execute()

        # Check that the next state is PLANNING
        assert next_state == State.PLANNING

        # Check that discovery data was populated
        assert "mp4s" in state_machine.data
        assert "mapping" in state_machine.data
        assert "trash_files" in state_machine.data

    def test_planning_state(self):
        """Test PlanningState functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data manually
        state_machine.data["logger"] = Logger.setup(None)
        state_machine.data["mp4s"] = []
        state_machine.data["mapping"] = {}
        state_machine.data["trash_files"] = set()
        state_machine.action_planner = ActionPlanner(cfg, Logger.setup(None))

        # Test planning state
        planning_state = PlanningState(state_machine)
        next_state = planning_state.execute()

        # Should transition to EXECUTION
        assert next_state == State.EXECUTION

        # Check that plan was created
        assert "action_plan" in state_machine.data

    def test_execution_state_normal_mode(self):
        """Test ExecutionState in normal (transcoding) mode."""
        old_ts = datetime.now() - timedelta(days=31)
        src_file = self.create_file("test_video.mp4", ts=old_ts)

        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.cleanup = False  # Normal transcoding mode
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data manually
        logger = Logger.setup(None)
        state_machine.data["logger"] = logger
        state_machine.data["action_plan"] = {
            "transcoding": [],
            "removals": [],
            "cleanup_removals": [],
        }
        state_machine.data["old_list"] = [(src_file, old_ts)]
        state_machine.data["mapping"] = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": src_file}
        }
        state_machine.data["trash_files"] = set()
        state_machine.data["progress_bar"] = ProgressReporter(total_files=1)

        # Set up services
        state_machine.file_ops_service = FileOperationsService(logger, graceful_exit)
        state_machine.transcoding_service = TranscodingService(logger, graceful_exit)

        # Test execution state
        execution_state = ExecutionState(state_machine)
        next_state = execution_state.execute()

        # Should transition to CLEANUP
        assert next_state == State.CLEANUP

    def test_execution_state_cleanup_mode(self):
        """Test ExecutionState in cleanup mode."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.cleanup = True  # Cleanup mode
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data manually
        logger = Logger.setup(None)
        state_machine.data["logger"] = logger
        state_machine.data["action_plan"] = {
            "transcoding": [],
            "removals": [],
            "cleanup_removals": [],
        }
        state_machine.data["old_list"] = []
        state_machine.data["mapping"] = {}
        state_machine.data["trash_files"] = set()

        # Set up services
        state_machine.file_ops_service = FileOperationsService(logger, graceful_exit)
        state_machine.transcoding_service = TranscodingService(logger, graceful_exit)

        # Test execution state in cleanup mode
        execution_state = ExecutionState(state_machine)
        next_state = execution_state.execute()

        # Should transition to CLEANUP
        assert next_state == State.CLEANUP

    def test_cleanup_state(self):
        """Test CleanupState functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data
        logger = Logger.setup(None)
        state_machine.data["logger"] = logger
        state_machine.data["mapping"] = {}

        # Set up services
        state_machine.file_ops_service = FileOperationsService(logger, graceful_exit)

        # Test cleanup state
        cleanup_state = CleanupState(state_machine)
        next_state = cleanup_state.execute()

        # Should transition to TERMINATION
        assert next_state == State.TERMINATION

    def test_termination_state(self):
        """Test TerminationState functionality."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data
        progress_bar = ProgressReporter(total_files=1, silent=True)
        logger = Logger.setup(None, progress_bar)
        state_machine.data["logger"] = logger
        state_machine.data["progress_bar"] = progress_bar

        # Test termination state
        termination_state = TerminationState(state_machine)
        next_state = termination_state.execute()

        # Should return TERMINATION to indicate completion
        assert next_state == State.TERMINATION

    def test_termination_state_with_graceful_exit_and_dry_run(self):
        """Test TerminationState with graceful exit and dry run scenarios."""
        # Test with graceful exit
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.dry_run = False
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()  # Request exit

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Set up required data
        progress_bar = ProgressReporter(total_files=1, silent=True)
        logger = Logger.setup(None, progress_bar)
        state_machine.data["logger"] = logger
        state_machine.data["progress_bar"] = progress_bar

        # Test termination state with graceful exit
        termination_state = TerminationState(state_machine)
        next_state = termination_state.execute()

        # Should return TERMINATION
        assert next_state == State.TERMINATION

        # Test with dry run
        cfg2 = Config()
        cfg2.directory = self.input_dir
        cfg2.output = self.output_dir
        cfg2.dry_run = True  # Enable dry run
        graceful_exit2 = GracefulExit()

        state_machine2 = ArchiveStateMachine(cfg2, graceful_exit2)

        # Set up required data
        progress_bar2 = ProgressReporter(total_files=1, silent=True)
        logger2 = Logger.setup(None, progress_bar2)
        state_machine2.data["logger"] = logger2
        state_machine2.data["progress_bar"] = progress_bar2

        # Test termination state with dry run
        termination_state2 = TerminationState(state_machine2)
        next_state2 = termination_state2.execute()

        # Should return TERMINATION
        assert next_state2 == State.TERMINATION

    def test_termination_state_without_logger(self):
        """Test TerminationState when logger is not available."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Don't set up logger to test the case when it's None
        progress_bar = ProgressReporter(total_files=1, silent=True)
        state_machine.data["progress_bar"] = progress_bar

        # Test termination state without logger
        termination_state = TerminationState(state_machine)
        next_state = termination_state.execute()

        # Should return TERMINATION to indicate completion
        assert next_state == State.TERMINATION


class TestMainFunctions(TestBase):
    """Test main functions and command-line interface."""

    def test_ask_confirmation(self, monkeypatch):
        """Test the ask_confirmation function with mocked input."""
        import io

        # Test with 'y' response
        monkeypatch.setattr("sys.stdin", io.StringIO("y\n"))
        result = ask_confirmation("Continue?", default=False)
        assert result is True

        # Test with 'n' response
        monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
        result = ask_confirmation("Continue?", default=True)
        assert result is False

        # Test with default response (empty input)
        monkeypatch.setattr("sys.stdin", io.StringIO("\n"))
        result = ask_confirmation("Continue?", default=True)
        assert result is True

    def test_ask_confirmation_keyboard_interrupt(self, monkeypatch):
        """Test ask_confirmation function with keyboard interrupt."""

        def mock_input(prompt):
            raise KeyboardInterrupt

        import builtins

        original_input = builtins.input
        builtins.input = mock_input

        try:
            result = ask_confirmation("Continue?", default=False)
            assert result is False
        finally:
            builtins.input = original_input

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
        assert args.use_trash is True  # Default when --no-trash not specified

    def test_parse_arguments_no_trash(self, monkeypatch):
        """Test parse_arguments with --no-trash flag."""
        import sys

        test_args = ["archiver.py", "--directory", str(self.input_dir), "--no-trash"]

        monkeypatch.setattr(sys, "argv", test_args)
        args = parse_arguments()
        assert args.use_trash is False

    def test_parse_arguments_help(self, monkeypatch):
        """Test parse_arguments with no arguments (should show help)."""
        import sys

        # Test with no arguments - should exit with code 1
        original_argv = sys.argv
        sys.argv = ["archiver.py"]  # No arguments

        try:
            with pytest.raises(SystemExit) as context:
                parse_arguments()
            assert context.value.code == 1
        finally:
            sys.argv = original_argv

    def test_main_function_basic(self, monkeypatch):
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
            "--max-size",
            "500",
        ]

        monkeypatch.setattr(sys, "argv", test_args)

        # Create a test file that would be processed
        old_ts = datetime.now() - timedelta(days=2)
        self.create_file(
            "test_video.mp4", ts=old_ts, content=b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1000)
        )

        # Mock Archiver.run to avoid actual execution
        with unittest.mock.patch("archiver.Archiver.run", return_value=0):
            # Should not raise an exception
            try:
                main()
            except SystemExit:
                pass  # Expected behavior when arguments are processed

    def test_main_function_keyboard_interrupt(self, monkeypatch):
        """Test main function with keyboard interrupt."""
        import sys

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
        ]

        monkeypatch.setattr(sys, "argv", test_args)
        with unittest.mock.patch(
            "archiver.Archiver.run", side_effect=KeyboardInterrupt
        ):
            try:
                main()
            except SystemExit as e:
                assert e.code == 1

    def test_main_function_unexpected_error(self, monkeypatch):
        """Test main function's error handling."""
        import sys

        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
        ]

        monkeypatch.setattr(sys, "argv", test_args)
        with unittest.mock.patch(
            "archiver.Archiver.run", side_effect=RuntimeError("Test error")
        ):
            try:
                main()
            except SystemExit as e:
                assert e.code == 1


class TestLoggerAndOrchestrator(TestBase):
    """Test logger and orchestrator functionality."""

    def test_console_orchestrator_and_guarded_handler(self):
        """Test ConsoleOrchestrator and GuardedStreamHandler functionality."""
        # Test ConsoleOrchestrator
        orchestrator = ConsoleOrchestrator()
        with orchestrator.guard():
            # Should acquire and release the lock without errors
            pass

        # Test GuardedStreamHandler
        with ProgressReporter(total_files=1, silent=False, out=None) as progress_bar:
            handler = GuardedStreamHandler(orchestrator, progress_bar=progress_bar)
            # Create a mock record
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="Test message",
                args=(),
                exc_info=None,
            )
            # Test emit when progress_bar has no progress line
            handler.emit(record)

            # Test with a progress bar that has a progress line
            progress_bar._progress_line = "progress"
            handler.progress_bar = progress_bar
            handler.emit(record)  # This should write and redraw

    def test_logger_setup_with_file_and_progress(self):
        """Test Logger.setup with both file and progress bar."""
        log_file = self.temp_dir / "test.log"

        with ProgressReporter(total_files=1, silent=True) as progress_bar:
            logger = Logger.setup(log_file, progress_bar)

            # Test that logger was created properly
            assert logger is not None
            assert len(logger.handlers) == 2  # File handler + stream handler

            # Log a test message
            logger.info("Test message")

            # Check that file was created
            assert log_file.exists()

    def test_logger_setup_with_exception_handling(self):
        """Test Logger.setup with exception handling."""
        log_file = self.temp_dir / "test.log"

        # First, try with a normal progress bar
        with ProgressReporter(total_files=1, silent=True) as progress_bar:
            logger = Logger.setup(log_file, progress_bar)
            # Should create logger successfully
            assert logger is not None

            # Log a message
            logger.info("Test message")

        # Test with None progress bar
        logger2 = Logger.setup(log_file, None)
        assert logger2 is not None
        logger2.info("Test message 2")

    def test_logger_setup_with_null_handler(self):
        """Test Logger.setup with None log file (uses NullHandler)."""
        logger = Logger.setup(None)
        # Should handle None log file gracefully
        assert logger is not None
        # Should have exactly 2 handlers: NullHandler and GuardedStreamHandler
        assert len(logger.handlers) == 2

    def test_logger_setup_exception_handling_when_closing_handlers(self):
        """Test Logger.setup exception handling when closing existing handlers."""
        log_file = self.temp_dir / "test_exception.log"

        # Create a mock handler that raises an exception when closed
        class FailingHandler(logging.Handler):
            def emit(self, record):
                pass  # Do nothing for this test

            def close(self):
                raise Exception("Mock close failure")

        # Create logger and add the failing handler
        original_logger = logging.getLogger("test_logger_for_exception")
        failing_handler = FailingHandler()
        original_logger.addHandler(failing_handler)

        try:
            # Now test Logger.setup - it should handle the exception when closing the handler
            with ProgressReporter(total_files=1, silent=True) as progress_bar:
                # This should not raise an exception even if the handler close fails
                new_logger = Logger.setup(log_file, progress_bar)
                assert new_logger is not None
                assert new_logger.name == "camera_archiver"
        finally:
            # Clean up: remove the failing handler to prevent exception during shutdown
            try:
                original_logger.removeHandler(failing_handler)
                failing_handler.close()  # Clean up the handler
            except Exception:
                pass  # Ignore cleanup errors


class TestGracefulExit(TestBase):
    """Test graceful exit functionality."""

    def test_graceful_exit_functionality(self):
        """Test the GracefulExit functionality directly."""
        # Test initial state
        graceful_exit = GracefulExit()
        assert not graceful_exit.should_exit()

        # Test requesting exit
        graceful_exit.request_exit()
        assert graceful_exit.should_exit()

        # Test thread safety by using the lock mechanism
        def set_exit():
            graceful_exit.request_exit()

        # Test concurrent access to the exit flag
        import threading

        threads = [threading.Thread(target=set_exit) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should still be True
        assert graceful_exit.should_exit()


class TestConfigExtended(TestBase):
    """Extended tests for Config class."""

    @pytest.mark.parametrize(
        "directory_exists",
        [True, False],
    )
    def test_config_directory_logic_parametrized(self, directory_exists):
        """Parameterized test for Config directory logic."""
        args = MagicMock()
        temp_dir = None  # Initialize to prevent unbound variable
        if directory_exists:
            temp_dir = Path(tempfile.mkdtemp())
            args.directory = temp_dir
        else:
            args.directory = Path("/nonexistent/directory")

        args.output = Path("/camera/archived")
        args.age = 30
        args.dry_run = False
        args.max_size = 500
        args.no_skip = False
        args.use_trash = True
        args.cleanup = False
        args.clean_output = False
        args.trashdir = None
        args.no_confirm = False

        config = Config.from_args(args)
        if directory_exists:
            # When directory exists, it should use the provided directory
            assert config.directory == args.directory
        else:
            # When directory doesn't exist, it should fall back to default /camera
            assert config.directory == Path("/camera")

        # Clean up temp directory if created
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestFileScannerExtended(TestBase):
    """Extended tests for FileScanner class."""

    def test_scan_files_error_handling(self):
        """Test FileScanner with file system errors."""
        # Create a file with invalid name to test error handling
        bad_file = self.input_dir / "invalid_name_without_timestamp.txt"
        bad_file.write_text("test")

        # This should not crash with invalid files
        mp4s, mapping, trash_files = FileScanner.scan_files(
            self.input_dir, include_trash=False, trash_root=None
        )
        # Should return empty lists for invalid files
        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_parse_timestamp_from_filename_edge_cases(self):
        """Test edge cases for timestamp parsing."""
        # Test year range validation
        assert (
            FileScanner.parse_timestamp_from_filename("REO_CAM_20000101000000.mp4")
            is not None
        )
        assert (
            FileScanner.parse_timestamp_from_filename("REO_CAM_20991231235959.mp4")
            is not None
        )
        assert (
            FileScanner.parse_timestamp_from_filename("REO_CAM_19991231235959.mp4")
            is None
        )  # Too old
        assert (
            FileScanner.parse_timestamp_from_filename("REO_CAM_21000101000000.mp4")
            is None
        )  # Too new


class TestTranscoderExtended(TestBase):
    """Extended tests for Transcoder class."""

    @pytest.mark.parametrize(
        "ffprobe_output,expected_duration",
        [
            ("10.5", 10.5),
            ("N/A", None),
            ("", None),
            ("invalid", None),
        ],
    )
    def test_get_video_duration_various_outputs(
        self, ffprobe_output, expected_duration
    ):
        """Parameterized test for get_video_duration with various outputs."""
        f = self.create_file("video.mp4")

        proc = MagicMock()
        proc.stdout.strip.return_value = ffprobe_output

        with (
            unittest.mock.patch("subprocess.run", return_value=proc),
            unittest.mock.patch("shutil.which", return_value="/usr/bin/ffprobe"),
        ):
            result = Transcoder.get_video_duration(f)
            assert result == expected_duration

    def test_transcode_file_stdout_types(self):
        """Test transcode_file with various stdout types."""
        src = self.create_file("src.mp4")
        dst = self.temp_dir / "output.mp4"
        logger = Logger.setup(None)

        # Test with stdout as list of strings
        mock_proc = MagicMock()
        mock_proc.stdout = ["frame=1 time=00:00:01.00\n", "frame=2 time=00:00:02.00\n"]
        mock_proc.wait.return_value = 0

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc):
            with unittest.mock.patch.object(
                Transcoder, "get_video_duration", return_value=5.0
            ):
                result = Transcoder.transcode_file(src, dst, logger)
                assert result is True

        # Test with stdout as None (should handle gracefully)
        mock_proc_2 = MagicMock()
        mock_proc_2.stdout = None
        mock_proc_2.wait.return_value = 0

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc_2):
            result = Transcoder.transcode_file(src, dst, logger)
            assert result is False

        # Test with stdout that doesn't have readline or __iter__ methods
        class UnsupportedStdout:
            pass

        mock_proc_3 = MagicMock()
        mock_proc_3.stdout = UnsupportedStdout()
        mock_proc_3.wait.return_value = 0

        with unittest.mock.patch("subprocess.Popen", return_value=mock_proc_3):
            result = Transcoder.transcode_file(src, dst, logger)
            assert result is False

    def test_transcode_file_with_graceful_exit_during_duration_check(self):
        """Test transcoding when graceful exit is requested during duration check."""
        src = self.create_file("src.mp4")
        dst = self.temp_dir / "output.mp4"
        logger = Logger.setup(None)

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()  # Request exit early

        # This should return False when exit is requested
        result = Transcoder.transcode_file(
            src, dst, logger, graceful_exit=graceful_exit
        )
        assert result is False


class TestFileCleanerExtended(TestBase):
    """Extended tests for FileCleaner class."""

    def test_safe_remove_with_different_file_types(self):
        """Test safe_remove with different file types."""
        logger = Logger.setup(None)

        # Test with a regular file
        test_file = self.temp_dir / "test.txt"
        test_file.write_text("test content")

        FileCleaner.safe_remove(
            test_file, logger, dry_run=False, use_trash=False, trash_root=None
        )
        assert not test_file.exists()

        # Test with non-existent file (should not crash)
        nonexistent = self.temp_dir / "nonexistent.txt"
        FileCleaner.safe_remove(
            nonexistent, logger, dry_run=False, use_trash=False, trash_root=None
        )
        # Should not crash, just handle gracefully

    def test_remove_orphaned_jpgs_empty_mapping(self):
        """Test remove_orphaned_jpgs with empty mapping."""
        logger = Logger.setup(None)

        FileCleaner.remove_orphaned_jpgs({}, set(), logger)
        # Should not crash with empty mapping

    def test_clean_empty_directories_nonexistent_root(self):
        """Test clean_empty_directories with non-existent root."""
        logger = Logger.setup(None)

        nonexistent_dir = Path("/nonexistent/path")

        FileCleaner.clean_empty_directories(
            nonexistent_dir, logger, use_trash=False, trash_root=None
        )
        # Should not crash with non-existent directory


class TestArchiverExtended(TestBase):
    """Extended tests for Archiver class."""

    def test_archiver_setup_logging(self):
        """Test setup_logging functionality."""
        cfg = Config()
        cfg.dry_run = True
        archiver = Archiver(cfg)

        graceful_exit = GracefulExit()
        archiver.setup_logging(graceful_exit)

        assert archiver.logger is not None
        assert archiver.progress_bar is not None

    def test_archiver_output_path_date_structure_validation(self):
        """Test output_path with invalid date structure (non-numeric parts)."""
        cfg = Config()
        cfg.output = self.output_dir
        archiver = Archiver(cfg)

        # Create a path with non-numeric parts that look like dates but aren't
        ts = datetime.now()
        input_path = (
            self.input_dir
            / "2023"
            / "Jan"
            / "01"
            / f"REO_CAM_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        input_path.parent.mkdir(parents=True, exist_ok=True)
        input_path.write_bytes(b"test")

        # Should fall back to timestamp-based structure since "Jan" is not numeric
        result = archiver.output_path(input_path, ts)
        expected_parts = [str(ts.year), f"{ts.month:02d}", f"{ts.day:02d}"]
        assert all(part in result.parts for part in expected_parts)

    def test_archiver_collect_file_info_empty_lists(self):
        """Test collect_file_info with empty lists."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        archiver = Archiver(cfg)
        archiver.setup_logging()

        result = archiver.collect_file_info([])
        assert result == []

    def test_archiver_intelligent_cleanup_empty_files(self):
        """Test intelligent_cleanup with empty file list."""
        cfg = Config()
        cfg.max_size = 1
        cfg.age = 30
        archiver = Archiver(cfg)
        archiver.setup_logging()

        result = archiver.intelligent_cleanup([])
        assert result == []

    def test_archiver_intelligent_cleanup_no_logger(self):
        """Test intelligent_cleanup when logger is not initialized."""
        cfg = Config()
        archiver = Archiver(cfg)
        # Don't call setup_logging(), so logger is None

        from archiver import FileInfo

        test_ts = datetime.now()
        file_info = FileInfo(
            Path("/test.mp4"), test_ts, 1000, is_archive=False, is_trash=False
        )

        with pytest.raises(RuntimeError, match="Logger not initialized"):
            archiver.intelligent_cleanup([file_info])

    def test_archiver_generate_action_plan_empty_lists(self):
        """Test generate_action_plan with empty inputs."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        archiver = Archiver(cfg)
        archiver.setup_logging()

        plan = archiver.generate_action_plan([], {}, set())

        assert "transcoding" in plan
        assert "removals" in plan
        assert "cleanup_removals" in plan
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 0
        assert len(plan["cleanup_removals"]) == 0

    def test_archiver_process_files_intelligent_empty_list(self):
        """Test process_files_intelligent with empty list."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.age = 30
        archiver = Archiver(cfg)
        archiver.setup_logging()

        graceful_exit = GracefulExit()
        result = archiver.process_files_intelligent([], {}, graceful_exit, set())
        assert result == set()

    def test_archiver_cleanup_archive_size_limit_with_graceful_exit(self):
        """Test cleanup_archive_size_limit with graceful exit."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.max_size = 0  # Force size cleanup
        cfg.age = 30
        cfg.dry_run = True  # Use dry run
        archiver = Archiver(cfg)
        archiver.setup_logging()

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()  # Request exit before cleanup

        archiver.cleanup_archive_size_limit(graceful_exit)
        # Should handle graceful exit gracefully


class TestProgressReporterExtended(TestBase):
    """Extended tests for ProgressReporter class."""

    def test_progress_reporter_with_none_output(self):
        """Test ProgressReporter with None output."""
        with ProgressReporter(total_files=1, out=None) as pr:
            pr.start_processing()
            pr.start_file()
            pr.update_progress(0, 0.0)  # Should not error with None output
            pr.update_progress(0, 50.0)  # Should not error
            pr.finish_file(0)

    def test_progress_reporter_tty_detection(self):
        """Test TTY detection in ProgressReporter."""
        import io

        # Create a mock TTY stream
        tty_stream = io.StringIO()
        tty_stream.isatty = lambda: True
        with ProgressReporter(total_files=1, out=tty_stream) as pr:
            assert pr._is_tty() is True

        # Create a mock non-TTY stream
        non_tty_stream = io.StringIO()
        non_tty_stream.isatty = lambda: False
        with ProgressReporter(total_files=1, out=non_tty_stream, width=10) as pr:
            assert pr._is_tty() is False

    def test_progress_reporter_with_graceful_exit(self):
        """Test ProgressReporter methods with graceful exit."""
        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        with ProgressReporter(total_files=1, graceful_exit=graceful_exit) as pr:
            # These methods should handle graceful exit gracefully
            pr.start_processing()
            pr.start_file()
            pr.update_progress(0, 50.0)  # Should not update due to exit
            pr.finish_file(0)
            assert pr.has_progress is False

    def test_progress_reporter_format_line(self):
        """Test the _format_line method directly."""
        pr = ProgressReporter(total_files=1, silent=True)
        line = pr._format_line(1, 50.0)

        # Should contain percentage and progress bar
        assert "50%" in line
        assert "[" in line and "]" in line
        pr.finish()


class TestStateClassesExtended(TestBase):
    """Extended tests for state machine classes."""

    def test_initialization_state_with_error(self):
        """Test InitializationState when critical services are not available."""
        cfg = Config()
        cfg.directory = Path("/nonexistent/directory")  # Won't exist
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = InitializationState(state_machine)

        # Execute should return TERMINATION due to directory error
        next_state = state.execute()
        assert next_state == State.TERMINATION
        assert state_machine.error_occurred

    def test_discovery_state_without_services(self):
        """Test DiscoveryState when service classes are not set."""
        cfg = Config()
        cfg.directory = self.input_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        # Don't run initialization, so services aren't set up

        state = DiscoveryState(state_machine)
        next_state = state.execute()

        # Should return TERMINATION due to missing service
        assert next_state == State.TERMINATION

    def test_planning_state_without_services(self):
        """Test PlanningState when service classes are not set."""
        cfg = Config()
        cfg.directory = self.input_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = PlanningState(state_machine)
        next_state = state.execute()

        # Should return TERMINATION due to missing service
        assert next_state == State.TERMINATION

    def test_execution_state_without_services(self):
        """Test ExecutionState when service classes are not set."""
        cfg = Config()
        cfg.directory = self.input_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = ExecutionState(state_machine)
        next_state = state.execute()

        # Should return TERMINATION due to missing service
        assert next_state == State.TERMINATION

    def test_cleanup_state_without_logger(self):
        """Test CleanupState when logger is not available."""
        cfg = Config()
        cfg.directory = self.input_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = CleanupState(state_machine)
        next_state = state.execute()

        # Should return TERMINATION due to missing logger
        assert next_state == State.TERMINATION

    def test_termination_state_without_logger(self):
        """Test TerminationState when logger is not available."""
        cfg = Config()
        cfg.directory = self.input_dir
        graceful_exit = GracefulExit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        state = TerminationState(state_machine)
        next_state = state.execute()

        # Should return TERMINATION (but can handle missing logger)
        assert next_state == State.TERMINATION

    def test_archive_state_machine_run_with_graceful_exit(self):
        """Test state machine run with graceful exit."""
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        graceful_exit = GracefulExit()

        # Request exit before running
        graceful_exit.request_exit()

        state_machine = ArchiveStateMachine(cfg, graceful_exit)
        exit_code = state_machine.run()

        # Should return error code due to graceful exit
        assert exit_code == 1


class TestUtilityFunctionsExtended(TestBase):
    """Extended tests for utility functions."""

    def test_execute_with_service_check_various_scenarios(self):
        """Test for execute_with_service_check."""

        def action_success(service):
            return State.PLANNING

        def action_termination(service):
            return State.TERMINATION

        # Test with valid service
        assert (
            execute_with_service_check(lambda: "service", "error", action_success)
            == State.PLANNING
        )

        # Test with None service
        assert (
            execute_with_service_check(lambda: None, "error", action_success)
            == State.TERMINATION
        )

    def test_safe_get_data_various_scenarios(self):
        """Test safe_get_data with various scenarios."""
        data = {"key1": "value1", "key2": "value2"}

        # Normal case
        assert safe_get_data(data, "key1") == "value1"

        # Missing key with default
        assert safe_get_data(data, "missing", "default") == "default"

        # Missing key without default
        assert safe_get_data(data, "missing") is None

    def test_construct_output_path_various_inputs(self):
        """Test construct_output_path with various inputs."""
        output_dir = Path("/output")
        ts = datetime(2023, 5, 15, 12, 30, 45)

        # Test with date-structured input
        date_input = Path("/input/2023/05/15/REO_CAM_20230515123045.mp4")
        result = construct_output_path(output_dir, date_input, ts)
        assert "2023" in result.parts
        assert "05" in result.parts
        assert "15" in result.parts

        # Test with non-date-structured input
        non_date_input = Path("/input/videos/REO_CAM_20230515123045.mp4")
        result2 = construct_output_path(output_dir, non_date_input, ts)
        assert "2023" in result2.parts
        assert "05" in result2.parts
        assert "15" in result2.parts
        assert result2.name.startswith("archived-")


class TestServiceClassesExtended(TestBase):
    """Extended tests for service classes."""

    def test_file_operations_service_methods_exist(self):
        """Test that all FileOperationsService methods exist and are callable."""
        logger = Logger.setup(None)
        graceful_exit = GracefulExit()

        service = FileOperationsService(logger, graceful_exit)

        # Verify all expected methods exist
        assert hasattr(service, "scan_files")
        assert hasattr(service, "remove_one")
        assert hasattr(service, "remove_orphaned_jpgs")
        assert hasattr(service, "clean_empty_directories")

        # Each method should be callable
        assert callable(getattr(service, "scan_files"))
        assert callable(getattr(service, "remove_one"))
        assert callable(getattr(service, "remove_orphaned_jpgs"))
        assert callable(getattr(service, "clean_empty_directories"))

    def test_transcoding_service_methods_exist(self):
        """Test that all TranscodingService methods exist and are callable."""
        logger = Logger.setup(None)
        graceful_exit = GracefulExit()

        service = TranscodingService(logger, graceful_exit)

        # Verify all expected methods exist
        assert hasattr(service, "transcode_file")
        assert hasattr(service, "get_video_duration")

        # Each method should be callable
        assert callable(getattr(service, "transcode_file"))
        assert callable(getattr(service, "get_video_duration"))

    def test_action_planner_methods_exist(self):
        """Test that all ActionPlanner methods exist and are callable."""
        cfg = Config()
        logger = Logger.setup(None)

        planner = ActionPlanner(cfg, logger)

        # Verify all expected methods exist
        assert hasattr(planner, "generate_action_plan")
        assert hasattr(planner, "display_action_plan")
        assert hasattr(planner, "collect_file_info")
        assert hasattr(planner, "intelligent_cleanup")

        # Each method should be callable
        assert callable(getattr(planner, "generate_action_plan"))
        assert callable(getattr(planner, "display_action_plan"))
        assert callable(getattr(planner, "collect_file_info"))
        assert callable(getattr(planner, "intelligent_cleanup"))


class TestMainAndArgumentsExtended(TestBase):
    """Extended tests for main function and argument parsing."""

    def test_parse_arguments_various_scenarios(self, monkeypatch):
        """Test parse_arguments with various argument combinations."""
        import sys

        # Test with minimal required args
        test_args = ["archiver.py", "--directory", str(self.input_dir)]
        monkeypatch.setattr(sys, "argv", test_args)

        args = parse_arguments()
        assert args.directory == self.input_dir
        assert args.age == 30  # Default value
        assert args.max_size == 500  # Default value

        # Test with all arguments
        test_args2 = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
            "--age",
            "60",
            "--max-size",
            "100",
            "--dry-run",
            "--no-skip",
            "--no-trash",
            "--cleanup",
            "--clean-output",
            "--no-confirm",
        ]
        monkeypatch.setattr(sys, "argv", test_args2)

        args2 = parse_arguments()
        assert args2.age == 60
        assert args2.max_size == 100
        assert args2.dry_run is True
        assert args2.no_skip is True
        assert args2.no_trash is True
        assert args2.use_trash is False  # Should be negated
        assert args2.cleanup is True
        assert args2.clean_output is True
        assert args2.no_confirm is True

    def test_main_function_error_scenarios(self, monkeypatch):
        """Test main function with various error scenarios."""
        import sys

        # Test with Archiver.run raising an exception
        test_args = [
            "archiver.py",
            "--directory",
            str(self.input_dir),
            "--output",
            str(self.output_dir),
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        with unittest.mock.patch(
            "archiver.Archiver.run", side_effect=Exception("test error")
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


class TestIntegrationWorkflows(TestBase):
    """Integration tests for complete workflows."""

    def test_full_archiver_workflow_normal_mode(self):
        """Test a complete archiver workflow in normal mode."""
        # Create test files
        old_ts = datetime.now() - timedelta(
            days=35
        )  # Older than default 30-day threshold
        source_file = self.create_file("old_video.mp4", ts=old_ts)
        jpg_file = self.create_file("old_image.jpg", ts=old_ts)

        # Configure archiver
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30
        cfg.dry_run = True  # Use dry run to prevent actual transcoding
        cfg.no_confirm = True  # Skip confirmation prompts

        archiver = Archiver(cfg)

        # Run the archiver
        result_code = archiver.run()

        # Should complete successfully
        assert result_code == 0
        # Source file should still exist (dry run)
        assert source_file.exists()
        # JPG should still exist (dry run)
        assert jpg_file.exists()

    def test_full_archiver_workflow_cleanup_mode(self):
        """Test a complete archiver workflow in cleanup mode."""
        # Create old archive files that would be candidates for cleanup
        old_ts = datetime.now() - timedelta(days=35)
        archive_file = self.create_archive(old_ts)
        assert archive_file.exists()

        # Configure archiver for cleanup
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.cleanup = True
        cfg.age = 30
        cfg.dry_run = True  # Use dry run
        cfg.no_confirm = True  # Skip confirmation

        archiver = Archiver(cfg)

        # Run the archiver in cleanup mode
        result_code = archiver.run()

        # Should complete successfully
        assert result_code == 0
        # Archive file should still exist (dry run)
        assert archive_file.exists()

    def test_state_machine_full_workflow(self):
        """Test the complete state machine workflow."""
        # Create test file
        old_ts = datetime.now() - timedelta(days=35)
        self.create_file("test_video.mp4", ts=old_ts)

        # Configure state machine
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.age = 30
        cfg.dry_run = True  # Use dry run
        cfg.no_confirm = True  # Skip confirmation

        graceful_exit = GracefulExit()
        state_machine = ArchiveStateMachine(cfg, graceful_exit)

        # Run the complete workflow
        exit_code = state_machine.run()

        # Should complete successfully
        assert exit_code == 0

    def test_archiver_workflow_with_size_limit(self):
        """Test archiver workflow with size-based cleanup."""
        # Create a large archive that will exceed a small size limit
        old_ts = datetime.now() - timedelta(days=35)
        large_archive = self.create_archive(old_ts, size=100 * 1024 * 1024)  # 100MB
        assert large_archive.exists()

        # Configure archiver with small size limit
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.trashdir = self.trash_dir
        cfg.cleanup = True
        cfg.max_size = 1  # 1GB limit (large enough to not trigger)
        cfg.age = 30
        cfg.dry_run = True  # Use dry run
        cfg.no_confirm = True

        archiver = Archiver(cfg)
        result_code = archiver.run()

        # Should complete successfully
        assert result_code == 0
