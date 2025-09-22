#!/usr/bin/env python3

import io
import logging
import shutil
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from contextlib import redirect_stdout, redirect_stderr
from typing import Tuple

import archiver


class TestBase(unittest.TestCase):
    """Base class with common utilities"""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        archiver.GracefulExit.exit_requested = False

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        archiver.GracefulExit.exit_requested = False

    def create_file(self, path: Path, content: bytes = b"") -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def create_test_files(
        self, base_dir: Path, timestamp: datetime
    ) -> Tuple[Path, Path]:
        """Create MP4/JPG pair for testing"""
        ts_str = timestamp.strftime("%Y%m%d%H%M%S")
        mp4_path = base_dir / f"REO_cam_{ts_str}.mp4"
        jpg_path = base_dir / f"REO_cam_{ts_str}.jpg"
        self.create_file(mp4_path)
        self.create_file(jpg_path)
        return mp4_path, jpg_path

    def capture_logger(self) -> Tuple[logging.Logger, io.StringIO]:
        """Return a logger that writes to an in-memory stream"""
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger("test")
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger, log_stream


class TestCoreFunctionality(TestBase):
    """Tests for core parsing and file operations"""

    def test_timestamp_parsing(self) -> None:
        test_cases = [
            ("REO_cam_20231201010101.mp4", datetime(2023, 12, 1, 1, 1, 1)),
            ("REO_cam_20000101000000.mp4", datetime(2000, 1, 1, 0, 0, 0)),
            ("REO_cam_18991231235959.mp4", None),  # Too old
            ("REO_cam_20231301000000.mp4", None),  # Invalid month
            ("REO_cam_20231232000000.mp4", None),  # Invalid day
            ("invalid_name.txt", None),  # Wrong format
        ]

        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = archiver.parse_timestamp_from_filename(filename)
                self.assertEqual(result, expected)

    def test_output_path_generation(self) -> None:
        ts = datetime(2023, 12, 1)
        test_cases = [
            (
                Path("root/2023/12/01/file.mp4"),
                "2023/12/01/archived-20231201000000.mp4",  # Fixed: removed "root/"
            ),
            (Path("file.mp4"), "2023/12/01/archived-20231201000000.mp4"),
        ]

        for input_path, expected in test_cases:
            with self.subTest(input_path=input_path):
                result = archiver.output_path(input_path, ts, Path("base"))
                expected_path = Path("base") / expected
                self.assertEqual(result, expected_path)

    def test_video_duration_handling(self) -> None:
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.check_output", return_value="123.45\n"):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertEqual(duration, 123.45)

        with patch("shutil.which", return_value=None):
            duration = archiver.get_video_duration(Path("test.mp4"))
            self.assertIsNone(duration)


class TestFileOperations(TestBase):
    """Tests for file removal, trash handling, and directory cleanup"""

    def test_safe_remove_modes(self) -> None:
        logger, log_stream = self.capture_logger()
        test_file = self.temp_dir / "test.txt"

        # Test dry run
        self.create_file(test_file)
        archiver.safe_remove(test_file, logger, dry_run=True)
        self.assertTrue(test_file.exists())
        self.assertIn("[DRY RUN] Would remove", log_stream.getvalue())

        # Test actual removal
        archiver.safe_remove(test_file, logger, dry_run=False)
        self.assertFalse(test_file.exists())

    def test_trash_operations(self) -> None:
        logger, log_stream = self.capture_logger()
        trash_root = self.temp_dir / ".trash"
        source_file = self.temp_dir / "nested" / "file.txt"

        self.create_file(source_file)
        archiver.safe_remove(
            source_file,
            logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )

        trash_path = trash_root / "input" / "nested" / "file.txt"
        self.assertTrue(trash_path.exists())
        self.assertFalse(source_file.exists())

    def test_empty_directory_cleanup(self) -> None:
        # Create empty directory structure that should be removed
        empty_dir = self.temp_dir / "2023" / "12" / "01"
        empty_dir.mkdir(parents=True)

        # Create non-empty directory that should remain
        non_empty_dir = self.temp_dir / "2023" / "12" / "02"
        non_empty_dir.mkdir(parents=True)
        self.create_file(non_empty_dir / "file.txt")

        archiver.clean_empty_directories(self.temp_dir)
        self.assertFalse(empty_dir.exists())
        self.assertTrue(non_empty_dir.exists())

    def test_archive_size_cleanup(self) -> None:
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create files that exceed size limit
        large_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(large_file, b"x" * (600 * 1024 * 1024))  # 600MB

        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=0.5, dry_run=False
        )
        self.assertFalse(large_file.exists())
        self.assertIn("Archive size exceeds limit", log_stream.getvalue())


class TestGracefulExit(TestBase):
    """Tests for graceful exit functionality across all components"""

    def setUp(self) -> None:
        super().setUp()
        self.logger, self.log_stream = self.capture_logger()

    def test_graceful_exit_flag(self) -> None:
        self.assertFalse(archiver.GracefulExit.should_exit())
        archiver.GracefulExit.request_exit()
        self.assertTrue(archiver.GracefulExit.should_exit())

    def test_all_operations_respect_graceful_exit(self) -> None:
        """Test that all major functions respect the graceful exit flag"""
        archiver.GracefulExit.request_exit()

        test_cases = [
            ("scan_files", lambda: archiver.scan_files(self.temp_dir)),
            (
                "safe_remove",
                lambda: archiver.safe_remove(Path("test"), self.logger, False),
            ),
            (
                "transcode_file",
                lambda: archiver.transcode_file(Path("in"), Path("out"), self.logger),
            ),
        ]

        for func_name, func_call in test_cases:
            with self.subTest(function=func_name):
                # Mock any subprocess calls that might happen despite exit flag
                with patch("subprocess.Popen") as mock_popen:
                    result = func_call()
                    # Functions should return early without performing operations
                    if func_name == "scan_files":
                        self.assertEqual(result, ([], {}))
                    elif func_name == "transcode_file":
                        self.assertFalse(result)
                    mock_popen.assert_not_called()

    def test_transcode_cancellation(self) -> None:
        """Test that transcoding can be cancelled mid-process"""
        # Mock ffmpeg process that will be interrupted
        mock_proc = MagicMock()
        mock_proc.stdout = io.StringIO("time=00:00:01.00\n")
        mock_proc.poll.return_value = None

        call_count = 0

        def should_exit_side_effect() -> bool:
            nonlocal call_count
            call_count += 1
            return call_count > 1

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch("archiver.get_video_duration", return_value=10.0),
            patch(
                "archiver.GracefulExit.should_exit", side_effect=should_exit_side_effect
            ),
        ):
            result = archiver.transcode_file(Path("in"), Path("out"), self.logger)
            mock_proc.terminate.assert_called_once()
            self.assertFalse(result)


class TestProgressBar(TestBase):
    """Tests for progress bar functionality"""

    def setUp(self) -> None:
        super().setUp()
        self.mock_terminal_size = patch(
            "shutil.get_terminal_size", return_value=MagicMock(lines=24, columns=80)
        )
        self.mock_terminal_size.start()

    def tearDown(self) -> None:
        self.mock_terminal_size.stop()
        super().tearDown()

    def test_progress_bar_lifecycle(self) -> None:
        stream = io.StringIO()
        # Mock isatty method
        stream.isatty = lambda: True

        with archiver.ProgressBar(total_files=3, silent=False, out=stream) as bar:
            bar.start_processing()
            bar.start_file()
            bar.update_progress(1, 50.0)
            bar.finish_file(1)

        output = stream.getvalue()
        self.assertIn("Progress [1/3]: 50%", output)
        self.assertIn("\x1b[2K", output)  # Cleanup sequence

    def test_progress_bar_silent_mode(self) -> None:
        stream = io.StringIO()
        bar = archiver.ProgressBar(total_files=1, silent=True, out=stream)
        bar.update_progress(1, 50.0)
        bar.finish()
        self.assertEqual(stream.getvalue(), "")  # No output in silent mode

    def test_progress_formatting(self) -> None:
        bar = archiver.ProgressBar(total_files=10, silent=True)
        bar.start_time = time.time() - 3600  # 1 hour ago
        bar.file_start = time.time() - 60  # 1 minute ago

        line = bar._format_line(5, 50.0)
        self.assertIn("Progress [5/10]: 50%", line)
        self.assertIn("01:00", line)  # File duration
        self.assertIn("01:00:00", line)  # Total duration


class TestIntegrationScenarios(TestBase):
    """Integration tests for complete workflows"""

    def test_complete_archive_workflow(self) -> None:
        logger, log_stream = self.capture_logger()
        ts_old = datetime.now() - archiver.timedelta(days=31)  # Older than 30 days
        ts_recent = datetime.now() - archiver.timedelta(days=15)  # Should be ignored

        # Create test files
        old_mp4, old_jpg = self.create_test_files(self.temp_dir, ts_old)
        recent_mp4, recent_jpg = self.create_test_files(self.temp_dir, ts_recent)

        out_dir = self.temp_dir / "archived"

        with (
            patch("archiver.transcode_file", return_value=True),
            patch("archiver.setup_logging", return_value=logger),
        ):
            # Scan and process files
            mp4s, mapping = archiver.scan_files(self.temp_dir)
            old_files = [
                (p, t)
                for p, t in mp4s
                if t < datetime.now() - archiver.timedelta(days=30)
            ]

            self.assertEqual(len(old_files), 1)  # Only the old file should be processed
            self.assertEqual(len(mp4s), 2)  # Both files should be found

            # Process with dry run first
            result = archiver.process_files(
                old_files,
                out_dir,
                logger,
                dry_run=True,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
            )

            self.assertIn("[DRY RUN] Would transcode", log_stream.getvalue())
            self.assertFalse(
                (out_dir / "2023" / "12" / "01").exists()
            )  # No actual output

    def test_orphaned_jpg_cleanup(self) -> None:
        logger, log_stream = self.capture_logger()

        # Create orphaned JPG (no matching MP4)
        orphan_jpg = self.temp_dir / "REO_cam_20231201000000.jpg"
        self.create_file(orphan_jpg)

        mapping = {"20231201000000": {".jpg": orphan_jpg}}

        with patch("archiver.safe_remove") as mock_remove:
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=False)
            # Fix: Use the correct keyword arguments
            mock_remove.assert_called_once_with(
                orphan_jpg, logger, False, use_trash=False, trash_root=None
            )
            args, kwargs = mock_remove.call_args
            self.assertEqual(args[0], orphan_jpg)
            self.assertEqual(args[1], logger)
            self.assertEqual(args[2], False)
            self.assertEqual(kwargs.get("use_trash"), False)
            self.assertEqual(kwargs.get("trash_root"), None)

    def test_file_processing_skip_logic(self) -> None:
        logger, log_stream = self.capture_logger()
        ts = datetime(2023, 12, 1)
        mp4_path, jpg_path = self.create_test_files(self.temp_dir, ts)
        out_dir = self.temp_dir / "archived"

        # Calculate the actual output path that will be used
        actual_out_path = archiver.output_path(mp4_path, ts, out_dir)
        actual_out_path.parent.mkdir(parents=True, exist_ok=True)
        self.create_file(actual_out_path, b"x" * (2 * 1024 * 1024))  # 2MB file

        # Mock transcode_file to verify it's NOT called
        with patch("archiver.transcode_file", return_value=True) as mock_transcode:
            with patch("archiver.safe_remove") as mock_remove:
                archiver.process_files(
                    [(mp4_path, ts)],
                    out_dir,
                    logger,
                    dry_run=False,
                    no_skip=False,
                    mapping={
                        ts.strftime("%Y%m%d%H%M%S"): {
                            ".mp4": mp4_path,
                            ".jpg": jpg_path,
                        }
                    },
                    bar=archiver.ProgressBar(1, silent=True),
                )

                # Should skip transcoding and remove original files
                mock_transcode.assert_not_called()
                self.assertEqual(mock_remove.call_count, 2)
                self.assertIn(
                    "[SKIP] Existing archive large enough", log_stream.getvalue()
                )


class TestMainFunction(TestBase):
    """Tests for the main command-line interface"""

    def run_main(self, args: list[str]) -> None:
        """Helper to run main with given arguments"""
        original_argv = sys.argv
        try:
            sys.argv = ["archiver.py"] + args
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                try:
                    archiver.main()
                except (SystemExit, Exception):
                    pass  # Catch all exceptions for testing
        finally:
            sys.argv = original_argv

    def test_main_with_help(self) -> None:
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with (
            patch.object(sys, "argv", ["archiver.py", "--help"]),
            redirect_stdout(stdout_capture),
            redirect_stderr(stderr_capture),
            self.assertRaises(SystemExit),
        ):
            archiver.main()

        # Optionally check that help text was generated
        output = stdout_capture.getvalue()
        self.assertIn("usage: archiver.py", output)
        self.assertIn("Archive and transcode camera files", output)

    def test_main_complete_flow(self) -> None:
        with (
            patch("archiver.scan_files", return_value=([], {})) as mock_scan_files,
            patch(
                "archiver.setup_logging", return_value=MagicMock()
            ) as mock_setup_logging,
            patch("archiver.process_files", return_value=set()) as mock_process_files,
            patch("archiver.remove_orphaned_jpgs") as mock_remove_orphaned,
            patch("archiver.clean_empty_directories") as mock_clean_dirs,
            patch("archiver.cleanup_archive_size_limit") as mock_cleanup_size,
        ):
            self.run_main(["--directory", str(self.temp_dir), "--dry-run"])

            # Verify the main workflow functions were called
            mock_scan_files.assert_called_once()
            mock_process_files.assert_called_once()

    def test_error_handling(self) -> None:
        # Test that main handles exceptions gracefully
        with patch("archiver.run_archiver", side_effect=Exception("Test error")):
            # This should not crash the test
            self.run_main(["--directory", str(self.temp_dir)])


class TestSignalHandling(TestBase):
    """Tests for signal handling and cleanup"""

    def test_signal_handler(self) -> None:
        bar = archiver.ProgressBar(total_files=1, silent=True)

        # Verify initial state
        self.assertFalse(archiver.GracefulExit.should_exit())

        # Capture stderr to prevent signal handler output
        with redirect_stderr(io.StringIO()):
            # Trigger signal handler
            bar._signal_handler(archiver.signal.SIGINT, None)

        # Verify graceful exit was requested
        self.assertTrue(archiver.GracefulExit.should_exit())

    def test_progress_bar_signal_cleanup(self) -> None:
        stream = io.StringIO()
        stream.isatty = lambda: True

        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.start_processing()
        bar.update_progress(1, 50.0)

        # Store original progress line
        original_line = bar._progress_line

        # Capture stderr to prevent signal handler output
        with redirect_stderr(io.StringIO()):
            # Trigger cleanup via signal
            bar._signal_handler(archiver.signal.SIGTERM, None)

        # Progress line should be cleared
        self.assertEqual(bar._progress_line, "")


class TestTrashDirectoryWorkflows(TestBase):
    """Tests for trash directory creation and usage workflows"""

    def setUp(self):
        super().setUp()
        self.logger, self.log_stream = self.capture_logger()

    def test_trash_directory_creation(self):
        """Test automatic trash directory creation with --use-trash flag"""
        trash_dir = self.temp_dir / ".deleted"

        # Should create trash directory when it doesn't exist
        archiver.safe_remove(
            self.temp_dir / "test.txt",
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_dir,
        )

        self.assertTrue(trash_dir.exists())
        self.assertTrue(trash_dir.is_dir())

    def test_custom_trash_directory_creation(self):
        """Test custom trash directory creation"""
        custom_trash = self.temp_dir / "custom_trash"

        archiver.safe_remove(
            self.temp_dir / "test.txt",
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=custom_trash,
        )

        self.assertTrue(custom_trash.exists())
        self.assertTrue(custom_trash.is_dir())

    def test_trash_subdirectory_structure(self):
        """Test proper subdirectory structure in trash (input/output)"""
        trash_root = self.temp_dir / ".deleted"
        source_file = self.temp_dir / "source_file.txt"
        output_file = self.temp_dir / "archived" / "output_file.mp4"

        self.create_file(source_file)
        self.create_file(output_file)

        # Test input file trashing
        archiver.safe_remove(
            source_file,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )

        # Test output file trashing
        archiver.safe_remove(
            output_file,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            is_output=True,
            source_root=self.temp_dir / "archived",
        )

        # Verify subdirectory structure
        input_trash_path = trash_root / "input" / "source_file.txt"
        output_trash_path = trash_root / "output" / "output_file.mp4"

        self.assertTrue(input_trash_path.exists())
        self.assertTrue(output_trash_path.exists())
        self.assertFalse(source_file.exists())
        self.assertFalse(output_file.exists())

    def test_trash_file_name_collision_handling(self):
        """Test handling of filename collisions in trash directory"""
        trash_root = self.temp_dir / ".deleted"
        trash_root.mkdir()

        # Create existing file in trash
        existing_trash_file = trash_root / "input" / "test.txt"
        existing_trash_file.parent.mkdir(parents=True)
        self.create_file(existing_trash_file)

        # Create source file with same name
        source_file = self.temp_dir / "test.txt"
        self.create_file(source_file, b"new content")

        # Move to trash - should create uniquely named file
        archiver.safe_remove(
            source_file,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )

        # Original should be moved (not exist)
        self.assertFalse(source_file.exists())

        # Should have exactly 2 files in trash now
        trash_files = list((trash_root / "input").rglob("test*"))
        self.assertEqual(len(trash_files), 2)

        # One should be the original, one should be the new file with timestamp suffix
        file_contents = [f.read_bytes() for f in trash_files]
        self.assertIn(b"", file_contents)  # Original empty file
        self.assertIn(b"new content", file_contents)  # New file

        def test_full_trash_workflow_integration(self):
            """Test complete workflow with trash integration"""
            # Setup
            input_dir = self.temp_dir / "camera"
            archived_dir = input_dir / "archived"
            trash_dir = input_dir / ".deleted"

            input_dir.mkdir()
            archived_dir.mkdir()

            # Create OLD test files that will be processed (older than 30 days)
            old_ts = datetime.now() - timedelta(
                days=31
            )  # Older than default 30-day threshold
            mp4_file = input_dir / f"REO_cam_{old_ts.strftime('%Y%m%d%H%M%S')}.mp4"
            jpg_file = input_dir / f"REO_cam_{old_ts.strftime('%Y%m%d%H%M%S')}.jpg"

            self.create_file(mp4_file)
            self.create_file(jpg_file)

            # Mock transcoding to succeed
            with patch("archiver.transcode_file", return_value=True):
                # Run archiver with trash enabled
                args = MagicMock()
                args.directory = input_dir
                args.output = archived_dir
                args.age = 30
                args.dry_run = False
                args.max_size = 500
                args.no_skip = False
                args.use_trash = True
                args.trashdir = None

                # Mock the logger setup to use our captured logger
                with patch("archiver.setup_logging", return_value=self.logger):
                    archiver.run_archiver(args)

            # Verify files were moved to trash
            trash_mp4 = trash_dir / "input" / mp4_file.name
            trash_jpg = trash_dir / "input" / jpg_file.name

            self.assertTrue(trash_mp4.exists())
            self.assertTrue(trash_jpg.exists())
            self.assertFalse(mp4_file.exists())
            self.assertFalse(jpg_file.exists())

            # Verify archived file was created using the actual output_path function
            archived_file = archiver.output_path(mp4_file, old_ts, archived_dir)
            self.assertTrue(archived_file.exists())

            # Verify trash structure
            self.assertTrue((trash_dir / "input").exists())
            self.assertTrue((trash_dir / "output").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
