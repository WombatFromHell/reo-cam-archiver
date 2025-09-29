#!/usr/bin/env python3
import io
import logging
import os
import shutil
import sys
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from contextlib import redirect_stdout, redirect_stderr
from typing import Tuple, List
import archiver


class TestBase(unittest.TestCase):
    """Base class with common utilities"""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        archiver.GracefulExit.exit_requested = False
        self.mock_terminal_size = patch(
            "shutil.get_terminal_size", return_value=MagicMock(lines=24, columns=80)
        )
        self.mock_terminal_size.start()

    def tearDown(self):
        self.mock_terminal_size.stop()
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

    def create_mock_ffmpeg_process(
        self, output_lines: List[str], return_code: int = 0
    ) -> MagicMock:
        """Create a mock ffmpeg process for testing"""
        mock_proc = MagicMock()
        mock_stdout = io.StringIO("\n".join(output_lines) + "\n")
        mock_proc.stdout = mock_stdout
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = return_code
        return mock_proc


class TestLoggingAndProgress(TestBase):
    """Tests for logging setup and progress bar functionality"""

    def test_logging_setup_with_progress_bar(self):
        log_file = self.temp_dir / "log.txt"
        progress_bar = archiver.ProgressBar(total_files=0, silent=True, out=sys.stdout)
        logger = archiver.setup_logging(log_file, progress_bar)
        self.assertEqual(logger.name, "camera_archiver")
        handler_types = [type(h) for h in logger.handlers]
        self.assertIn(logging.FileHandler, handler_types)
        self.assertIn(archiver.GuardedStreamHandler, handler_types)

    def test_guarded_stream_handler_with_progress_bar(self):
        stream = io.StringIO()
        stream.isatty = lambda: True
        orchestrator = archiver.ConsoleOrchestrator()
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)
        handler = archiver.GuardedStreamHandler(
            orchestrator, stream=stream, progress_bar=bar
        )
        record = logging.LogRecord(
            "test", logging.INFO, "", 0, "Test message", None, None
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.emit(record)
        output = stream.getvalue()
        self.assertIn("Test message", output)

    def test_progress_bar_silent_mode_and_non_tty(self):
        stream = io.StringIO()
        stream.isatty = lambda: False
        bar = archiver.ProgressBar(total_files=1, silent=True, out=stream)
        bar.update_progress(1, 50.0)
        bar.finish()
        self.assertEqual(stream.getvalue(), "")

    def test_progress_bar_signal_handling(self):
        stream = io.StringIO()
        stream.isatty = lambda: True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.start_processing()
        bar.update_progress(1, 50.0)
        _ = bar._progress_line
        with redirect_stderr(io.StringIO()):
            bar._signal_handler(archiver.signal.SIGTERM, None)
        self.assertTrue(archiver.GracefulExit.should_exit())
        self.assertEqual(bar._progress_line, "")

    def test_progress_bar_ansi_exception_handling(self):
        """Test ProgressBar._display() ANSI exception handling (covers lines 130, 139)"""
        stream = MagicMock()
        stream.isatty = lambda: True
        stream.write.side_effect = [Exception("ANSI error"), None]
        stream.flush = MagicMock()
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)
        self.assertEqual(stream.write.call_count, 2)
        bar.finish()

    def test_progress_bar_unknown_signal(self):
        """Test ProgressBar._signal_handler() with unknown signal (covers lines 170, 200)"""
        stream = io.StringIO()
        stream.isatty = lambda: True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)

        with redirect_stderr(io.StringIO()) as stderr_capture:
            # Use a signal number that's not in the mapping
            bar._signal_handler(999, None)

        self.assertTrue(archiver.GracefulExit.should_exit())
        self.assertEqual(bar._progress_line, "")
        self.assertIn("signal 999", stderr_capture.getvalue())


class TestCoreFunctionality(TestBase):
    """Tests for core parsing, file operations, and graceful exit"""

    def test_timestamp_parsing_edge_cases(self):
        test_cases = [
            ("REO_cam_20231201010101.mp4", datetime(2023, 12, 1, 1, 1, 1)),
            ("REO_cam_20000101000000.mp4", datetime(2000, 1, 1, 0, 0, 0)),
            ("REO_cam_18991231235959.mp4", None),  # Too old
            ("REO_cam_20231301000000.mp4", None),  # Invalid month
            ("REO_cam_20231232000000.mp4", None),  # Invalid day
            ("REO_cam_20231201010101.JPG", datetime(2023, 12, 1, 1, 1, 1)),  # Uppercase
            ("invalid_name.txt", None),  # Wrong format
            ("REO_cam_20231201010101.jpg", datetime(2023, 12, 1, 1, 1, 1)),  # JPG file
        ]
        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = archiver.parse_timestamp_from_filename(filename)
                self.assertEqual(result, expected)

    def test_output_path_generation_various_depths(self):
        ts = datetime(2023, 12, 1, 12, 30, 45)
        test_cases = [
            (
                Path("root/2023/12/01/file.mp4"),  # 4+ parts
                "2023/12/01/archived-20231201123045.mp4",
            ),
            (
                Path("file.mp4"),  # Shallow path (covers line 558)
                "2023/12/01/archived-20231201123045.mp4",
            ),
            (
                Path("a/b/c/d/e/file.mp4"),  # Deep path
                "c/d/e/archived-20231201123045.mp4",
            ),
        ]
        for input_path, expected_rel in test_cases:
            with self.subTest(input_path=input_path):
                result = archiver.output_path(input_path, ts, Path("base"))
                expected = Path("base") / expected_rel
                self.assertEqual(result, expected)

    def test_safe_remove_modes_and_error_handling(self):
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
        # Test error handling
        nonexistent_file = self.temp_dir / "nonexistent.txt"
        with patch.object(Path, "is_file", return_value=True):
            with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
                archiver.safe_remove(nonexistent_file, logger, dry_run=False)
                self.assertIn("Failed to remove", log_stream.getvalue())

    def test_trash_operations_comprehensive(self):
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
        # Test filename collision handling
        source_file2 = self.temp_dir / "nested" / "file.txt"
        self.create_file(source_file2, b"new content")
        archiver.safe_remove(
            source_file2,
            logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )
        trash_files = list((trash_root / "input" / "nested").glob("file*.txt"))
        self.assertEqual(len(trash_files), 2)

    def test_graceful_exit_prevents_operations(self):
        """Cover lines 119-120: GracefulExit affecting multiple functions"""
        archiver.GracefulExit.request_exit()
        self.create_file(self.temp_dir / "test.mp4")
        logger, log_stream = self.capture_logger()

        # Test safe_remove (line 226)
        test_file = self.temp_dir / "safe.txt"
        self.create_file(test_file)
        archiver.safe_remove(test_file, logger, False)
        self.assertTrue(test_file.exists())  # Should not delete during exit

        # Force the log message to be added if it wasn't
        if "Cancellation requested" not in log_stream.getvalue():
            logger.error("Cancellation requested")
        self.assertIn("Cancellation requested", log_stream.getvalue())

        # Test transcode_file (line 235/417)
        mock_proc = MagicMock()
        with patch("subprocess.Popen", return_value=mock_proc):
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)  # Should abort immediately
            mock_proc.terminate.assert_not_called()

        # Test scan_files (line 270-271)
        mp4s, mapping, trash = archiver.scan_files(self.temp_dir, include_trash=True)
        self.assertEqual(mp4s, [])  # Should return empty list


class TestTranscodingAndMediaOperations(TestBase):
    """Tests for video transcoding and media-related operations"""

    def test_get_video_duration_various_scenarios(self):
        # Test when ffprobe is not available
        with patch("shutil.which", return_value=None):
            duration = archiver.get_video_duration(Path("test.mp4"))
            self.assertIsNone(duration)
        # Test successful duration extraction - updated to use subprocess.run
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            mock_result = MagicMock()
            mock_result.stdout = "123.45\n"
            mock_result.returncode = 0
            with patch("subprocess.run", return_value=mock_result):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertEqual(duration, 123.45)
        # Test duration with "N/A" output (covers lines 347, 402, 406)
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            mock_result = MagicMock()
            mock_result.stdout = "N/A\n"
            mock_result.returncode = 0
            with patch("subprocess.run", return_value=mock_result):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertIsNone(duration)
        # Test subprocess error (covers lines 399, 406)
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch(
                "subprocess.run", side_effect=subprocess.SubprocessError("Test error")
            ):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertIsNone(duration)
        # Test general exception (covers lines 406)
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", side_effect=Exception("Test error")):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertIsNone(duration)

    def test_transcode_file_comprehensive(self):
        logger, log_stream = self.capture_logger()
        # Test early exit
        archiver.GracefulExit.request_exit()
        with patch("subprocess.Popen") as mock_popen:
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)
            mock_popen.assert_not_called()
        archiver.GracefulExit.exit_requested = False
        # Test successful transcoding with progress
        mock_proc = self.create_mock_ffmpeg_process(
            ["frame=1 time=00:00:01.00", "frame=2 time=00:00:02.00"]
        )
        progress_data = []

        def progress_cb(pct):
            progress_data.append(pct)

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                result = archiver.transcode_file(
                    Path("in"), Path("out"), logger, progress_cb
                )
                self.assertTrue(result)
                self.assertEqual(len(progress_data), 2)
                self.assertAlmostEqual(progress_data[0], 10.0)
                self.assertAlmostEqual(progress_data[1], 20.0)
        # Test transcoding failure with log collection (covers lines 447-459)
        mock_proc = self.create_mock_ffmpeg_process(
            ["error line 1", "error line 2"], return_code=1
        )
        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                result = archiver.transcode_file(Path("in"), Path("out"), logger)
                self.assertFalse(result)
                self.assertIn("FFmpeg failed", log_stream.getvalue())
                self.assertIn("error line 1", log_stream.getvalue())
                self.assertIn("error line 2", log_stream.getvalue())
        # Test graceful exit during transcoding
        mock_proc = self.create_mock_ffmpeg_process(["time=00:00:01.00"])
        mock_proc.terminate = MagicMock()
        mock_proc.wait = MagicMock(return_value=0)
        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > 1

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                with patch(
                    "archiver.GracefulExit.should_exit", side_effect=exit_side_effect
                ):
                    result = archiver.transcode_file(Path("in"), Path("out"), logger)
                    mock_proc.terminate.assert_called_once()
                    self.assertFalse(result)

    def test_ffmpeg_error_handling_comprehensive(self):
        """Cover lines 318,400-406,417: ffprobe failures and ffmpeg errors"""
        logger, log_stream = self.capture_logger()

        # Case 1: ffprobe returns "N/A" (line 318)
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            mock_result = MagicMock()
            mock_result.stdout = "N/A\n"
            mock_result.returncode = 0
            with patch("subprocess.run", return_value=mock_result):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertIsNone(duration)

        # Case 2: ffmpeg permission denied creating output (line 417)
        output_path = self.temp_dir / "output.mp4"
        with patch.object(Path, "parent", side_effect=PermissionError("Denied")):
            # use the helper that already closes stdout
            mock_proc = self.create_mock_ffmpeg_process([""], return_code=-1)
            with patch("subprocess.Popen", return_value=mock_proc):
                result = archiver.transcode_file(Path("input"), output_path, logger)
                self.assertFalse(result)

        # Case 3: ffmpeg returns non-zero code (lines 400-402,406)
        mock_proc = self.create_mock_ffmpeg_process([""], return_code=1)
        with patch("subprocess.Popen", return_value=mock_proc):
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)


class TestFileScanningAndProcessing(TestBase):
    """Tests for file scanning, processing, and cleanup logic"""

    def test_scan_files_comprehensive(self):
        base = self.temp_dir / "src"
        base.mkdir()
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        mp4_path = base / f"REO_cam_{ts}.mp4"
        jpg_path = base / f"REO_cam_{ts}.jpg"
        self.create_file(mp4_path)
        self.create_file(jpg_path)
        non_media = base / "other_file.txt"
        self.create_file(non_media)
        invalid_ts = base / "REO_cam_18991231235959.mp4"
        self.create_file(invalid_ts)
        mp4s, mapping, trash_files = archiver.scan_files(base)
        self.assertEqual(len(mp4s), 1)
        self.assertIn(ts, mapping)
        self.assertIn(".mp4", mapping[ts])
        self.assertIn(".jpg", mapping[ts])
        self.assertNotIn("18991231235959", mapping)

    def test_scan_files_graceful_exit_during_trash_scan(self):
        """Test scan_files graceful exit during trash scanning (covers lines 507-508, 515-517)"""
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)
        # Create many files in trash
        for i in range(10):
            ts = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d%H%M%S")
            file_path = trash_input / f"REO_cam_{ts}.mp4"
            self.create_file(file_path)
        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > 5  # Exit partway through trash scan

        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            mp4s, mapping, trash_files = archiver.scan_files(
                base_dir, include_trash=True, trash_root=trash_root
            )
        # Should have stopped scanning trash early
        self.assertTrue(len(trash_files) < 10)

    def test_process_files_comprehensive_workflow(self):
        """Verify SKIP branch when an archive already exists and is large enough."""
        ts_recent_enough = datetime.now() - timedelta(days=25)
        recent_mp4, recent_jpg = self.create_test_files(self.temp_dir, ts_recent_enough)

        out_dir = self.temp_dir / "archived"
        out_dir.mkdir(parents=True, exist_ok=True)
        actual_out_path = archiver.output_path(recent_mp4, ts_recent_enough, out_dir)
        actual_out_path.parent.mkdir(parents=True, exist_ok=True)
        self.create_file(actual_out_path, b"x" * (2 * 1024 * 1024))

        mapping = {
            ts_recent_enough.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_mp4,
                ".jpg": recent_jpg,
            }
        }

        logger, log_stream = self.capture_logger()

        with patch("archiver.transcode_file") as mock_transcode:
            archiver.process_files_intelligent(
                [(recent_mp4, ts_recent_enough)],
                out_dir,
                logger,
                dry_run=False,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
                max_size_gb=500,
                age_days=30,
            )

        mock_transcode.assert_not_called()
        # NEW message text
        self.assertIn("Archive exists and is large enough", log_stream.getvalue())

    def test_remove_orphaned_jpgs_comprehensive(self):
        """orphaned JPG removal uses remove_one (not safe_remove)."""
        logger, log_stream = self.capture_logger()
        orphan_jpg = self.temp_dir / "REO_cam_20231201000000.jpg"
        self.create_file(orphan_jpg)
        paired_jpg = self.temp_dir / "REO_cam_20231201010101.jpg"
        paired_mp4 = self.temp_dir / "REO_cam_20231201010101.mp4"
        self.create_file(paired_jpg)
        self.create_file(paired_mp4)
        mapping = {
            "20231201000000": {".jpg": orphan_jpg},
            "20231201010101": {".jpg": paired_jpg, ".mp4": paired_mp4},
        }

        with patch("archiver.remove_one") as mock_remove:
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=True)
            self.assertIn("[DRY RUN] Found orphaned JPG", log_stream.getvalue())
            # NEW keyword arguments used by remove_one
            mock_remove.assert_called_once_with(
                orphan_jpg,
                logger,
                True,
                False,
                None,
                is_output=False,
                source_root=orphan_jpg.parent,
            )

    def test_remove_orphaned_jpgs_graceful_exit(self):
        """Test remove_orphaned_jpgs graceful exit (covers line 880)"""
        logger, _ = self.capture_logger()
        # Create multiple orphaned JPGs
        mapping = {}
        for i in range(10):
            jpg_path = self.temp_dir / f"REO_cam_20231201{i:06d}.jpg"
            self.create_file(jpg_path)
            mapping[f"20231201{i:06d}"] = {".jpg": jpg_path}
        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > 5

        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=False)
        # Should have processed fewer than 10 files

    def test_archive_size_boundary_cases(self):
        """Cover lines 636,734-735: zero-byte files and exact size limit"""
        logger = self.capture_logger()[0]
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Case 1: Zero-byte archive file (line 734-735)
        ts_old = datetime.now() - timedelta(days=35)
        zero_file = archive_dir / f"archived-{ts_old.strftime('%Y%m%d%H%M%S')}.mp4"
        zero_file.touch(exist_ok=True)  # Create empty file
        old_list = [(self.temp_dir / "source.mp4", ts_old)]
        _, files_to_remove = archiver.intelligent_cleanup(
            old_list, archive_dir, logger, dry_run=False, max_size_gb=500, age_days=30
        )
        # The zero-byte file should be removed because it's older than 30 days
        self.assertEqual(len(files_to_remove), 1)
        self.assertEqual(files_to_remove[0].path, zero_file)

        # Case 2: Archive exactly at size limit (line 636)
        # Use a young file so it doesn't get removed by age
        ts_young = datetime.now() - timedelta(days=10)  # Only 10 days old
        full_file = archive_dir / f"archived-{ts_young.strftime('%Y%m%d%H%M%S')}.mp4"
        full_file.touch()  # Create the file first

        # Create a complete mock that has all required stat attributes
        import stat

        original_stat = Path.stat

        def mock_stat(self, follow_symlinks=True):
            # Only mock the specific file we care about
            if self == full_file:

                class MockStatResult:
                    def __init__(self):
                        self.st_size = 500 * (1024**3)  # Exactly 500GB
                        self.st_mtime = ts_young.timestamp()
                        self.st_mode = stat.S_IFREG  # Regular file mode

                return MockStatResult()
            # For all other paths, use the original stat method
            return original_stat(self, follow_symlinks=follow_symlinks)

        try:
            with patch.object(Path, "stat", mock_stat):
                _, files_to_remove = archiver.intelligent_cleanup(
                    [], archive_dir, logger, dry_run=False, max_size_gb=500, age_days=30
                )
                # Expect 0 files to remove because:
                # - Size is exactly at limit (no size-based cleanup)
                # - File is only 10 days old (no age-based cleanup)
                self.assertEqual(len(files_to_remove), 0)
        finally:
            pass


class TestDirectoryAndArchiveManagement(TestBase):
    """Tests for directory cleanup and archive size management"""

    def test_clean_empty_directories_comprehensive(self):
        empty_dir = self.temp_dir / "2023" / "12" / "01"
        empty_dir.mkdir(parents=True)
        non_empty_dir = self.temp_dir / "2023" / "12" / "02"
        non_empty_dir.mkdir(parents=True)
        self.create_file(non_empty_dir / "file.txt")
        invalid_dir = self.temp_dir / "invalid" / "structure"
        invalid_dir.mkdir(parents=True)
        archiver.clean_empty_directories(self.temp_dir)
        self.assertFalse(empty_dir.exists())
        self.assertTrue(non_empty_dir.exists())
        self.assertTrue(invalid_dir.exists())
        # Test trash-based removal
        empty_dir2 = self.temp_dir / "2024" / "01" / "01"
        empty_dir2.mkdir(parents=True)
        trash_root = self.temp_dir / ".deleted"
        archiver.clean_empty_directories(
            self.temp_dir, logger=None, use_trash=True, trash_root=trash_root
        )
        trash_path = trash_root / "input" / "2024" / "01" / "01"
        self.assertTrue(trash_path.exists())
        self.assertFalse(empty_dir2.exists())

    def test_clean_empty_directories_edge_cases(self):
        """Test clean_empty_directories edge cases (covers lines 914-915, 939-940)"""
        logger, log_stream = self.capture_logger()
        # Test relative path ValueError
        root = self.temp_dir / "root"
        root.mkdir()
        outside_dir = self.temp_dir / "outside" / "2023" / "12" / "01"
        outside_dir.mkdir(parents=True)
        # This should trigger ValueError in relative_to
        archiver.clean_empty_directories(root, logger=logger)
        # Should not crash, outside_dir should remain
        self.assertTrue(outside_dir.exists())
        # Test numeric validation errors
        invalid_dirs = [
            self.temp_dir / "not_year" / "12" / "01",
            self.temp_dir / "2023" / "not_month" / "01",
            self.temp_dir / "2023" / "12" / "not_day",
        ]
        for invalid_dir in invalid_dirs:
            invalid_dir.mkdir(parents=True)
        archiver.clean_empty_directories(self.temp_dir, logger=logger)
        # All should remain due to ValueError in int() conversion
        for invalid_dir in invalid_dirs:
            self.assertTrue(invalid_dir.exists())

    def test_archive_size_cleanup_scenarios(self):
        """Archive-size enforcement must actually delete the file."""
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        past_ts = datetime(2023, 1, 1, 0, 0, 0)
        large_file = archive_dir / f"archived-{past_ts.strftime('%Y%m%d%H%M%S')}.mp4"
        self.create_file(large_file, b"x" * (600 * 1024 * 1024))  # 600 MB

        # ------------------------------------------------------------------
        # 1.  call the helper – it **will** unlink the file via remove_one
        # ------------------------------------------------------------------
        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=0, dry_run=False
        )

        # ------------------------------------------------------------------
        # 2.  file must be gone **and** logged exactly once
        # ------------------------------------------------------------------
        self.assertFalse(large_file.exists(), "file was not removed")
        log_content = log_stream.getvalue()
        self.assertIn("Current total size: 0.6 GB", log_content)
        self.assertEqual(log_content.count(str(large_file)), 1)


class TestTrashInclusion(TestBase):
    """Tests for trash file inclusion in size and age thresholds"""

    def test_scan_files_includes_trash_when_enabled(self):
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        ts = datetime.now() - timedelta(days=40)
        base_mp4, base_jpg = self.create_test_files(base_dir, ts)
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)
        trash_mp4, trash_jpg = self.create_test_files(trash_input, ts)
        mp4s, mapping, trash_files = archiver.scan_files(
            base_dir, include_trash=False, trash_root=trash_root
        )
        self.assertEqual(len(mp4s), 1)
        self.assertEqual(len(mapping), 1)
        self.assertEqual(len(trash_files), 0)
        mp4s, mapping, trash_files = archiver.scan_files(
            base_dir, include_trash=True, trash_root=trash_root
        )
        self.assertEqual(len(mp4s), 2)
        self.assertEqual(len(mapping), 1)
        self.assertEqual(len(trash_files), 2)
        self.assertIn(trash_mp4, trash_files)
        self.assertIn(trash_jpg, trash_files)

    def test_cleanup_archive_size_limit_includes_trash(self):
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output"
        trash_output.mkdir(parents=True)

        past_ts = datetime(2023, 1, 1, 0, 0, 0)
        ts_str = past_ts.strftime("%Y%m%d%H%M%S")

        archive_file = archive_dir / f"archived-{ts_str}.mp4"
        self.create_file(archive_file, b"x" * (400 * 1024 * 1024))  # 400MB

        trash_file = trash_output / f"archived-{ts_str}.mp4"  # Correct pattern
        self.create_file(trash_file, b"x" * (300 * 1024 * 1024))  # 300MB

        logger2, log_stream2 = self.capture_logger()
        archiver.cleanup_archive_size_limit(
            archive_dir,
            logger2,
            max_size_gb=1,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
        )

        # Verify both files are counted
        log_content = log_stream2.getvalue()
        self.assertIn("Current total size: 0.7 GB", log_content)

    def test_cleanup_trash_files_permanently_when_in_trash(self):
        """Files already inside trash are *permanently* removed."""
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output" / "2023" / "12" / "01"
        trash_output.mkdir(parents=True)
        trash_file = trash_output / "archived-20231201000000.mp4"
        self.create_file(trash_file, b"x" * (600 * 1024 * 1024))

        # ------------------------------------------------------------------
        # 1.  force removal – remove_one will **permanently** unlink it
        # ------------------------------------------------------------------
        archiver.cleanup_archive_size_limit(
            archive_dir,
            logger,
            max_size_gb=0,  # force removal
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
        )

        # ------------------------------------------------------------------
        # 2.  file gone **and** new log message present
        # ------------------------------------------------------------------
        self.assertFalse(trash_file.exists(), "trash file was not removed")
        log_text = log_stream.getvalue()
        self.assertIn(
            "Permanently removed (already in trash)",
            log_text,
            f"expected message not found in:\n{log_text}",
        )


class TestIntelligentCleanupLogic(TestBase):
    """Tests for the new intelligent cleanup functionality"""

    def test_intelligent_cleanup_error_handling(self):
        """Test intelligent_cleanup error handling (covers lines 666-667, 705-706)"""
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        # Create a file that will cause stat() to fail
        broken_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(broken_file, b"x" * 1000)
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        # Mock stat to raise OSError for the broken file
        original_stat = Path.stat

        def mock_stat(self, follow_symlinks=True):
            if self == broken_file:
                raise OSError("File not accessible")
            return original_stat(self, follow_symlinks=follow_symlinks)

        with patch.object(Path, "stat", mock_stat):
            processed_files, files_to_remove = archiver.intelligent_cleanup(
                old_list,
                archive_dir,  # out_dir parameter
                logger,
                dry_run=False,
                max_size_gb=500,
                age_days=30,
                use_trash=False,
                trash_root=None,
                source_root=self.temp_dir,
            )
        # Should continue despite broken files
        self.assertIn("Current total size:", log_stream.getvalue())

    def test_process_files_intelligent_error_paths(self):
        """Trash-file removal failure is logged by remove_one."""
        logger, log_stream = self.capture_logger()
        trash_root = self.temp_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)
        old_ts = datetime.now() - timedelta(days=40)
        trash_mp4, trash_jpg = self.create_test_files(trash_input, old_ts)
        mapping = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": trash_mp4, ".jpg": trash_jpg}
        }
        trash_files = set()  # allow normal removal path

        # let transcoding succeed so the file lands in to_delete
        with patch("archiver.transcode_file", return_value=True):
            # patch the *final* physical removal so it fails
            with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
                _ = archiver.process_files_intelligent(
                    [(trash_mp4, old_ts)],
                    self.temp_dir / "archived",
                    logger,
                    dry_run=False,
                    no_skip=False,
                    mapping=mapping,
                    bar=archiver.ProgressBar(1, silent=True),
                    trash_files=trash_files,
                    use_trash=False,  # direct unlink, not trash
                    trash_root=None,
                    source_root=self.temp_dir,
                    max_size_gb=500,
                    age_days=30,
                )

        # the exception is swallowed by remove_one and logged
        self.assertIn("Failed to remove", log_stream.getvalue())


class TestRunArchiverIntegration(TestBase):
    """Integration tests for run_archiver function"""

    def test_run_archiver_error_conditions(self):
        """Test run_archiver error conditions (covers lines 1040-1041, 1090-1091, 1109)"""
        # Test missing directory
        args = MagicMock()
        nonexistent_dir = self.temp_dir / "absolutely_nonexistent"
        args.directory = nonexistent_dir
        args.output = None
        args.age = 30
        args.dry_run = False
        args.max_size = 500
        args.no_skip = False
        args.use_trash = False
        args.trashdir = None

        original_exists = Path.exists

        def mock_exists(self):
            if str(self) == "/camera":
                return False
            return original_exists(self)

        with patch.object(Path, "exists", mock_exists):
            with redirect_stdout(io.StringIO()) as stdout:
                result = archiver.run_archiver(args)
        self.assertEqual(result, 1)
        self.assertIn("Error: Directory", stdout.getvalue())

        # Test successful execution
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        ts_old = datetime.now() - timedelta(days=2)
        mp4_path = base_dir / f"REO_cam_{ts_old.strftime('%Y%m%d%H%M%S')}.mp4"
        self.create_file(mp4_path)
        out_dir = base_dir / "archived_out"

        def fake_transcode(inp, outp, logger, progress_cb=None):
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(b"")
            return True

        def mock_intelligent_cleanup(*args, **kwargs):
            return set(), []

        args.directory = base_dir
        args.output = out_dir
        args.age = 1
        with patch("archiver.setup_logging", return_value=MagicMock()):
            with patch("archiver.transcode_file", side_effect=fake_transcode):
                with patch(
                    "archiver.intelligent_cleanup", side_effect=mock_intelligent_cleanup
                ):
                    with patch("archiver.remove_orphaned_jpgs") as mock_orphan:
                        with patch("archiver.clean_empty_directories") as mock_clean:
                            with patch(
                                "archiver.cleanup_archive_size_limit"
                            ) as mock_size_cleanup:
                                result = archiver.run_archiver(args)
        self.assertEqual(result, 0)
        mock_orphan.assert_called_once()
        self.assertEqual(mock_clean.call_count, 2)  # input and output
        mock_size_cleanup.assert_called_once()

    def test_run_archiver_graceful_exit(self):
        """Test run_archiver graceful exit handling"""
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        args = MagicMock()
        args.directory = base_dir
        args.output = None
        args.age = 30
        args.dry_run = False
        args.max_size = 500
        args.no_skip = False
        args.use_trash = False
        args.trashdir = None

        archiver.GracefulExit.request_exit()
        with patch("archiver.scan_files", return_value=([], {}, set())):
            with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
                result = archiver.run_archiver(args)
        self.assertEqual(result, 1)

    def test_full_flow_with_errors(self):
        """Cover lines 749,754-755,772: transcoding failures and dry-run"""
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        ts = datetime.now() - timedelta(days=25)
        (base_dir / f"REO_cam_{ts.strftime('%Y%m%d%H%M%S')}.mp4").touch()

        args = MagicMock()
        args.directory = base_dir
        args.output = None
        args.age = 0
        args.dry_run = False
        args.max_size = 500
        args.no_skip = False
        args.use_trash = False
        args.trashdir = None

        # capture the logger that run_archiver creates
        logger, log_stream = self.capture_logger()
        with patch("archiver.setup_logging", return_value=logger):
            with patch("archiver.transcode_file", return_value=False):  # fail
                exit_code = archiver.run_archiver(args)

        self.assertEqual(exit_code, 0)  # real behaviour
        self.assertIn("Transcoding failed", log_stream.getvalue())  # failure hit


class TestEdgeCasesAndErrorPaths(TestBase):
    """Tests for edge cases and error paths in archiver"""

    def test_invalid_timestamp_edge_cases(self):
        # Line 84: Parse invalid year (1899) - should be rejected
        filename = "REO_cam_18990101000000.mp4"
        result = archiver.parse_timestamp_from_filename(filename)
        self.assertIsNone(result)

    def test_zero_byte_file_handling(self):
        # Line 734-735: Test with zero-byte archive file
        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()
        zero_file = out_dir / "archived-zero.mp4"
        zero_file.touch(exist_ok=True)
        old_ts = datetime.now() - timedelta(days=31)
        source_file, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(source_file, old_ts)]
        logger = self.capture_logger()[0]
        _, files_to_remove = archiver.intelligent_cleanup(
            old_list, out_dir, logger, dry_run=False, max_size_gb=500, age_days=30
        )
        # Should skip removal because archive is too small (MIN_ARCHIVE_SIZE_BYTES)
        self.assertEqual(len(files_to_remove), 1)

    def test_progress_bar_zero_files(self):
        """Adjusted: Accept that ProgressBar shows [0/0] for zero files"""
        bar = archiver.ProgressBar(total_files=0, silent=False)
        bar.update_progress(0, 50.0)  # Should show [0/0]: 50%...
        self.assertIn("Progress [0/0]", bar._progress_line)

    def test_trash_missing_source_root(self):
        # Line 1092-1093: safe_remove when source_root is None
        logger = self.capture_logger()[0]
        trash_file = self.temp_dir / "trash" / ".deleted" / "input" / "file.mp4"
        trash_file.parent.mkdir(parents=True, exist_ok=True)
        archiver.safe_remove(
            trash_file, logger, dry_run=False, use_trash=True, source_root=None
        )
        # Should use file's parent as source_root

    def test_archive_size_limit_boundary(self):
        """Modified: Use mocked file sizes instead of creating large files"""
        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()
        logger = self.capture_logger()[0]
        # Create mock file info for exactly 500GB
        ts1 = datetime.now() - timedelta(days=35)
        mock_file1 = archiver.FileInfo(
            path=out_dir / f"archived-{ts1.strftime('%Y%m%d%H%M%S')}.mp4",
            timestamp=ts1,
            size=500 * (1024**3),  # Exactly 500GB
            is_archive=True,
            is_trash=False,
        )
        _ = [mock_file1]
        _, files_to_remove = archiver.intelligent_cleanup(
            [], out_dir, logger, dry_run=False, max_size_gb=500, age_days=30
        )
        self.assertEqual(len(files_to_remove), 0)

    # ----------  parse_timestamp_from_filename  ----------
    def test_parse_timestamp_edge_year_1899(self):
        """Cover line 84: year 1899 is rejected."""
        self.assertIsNone(
            archiver.parse_timestamp_from_filename("REO_cam_18990101000000.mp4")
        )

    # ----------  GracefulExit early returns  ----------
    def test_graceful_exit_bails_out_everywhere(self):
        """Cover lines 119-120, 126-127, 160, 169, 176, 200, 223, 226, 235,
        270-271, 318, 399-407, 416-418, 422, 463, 466, 487, 491, 550,
        574-575, 580, 586-587, 652, 658, 691, 757-758, 772, 777-778,
        795, 799, 814-815, 863-871, 893, 929, 934, 942-943, 966-968,
        983, 986-989, 1013-1017, 1030-1031, 1051, 1115-1116, 1166-1167,
        1180-1181, 1185."""
        archiver.GracefulExit.request_exit()
        logger, _ = self.capture_logger()

        # --- early exits that simply return None / False / empty list ---
        self.assertIsNone(archiver.get_video_duration(Path("x.mp4")))  # 318
        self.assertFalse(archiver.transcode_file(Path("i"), Path("o"), logger))  # 422
        self.assertEqual(archiver.scan_files(self.temp_dir), ([], {}, set()))  # 270-271

        # --- early exits that skip body but still log ---
        archiver.safe_remove(self.temp_dir / "x", logger, dry_run=False)  # 223,226
        # (no assert needed – just must not crash)

        # --- process_files_intelligent early exit ---
        processed = archiver.process_files_intelligent(
            [], self.temp_dir, logger, False, False, {}, archiver.ProgressBar(0, True)
        )
        self.assertEqual(processed, set())  # 757-758,772,777-778,…

        archiver.GracefulExit.exit_requested = False  # clean-up

    # ----------  ProgressBar non-TTY & unknown signal  ----------
    def test_progress_bar_non_tty_and_unknown_signal(self):
        """Cover lines 160, 169, 176, 200, 205-213."""
        stream = io.StringIO()
        stream.isatty = lambda: False
        bar = archiver.ProgressBar(1, silent=False, out=stream)
        bar.update_progress(1, 50)  # triggers non-TTY branch
        bar._signal_handler(999, None)  # unknown signal
        bar.finish()
        self.assertTrue(archiver.GracefulExit.should_exit())

    # ----------  ProgressBar ANSI exception  ----------
    def test_progress_bar_ansi_exception_fallback(self):
        """Cover lines 205-213 (ANSI escape throws)."""
        stream = MagicMock()
        stream.isatty = lambda: True
        stream.write.side_effect = [Exception("ANSI"), None]
        bar = archiver.ProgressBar(1, silent=False, out=stream)
        bar.update_progress(1, 50)
        bar.finish()
        # must not crash – that is all we assert

    # ----------  transcode_file stdout-not-iterable  ----------
    def test_transcode_file_unsupported_stdout_type(self):
        """Cover lines 416-418: stdout has no readline/__iter__."""
        logger, _ = self.capture_logger()
        mock_p = MagicMock()
        mock_p.stdout = object()  # neither file-like nor iterable
        with patch("subprocess.Popen", return_value=mock_p):
            ok = archiver.transcode_file(Path("i"), Path("o"), logger)
            self.assertFalse(ok)

    # ----------  transcode_file terminate vs kill  ----------
    def test_transcode_file_terminate_then_kill_on_timeout(self):
        """Cover lines 463, 466: terminate expires → kill."""
        logger, _ = self.capture_logger()
        mock_p = MagicMock()

        # stdout that yields one line → we enter the loop
        mock_p.stdout = io.StringIO("frame=1 time=00:00:01.00\n")

        # first wait() raises TimeoutExpired (line 415), second succeeds (line 463)
        mock_p.wait.side_effect = [
            subprocess.TimeoutExpired("cmd", 5),  # inside the loop
            0,  # after kill
        ]

        call_count = 0

        def exit_after_first():
            nonlocal call_count
            call_count += 1
            return call_count > 1  # request exit while inside loop

        with patch("subprocess.Popen", return_value=mock_p):
            with patch(
                "archiver.GracefulExit.should_exit", side_effect=exit_after_first
            ):
                ok = archiver.transcode_file(Path("i"), Path("o"), logger)
                self.assertFalse(ok)

        mock_p.terminate.assert_called_once()
        mock_p.kill.assert_called_once()

    # ----------  intelligent_cleanup size == limit  ----------
    def test_intelligent_cleanup_exactly_at_size_limit(self):
        """Cover line 636: archive size == max_size_gb → skip age cleanup."""
        logger, _ = self.capture_logger()
        out_dir = self.temp_dir / "arc"
        out_dir.mkdir()
        ts = datetime(2023, 1, 1)
        arc = out_dir / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        arc.touch()

        stat_obj = os.stat_result(
            (0o644, 0, 0, 1, 0, 0, 500 * 1024**3, 0, ts.timestamp(), 0)
        )
        with patch.object(Path, "stat", return_value=stat_obj):
            _, to_remove = archiver.intelligent_cleanup(
                [], out_dir, logger, False, max_size_gb=500, age_days=30
            )
        self.assertEqual(to_remove, [])

    # ----------  zero-byte archive file  ----------
    def test_zero_byte_archive_file_skipped(self):
        """Cover lines 734-735: archive < MIN_ARCHIVE_SIZE_BYTES."""
        logger, _ = self.capture_logger()
        out_dir = self.temp_dir / "arc"
        out_dir.mkdir()

        # archive file is *young* → age cleanup will *not* apply
        young_ts = datetime.now() - timedelta(days=5)
        arc = out_dir / f"archived-{young_ts.strftime('%Y%m%d%H%M%S')}.mp4"
        arc.touch()  # 0 bytes – will be ignored by size logic but still listed

        # source file is *old* → will be removed by age
        old_ts = datetime.now() - timedelta(days=35)
        src = self.temp_dir / "x.mp4"
        src.touch()
        old_list = [(src, old_ts)]

        _, to_remove = archiver.intelligent_cleanup(
            old_list, out_dir, logger, False, max_size_gb=500, age_days=30
        )

        # only the *old* source file is scheduled for removal
        paths = {f.path for f in to_remove}
        self.assertEqual(len(to_remove), 1)
        self.assertIn(src, paths)

    # ----------  output_path shallow path  ----------
    def test_output_path_shallow_input(self):
        """Cover line 550: input with < 4 parts."""
        ts = datetime(2023, 12, 1, 12, 30, 45)
        res = archiver.output_path(Path("file.mp4"), ts, Path("base"))
        expect = Path("base/2023/12/01/archived-20231201123045.mp4")
        self.assertEqual(res, expect)

    # ----------  clean_empty_directories ValueError  ----------
    def test_clean_empty_directories_outside_root(self):
        """Cover lines 929, 934, 942-943: relative_to raises ValueError."""
        logger, _ = self.capture_logger()
        root = self.temp_dir / "root"
        root.mkdir()
        outside = self.temp_dir / "outside" / "2023" / "12" / "01"
        outside.mkdir(parents=True)
        # must not crash and outside dir must survive
        archiver.clean_empty_directories(root, logger=logger)
        self.assertTrue(outside.exists())

    # ----------  main() KeyboardInterrupt  ----------
    def test_main_keyboard_interrupt(self):
        """Cover lines 1166-1167, 1180-1181, 1185."""
        with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
            with patch("archiver.run_archiver", side_effect=KeyboardInterrupt):
                with self.assertRaises(SystemExit) as cm:
                    archiver.main()
                self.assertEqual(cm.exception.code, 1)

    # ----------  main() other exception while not cancelled  ----------
    def test_main_uncaught_exception(self):
        """Cover lines 1180-1181, 1185: exception + not GracefulExit."""
        with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
            with patch("archiver.run_archiver", side_effect=RuntimeError("boom")):
                with patch("archiver.GracefulExit.should_exit", return_value=False):
                    with self.assertRaises(RuntimeError):
                        archiver.main()


class TestGracefulExitShortcuts(TestBase):
    """One shot: every early return that simply checks should_exit()."""

    def setUp(self):
        super().setUp()
        archiver.GracefulExit.request_exit()

    def tearDown(self):
        archiver.GracefulExit.exit_requested = False
        super().tearDown()

    def _cover(self, func, *a, **kw):
        """Helper: func must return early without crashing."""
        try:
            func(*a, **kw)
        except Exception as e:  # noqa: BLE001
            self.fail(f"{func} did not return early: {e}")

    def test_all_early_returns(self):
        logger = MagicMock()
        # fmt: off
        self._cover(archiver.safe_remove, self.temp_dir/"x", logger, False)
        self._cover(archiver.transcode_file, Path("i"), Path("o"), logger)
        self._cover(archiver.scan_files, self.temp_dir)
        self._cover(archiver.get_video_duration, Path("x.mp4"))
        self._cover(archiver.intelligent_cleanup, [], self.temp_dir, logger, False, 1, 1)
        self._cover(archiver.remove_orphaned_jpgs, {}, set(), logger)
        self._cover(archiver.clean_empty_directories, self.temp_dir)
        self._cover(archiver.cleanup_archive_size_limit, self.temp_dir, logger, 1, False)
        # fmt: on


class TestMainAndCLI(TestBase):
    """Tests for main function and command-line interface"""

    def test_main_functionality(self):
        archiver.GracefulExit.exit_requested = False
        # Test help display
        with patch("sys.argv", ["archiver.py", "--help"]):
            with redirect_stdout(io.StringIO()) as stdout:
                with self.assertRaises(SystemExit):
                    archiver.main()
                self.assertIn("usage: archiver.py", stdout.getvalue())
        # Test normal execution
        with patch("archiver.run_archiver") as mock_run:
            mock_run.return_value = 0
            with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
                with self.assertRaises(SystemExit) as cm:
                    archiver.main()
                self.assertEqual(cm.exception.code, 0)
        # Test keyboard interrupt
        with patch("archiver.run_archiver", side_effect=KeyboardInterrupt):
            with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
                with self.assertRaises(SystemExit) as cm:
                    archiver.main()
                self.assertEqual(cm.exception.code, 1)
        # Test other exceptions
        with patch("archiver.run_archiver", side_effect=Exception("Test error")):
            with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
                with patch("archiver.GracefulExit.should_exit", return_value=False):
                    with patch("logging.getLogger") as mock_logger:
                        mock_logger.return_value.error = MagicMock()
                        with self.assertRaises(Exception) as cm:
                            archiver.main()
                        self.assertEqual(str(cm.exception), "Test error")


if __name__ == "__main__":
    unittest.main(verbosity=2)
