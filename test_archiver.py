#!/usr/bin/env python3

import io
import logging
import shutil
import sys
import subprocess
import tempfile
import time
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
        bar.update_progress(1, 50.0)  # Force progress line

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

        self.assertEqual(stream.getvalue(), "")  # No output in silent mode

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
                Path("file.mp4"),  # Shallow path
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

        # Test error handling - create a scenario that will actually fail
        # Try to remove a file that doesn't exist and mock the unlink to raise an exception
        nonexistent_file = self.temp_dir / "nonexistent.txt"
        with patch.object(Path, "is_file", return_value=True):
            with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
                archiver.safe_remove(nonexistent_file, logger, dry_run=False)
                self.assertIn("Failed to remove", log_stream.getvalue())

    def test_trash_operations_comprehensive(self):
        logger, log_stream = self.capture_logger()
        trash_root = self.temp_dir / ".trash"

        # Test input file trashing with source root
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
        source_file2 = self.temp_dir / "nested" / "file.txt"  # Same name
        self.create_file(source_file2, b"new content")

        archiver.safe_remove(
            source_file2,
            logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )

        # Should have two files with different names
        trash_files = list((trash_root / "input" / "nested").glob("file*.txt"))
        self.assertEqual(len(trash_files), 2)

    def test_graceful_exit_respected_across_operations(self):
        """Test that all major functions respect the graceful exit flag"""
        archiver.GracefulExit.request_exit()
        logger, _ = self.capture_logger()

        # Test scan_files
        result = archiver.scan_files(self.temp_dir)
        self.assertEqual(result, ([], {}))

        # Test safe_remove
        with patch("archiver.shutil.move") as mock_move:
            archiver.safe_remove(Path("test"), logger, False)
            mock_move.assert_not_called()

        # Test transcode_file
        with patch("subprocess.Popen") as mock_popen:
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)
            mock_popen.assert_not_called()

        # Test get_video_duration
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            duration = archiver.get_video_duration(Path("test.mp4"))
            self.assertIsNone(duration)


class TestFileScanningAndProcessing(TestBase):
    """Tests for file scanning, processing, and cleanup logic"""

    def test_scan_files_comprehensive(self):
        base = self.temp_dir / "src"
        base.mkdir()

        # Create valid files
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        mp4_path = base / f"REO_cam_{ts}.mp4"
        jpg_path = base / f"REO_cam_{ts}.jpg"
        self.create_file(mp4_path)
        self.create_file(jpg_path)

        # Create files that should be ignored
        del_dir = base / ".deleted"
        del_dir.mkdir()
        del_file = del_dir / "REO_cam_20231201000000.mp4"
        self.create_file(del_file)

        non_media = base / "other_file.txt"
        self.create_file(non_media)

        invalid_ts = base / "REO_cam_18991231235959.mp4"  # Too old
        self.create_file(invalid_ts)

        mp4s, mapping = archiver.scan_files(base)

        self.assertEqual(len(mp4s), 1)
        self.assertIn(ts, mapping)
        self.assertIn(".mp4", mapping[ts])
        self.assertIn(".jpg", mapping[ts])
        self.assertNotIn("18991231235959", mapping)

    def test_process_files_comprehensive_workflow(self):
        logger, log_stream = self.capture_logger()

        # Create test files
        ts_old = datetime.now() - timedelta(days=35)
        ts_recent = datetime.now() - timedelta(days=15)

        old_mp4, old_jpg = self.create_test_files(self.temp_dir, ts_old)
        recent_mp4, recent_jpg = self.create_test_files(self.temp_dir, ts_recent)

        out_dir = self.temp_dir / "archived"
        mapping = {
            ts_old.strftime("%Y%m%d%H%M%S"): {".mp4": old_mp4, ".jpg": old_jpg},
            ts_recent.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_mp4,
                ".jpg": recent_jpg,
            },
        }

        # Test dry run
        with patch("archiver.transcode_file", return_value=True):
            _ = archiver.process_files(
                [(old_mp4, ts_old)],
                out_dir,
                logger,
                dry_run=True,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
            )

        self.assertIn("[DRY RUN] Would transcode", log_stream.getvalue())
        self.assertTrue(old_mp4.exists())  # Not actually removed

        # Test skip logic when output exists and is large enough
        actual_out_path = archiver.output_path(old_mp4, ts_old, out_dir)
        actual_out_path.parent.mkdir(parents=True, exist_ok=True)
        self.create_file(actual_out_path, b"x" * (2 * 1024 * 1024))  # 2MB file

        with patch("archiver.transcode_file") as mock_transcode:
            with patch("archiver.safe_remove") as mock_remove:
                archiver.process_files(
                    [(old_mp4, ts_old)],
                    out_dir,
                    logger,
                    dry_run=False,
                    no_skip=False,
                    mapping=mapping,
                    bar=archiver.ProgressBar(1, silent=True),
                )

                mock_transcode.assert_not_called()
                self.assertEqual(mock_remove.call_count, 2)  # MP4 and JPG
                self.assertIn(
                    "[SKIP] Existing archive large enough", log_stream.getvalue()
                )

        # Test transcoding failure handling
        with patch("archiver.transcode_file", return_value=False):
            archiver.process_files(
                [(recent_mp4, ts_recent)],  # Use recent file that hasn't been processed
                out_dir,
                logger,
                dry_run=False,
                no_skip=True,  # Force processing
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
            )

            self.assertTrue(recent_mp4.exists())  # Should remain on failure

    def test_remove_orphaned_jpgs_comprehensive(self):
        logger, log_stream = self.capture_logger()

        # Create orphaned JPG
        orphan_jpg = self.temp_dir / "REO_cam_20231201000000.jpg"
        self.create_file(orphan_jpg)

        # Create JPG with MP4 pair
        paired_jpg = self.temp_dir / "REO_cam_20231201010101.jpg"
        paired_mp4 = self.temp_dir / "REO_cam_20231201010101.mp4"
        self.create_file(paired_jpg)
        self.create_file(paired_mp4)

        mapping = {
            "20231201000000": {".jpg": orphan_jpg},  # Orphaned
            "20231201010101": {".jpg": paired_jpg, ".mp4": paired_mp4},  # Paired
        }

        with patch("archiver.safe_remove") as mock_remove:
            # Test with dry run only
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=True)
            self.assertIn("[DRY RUN] Found orphaned JPG", log_stream.getvalue())
            # Should be called once for dry run
            mock_remove.assert_called_once_with(
                orphan_jpg, logger, True, use_trash=False, trash_root=None
            )

        # Reset the mock for the actual test
        with patch("archiver.safe_remove") as mock_remove:
            # Test actual removal
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=False)
            mock_remove.assert_called_once_with(
                orphan_jpg, logger, False, use_trash=False, trash_root=None
            )

        # Test skipping already processed JPGs
        with patch("archiver.safe_remove") as mock_remove:
            archiver.remove_orphaned_jpgs(mapping, {orphan_jpg}, logger, dry_run=False)
            mock_remove.assert_not_called()


class TestTranscodingAndMediaOperations(TestBase):
    """Tests for video transcoding and media-related operations"""

    def test_get_video_duration_various_scenarios(self):
        # Test when ffprobe is not available
        with patch("shutil.which", return_value=None):
            duration = archiver.get_video_duration(Path("test.mp4"))
            self.assertIsNone(duration)

        # Test successful duration extraction
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.check_output", return_value="123.45\n"):
                duration = archiver.get_video_duration(Path("test.mp4"))
                self.assertEqual(duration, 123.45)

        # Test error handling
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.check_output", side_effect=Exception("Test error")):
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
                self.assertAlmostEqual(progress_data[0], 10.0)  # 1s of 10s = 10%
                self.assertAlmostEqual(progress_data[1], 20.0)  # 2s of 10s = 20%

        # Test transcoding failure
        mock_proc = self.create_mock_ffmpeg_process([], return_code=1)
        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                result = archiver.transcode_file(Path("in"), Path("out"), logger)
                self.assertFalse(result)
                self.assertIn("FFmpeg failed", log_stream.getvalue())

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


class TestDirectoryAndArchiveManagement(TestBase):
    """Tests for directory cleanup and archive size management"""

    def test_clean_empty_directories_comprehensive(self):
        # Create empty directory structure that should be removed
        empty_dir = self.temp_dir / "2023" / "12" / "01"
        empty_dir.mkdir(parents=True)

        # Create non-empty directory that should remain
        non_empty_dir = self.temp_dir / "2023" / "12" / "02"
        non_empty_dir.mkdir(parents=True)
        self.create_file(non_empty_dir / "file.txt")

        # Create invalid directory structure (not YYYY/MM/DD)
        invalid_dir = self.temp_dir / "invalid" / "structure"
        invalid_dir.mkdir(parents=True)

        # Test regular removal
        archiver.clean_empty_directories(self.temp_dir)
        self.assertFalse(empty_dir.exists())
        self.assertTrue(non_empty_dir.exists())
        self.assertTrue(invalid_dir.exists())  # Should remain

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

    def test_archive_size_cleanup_scenarios(self):
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Test when under limit
        small_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(small_file, b"x" * (100 * 1024 * 1024))  # 100MB

        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=1, dry_run=False
        )
        self.assertTrue(small_file.exists())
        self.assertIn("Current archive size: 0.1 GB", log_stream.getvalue())

        # Test when over limit
        large_file = archive_dir / "archived-20230101000001.mp4"
        self.create_file(large_file, b"x" * (600 * 1024 * 1024))  # 600MB

        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=0.5, dry_run=False
        )
        self.assertFalse(large_file.exists())
        self.assertIn("Archive size exceeds limit", log_stream.getvalue())

        # Test dry run with fresh logger to avoid previous messages
        logger_dry, log_stream_dry = self.capture_logger()
        archiver.cleanup_archive_size_limit(
            archive_dir, logger_dry, max_size_gb=0.5, dry_run=True
        )
        self.assertIn(
            "[DRY RUN] Would check archive size limit", log_stream_dry.getvalue()
        )


class TestProgressBarAdvanced(TestBase):
    """Tests for advanced progress bar functionality and edge cases"""

    def test_progress_bar_display_modes(self):
        """Test progress bar under TTY, non-TTY, and silent conditions"""
        # TTY mode
        stream_tty = io.StringIO()
        stream_tty.isatty = lambda: True
        with archiver.ProgressBar(total_files=2, silent=False, out=stream_tty) as bar:
            bar.start_processing()
            bar.start_file()
            bar.update_progress(1, 50.0)
            bar.finish_file(1)
        output_tty = stream_tty.getvalue()
        self.assertIn("Progress [1/2]: 50%", output_tty)
        self.assertIn("\x1b[2K", output_tty)

        # Non-TTY periodic output
        stream_non_tty = io.StringIO()
        stream_non_tty.isatty = lambda: False
        bar2 = archiver.ProgressBar(total_files=2, silent=False, out=stream_non_tty)
        bar2.start_processing()
        bar2.update_progress(1, 25.0)  # Should not print yet
        self.assertEqual(stream_non_tty.getvalue(), "")
        with patch("time.time", return_value=time.time() + 10):
            bar2.update_progress(1, 100.0)  # Should print now
        output_non_tty = stream_non_tty.getvalue()
        self.assertIn("Progress [1/2]: 100%", output_non_tty)
        bar2.finish()

        # Silent mode
        stream_silent = io.StringIO()
        bar3 = archiver.ProgressBar(total_files=1, silent=True, out=stream_silent)
        bar3.update_progress(1, 50.0)
        bar3.finish()
        self.assertEqual(stream_silent.getvalue(), "")

    def test_progress_bar_display_exception_handling(self):
        """Test progress bar gracefully handles display exceptions"""
        stream = MagicMock()
        stream.isatty = lambda: True
        stream.write.side_effect = [
            Exception("ANSI error"),
            None,
        ]  # First call fails, second succeeds
        stream.flush = MagicMock()

        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)

        # Should have tried twice - once with ANSI codes, once without
        self.assertEqual(stream.write.call_count, 2)
        bar.finish()

    def test_progress_bar_signal_handling_edge_cases(self):
        """Test progress bar signal registration edge cases"""
        with patch("signal.signal", side_effect=ValueError("Invalid signal")):
            bar = archiver.ProgressBar(total_files=1, silent=False)
            # Should not crash even if signal registration fails
            self.assertIsInstance(bar, archiver.ProgressBar)
            bar.finish()


class TestGuardedStreamHandlerEdgeCases(TestBase):
    """Tests for GuardedStreamHandler edge cases"""

    def test_guarded_stream_handler_without_progress_bar(self):
        """Test GuardedStreamHandler when no progress bar is present"""
        stream = io.StringIO()
        orchestrator = archiver.ConsoleOrchestrator()
        handler = archiver.GuardedStreamHandler(orchestrator, stream=stream)

        record = logging.LogRecord(
            "test", logging.INFO, "", 0, "Test message", None, None
        )
        handler.setFormatter(logging.Formatter("%(message)s"))

        handler.emit(record)
        output = stream.getvalue()
        self.assertIn("Test message", output)
        # Should not contain progress bar cleanup sequences
        self.assertNotIn("\x1b[2K", output)


class TestSafeRemoveAdvanced(TestBase):
    """Tests for advanced safe_remove functionality"""

    def test_safe_remove_comprehensive_scenarios(self):
        scenarios = [
            (
                "dry_run",
                {
                    "dry_run": True,
                    "use_trash": False,
                    "is_dir": False,
                    "unsupported": False,
                    "collision": False,
                    "mock_error": False,
                },
            ),
            (
                "actual_remove_file",
                {
                    "dry_run": False,
                    "use_trash": False,
                    "is_dir": False,
                    "unsupported": False,
                    "collision": False,
                    "mock_error": False,
                },
            ),
            (
                "actual_remove_dir",
                {
                    "dry_run": False,
                    "use_trash": False,
                    "is_dir": True,
                    "unsupported": False,
                    "collision": False,
                    "mock_error": False,
                },
            ),
            (
                "unsupported_type",
                {
                    "dry_run": False,
                    "use_trash": False,
                    "is_dir": False,
                    "unsupported": True,
                    "collision": False,
                    "mock_error": False,
                },
            ),
            (
                "trash_mode",
                {
                    "dry_run": False,
                    "use_trash": True,
                    "is_dir": False,
                    "unsupported": False,
                    "collision": False,
                    "mock_error": False,
                },
            ),
            (
                "trash_collision",
                {
                    "dry_run": False,
                    "use_trash": True,
                    "is_dir": False,
                    "unsupported": False,
                    "collision": True,
                    "mock_error": False,
                },
            ),
            (
                "error_handling",
                {
                    "dry_run": False,
                    "use_trash": False,
                    "is_dir": False,
                    "unsupported": False,
                    "collision": False,
                    "mock_error": True,
                },
            ),
        ]

        for name, params in scenarios:
            with self.subTest(name=name):
                logger, log_stream = self.capture_logger()
                use_trash = params["use_trash"]
                dry_run = params["dry_run"]
                is_dir = params["is_dir"]
                unsupported = params["unsupported"]
                collision = params["collision"]
                mock_error = params["mock_error"]

                trash_root = self.temp_dir / ".deleted"
                test_path = self.temp_dir / "test_item"

                if is_dir:
                    test_path.mkdir()
                elif unsupported:
                    pass  # Don't create anything for unsupported types
                else:
                    self.create_file(test_path)

                if collision and use_trash:
                    trash_sub = trash_root / "input"
                    trash_sub.mkdir(parents=True, exist_ok=True)
                    # Create a file that will cause initial collision
                    self.create_file(trash_sub / "test_item")

                if mock_error:
                    with patch.object(Path, "is_file", return_value=True):
                        with patch.object(
                            Path, "unlink", side_effect=OSError("Permission denied")
                        ):
                            archiver.safe_remove(
                                test_path,
                                logger,
                                dry_run=dry_run,
                                use_trash=use_trash,
                                trash_root=trash_root,
                                source_root=self.temp_dir,
                            )
                    self.assertIn("Failed to remove", log_stream.getvalue())
                    continue

                if unsupported:
                    # For unsupported types, mock the file checks
                    with patch.object(Path, "is_file", return_value=False):
                        with patch.object(Path, "is_dir", return_value=False):
                            archiver.safe_remove(
                                test_path,
                                logger,
                                dry_run=dry_run,
                                use_trash=use_trash,
                                trash_root=trash_root,
                                source_root=self.temp_dir,
                            )
                else:
                    # For normal cases, don't mock anything - let the filesystem work
                    archiver.safe_remove(
                        test_path,
                        logger,
                        dry_run=dry_run,
                        use_trash=use_trash,
                        trash_root=trash_root,
                        source_root=self.temp_dir,
                    )

                # Assertions
                log_output = log_stream.getvalue()
                if dry_run:
                    self.assertIn("[DRY RUN] Would remove", log_output)
                    self.assertTrue(test_path.exists())
                elif unsupported:
                    self.assertIn("Unsupported file type", log_output)
                elif use_trash:
                    # Check that file was moved to trash
                    trash_input = trash_root / "input"
                    trash_files = list(trash_input.glob("test_item*"))
                    self.assertGreaterEqual(len(trash_files), 1)
                    self.assertFalse(test_path.exists())
                else:
                    self.assertFalse(test_path.exists())
                    self.assertIn("Removed:", log_output)


class TestTranscodingAdvanced(TestBase):
    """Tests for advanced transcoding scenarios"""

    def test_transcode_file_timeout_handling(self):
        """Test transcoding with process termination timeout"""
        logger, _ = self.capture_logger()

        mock_proc = MagicMock()
        mock_proc.stdout = io.StringIO("time=00:00:01.00\n")
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()
        mock_proc.wait = MagicMock(
            side_effect=[subprocess.TimeoutExpired("cmd", 5), None]
        )

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
                    mock_proc.kill.assert_called_once()
                    self.assertEqual(mock_proc.wait.call_count, 2)
                    self.assertFalse(result)

    def test_transcode_file_no_duration_progress(self):
        """Test transcoding progress when duration cannot be determined"""
        logger, _ = self.capture_logger()
        progress_data = []

        def progress_cb(pct):
            progress_data.append(pct)

        mock_proc = self.create_mock_ffmpeg_process(
            [
                "frame=1",
                "frame=2",
                "frame=3",  # No time info
            ]
        )

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=None):
                result = archiver.transcode_file(
                    Path("in"), Path("out"), logger, progress_cb
                )

                self.assertTrue(result)
                self.assertTrue(len(progress_data) > 0)
                # Should increment without duration info
                self.assertTrue(all(p <= 99.0 for p in progress_data))


class TestCleanupOperationsAdvanced(TestBase):
    """Tests for advanced cleanup operations"""

    def test_clean_empty_directories_invalid_structure_patterns(self):
        """Test cleanup with various invalid directory structures"""
        logger, log_stream = self.capture_logger()

        # Create directories with invalid patterns
        invalid_dirs = [
            self.temp_dir / "not_year" / "12" / "01",  # Non-numeric year
            self.temp_dir / "2023" / "not_month" / "01",  # Non-numeric month
            self.temp_dir / "2023" / "12" / "not_day",  # Non-numeric day
            self.temp_dir / "too" / "few" / "levels",  # Not exactly 3 levels from root
            self.temp_dir / "2023",  # Single level
            self.temp_dir / "2023" / "12",  # Two levels
        ]

        for invalid_dir in invalid_dirs:
            invalid_dir.mkdir(parents=True, exist_ok=True)

        archiver.clean_empty_directories(self.temp_dir, logger=logger)

        # All invalid directories should still exist
        for invalid_dir in invalid_dirs:
            self.assertTrue(invalid_dir.exists())

    def test_clean_empty_directories_removal_failure(self):
        """Test cleanup when directory removal fails"""
        logger, log_stream = self.capture_logger()

        test_dir = self.temp_dir / "2023" / "12" / "01"
        test_dir.mkdir(parents=True)

        # Mock rmdir to fail
        with patch.object(Path, "rmdir", side_effect=OSError("Permission denied")):
            archiver.clean_empty_directories(self.temp_dir, logger=logger)
            self.assertIn("Failed to remove empty directory", log_stream.getvalue())

    def test_cleanup_archive_size_with_graceful_exit(self):
        """Test archive cleanup with graceful exit during process"""
        logger, _ = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create files that exceed limit
        for i in range(3):
            large_file = archive_dir / f"archived-{i:014d}.mp4"
            self.create_file(large_file, b"x" * (300 * 1024 * 1024))  # 300MB each

        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > 1  # Exit after processing one file

        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            archiver.cleanup_archive_size_limit(
                archive_dir, logger, max_size_gb=0.5, dry_run=False
            )

        # Should have stopped processing due to graceful exit
        remaining_files = list(archive_dir.glob("*.mp4"))
        self.assertTrue(len(remaining_files) > 0)


class TestRunArchiverIntegration(TestBase):
    """Integration tests for run_archiver function"""

    def test_run_archiver_with_custom_trash_directory(self):
        """Test run_archiver with custom trash directory"""
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        custom_trash = self.temp_dir / "custom_trash"

        # Create old file
        ts_old = datetime.now() - timedelta(days=35)
        mp4_file, _ = self.create_test_files(base_dir, ts_old)

        def mock_transcode(inp, outp, logger, progress_cb=None):
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(b"content")
            return True

        with patch("archiver.transcode_file", side_effect=mock_transcode):
            with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
                args = MagicMock()
                args.directory = base_dir
                args.output = None  # Use default
                args.age = 30
                args.dry_run = False
                args.max_size = 500
                args.no_skip = False
                args.use_trash = False
                args.trashdir = custom_trash

                result = archiver.run_archiver(args)

        self.assertEqual(result, 0)
        self.assertTrue(custom_trash.exists())

    def test_run_archiver_with_graceful_exit_states(self):
        """Test run_archiver behavior with graceful exit in different states"""
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

        # Test graceful exit during processing
        archiver.GracefulExit.request_exit()

        with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
            result = archiver.run_archiver(args)

        self.assertEqual(result, 1)  # Should return error code for cancelled process

        # Reset for next test
        archiver.GracefulExit.exit_requested = False

        # Test successful completion
        with patch("archiver.scan_files", return_value=([], {})):
            with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
                result = archiver.run_archiver(args)

        self.assertEqual(result, 0)  # Should return success code


class TestFileScanningEdgeCases(TestBase):
    """Tests for edge cases in file scanning"""

    def test_scan_files_with_graceful_exit_during_scan(self):
        """Test file scanning interrupted by graceful exit"""
        base_dir = self.temp_dir / "src"
        base_dir.mkdir()

        # Create many files to scan
        for i in range(10):
            ts = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d%H%M%S")
            file_path = base_dir / f"REO_cam_{ts}.mp4"
            self.create_file(file_path)

        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > 5  # Exit partway through

        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            mp4s, mapping = archiver.scan_files(base_dir)

        # Should have stopped scanning early
        self.assertTrue(len(mp4s) < 10)
        self.assertTrue(len(mapping) < 10)

    def test_process_files_skip_trashed_files(self):
        """Test that process_files skips files in trash directory"""
        logger, log_stream = self.capture_logger()

        trash_root = self.temp_dir / ".trash"
        trash_root.mkdir(parents=True)

        # Create file in trash
        ts = datetime.now() - timedelta(days=35)
        trashed_file = trash_root / f"REO_cam_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        self.create_file(trashed_file)

        out_dir = self.temp_dir / "archived"

        processed = archiver.process_files(
            old_list=[(trashed_file, ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=False,
            no_skip=False,
            mapping={},
            bar=archiver.ProgressBar(1, silent=True),
            trash_root=trash_root,
        )

        self.assertIn("[SKIP] Trashed file", log_stream.getvalue())
        self.assertEqual(len(processed), 0)


class TestMainFunctionEdgeCases(TestBase):
    """Tests for main function edge cases"""

    def test_main_with_graceful_exit_exception(self):
        """Test main function exception handling when graceful exit is requested"""
        archiver.GracefulExit.request_exit()

        with patch("archiver.run_archiver", side_effect=Exception("Test error")):
            with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
                with patch("archiver.GracefulExit.should_exit", return_value=True):
                    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit) as cm:
                            archiver.main()
                        self.assertEqual(cm.exception.code, 1)


class TestProgressBarTimeFormatting(TestBase):
    """Test progress bar time formatting edge cases"""

    def test_progress_bar_time_formatting_edge_cases(self):
        """Test progress bar handles various time scenarios"""
        stream = io.StringIO()
        stream.isatty = lambda: True

        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)

        # Test with no start time set
        line = bar._format_line(1, 50.0)
        self.assertIn("00:00", line)  # Should show zero elapsed time

        # Test with start time but no file start time
        bar.start_time = time.time() - 3661  # 1 hour, 1 minute, 1 second ago
        bar.file_start = None
        line = bar._format_line(1, 50.0)
        self.assertIn("01:01:01", line)  # Should show total elapsed time
        self.assertIn("00:00", line)  # Should show zero file time

        bar.finish()


class TestIntegrationScenarios(TestBase):
    """Integration tests for complete workflows"""

    def test_complete_archive_workflow_with_trash(self):
        # Setup directories
        input_dir = self.temp_dir / "camera"
        archived_dir = input_dir / "archived"
        trash_dir = input_dir / ".deleted"

        input_dir.mkdir()
        archived_dir.mkdir()

        # Create old files for processing
        old_ts = datetime.now() - timedelta(days=35)
        mp4_file, jpg_file = self.create_test_files(input_dir, old_ts)

        # Create recent files that should be ignored
        recent_ts = datetime.now() - timedelta(days=15)
        self.create_test_files(input_dir, recent_ts)

        # Create orphaned JPG
        orphan_jpg = input_dir / "REO_cam_20231201000000.jpg"
        self.create_file(orphan_jpg)

        # Mock successful transcoding
        def mock_transcode(inp, outp, logger, progress_cb=None):
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(b"transcoded content")
            if progress_cb:
                progress_cb(100.0)
            return True

        # Run the complete archiver
        with patch("archiver.transcode_file", side_effect=mock_transcode):
            with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
                args = MagicMock()
                args.directory = input_dir
                args.output = archived_dir
                args.age = 30
                args.dry_run = False
                args.max_size = 500
                args.no_skip = False
                args.use_trash = True
                args.trashdir = None

                archiver.run_archiver(args)

        # Verify results
        archived_file = archiver.output_path(mp4_file, old_ts, archived_dir)
        self.assertTrue(archived_file.exists())

        # Verify files were moved to trash
        trash_mp4 = trash_dir / "input" / mp4_file.relative_to(input_dir)
        trash_jpg = trash_dir / "input" / jpg_file.relative_to(input_dir)
        self.assertTrue(trash_mp4.exists())
        self.assertTrue(trash_jpg.exists())

        # Verify orphaned JPG was removed
        self.assertFalse(orphan_jpg.exists())

    def test_run_archiver_error_conditions(self):
        # Test missing directory by ensuring both the specified directory and /camera don't exist
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

        # The run_archiver logic is:
        # base_dir = args.directory if args.directory.exists() else Path("/camera")
        # if not base_dir.exists(): return 1

        # We need to ensure that when Path("/camera").exists() is called, it returns False
        original_exists = Path.exists

        def mock_exists(self):
            if str(self) == "/camera":
                return False
            return original_exists(self)

        with patch.object(Path, "exists", mock_exists):
            with redirect_stdout(io.StringIO()) as stdout:
                result = archiver.run_archiver(args)

        # Should return 1 since both directories don't exist
        self.assertEqual(result, 1)
        output = stdout.getvalue()
        self.assertIn("Error: Directory", output)

        # Test successful execution with directory creation
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

        args.directory = base_dir
        args.output = out_dir
        args.age = 1

        with patch("archiver.setup_logging", return_value=MagicMock()):
            with patch("archiver.transcode_file", side_effect=fake_transcode):
                result = archiver.run_archiver(args)

        # Should return 0 for success
        self.assertEqual(result, 0)
        self.assertTrue(out_dir.exists())
        self.assertTrue(any(out_dir.rglob("archived-*.mp4")))


class TestMainAndCLI(TestBase):
    """Tests for main function and command-line interface"""

    def test_main_functionality(self):
        # Reset graceful exit state to avoid interference from other tests
        archiver.GracefulExit.exit_requested = False

        # Test help display
        with patch("sys.argv", ["archiver.py", "--help"]):
            with (
                redirect_stdout(io.StringIO()) as stdout,
                redirect_stderr(io.StringIO()),
            ):
                with self.assertRaises(SystemExit):
                    archiver.main()
                output = stdout.getvalue()
                self.assertIn("usage: archiver.py", output)

        # Test normal execution with arguments - should call run_archiver and exit with code 0
        with patch("archiver.run_archiver") as mock_run:
            # Mock run_archiver to return 0 (success)
            mock_run.return_value = 0
            with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as cm:
                        archiver.main()
                self.assertEqual(cm.exception.code, 0)  # Should exit with code 0
                mock_run.assert_called_once()

        # Test keyboard interrupt
        with patch("archiver.run_archiver", side_effect=KeyboardInterrupt):
            with patch(
                "sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]
            ):  # Provide valid args
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as cm:
                        archiver.main()
                    self.assertEqual(
                        cm.exception.code, 1
                    )  # Keyboard interrupt should exit with code 1

        # Test other exceptions
        with patch("archiver.run_archiver", side_effect=Exception("Test error")):
            with patch(
                "sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]
            ):  # Provide valid args
                with patch("logging.getLogger") as mock_logger:
                    # Ensure GracefulExit.should_exit() returns False so exception gets re-raised
                    with patch("archiver.GracefulExit.should_exit", return_value=False):
                        mock_logger.return_value.error = MagicMock()
                        with (
                            redirect_stdout(io.StringIO()),
                            redirect_stderr(io.StringIO()),
                        ):
                            # Regular exceptions should not cause SystemExit, they should be raised
                            with self.assertRaises(Exception) as cm:
                                archiver.main()
                        self.assertEqual(str(cm.exception), "Test error")
                        mock_logger.return_value.error.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
