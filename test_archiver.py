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
        self.assertEqual(result, ([], {}, set()))

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

        # Create files that should be ignored - move to a directory that won't be scanned
        # Use a different base directory for deleted files
        del_base = self.temp_dir / "deleted_src"
        del_dir = del_base / ".deleted"
        del_dir.mkdir(parents=True)
        del_file = del_dir / "REO_cam_20231201000000.mp4"
        self.create_file(del_file)

        non_media = base / "other_file.txt"
        self.create_file(non_media)

        invalid_ts = base / "REO_cam_18991231235959.mp4"  # Too old
        self.create_file(invalid_ts)

        # Only scan the base directory, not the deleted one
        mp4s, mapping, trash_files = archiver.scan_files(base)

        # Should only find files in base directory, not in deleted_src
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

        # Test dry run - use a file that's within age threshold to avoid removal
        ts_recent_enough = datetime.now() - timedelta(
            days=25
        )  # Within 30-day threshold
        recent_mp4, recent_jpg = self.create_test_files(self.temp_dir, ts_recent_enough)
        mapping[ts_recent_enough.strftime("%Y%m%d%H%M%S")] = {
            ".mp4": recent_mp4,
            ".jpg": recent_jpg,
        }

        with patch("archiver.transcode_file", return_value=True):
            _ = archiver.process_files_intelligent(
                [
                    (recent_mp4, ts_recent_enough)
                ],  # Use recent file that won't be removed
                out_dir,
                logger,
                dry_run=True,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
                max_size_gb=500,  # Add required parameters
                age_days=30,
            )

        self.assertIn("[DRY RUN] Would transcode", log_stream.getvalue())
        self.assertTrue(recent_mp4.exists())  # Not actually removed

        # Test skip logic when output exists and is large enough
        # Use the recent file that won't be removed by age threshold
        actual_out_path = archiver.output_path(recent_mp4, ts_recent_enough, out_dir)
        actual_out_path.parent.mkdir(parents=True, exist_ok=True)
        self.create_file(actual_out_path, b"x" * (2 * 1024 * 1024))  # 2MB file

        with patch("archiver.transcode_file") as mock_transcode:
            with patch("archiver.safe_remove") as mock_remove:
                archiver.process_files_intelligent(
                    [(recent_mp4, ts_recent_enough)],  # Use recent file
                    out_dir,
                    logger,
                    dry_run=False,
                    no_skip=False,
                    mapping=mapping,
                    bar=archiver.ProgressBar(1, silent=True),
                    max_size_gb=500,  # Add required parameters
                    age_days=30,
                )

                mock_transcode.assert_not_called()
                self.assertEqual(mock_remove.call_count, 2)  # MP4 and JPG
                self.assertIn(
                    "[SKIP] Existing archive large enough", log_stream.getvalue()
                )

        # Test transcoding failure handling
        with patch("archiver.transcode_file", return_value=False):
            archiver.process_files_intelligent(
                [(recent_mp4, ts_recent)],  # Use recent file that hasn't been processed
                out_dir,
                logger,
                dry_run=False,
                no_skip=True,  # Force processing
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
                max_size_gb=500,  # Add required parameters
                age_days=30,
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
        self.assertIn(
            "Current archive size (including trash): 0.1 GB", log_stream.getvalue()
        )

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


class TestTrashInclusion(TestBase):
    """Tests for trash file inclusion in size and age thresholds"""

    def test_scan_files_includes_trash_when_enabled(self):
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        # Use a trash root that's separate from base_dir to avoid double-scanning
        trash_root = self.temp_dir / ".deleted"

        # Create files in base directory
        ts = datetime.now() - timedelta(days=40)
        base_mp4, base_jpg = self.create_test_files(base_dir, ts)

        # Create files in trash
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)
        trash_mp4, trash_jpg = self.create_test_files(trash_input, ts)

        # Test without trash inclusion - should only find base files
        mp4s, mapping, trash_files = archiver.scan_files(
            base_dir, include_trash=False, trash_root=trash_root
        )
        self.assertEqual(len(mp4s), 1)  # Only base file
        self.assertEqual(len(mapping), 1)  # Only base file mapping
        self.assertEqual(len(trash_files), 0)  # No trash files

        # Test with trash inclusion - should find both base and trash files
        mp4s, mapping, trash_files = archiver.scan_files(
            base_dir, include_trash=True, trash_root=trash_root
        )
        # With trash included, we should find both files
        self.assertEqual(len(mp4s), 2)  # Both base and trash MP4s
        self.assertEqual(len(mapping), 1)  # Same timestamp key
        self.assertEqual(len(trash_files), 2)  # Both MP4 and JPG in trash
        self.assertIn(trash_mp4, trash_files)
        self.assertIn(trash_jpg, trash_files)

    def test_cleanup_archive_size_limit_includes_trash(self):
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output" / "2023" / "12" / "01"
        trash_output.mkdir(parents=True)

        # Create archive files
        archive_file = archive_dir / "archived-20231201000000.mp4"
        self.create_file(archive_file, b"x" * (400 * 1024 * 1024))  # 400MB

        # Create trash archive files
        trash_file = trash_output / "archived-20231201000001.mp4"
        self.create_file(trash_file, b"x" * (300 * 1024 * 1024))  # 300MB

        # Test without trash inclusion
        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=0.5, dry_run=False, use_trash=False
        )
        # Only archive file should be considered (400MB < 500MB limit)
        self.assertTrue(archive_file.exists())
        self.assertTrue(trash_file.exists())

        # Test with trash inclusion (total 700MB > 500MB limit)
        archiver.cleanup_archive_size_limit(
            archive_dir,
            logger,
            max_size_gb=0.5,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
        )

        # Should remove files until under limit
        self.assertIn(
            "Including 1 trash files in size calculation", log_stream.getvalue()
        )
        self.assertIn("Archive size exceeds limit", log_stream.getvalue())

    def test_process_files_skips_trash_files(self):
        """Test that process_files skips files in trash directory"""
        logger, log_stream = self.capture_logger()

        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        trash_root = base_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)

        # Create trash file
        ts = datetime.now() - timedelta(days=40)
        trash_mp4, _ = self.create_test_files(trash_input, ts)

        # Create mapping and trash_files set
        mapping = {
            ts.strftime("%Y%m%d%H%M%S"): {
                ".mp4": trash_mp4,
            }
        }
        trash_files = {trash_mp4}

        out_dir = self.temp_dir / "archived"
        with patch("archiver.transcode_file") as mock_transcode:
            processed = archiver.process_files_intelligent(
                old_list=[(trash_mp4, ts)],
                out_dir=out_dir,
                logger=logger,
                dry_run=False,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
                trash_files=trash_files,
                use_trash=True,
                trash_root=trash_root,
                source_root=base_dir,
                max_size_gb=500,
                age_days=30,
            )

        mock_transcode.assert_not_called()
        # Look for actual log message
        self.assertIn("Permanently removed:", log_stream.getvalue())
        self.assertEqual(len(processed), 0)

    def test_run_archiver_with_trash_inclusion(self):
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        trash_root = base_dir / ".deleted"

        # Create old files in base and trash
        ts_old = datetime.now() - timedelta(days=40)
        base_mp4, _ = self.create_test_files(base_dir, ts_old)
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)
        trash_mp4, _ = self.create_test_files(trash_input, ts_old)

        def mock_transcode(inp, outp, logger, progress_cb=None):
            outp.parent.mkdir(parents=True, exist_ok=True)
            outp.write_bytes(b"transcoded")
            return True

        def mock_scan_files(base_dir, include_trash=False, trash_root=None):
            mp4s = [(base_mp4, ts_old)]
            mapping = {ts_old.strftime("%Y%m%d%H%M%S"): {".mp4": base_mp4}}
            trash_files = set()

            if include_trash and trash_root:
                mp4s.append((trash_mp4, ts_old))
                mapping[ts_old.strftime("%Y%m%d%H%M%S")][".mp4"] = (
                    trash_mp4  # Override or add separate entry
                )
                trash_files.add(trash_mp4)
            return mp4s, mapping, trash_files

        # Mock the intelligent cleanup functions to avoid complex setup
        with patch("archiver.intelligent_cleanup", return_value=(set(), [])):
            with patch("archiver.scan_files", side_effect=mock_scan_files):
                with patch("archiver.transcode_file", side_effect=mock_transcode):
                    with patch(
                        "archiver.setup_logging", return_value=self.capture_logger()[0]
                    ):
                        args = MagicMock()
                        args.directory = base_dir
                        args.output = base_dir / "archived"
                        args.age = 30
                        args.dry_run = False
                        args.max_size = 500
                        args.no_skip = False
                        args.use_trash = True
                        args.trashdir = None

                        result = archiver.run_archiver(args)

        self.assertEqual(result, 0)

    def test_cleanup_trash_files_permanently_when_in_trash(self):
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output" / "2023" / "12" / "01"
        trash_output.mkdir(parents=True)

        # Create trash archive file
        trash_file = trash_output / "archived-20231201000000.mp4"
        self.create_file(trash_file, b"x" * (600 * 1024 * 1024))  # 600MB

        archiver.cleanup_archive_size_limit(
            archive_dir,
            logger,
            max_size_gb=0.5,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
        )

        self.assertIn("Permanently removed from trash", log_stream.getvalue())
        self.assertFalse(trash_file.exists())


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


class TestIntelligentCleanupLogic(TestBase):
    """Tests for the new intelligent cleanup functionality"""

    def test_intelligent_cleanup_size_priority_over_age(self):
        """Test that size threshold takes priority over age threshold"""
        logger, log_stream = self.capture_logger()

        # Create archive directory with existing files
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create large archive files that exceed size limit
        large_file1 = archive_dir / "archived-20230101000000.mp4"
        large_file2 = archive_dir / "archived-20230102000000.mp4"
        self.create_file(large_file1, b"x" * (300 * 1024 * 1024))  # 300MB
        self.create_file(large_file2, b"x" * (300 * 1024 * 1024))  # 300MB

        # Create old source files (should normally be removed by age, but size takes priority)
        old_ts1 = datetime.now() - timedelta(days=40)
        old_ts2 = datetime.now() - timedelta(days=35)
        old_mp4_1, _ = self.create_test_files(self.temp_dir, old_ts1)
        old_mp4_2, _ = self.create_test_files(self.temp_dir, old_ts2)

        old_list = [(old_mp4_1, old_ts1), (old_mp4_2, old_ts2)]

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            old_list,
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=0.5,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        # Should prioritize size over age - remove files to get under 500MB limit
        self.assertGreater(len(files_to_remove), 0)
        self.assertIn("Size threshold exceeded", log_stream.getvalue())

    def test_intelligent_cleanup_age_removal_with_size_buffer(self):
        """Test age-based removal stops when it would make archive too small"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create small archive that's well under size limit
        small_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(small_file, b"x" * (50 * 1024 * 1024))  # 50MB

        # Create many old files that exceed age threshold
        old_list = []
        for i in range(10):
            old_ts = datetime.now() - timedelta(days=35 + i)
            old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
            old_list.append((old_mp4, old_ts))

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            old_list,
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=10,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        # Should stop age-based removal to maintain reasonable archive size
        self.assertIn(
            "Stopping age-based removal to maintain reasonable archive size",
            log_stream.getvalue(),
        )

    def test_intelligent_cleanup_no_files_over_age(self):
        """Test when no files exceed age threshold"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create recent files (within age threshold)
        recent_list = []
        for i in range(3):
            recent_ts = datetime.now() - timedelta(days=i + 1)
            recent_mp4, _ = self.create_test_files(self.temp_dir, recent_ts)
            recent_list.append((recent_mp4, recent_ts))

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            recent_list,
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=10,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        self.assertEqual(len(files_to_remove), 0)
        self.assertIn("No files older than 30 days found", log_stream.getvalue())

    def test_intelligent_cleanup_with_trash_files(self):
        """Test intelligent cleanup includes trash files in size calculations"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output"
        trash_output.mkdir(parents=True)

        # Create archive file
        archive_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(archive_file, b"x" * (200 * 1024 * 1024))  # 200MB

        # Create trash archive file
        trash_file = trash_output / "archived-20230102000000.mp4"
        self.create_file(trash_file, b"x" * (400 * 1024 * 1024))  # 400MB

        # Create source files
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            old_list,
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=0.5,
            age_days=30,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
        )

        # Should include trash files in size calculation (600MB total > 500MB limit)
        self.assertGreater(len(files_to_remove), 0)
        self.assertIn("Size threshold exceeded", log_stream.getvalue())

    def test_intelligent_cleanup_malformed_archive_filenames(self):
        """Test intelligent cleanup handles malformed archive filenames gracefully"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create files with malformed archive names (should be ignored)
        bad_file1 = archive_dir / "archived-invalid.mp4"
        bad_file2 = archive_dir / "archived-2023010100000.mp4"  # Wrong length
        bad_file3 = archive_dir / "not-archived-20230101000000.mp4"  # Wrong prefix
        self.create_file(bad_file1, b"x" * (100 * 1024 * 1024))
        self.create_file(bad_file2, b"x" * (100 * 1024 * 1024))
        self.create_file(bad_file3, b"x" * (100 * 1024 * 1024))

        # Create source files
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            old_list,
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=0.5,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        # Should handle malformed filenames without crashing
        # Size should be much smaller since malformed files are ignored
        log_output = log_stream.getvalue()
        self.assertIn("Current total size: 0.0 GB", log_output)

    def test_process_files_intelligent_trash_permanent_removal(self):
        """Test that trash files are permanently removed when they exceed thresholds"""
        logger, log_stream = self.capture_logger()

        trash_root = self.temp_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)

        # Create old trash file
        old_ts = datetime.now() - timedelta(days=40)
        trash_mp4, trash_jpg = self.create_test_files(trash_input, old_ts)

        mapping = {
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": trash_mp4, ".jpg": trash_jpg}
        }
        trash_files = {trash_mp4, trash_jpg}

        out_dir = self.temp_dir / "archived"

        processed = archiver.process_files_intelligent(
            old_list=[(trash_mp4, old_ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=False,
            no_skip=False,
            mapping=mapping,
            bar=archiver.ProgressBar(1, silent=True),
            trash_files=trash_files,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
            max_size_gb=500,
            age_days=30,
        )

        # Files should be permanently removed
        self.assertFalse(trash_mp4.exists())
        self.assertFalse(trash_jpg.exists())
        # Look for actual log messages from safe_remove/unlink
        self.assertIn("Permanently removed:", log_stream.getvalue())
        self.assertIn("Permanently removed paired trash JPG:", log_stream.getvalue())

    def test_process_files_intelligent_regular_file_removal_by_threshold(self):
        """Test that regular files are moved to trash when they exceed thresholds"""
        logger, log_stream = self.capture_logger()

        base_dir = self.temp_dir / "camera"
        base_dir.mkdir()
        trash_root = base_dir / ".deleted"

        # Create old regular file (not in trash)
        old_ts = datetime.now() - timedelta(days=40)
        old_mp4, old_jpg = self.create_test_files(base_dir, old_ts)

        mapping = {old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": old_mp4, ".jpg": old_jpg}}

        out_dir = self.temp_dir / "archived"

        processed = archiver.process_files_intelligent(
            old_list=[(old_mp4, old_ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=False,
            no_skip=False,
            mapping=mapping,
            bar=archiver.ProgressBar(1, silent=True),
            trash_files=set(),
            use_trash=True,
            trash_root=trash_root,
            source_root=base_dir,
            max_size_gb=500,
            age_days=30,
        )

        # Files should be moved to trash
        self.assertFalse(old_mp4.exists())
        self.assertFalse(old_jpg.exists())
        self.assertIn("Removing file (threshold)", log_stream.getvalue())

        # Check they're in trash
        trash_input = trash_root / "input"
        self.assertTrue(any(trash_input.rglob("*REO_cam*")))

    def test_process_files_intelligent_dry_run_with_threshold_removal(self):
        """Test dry run mode with threshold-based removal"""
        logger, log_stream = self.capture_logger()

        # Create old file that should be removed by age threshold
        old_ts = datetime.now() - timedelta(days=40)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)

        mapping = {old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": old_mp4}}
        out_dir = self.temp_dir / "archived"

        processed = archiver.process_files_intelligent(
            old_list=[(old_mp4, old_ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=True,  # DRY RUN
            no_skip=False,
            mapping=mapping,
            bar=archiver.ProgressBar(1, silent=True),
            trash_files=set(),
            use_trash=True,
            trash_root=self.temp_dir / ".deleted",
            source_root=self.temp_dir,
            max_size_gb=500,
            age_days=30,
        )

        # File should still exist in dry run
        self.assertTrue(old_mp4.exists())
        # Look for actual dry run message from safe_remove
        self.assertIn("[DRY RUN] Would remove", log_stream.getvalue())

    def test_process_files_intelligent_trash_file_kept_within_thresholds(self):
        """Test that trash files within thresholds are kept"""
        logger, log_stream = self.capture_logger()

        trash_root = self.temp_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)

        # Create recent trash file (within age threshold)
        recent_ts = datetime.now() - timedelta(days=15)
        trash_mp4, _ = self.create_test_files(trash_input, recent_ts)

        mapping = {recent_ts.strftime("%Y%m%d%H%M%S"): {".mp4": trash_mp4}}
        trash_files = {trash_mp4}

        out_dir = self.temp_dir / "archived"

        processed = archiver.process_files_intelligent(
            old_list=[(trash_mp4, recent_ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=False,
            no_skip=False,
            mapping=mapping,
            bar=archiver.ProgressBar(1, silent=True),
            trash_files=trash_files,
            use_trash=True,
            trash_root=trash_root,
            source_root=self.temp_dir,
            max_size_gb=500,
            age_days=30,
        )

        # File should still exist (within thresholds)
        self.assertTrue(trash_mp4.exists())
        # Since file is within thresholds, it won't be in removal plan
        # Just check it wasn't removed
        self.assertIn("No files older than 30 days found", log_stream.getvalue())

    def test_intelligent_cleanup_empty_file_list(self):
        """Test intelligent cleanup with empty file list"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            [],
            {},
            archive_dir,
            logger,
            dry_run=False,
            max_size_gb=500,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        self.assertEqual(len(files_to_remove), 0)
        self.assertEqual(len(processed_files), 0)

    def test_intelligent_cleanup_nonexistent_archive_dir(self):
        """Test intelligent cleanup when archive directory doesn't exist"""
        logger, log_stream = self.capture_logger()

        nonexistent_dir = self.temp_dir / "nonexistent_archived"

        # Create source files
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        processed_files, files_to_remove = archiver.intelligent_cleanup(
            old_list,
            {},
            nonexistent_dir,
            logger,
            dry_run=False,
            max_size_gb=500,
            age_days=30,
            use_trash=False,
            trash_root=None,
            source_root=self.temp_dir,
        )

        # Should handle gracefully - only source files considered
        self.assertIn("Found 1 files older than 30 days", log_stream.getvalue())


class TestErrorHandlingAndEdgeCases(TestBase):
    """Tests for error handling in the new intelligent removal logic"""

    def test_process_files_intelligent_file_removal_error(self):
        """Test error handling when file removal fails"""
        logger, log_stream = self.capture_logger()

        trash_root = self.temp_dir / ".deleted"
        trash_input = trash_root / "input"
        trash_input.mkdir(parents=True)

        # Create trash file
        old_ts = datetime.now() - timedelta(days=40)
        trash_mp4, _ = self.create_test_files(trash_input, old_ts)

        mapping = {old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": trash_mp4}}
        trash_files = {trash_mp4}

        out_dir = self.temp_dir / "archived"

        # Mock unlink to raise an exception
        with patch.object(Path, "unlink", side_effect=OSError("Permission denied")):
            processed = archiver.process_files_intelligent(
                old_list=[(trash_mp4, old_ts)],
                out_dir=out_dir,
                logger=logger,
                dry_run=False,
                no_skip=False,
                mapping=mapping,
                bar=archiver.ProgressBar(1, silent=True),
                trash_files=trash_files,
                use_trash=True,
                trash_root=trash_root,
                source_root=self.temp_dir,
                max_size_gb=500,
                age_days=30,
            )

        # Look for the actual error message from safe_remove
        self.assertIn("Failed to remove", log_stream.getvalue())

    def test_intelligent_cleanup_size_calculation_with_broken_files(self):
        """Test size calculation when some files can't be accessed"""
        logger, log_stream = self.capture_logger()

        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()

        # Create a file that will be "broken" (inaccessible)
        broken_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(broken_file, b"x" * 1000)

        # Create a working file for comparison
        working_file = archive_dir / "archived-20230102000000.mp4"
        self.create_file(working_file, b"x" * 2000)

        # Create source files
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        # Store the original methods
        original_is_file = Path.is_file
        original_stat = Path.stat

        def mock_is_file(self):
            # Always return True for our test files
            if self in [broken_file, working_file, old_mp4]:
                return True
            return original_is_file(self)

        def mock_stat(self, follow_symlinks=True):
            # Only raise error for the specific broken file
            if self == broken_file:
                raise OSError("File not accessible")
            # For all other files, use the original stat method
            return original_stat(self, follow_symlinks=follow_symlinks)

        with patch.object(Path, "is_file", mock_is_file):
            with patch.object(Path, "stat", mock_stat):
                processed_files, files_to_remove = archiver.intelligent_cleanup(
                    old_list,
                    {},
                    archive_dir,
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

        # Mock scan_files to return 3 values instead of 2
        with patch("archiver.scan_files", return_value=([], {}, set())):  # ← Fix here
            with patch("archiver.setup_logging", return_value=self.capture_logger()[0]):
                result = archiver.run_archiver(args)

        self.assertEqual(result, 1)  # Should return error code for cancelled process

        # Reset for next test
        archiver.GracefulExit.exit_requested = False

        # Test successful completion - also fix this mock
        with patch("archiver.scan_files", return_value=([], {}, set())):  # ← Fix here
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
            mp4s, mapping, trash_files = archiver.scan_files(base_dir)

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

        processed = archiver.process_files_intelligent(
            old_list=[(trashed_file, ts)],
            out_dir=out_dir,
            logger=logger,
            dry_run=False,
            no_skip=False,
            mapping={},
            bar=archiver.ProgressBar(1, silent=True),
            trash_root=trash_root,
            max_size_gb=500,
            age_days=30,
        )

        # Look for "Permanently removed" instead of specific trash message
        self.assertIn("Permanently removed:", log_stream.getvalue())
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

        # Mock the intelligent cleanup to avoid complex archive size calculations
        def mock_intelligent_cleanup(*args, **kwargs):
            return set(), []  # No files marked for removal by size/age logic

        # Run the complete archiver
        with patch("archiver.transcode_file", side_effect=mock_transcode):
            with patch(
                "archiver.intelligent_cleanup", side_effect=mock_intelligent_cleanup
            ):
                with patch(
                    "archiver.setup_logging", return_value=self.capture_logger()[0]
                ):
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

        # Mock intelligent cleanup to avoid complex setup
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
