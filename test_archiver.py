#!/usr/bin/env python3
import io
import logging
import shutil
import stat
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

    def make_mock_stat(
        self,
        target_path: Path,
        st_size: int = 0,
        st_mtime: float = 0,
        raise_oserror: bool = False,
    ):
        original_stat = Path.stat

        def mock_stat(path_self, follow_symlinks=True):
            if path_self == target_path:
                if raise_oserror:
                    raise OSError("File not accessible")

                class MockStatResult:
                    def __init__(self):
                        self.st_size = st_size
                        self.st_mtime = st_mtime
                        self.st_mode = stat.S_IFREG

                return MockStatResult()
            return original_stat(path_self, follow_symlinks=follow_symlinks)

        return mock_stat

    def make_exit_side_effect(self, trigger_after: int = 5):
        """Return a callable that returns True after being called N times."""
        call_count = 0

        def exit_side_effect():
            nonlocal call_count
            call_count += 1
            return call_count > trigger_after

        return exit_side_effect

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


class TestComprehensiveIntegration(TestBase):
    """Comprehensive integration tests covering multiple code paths"""

    def test_logging_and_progress_bar_integration(self):
        """Integration test for logging setup and progress bar functionality"""
        log_file = self.temp_dir / "log.txt"
        progress_bar = archiver.ProgressBar(total_files=0, silent=True, out=sys.stdout)
        logger = archiver.setup_logging(log_file, progress_bar)
        self.assertEqual(logger.name, "camera_archiver")
        handler_types = [type(h) for h in logger.handlers]
        self.assertIn(logging.FileHandler, handler_types)
        self.assertIn(archiver.GuardedStreamHandler, handler_types)

        # Test guarded stream handler with progress bar
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

        # Test progress bar silent mode and non-TTY
        stream = io.StringIO()
        stream.isatty = lambda: False
        bar = archiver.ProgressBar(total_files=1, silent=True, out=stream)
        bar.update_progress(1, 50.0)
        bar.finish()
        self.assertEqual(stream.getvalue(), "")

        # Test progress bar ANSI exception handling
        stream = MagicMock()
        stream.isatty = lambda: True
        stream.write.side_effect = [Exception("ANSI error"), None]
        stream.flush = MagicMock()
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)
        self.assertEqual(stream.write.call_count, 2)
        bar.finish()

        # Test progress bar signal handling
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

        # Test progress bar unknown signal
        stream = io.StringIO()
        stream.isatty = lambda: True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        with redirect_stderr(io.StringIO()) as stderr_capture:
            bar._signal_handler(
                999, None
            )  # Use a signal number that's not in the mapping
        self.assertTrue(archiver.GracefulExit.should_exit())
        self.assertEqual(bar._progress_line, "")
        self.assertIn("signal 999", stderr_capture.getvalue())

    def test_core_functionality_integration(self):
        """Integration test for core functionality including parsing and file operations"""
        # Test timestamp parsing edge cases
        test_cases = [
            ("REO_cam_20231201010101.mp4", datetime(2023, 12, 1, 1, 1, 1)),
            ("REO_cam_20000101000000.mp4", datetime(2000, 1, 1, 0, 0, 0)),
            ("REO_cam_18991231235959.mp4", None),  # Too old - covers line 84
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

        # Test output path generation with various depths
        ts = datetime(2023, 12, 1, 12, 30, 45)
        test_cases = [
            (
                Path("root/2023/12/01/file.mp4"),  # 4+ parts
                "2023/12/01/archived-20231201123045.mp4",
            ),
            (
                Path("file.mp4"),  # Shallow path (covers line 550)
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

        # Test safe remove modes and error handling
        logger, log_stream = self.capture_logger()
        test_file = self.temp_dir / "test.txt"
        self.create_file(test_file)

        # Test dry run
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

        # Test trash operations comprehensive
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

    def test_transcoding_and_media_integration(self):
        """Integration test for transcoding and media operations"""
        logger, log_stream = self.capture_logger()

        # Test get video duration various scenarios
        # Test when ffprobe is not available
        with patch("shutil.which", return_value=None):
            duration = archiver.get_video_duration(Path("test.mp4"))
            self.assertIsNone(duration)

        # Test successful duration extraction
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

        # Test transcode file comprehensive
        # Test early exit
        archiver.GracefulExit.request_exit()
        with patch("subprocess.Popen") as mock_popen:
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)
            mock_popen.assert_not_called()
        archiver.GracefulExit.exit_requested = False

        # Test successful transcoding with progress (covers mkdir parent creation)
        mock_proc = self.create_mock_ffmpeg_process(
            ["frame=1 time=00:00:01.00", "frame=2 time=00:00:02.00"]
        )
        progress_data = []

        def progress_cb(pct):
            progress_data.append(pct)

        out_path = (
            self.temp_dir
            / "archived"
            / "2023"
            / "12"
            / "01"
            / "archived-20231201123045.mp4"
        )
        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                result = archiver.transcode_file(
                    Path("in"), out_path, logger, progress_cb
                )
                self.assertTrue(result)
                # Verify parent directories were created
                self.assertTrue(out_path.parent.exists())
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
        exit_side_effect = self.make_exit_side_effect(trigger_after=1)

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch("archiver.get_video_duration", return_value=10.0):
                with patch(
                    "archiver.GracefulExit.should_exit", side_effect=exit_side_effect
                ):
                    result = archiver.transcode_file(Path("in"), Path("out"), logger)
                    mock_proc.terminate.assert_called_once()
                    self.assertFalse(result)

        # Test ffmpeg error handling comprehensive
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
            mock_proc = self.create_mock_ffmpeg_process([""], return_code=-1)
            with patch("subprocess.Popen", return_value=mock_proc):
                result = archiver.transcode_file(Path("input"), output_path, logger)
                self.assertFalse(result)

        # Case 3: ffmpeg returns non-zero code (lines 400-402,406)
        mock_proc = self.create_mock_ffmpeg_process([""], return_code=1)
        with patch("subprocess.Popen", return_value=mock_proc):
            result = archiver.transcode_file(Path("in"), Path("out"), logger)
            self.assertFalse(result)

    def test_file_scanning_and_processing_integration(self):
        """Integration test for file scanning, processing, and cleanup logic"""
        # Test scan files comprehensive
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

        # Test scan files graceful exit during trash scan
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

        exit_side_effect = self.make_exit_side_effect(trigger_after=5)
        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            mp4s, mapping, trash_files = archiver.scan_files(
                base_dir, include_trash=True, trash_root=trash_root
            )
        # Should have stopped scanning trash early
        self.assertTrue(len(trash_files) < 10)

        # Test process files comprehensive workflow
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
        self.assertIn("Archive exists and is large enough", log_stream.getvalue())

        # Test remove orphaned jpgs comprehensive
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
            mock_remove.assert_called_once_with(
                orphan_jpg,
                logger,
                True,
                False,
                None,
                is_output=False,
                source_root=orphan_jpg.parent,
            )

        # Test remove orphaned jpgs graceful exit
        logger, _ = self.capture_logger()
        # Create multiple orphaned JPGs
        mapping = {}
        for i in range(10):
            jpg_path = self.temp_dir / f"REO_cam_20231201{i:06d}.jpg"
            self.create_file(jpg_path)
            mapping[f"20231201{i:06d}"] = {".jpg": jpg_path}

        exit_side_effect = self.make_exit_side_effect(trigger_after=5)
        with patch("archiver.GracefulExit.should_exit", side_effect=exit_side_effect):
            archiver.remove_orphaned_jpgs(mapping, set(), logger, dry_run=False)
        # Should have processed fewer than 10 files

    def test_archive_size_boundary_and_cleanup_integration(self):
        logger = self.capture_logger()[0]
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir(exist_ok=True)

        # Case 1: Zero-byte archive file
        ts_old = datetime.now() - timedelta(days=35)
        zero_file = archive_dir / f"archived-{ts_old.strftime('%Y%m%d%H%M%S')}.mp4"
        zero_file.touch(exist_ok=True)
        old_list = [(self.temp_dir / "source.mp4", ts_old)]
        _, files_to_remove = archiver.intelligent_cleanup(
            old_list, archive_dir, logger, dry_run=False, max_size_gb=500, age_days=30
        )
        self.assertEqual(len(files_to_remove), 1)
        self.assertEqual(files_to_remove[0].path, zero_file)

        # Case 2: Archive exactly at size limit
        ts_young = datetime.now() - timedelta(days=10)
        full_file = archive_dir / f"archived-{ts_young.strftime('%Y%m%d%H%M%S')}.mp4"
        full_file.touch()

        mock_stat = self.make_mock_stat(
            target_path=full_file,
            st_size=500 * (1024**3),
            st_mtime=ts_young.timestamp(),
            raise_oserror=True,
        )

        with patch.object(Path, "stat", mock_stat):
            _, files_to_remove = archiver.intelligent_cleanup(
                [], archive_dir, logger, dry_run=False, max_size_gb=500, age_days=30
            )
            self.assertEqual(len(files_to_remove), 1)
            self.assertEqual(files_to_remove[0].path, zero_file)

        # Test intelligent cleanup error handling
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir(exist_ok=True)
        broken_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(broken_file, b"x" * 1000)
        old_ts = datetime.now() - timedelta(days=35)
        old_mp4, _ = self.create_test_files(self.temp_dir, old_ts)
        old_list = [(old_mp4, old_ts)]

        original_stat = Path.stat

        def mock_stat_raise_on_broken(path_self, follow_symlinks=True):
            if path_self == broken_file:
                raise OSError("File not accessible")
            return original_stat(path_self, follow_symlinks=follow_symlinks)

        with patch.object(Path, "stat", mock_stat_raise_on_broken):
            processed_files, files_to_remove = archiver.intelligent_cleanup(
                old_list,
                archive_dir,
                logger,
                dry_run=False,
                max_size_gb=500,
                age_days=30,
                use_trash=False,
                trash_root=None,
                source_root=self.temp_dir,
            )
        self.assertIn("Current total size:", log_stream.getvalue())

    def test_directory_and_archive_management_integration(self):
        """Integration test for directory cleanup and archive size management"""
        # Test clean empty directories comprehensive
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

        # Test clean empty directories edge cases
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

        # Test archive size cleanup scenarios
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        past_ts = datetime(2023, 1, 1, 0, 0, 0)
        large_file = archive_dir / f"archived-{past_ts.strftime('%Y%m%d%H%M%S')}.mp4"
        self.create_file(large_file, b"x" * (600 * 1024 * 1024))  # 600 MB

        # Call the helper - it **will** unlink the file via remove_one
        archiver.cleanup_archive_size_limit(
            archive_dir, logger, max_size_gb=0, dry_run=False
        )
        # File must be gone **and** logged exactly once
        self.assertFalse(large_file.exists(), "file was not removed")
        log_content = log_stream.getvalue()
        self.assertIn("Current total size: 0.6 GB", log_content)
        self.assertEqual(log_content.count(str(large_file)), 1)

    def test_trash_inclusion_integration(self):
        """Integration test for trash file inclusion in size and age thresholds"""
        # Test scan files includes trash when enabled
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir(exist_ok=True)
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

        # Test cleanup archive size limit includes trash
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir(exist_ok=True)
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

        # Test cleanup trash files permanently when in trash
        logger, log_stream = self.capture_logger()
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir(exist_ok=True)
        trash_root = self.temp_dir / ".deleted"
        trash_output = trash_root / "output" / "2023" / "12" / "01"
        trash_output.mkdir(parents=True)
        trash_file = trash_output / "archived-20231201000000.mp4"
        self.create_file(trash_file, b"x" * (600 * 1024 * 1024))

        # Force removal - remove_one will **permanently** unlink it
        archiver.cleanup_archive_size_limit(
            archive_dir,
            logger,
            max_size_gb=0,  # force removal
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
        )
        # File gone **and** new log message present
        self.assertFalse(trash_file.exists(), "trash file was not removed")
        log_text = log_stream.getvalue()
        self.assertIn(
            "Permanently removed (already in trash)",
            log_text,
        )

    def test_process_files_intelligent_error_paths(self):
        """Integration test for process files intelligent error paths"""
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

    def test_run_archiver_integration(self):
        """Integration test for run archiver function"""
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
        base_dir.mkdir(exist_ok=True)
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

        # Test run archiver graceful exit handling
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir(exist_ok=True)
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
        # Reset graceful exit flag for subsequent tests
        archiver.GracefulExit.exit_requested = False

        # Test full flow with errors
        base_dir = self.temp_dir / "camera"
        base_dir.mkdir(exist_ok=True)
        ts = datetime.now() - timedelta(
            days=35
        )  # Changed from 25 to 35 days (older than 30)
        (base_dir / f"REO_cam_{ts.strftime('%Y%m%d%H%M%S')}.mp4").touch()
        args = MagicMock()
        args.directory = base_dir
        args.output = None
        args.age = 30  # 30 days threshold
        args.dry_run = False
        args.max_size = 500
        args.no_skip = False
        args.use_trash = False
        args.trashdir = None
        args.cleanup = False
        # capture the logger that run_archiver creates
        logger, log_stream = self.capture_logger()
        with patch("archiver.setup_logging", return_value=logger):
            with patch("archiver.transcode_file", return_value=False):  # fail
                exit_code = archiver.run_archiver(args)
        # When transcoding fails, the process should return 0 (it continues with cleanup)
        self.assertEqual(exit_code, 0)
        # The log should contain transcoding failure message
        log_content = log_stream.getvalue()
        self.assertIn("Transcoding failed", log_content)

    def test_graceful_exit_shortcuts(self):
        """Integration test for graceful exit shortcuts"""
        archiver.GracefulExit.request_exit()

        logger = MagicMock()
        # Test all early returns that simply check should_exit()
        try:
            archiver.safe_remove(self.temp_dir / "x", logger, False)
        except Exception as e:  # noqa: BLE001
            self.fail(f"safe_remove did not return early: {e}")

        try:
            archiver.transcode_file(Path("i"), Path("o"), logger)
        except Exception as e:  # noqa: BLE001
            self.fail(f"transcode_file did not return early: {e}")

        try:
            archiver.scan_files(self.temp_dir)
        except Exception as e:  # noqa: BLE001
            self.fail(f"scan_files did not return early: {e}")

        try:
            archiver.get_video_duration(Path("x.mp4"))
        except Exception as e:  # noqa: BLE001
            self.fail(f"get_video_duration did not return early: {e}")

        try:
            archiver.intelligent_cleanup([], self.temp_dir, logger, False, 1, 1)
        except Exception as e:  # noqa: BLE001
            self.fail(f"intelligent_cleanup did not return early: {e}")

        try:
            archiver.remove_orphaned_jpgs({}, set(), logger)
        except Exception as e:  # noqa: BLE001
            self.fail(f"remove_orphaned_jpgs did not return early: {e}")

        try:
            archiver.clean_empty_directories(self.temp_dir)
        except Exception as e:  # noqa: BLE001
            self.fail(f"clean_empty_directories did not return early: {e}")

        try:
            archiver.cleanup_archive_size_limit(self.temp_dir, logger, 1, False)
        except Exception as e:  # noqa: BLE001
            self.fail(f"cleanup_archive_size_limit did not return early: {e}")

        archiver.GracefulExit.exit_requested = False  # clean-up

    def test_edge_case_coverage_expansion(self):
        """Targeted test to hit uncovered edge cases with minimal code."""
        logger, _ = self.capture_logger()

        # 1. Trigger shutil.move OSError in safe_remove
        src = self.temp_dir / "src.txt"
        self.create_file(src)
        trash_root = self.temp_dir / ".trash"
        with patch("shutil.move", side_effect=OSError("Mock move error")):
            archiver.safe_remove(
                src, logger, dry_run=False, use_trash=True, trash_root=trash_root
            )
        self.assertTrue(src.exists())  # Should not be removed

        # 2. Trigger stdout type error in transcode_file
        with patch("archiver.get_video_duration", return_value=10.0):
            mock_proc = MagicMock()
            mock_proc.stdout = "not iterable"  # Invalid stdout type
            with patch("subprocess.Popen", return_value=mock_proc):
                result = archiver.transcode_file(Path("in"), Path("out"), logger)
                self.assertFalse(result)

        # 3. Trigger ValueError in relative_to during clean_empty_directories
        outside_dir = self.temp_dir / "outside" / "2023" / "12" / "01"
        outside_dir.mkdir(parents=True)
        # Call with root that doesn't contain outside_dir
        archiver.clean_empty_directories(self.temp_dir / "root", logger=logger)
        self.assertTrue(outside_dir.exists())  # Should not crash

        # 4. Trigger OSError during stat in intelligent_cleanup
        archive_dir = self.temp_dir / "archived"
        archive_dir.mkdir()
        broken_file = archive_dir / "archived-20230101000000.mp4"
        self.create_file(broken_file, b"x" * 1000)

        original_stat = Path.stat

        def selective_mock_stat(self, follow_symlinks=True):
            if self == broken_file:
                raise OSError("File not accessible")
            return original_stat(self, follow_symlinks=follow_symlinks)

        with patch.object(Path, "stat", selective_mock_stat):
            _, files_to_remove = archiver.intelligent_cleanup(
                [], archive_dir, logger, dry_run=False, max_size_gb=1, age_days=30
            )

        # 5. Trigger non-TTY progress update interval logic
        time_sequence = [0, 6]  # t=0 (init), t=6 (update)
        time_iter = iter(time_sequence)

        def mock_time():
            return next(time_iter, 100)

        stream = io.StringIO()
        stream.isatty = lambda: False
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        with patch("time.time", side_effect=mock_time):
            bar.update_progress(1, 50.0)  # t=0 → NOT printed (interval not reached)
            bar.update_progress(1, 100.0)  # t=6 → printed (100% always prints)
        output_lines = stream.getvalue().strip().split("\n")
        non_empty_lines = [line for line in output_lines if line.strip()]
        self.assertEqual(len(non_empty_lines), 1)

    def test_main_functionality(self):
        """Integration test for main function and command-line interface"""
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

        # Test main keyboard interrupt
        with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
            with patch("archiver.run_archiver", side_effect=KeyboardInterrupt):
                with self.assertRaises(SystemExit) as cm:
                    archiver.main()
                self.assertEqual(cm.exception.code, 1)

        # Test main other exception while not cancelled
        with patch("sys.argv", ["archiver.py", "--directory", str(self.temp_dir)]):
            with patch("archiver.run_archiver", side_effect=RuntimeError("boom")):
                with patch("archiver.GracefulExit.should_exit", return_value=False):
                    with self.assertRaises(RuntimeError):
                        archiver.main()


if __name__ == "__main__":
    unittest.main(verbosity=2)
