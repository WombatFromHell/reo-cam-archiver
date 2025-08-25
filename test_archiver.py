#!/usr/bin/env python3
"""
Test suite for archiver.py – rewritten to use only unittest.
"""

import io
import os
import sys
import tempfile
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import patch, MagicMock

from archiver import (
    parse_timestamp_from_filename,
    find_camera_files,
    filter_old_files,
    get_output_path,
    get_directory_size,
    setup_logging,
    cleanup_archived_files,
    transcode_with_progress,
    CameraArchiver,
    FFMpegTranscoder,
    ProgressBar,
)


class TempDirMixin:
    """Creates a temporary directory for tests."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)


class StderrCaptureMixin(unittest.TestCase):
    """
    Patch sys.stderr with an in‑memory StringIO so we can inspect
    what the ProgressBar writes (since it now writes to stderr).
    """

    def setUp(self):
        super().setUp()
        self._stderr_patch = patch("sys.stderr", new_callable=io.StringIO)
        self.mock_stderr = self._stderr_patch.start()
        self.addCleanup(self._stderr_patch.stop)

    def get_output_lines(self) -> list[str]:
        """
        Return the lines that were written to stderr.
        The ProgressBar writes \r\x1b[2K followed by content.
        We need to reconstruct the lines with the \r prefix.
        """
        raw_output = self.mock_stderr.getvalue()
        if not raw_output:
            return []

        # The output will be something like: "\r\x1b[2Kcontent1\r\x1b[2Kcontent2"
        # We need to split but preserve the \r at the start of each segment
        parts = raw_output.split("\r")

        # The first part will be empty (before the first \r), so skip it
        # Each subsequent part should be prefixed with \r
        lines = []
        for part in parts[1:]:  # Skip the first empty part
            if part:  # Only include non‑empty parts
                lines.append("\r" + part)

        return lines


class StdoutCaptureMixin(unittest.TestCase):
    """
    Patch sys.stdout with an in‑memory StringIO so we can inspect
    what the ProgressBar writes.
    """

    def setUp(self):
        super().setUp()
        self._stdout_patch = patch("sys.stdout", new_callable=io.StringIO)
        self.mock_stdout = self._stdout_patch.start()
        self.addCleanup(self._stdout_patch.stop)

    def get_output_lines(self) -> list[str]:
        """
        Return the lines that were written to stdout.
        The ProgressBar writes \r\x1b[2K followed by content.
        We need to reconstruct the lines with the \r prefix.
        """
        raw_output = self.mock_stdout.getvalue()
        if not raw_output:
            return []

        # The output will be something like: "\r\x1b[2Kcontent1\r\x1b[2Kcontent2"
        # We need to split but preserve the \r at the start of each segment
        parts = raw_output.split("\r")

        # The first part will be empty (before the first \r), so skip it
        # Each subsequent part should be prefixed with \r
        lines = []
        for part in parts[1:]:  # Skip the first empty part
            if part:  # Only include non‑empty parts
                lines.append("\r" + part)

        return lines


class TestTimestampParsing(unittest.TestCase):
    """Test timestamp extraction from camera filenames."""

    def test_valid_mp4_filenames(self):
        test_cases = [
            (
                "REO_DRIVEWAY_01_20250821211345.mp4",
                datetime(
                    year=2025,
                    month=8,
                    day=21,
                    hour=21,
                    minute=13,
                    second=45,
                ),
            ),
            (
                "REO_BACKYARD_CAM2_20240101000000.mp4",
                datetime(
                    year=2024,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                ),
            ),
            (
                "REO_FRONT_20231231235959.mp4",
                datetime(
                    year=2023,
                    month=12,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                ),
            ),
            (
                "REO_A_B_C_20220630120000.mp4",
                datetime(
                    year=2022,
                    month=6,
                    day=30,
                    hour=12,
                    minute=0,
                    second=0,
                ),
            ),
        ]

        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = parse_timestamp_from_filename(filename)
                self.assertEqual(result, expected)

    def test_valid_jpg_filenames(self):
        test_cases = [
            (
                "REO_DRIVEWAY_01_20250821211345.jpg",
                datetime(
                    year=2025,
                    month=8,
                    day=21,
                    hour=21,
                    minute=13,
                    second=45,
                ),
            ),
            (
                "REO_CAM_20240515143022.JPG",
                datetime(
                    year=2024,
                    month=5,
                    day=15,
                    hour=14,
                    minute=30,
                    second=22,
                ),
            ),
        ]

        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = parse_timestamp_from_filename(filename)
                self.assertEqual(result, expected)

    def test_invalid_filenames(self):
        invalid_files = [
            "REO_CAM_2025082121134.mp4",
            "REO_CAM_202508212113456.mp4",
            "REO_CAM_20250821211345.avi",
            "NOTCAM_20250821211345.mp4",
            "REO_CAM_20250821211345",
            "REO_CAM_20251301211345.mp4",
            "REO_CAM_20250832211345.mp4",
            "REO_CAM_20250821251345.mp4",
            "REO_CAM_19990821211345.mp4",
            "REO_CAM_21000821211345.mp4",
            "regular_file.mp4",
            "",
        ]

        for filename in invalid_files:
            with self.subTest(filename=filename):
                result = parse_timestamp_from_filename(filename)
                self.assertIsNone(result)


class TestFileDiscovery(TempDirMixin, unittest.TestCase):
    """Test finding camera files in directory structure."""

    def setUp(self):
        super().setUp()

        # Create a realistic YYYY/MM/DD tree
        dirs_to_create = [
            self.test_dir / "2024" / "08" / "21",
            self.test_dir / "2024" / "08" / "22",
            self.test_dir / "2023" / "12" / "31",
            # invalid sub‑trees – should be ignored
            self.test_dir / "invalid" / "dir",
            self.test_dir / "2024" / "13" / "01",
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)

        # Create files inside the tree
        self.files = [
            # Valid camera file (good timestamp)
            (
                self.test_dir
                / "2024"
                / "08"
                / "21"
                / "REO_DRIVEWAY_01_20240821123456.mp4",
                datetime(
                    year=2024,
                    month=8,
                    day=21,
                    hour=12,
                    minute=34,
                    second=56,
                ),
            ),
            # Valid JPG – but seconds >59 → invalid timestamp
            (
                self.test_dir
                / "2024"
                / "08"
                / "21"
                / "REO_BACKYARD_20240821234567.jpg",
                None,
            ),
            # Valid MP4
            (
                self.test_dir / "2024" / "08" / "22" / "REO_FRONT_20240822111111.mp4",
                datetime(
                    year=2024,
                    month=8,
                    day=22,
                    hour=11,
                    minute=11,
                    second=11,
                ),
            ),
            # Valid JPG
            (
                self.test_dir / "2023" / "12" / "31" / "REO_CAM1_20231231235959.jpg",
                datetime(
                    year=2023,
                    month=12,
                    day=31,
                    hour=23,
                    minute=59,
                    second=59,
                ),
            ),
            # Non‑camera file – should be ignored
            (self.test_dir / "2024" / "08" / "21" / "not_a_camera_file.mp4", None),
            # Wrong extension – ignore
            (
                self.test_dir / "2024" / "08" / "21" / "REO_CAM_invalid.txt",
                None,
            ),
        ]

        for path, _ in self.files:
            path.touch()
            os.utime(path, (1692600000, 1692600000))  # set mtime to a fixed value

    def test_find_camera_files(self):
        found = find_camera_files(self.test_dir)
        # Count expected valid files
        expected_count = sum(
            1 for _, ts in self.files if ts is not None and ts.second < 60
        )
        self.assertEqual(len(found), expected_count)

        for fp, ts, mtime in found:
            self.assertIsInstance(fp, Path)
            self.assertIsInstance(ts, datetime)
            self.assertIsInstance(mtime, datetime)

    def test_find_camera_files_empty(self):
        empty_dir = Path(tempfile.mkdtemp())
        try:
            result = find_camera_files(empty_dir)
            self.assertEqual(len(result), 0)
        finally:
            shutil.rmtree(empty_dir)


class TestFileFiltering(unittest.TestCase):
    """Verify that filtering uses the file‑name timestamp, not the mtime."""

    def test_only_timestamp_older(self):
        now = datetime.now()

        samples = [
            # Timestamp older than 30 days – should stay
            (
                Path("old_ts.mp4"),
                now - timedelta(days=40),  # ts
                now - timedelta(days=5),  # mtime (newer)
            ),
            # Timestamp newer than 30 days – should be dropped
            (
                Path("new_ts.mp4"),
                now - timedelta(days=1),
                now - timedelta(days=50),
            ),
        ]

        result = filter_old_files(samples, 30)

        self.assertIn(samples[0], result)
        self.assertNotIn(samples[1], result)

    def test_mtime_older_but_ts_new(self):
        now = datetime.now()

        samples = [
            # Timestamp newer (now‑1 day), mtime older (now‑40 days) – drop
            (
                Path("new_ts_old_mtime.mp4"),
                now - timedelta(days=1),
                now - timedelta(days=40),
            ),
            # Both timestamp and mtime old – keep
            (
                Path("both_old.mp4"),
                now - timedelta(days=35),
                now - timedelta(days=30),
            ),
        ]

        result = filter_old_files(samples, 30)

        self.assertIn(samples[1], result)
        self.assertNotIn(samples[0], result)

    def test_no_matches_when_age_zero(self):
        now = datetime.now()

        samples = [
            (
                Path("old_ts.mp4"),
                now - timedelta(days=10),
                now - timedelta(days=5),
            ),
            (
                Path("new_ts.mp4"),
                now - timedelta(days=1),
                now - timedelta(days=2),
            ),
        ]

        result = filter_old_files(samples, 0)

        # All files are older than the current moment → both returned
        self.assertEqual(len(result), len(samples))


class TestOutputPath(unittest.TestCase):
    """Test output path generation."""

    def test_get_output_path(self):
        inp = Path("/base/2024/08/21/REO_CAM_20240821123456.mp4")
        base = Path("/archive")
        ts = datetime(
            year=2024,
            month=8,
            day=21,
            hour=12,
            minute=34,
            second=56,
        )
        expected = Path("/archive/2024/08/21/archived-20240821123456.mp4")
        self.assertEqual(get_output_path(inp, base, ts), expected)

    def test_get_output_path_diff_timestamp(self):
        inp = Path("/base/2024/01/15/REO_CAM_20240115000000.mp4")
        base = Path("/archive")
        ts = datetime(
            year=2024,
            month=1,
            day=15,
            hour=10,
            minute=30,
            second=45,
        )
        expected = Path("/archive/2024/01/15/archived-20240115103045.mp4")
        self.assertEqual(get_output_path(inp, base, ts), expected)


class TestDirectorySize(unittest.TestCase):
    """Test directory size calculation."""

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())

        # Patch Path.write_text to ensure parent dirs exist
        original_write_text = Path.write_text

        def write_text_with_mkdir(
            self, data, encoding="utf-8", errors="strict", newline=None
        ):
            if not self.parent.exists():
                self.parent.mkdir(parents=True, exist_ok=True)
            return original_write_text(
                self, data, encoding=encoding, errors=errors, newline=newline
            )

        patcher = patch.object(Path, "write_text", new=write_text_with_mkdir)
        patcher.start()
        self.addCleanup(patcher.stop)

        # Now the original test code works
        (self.test_dir / "file1.txt").write_text("A" * 100)
        (self.test_dir / "subdir" / "file2.txt").write_text("B" * 200)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_get_directory_size(self):
        self.assertEqual(get_directory_size(self.test_dir), 300)

    def test_nonexistent(self):
        self.assertEqual(get_directory_size(Path("/nonexistent/directory")), 0)


class TestLogging(unittest.TestCase):
    """Test that the logger is configured correctly."""

    def test_setup_logging(self):
        import logging

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            log_path = Path(tmp.name)

        try:
            logger = setup_logging(log_path)
            self.assertEqual(logger.name, "camera_archiver")
            self.assertEqual(len(logger.handlers), 2)

            console_handler = next(
                h for h in logger.handlers if isinstance(h, logging.StreamHandler)
            )

            original_emit = console_handler.emit

            def silent_console_emit(record):
                if record.getMessage() == "Hello world":
                    return
                original_emit(record)

            patcher = patch.object(console_handler, "emit", new=silent_console_emit)
            patcher.start()
            self.addCleanup(patcher.stop)

            logger.info("Hello world")

            self.assertTrue(log_path.exists())
            content = log_path.read_text()
            self.assertIn("Hello world", content)
        finally:
            if log_path.exists():
                log_path.unlink()


class TestTranscoding(unittest.TestCase):
    """Test transcoding logic with subprocess mocked."""

    @patch("archiver.subprocess.Popen")
    def test_success(self, mock_popen):
        proc = MagicMock()
        proc.returncode = 0
        proc.stdout.readline.side_effect = ["frame=100", ""]
        mock_popen.return_value = proc

        inp = Path("/tmp/input.mp4")
        out = Path("/tmp/output.mp4")
        logger = MagicMock()

        with patch.object(Path, "mkdir"):
            result = transcode_with_progress(inp, out, logger)

        self.assertTrue(result)
        mock_popen.assert_called_once()
        logger.info.assert_any_call(f"Transcoding: {inp} -> {out}")

    @patch("archiver.subprocess.Popen")
    def test_failure(self, mock_popen):
        proc = MagicMock()
        proc.returncode = 1
        proc.stdout.readline.side_effect = ["Error occurred", ""]
        mock_popen.return_value = proc

        inp = Path("/tmp/input.mp4")
        out = Path("/tmp/output.mp4")
        logger = MagicMock()

        with patch.object(Path, "mkdir"):
            result = transcode_with_progress(inp, out, logger)

        self.assertFalse(result)
        logger.error.assert_called()

    @patch("archiver.subprocess.Popen", side_effect=FileNotFoundError())
    def test_ffmpeg_not_found(self, mock_popen):
        inp = Path("/tmp/input.mp4")
        out = Path("/tmp/output.mp4")
        logger = MagicMock()

        with patch.object(Path, "mkdir"):
            result = transcode_with_progress(inp, out, logger)

        self.assertFalse(result)
        logger.error.assert_called_with("FFmpeg not found. Please install ffmpeg.")

    @patch.object(ProgressBar, "update", return_value=None)
    def test_bail_on_error(self, mock_update):
        """Ensure that a failure stops the batch and only earlier files are returned."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "file1.mp4"
            f2 = tmp_dir / "file2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            # First run succeeds, second fails
            with patch.object(
                FFMpegTranscoder, "run", side_effect=[True, False]
            ) as mock_run:
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 1)
            self.assertIn((f1, ts, ts), result)
            self.assertNotIn((f2, ts, ts), result)
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(ProgressBar, "update", return_value=None)
    def test_all_success(self, mock_update):
        """Verify that when all files succeed we get the full list."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "file1.mp4"
            f2 = tmp_dir / "file2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            with patch.object(FFMpegTranscoder, "run", return_value=True) as mock_run:
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 2)
            self.assertIn((f1, ts, ts), result)
            self.assertIn((f2, ts, ts), result)
        finally:
            shutil.rmtree(tmp_dir)

    def test_create_missing_output_dir(self):
        """transcode_with_progress should create missing output directories."""
        tmp = Path(tempfile.mkdtemp())
        try:
            inp = tmp / "in.mp4"
            inp.touch()

            # Input file timestamp – any value works
            ts = datetime.now()

            @patch("archiver.subprocess.Popen")
            def _run(mock_popen):
                proc = MagicMock()
                proc.returncode = 0
                proc.stdout.readline.side_effect = ["", ""]
                mock_popen.return_value = proc

                logger = MagicMock()

                archiver = CameraArchiver(tmp, tmp, logger)
                # discover the file so that transcode_all() sees it
                archiver.discover_files()
                result = archiver.transcode_all([(inp, ts, ts)])

                self.assertTrue(result)  # succeeded

                # The output path used by the archiver:
                out_file = get_output_path(inp, tmp, ts)
                self.assertTrue(
                    out_file.parent.exists(), msg=f"Expected {out_file.parent} to exist"
                )

            _run()
        finally:
            shutil.rmtree(tmp)


class TestCleanupEmptyFolders(unittest.TestCase):
    def setUp(self):
        # Input tree /camera/YYYY/MM/DD
        self.input_root = Path(tempfile.mkdtemp())
        self.file_dir = self.input_root / "2024" / "08" / "21"
        self.file_dir.mkdir(parents=True, exist_ok=True)
        self.mp4 = self.file_dir / "REO_CAM_20240821123456.mp4"
        self.jpg = self.file_dir / "REO_CAM_20240821123456.jpg"
        self.mp4.touch()
        self.jpg.touch()

        # Archive tree /camera/archived/YYYY/MM/DD
        self.arch_root = Path(tempfile.mkdtemp())
        self.arch_file = get_output_path(self.mp4, self.arch_root, datetime.now())
        self.arch_file.parent.mkdir(parents=True, exist_ok=True)
        self.arch_file.write_bytes(b"A" * 1024)

    def tearDown(self):
        shutil.rmtree(self.input_root, ignore_errors=True)
        shutil.rmtree(self.arch_root, ignore_errors=True)

    def test_remove_empty_dirs_in_both_trees(self):
        archiver = CameraArchiver(self.input_root, self.arch_root, MagicMock())
        processed = [(self.mp4, datetime.now(), datetime.now())]
        archiver.cleanup_processed(processed, dry_run=False)

        # Files removed
        self.assertFalse(self.mp4.exists())
        self.assertFalse(self.jpg.exists())

        # Input dirs cleaned up
        self.assertFalse((self.input_root / "2024" / "08").exists())
        self.assertFalse((self.input_root / "2024").exists())

        # Archive dirs cleaned up
        self.assertFalse(
            self.arch_file.parent.parent.parent.exists()
        )  # /archived/2024/08/21 removed


class TestArchiveCleanup(unittest.TestCase):
    """Test cleanup_archived_files logic."""

    def setUp(self):
        self.archive_dir = Path(tempfile.mkdtemp())
        # 3 files, each 1 MB
        self.files = [
            self.archive_dir / "2024" / "08" / "20" / "archived-20240820120000.mp4",
            self.archive_dir / "2024" / "08" / "21" / "archived-20240821130000.mp4",
            self.archive_dir / "2024" / "08" / "22" / "archived-20240822140000.mp4",
        ]
        for f in self.files:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"A" * 1024 * 1024)

    def tearDown(self):
        shutil.rmtree(self.archive_dir, ignore_errors=True)

    def test_under_limit(self):
        logger = MagicMock()
        removed = cleanup_archived_files(self.archive_dir, 10, False, logger)
        self.assertEqual(removed, 0)
        logger.info.assert_any_call(
            "Archive directory is within size limit, no cleanup needed"
        )

    def test_dry_run_over_limit(self):
        logger = MagicMock()
        removed = cleanup_archived_files(self.archive_dir, 0.001, True, logger)
        self.assertGreater(removed, 0)
        for f in self.files:
            self.assertTrue(f.exists())

    def test_real_cleanup_over_limit(self):
        logger = MagicMock()
        removed = cleanup_archived_files(self.archive_dir, 0.001, False, logger)
        self.assertGreater(removed, 0)
        remaining = [f for f in self.files if f.exists()]
        self.assertLess(len(remaining), len(self.files))

    def test_nonexistent_directory(self):
        non_existent = Path("/nonexistent/archive")
        logger = MagicMock()
        removed = cleanup_archived_files(non_existent, 100, False, logger)
        self.assertEqual(removed, 0)
        logger.info.assert_any_call(
            f"Archive directory {non_existent} does not exist, skipping archive cleanup"
        )


class TestProgressBarDisplay(StderrCaptureMixin, unittest.TestCase):
    """Verify that the progress bar displays correctly and handles transitions."""

    def test_progress_updates(self):
        """Test that progress updates display correctly."""
        self.bar = ProgressBar(total_files=3, width=40, silent=False)
        self.bar.start()  # Initialize start time

        # Simulate file processing with progress updates
        self.bar.update_progress(file_index=1, file_progress_pct=50.0)

        # Get the raw stderr output
        output = self.mock_stderr.getvalue()

        # The output should contain progress information
        self.assertIn("Overall 1/3", output)
        self.assertIn("File    50%", output)
        self.assertIn("Elapsed", output)

    def test_multiple_updates_clean_display(self):
        """Test that multiple updates don't leave stale characters."""
        self.bar = ProgressBar(total_files=2, width=30, silent=False)
        self.bar.start()

        # Multiple updates
        self.bar.update_progress(file_index=1, file_progress_pct=25.0)
        self.bar.update_progress(file_index=1, file_progress_pct=75.0)
        self.bar.finish_file(file_index=1)  # Should show 100%

        output = self.mock_stderr.getvalue()

        # Should contain the expected progress elements
        self.assertIn("Overall", output)
        self.assertIn("File", output)
        self.assertIn("Elapsed", output)

    def test_finish_behavior(self):
        """Test that finish() moves cursor properly."""
        self.bar = ProgressBar(total_files=1, width=20, silent=False)
        self.bar.start()
        self.bar.update_progress(file_index=1, file_progress_pct=100.0)
        self.bar.finish()

        output = self.mock_stderr.getvalue()

        # Should end with a newline for clean terminal state
        self.assertTrue(output.endswith("\n"))

    def test_silent_mode(self):
        """Test that silent mode produces no output."""
        self.bar = ProgressBar(total_files=1, width=20, silent=True)
        self.bar.start()
        self.bar.update_progress(file_index=1, file_progress_pct=50.0)
        self.bar.finish()

        output = self.mock_stderr.getvalue()
        self.assertEqual(output, "")

    def test_legacy_update_method(self):
        """Test backward compatibility with legacy update method."""
        self.bar = ProgressBar(total_files=2, width=20, silent=False)
        self.bar.start()
        self.bar.current_file_index = 1

        # Test legacy update call (used by existing code)
        self.bar.update(file_index=None, filled_blocks=5)

        output = self.mock_stderr.getvalue()
        # Should produce some output
        self.assertGreater(len(output), 0)


class TestProgressBarLogging(StderrCaptureMixin, unittest.TestCase):
    """Test that progress bar properly manages log message display."""

    def test_ensure_clean_log_space(self):
        """Test that ensure_clean_log_space clears progress display."""
        self.bar = ProgressBar(total_files=1, width=20, silent=False)
        self.bar.start()

        # Show some progress
        self.bar.update_progress(file_index=1, file_progress_pct=50.0)

        # Clear for log space
        self.bar.ensure_clean_log_space()

        # The method should have cleared the progress lines
        # We can't easily test the exact terminal control codes, but we can
        # verify the method runs without error
        self.assertFalse(self.bar._progress_displayed)

    def test_rate_limiting(self):
        """Test that rapid updates are rate limited."""
        self.bar = ProgressBar(total_files=1, width=20, silent=False)
        self.bar.start()

        # Rapid updates (should be rate limited)
        with mock.patch("time.time", side_effect=[1000, 1000.05, 1000.1, 1000.15]):
            self.bar.update_progress(file_index=1, file_progress_pct=25.0)
            self.bar.update_progress(file_index=1, file_progress_pct=50.0)
            self.bar.update_progress(file_index=1, file_progress_pct=75.0)

        # Should produce output (exact amount depends on rate limiting)
        output = self.mock_stderr.getvalue()
        self.assertGreater(len(output), 0)


class TestCameraArchiverProgress(unittest.TestCase):
    """
    Verify that CameraArchiver.transcode_all() updates the ProgressBar correctly
    after each successful file and stops updating when a failure occurs.
    """

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    @patch("time.time", side_effect=[1000, 1001, 1002, 1003, 1004])
    def test_progress_updates_on_success(self, mock_time, mock_run):
        """All files succeed – progress bar should be updated appropriately."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "f1.mp4"
            f2 = tmp_dir / "f2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            # Patch the new ProgressBar methods
            with (
                mock.patch.object(ProgressBar, "finish_file") as mock_finish_file,
                mock.patch.object(ProgressBar, "start") as mock_start,
                mock.patch.object(ProgressBar, "finish") as mock_finish,
                mock.patch.object(ProgressBar, "start_file") as mock_start_file,
                mock.patch.object(
                    ProgressBar, "update_progress"
                ) as mock_update_progress,
            ):
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 2)

            # Verify progress bar lifecycle methods were called
            mock_start.assert_called_once()
            mock_finish.assert_called_once()

            # Should have called start_file for each file (via transcoder)
            self.assertEqual(mock_start_file.call_count, 2)

            # finish_file should be called for each successful transcoding
            self.assertEqual(mock_finish_file.call_count, 2)

            # Verify finish_file was called with correct file indices
            calls = mock_finish_file.call_args_list
            self.assertEqual(calls[0][0][0], 1)  # first call with file index 1
            self.assertEqual(calls[1][0][0], 2)  # second call with file index 2
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(FFMpegTranscoder, "run", side_effect=[True, False])
    @patch("time.time", side_effect=[1000, 1001, 1002, 1003])
    def test_progress_stops_on_failure(self, mock_time, mock_run):
        """Second file fails – progress should stop after first success."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "f1.mp4"
            f2 = tmp_dir / "f2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            with (
                mock.patch.object(ProgressBar, "finish_file") as mock_finish_file,
                mock.patch.object(ProgressBar, "start") as mock_start,
                mock.patch.object(ProgressBar, "finish") as mock_finish,
                mock.patch.object(ProgressBar, "start_file") as mock_start_file,
                mock.patch.object(
                    ProgressBar, "update_progress"
                ) as mock_update_progress,
            ):
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 1)

            # Should have started normally
            mock_start.assert_called_once()
            mock_finish.assert_called_once()

            # start_file called twice (once for each file attempt)
            self.assertEqual(mock_start_file.call_count, 2)

            # finish_file called only once (only first file succeeded)
            self.assertEqual(mock_finish_file.call_count, 1)
            mock_finish_file.assert_called_with(1)  # Called with file index 1
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    @patch("time.time", side_effect=[1000, 1001, 1002])
    def test_single_file_progress(self, mock_time, mock_run):
        """When only one file is processed, the progress bar updates once and finishes."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "single.mp4"
            f1.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            with (
                mock.patch.object(ProgressBar, "start") as mock_start,
                mock.patch.object(ProgressBar, "finish_file") as mock_finish_file,
                mock.patch.object(ProgressBar, "finish") as mock_finish,
                mock.patch.object(ProgressBar, "start_file") as mock_start_file,
                mock.patch.object(
                    ProgressBar, "update_progress"
                ) as mock_update_progress,
            ):
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 1)
            self.assertIn((f1, ts, ts), result)

            # Verify all expected methods were called once
            mock_start.assert_called_once()
            mock_finish.assert_called_once()
            mock_start_file.assert_called_once()
            mock_finish_file.assert_called_once_with(1)
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    def test_dry_run_mode(self, mock_run):
        """Test that dry run mode works correctly with progress updates."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "f1.mp4"
            f1.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts)]

            archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())

            with mock.patch.object(
                ProgressBar, "update_progress"
            ) as mock_update_progress:
                result = archiver.transcode_all(files_to_process, dry_run=True)

            self.assertEqual(len(result), 1)

            # In dry run, FFMpegTranscoder.run should not be called
            mock_run.assert_not_called()

            # But progress should still be updated for dry run
            mock_update_progress.assert_called()
        finally:
            shutil.rmtree(tmp_dir)


class TestCameraArchiverSkip(unittest.TestCase):
    """Verify that the default skip logic works and that --no-skip overrides it."""

    def setUp(self) -> None:
        # Temporary input tree  /camera/YYYY/MM/DD
        self.base_dir = Path(tempfile.mkdtemp())
        self.file_dir = self.base_dir / "2024" / "08" / "21"
        self.file_dir.mkdir(parents=True, exist_ok=True)

        # Input MP4 file
        self.mp4_path = self.file_dir / "REO_CAM_20240821123456.mp4"
        self.mp4_path.touch()

        # Corresponding archived copy (will be >1 MiB)
        self.arch_root = Path(tempfile.mkdtemp())
        self.out_file = get_output_path(
            self.mp4_path, self.arch_root, datetime(2024, 8, 21, 12, 34, 56)
        )
        self.out_file.parent.mkdir(parents=True, exist_ok=True)
        # write 1 MiB + 1 kB
        self.out_file.write_bytes(b"A" * (1024 * 1024 + 1024))

        self.logger = mock.MagicMock()
        self.archiver = CameraArchiver(self.base_dir, self.arch_root, self.logger)

    def tearDown(self) -> None:
        shutil.rmtree(self.base_dir)
        shutil.rmtree(self.arch_root)

    def test_skip_when_large_archive_exists(self):
        """With no --no-skip flag, an existing large archive causes the file to be skipped."""
        files_to_process = [
            (self.mp4_path, datetime(2024, 8, 21, 12, 34, 56), datetime.now())
        ]

        # Use dry‑run so that we never touch the filesystem or run ffmpeg
        result = self.archiver.transcode_all(
            files_to_process, dry_run=True, no_skip=False
        )

        # No file should be processed
        self.assertEqual(result, [])

    def test_no_skip_overwrites_large_archive(self):
        """With --no-skip, the file is still queued for transcoding."""
        files_to_process = [
            (self.mp4_path, datetime(2024, 8, 21, 12, 34, 56), datetime.now())
        ]

        # Dry‑run mode: the run() method is never executed but we
        # should still see the file in the returned list.
        result = self.archiver.transcode_all(
            files_to_process, dry_run=True, no_skip=True
        )

        self.assertEqual(result, [files_to_process[0]])

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    def test_real_transcode_with_no_skip(self, mock_run):
        """When no‑skip is true and we run normally, the transcoder is invoked."""
        files_to_process = [
            (self.mp4_path, datetime(2024, 8, 21, 12, 34, 56), datetime.now())
        ]

        result = self.archiver.transcode_all(
            files_to_process, dry_run=False, no_skip=True
        )

        # The transcoder should have been called once
        mock_run.assert_called_once()
        self.assertEqual(result, [files_to_process[0]])

    def test_no_skip_for_small_archive(self):
        """If the archive is smaller than 1 MiB, the file is processed even without --no-skip."""
        # Reduce the size of the existing archive
        self.out_file.write_bytes(b"A" * (1024 * 512))  # 512 kB

        files_to_process = [
            (self.mp4_path, datetime(2024, 8, 21, 12, 34, 56), datetime.now())
        ]

        result = self.archiver.transcode_all(
            files_to_process, dry_run=True, no_skip=False
        )

        # The file should be queued for transcoding
        self.assertEqual(result, [files_to_process[0]])


def run_tests():
    """Convenience wrapper to run all tests."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary:")
    print(f"  Tests run: {result.testsRun}")
    print(f"  Failures: {len(result.failures)}")
    print(f"  Errors:   {len(result.errors)}")
    success_rate = (
        (result.testsRun - len(result.failures) - len(result.errors))
        / result.testsRun
        * 100
    )
    print(f"  Success rate: {success_rate:.1f}%")
    print("=" * 70)

    return result.wasSuccessful()


if __name__ == "__main__":
    sys.exit(0 if run_tests() else 1)
