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
from unittest.mock import patch, MagicMock, call

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
    """Test filtering files by age."""

    def test_filter_old_files(self):
        now = datetime.now()
        samples = [
            (Path("old1.mp4"), now - timedelta(days=5), now - timedelta(days=35)),
            (Path("old2.mp4"), now - timedelta(days=10), now - timedelta(days=45)),
            (Path("new1.mp4"), now - timedelta(days=1), now - timedelta(days=5)),
            (Path("new2.mp4"), now - timedelta(days=2), now - timedelta(days=15)),
        ]

        old = filter_old_files(samples, 30)
        self.assertEqual(len(old), 2)
        self.assertIn(samples[0], old)
        self.assertIn(samples[1], old)

    def test_filter_no_old(self):
        now = datetime.now()
        samples = [
            (Path("new1.mp4"), now, now - timedelta(days=5)),
            (Path("new2.mp4"), now, now - timedelta(days=10)),
        ]
        result = filter_old_files(samples, 30)
        self.assertEqual(len(result), 0)


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
    """Verify that the progress bar never leaves stale characters on screen."""

    def test_long_to_short_transition(self):
        """
        The first update prints a longer string than the second one.
        After the second call no trailing characters from the first should remain.
        """
        self.bar = ProgressBar(total_files=3, width=40, silent=False)
        # 1st: long bar (filled=20 of 30)
        self.bar.update(file_index=1, elapsed_sec=10, filled_blocks=20)

        # 2nd: short bar (filled=5 of 30)
        self.bar.update(file_index=2, elapsed_sec=12, filled_blocks=5)

        lines = self.get_output_lines()
        self.assertEqual(len(lines), 2)

        pb = ProgressBar(total_files=1)  # dummy instance to call the helper
        expected1 = (
            f"File    {int(round(1 / 3 * 100))}% "
            f"{self.bar._build_bar(20)} Elapsed " + pb._format_elapsed(10)
        )
        expected2 = (
            f"File    {int(round(2 / 3 * 100))}% "
            f"{self.bar._build_bar(5)} Elapsed " + pb._format_elapsed(12)
        )

        esc_clear = "\x1b[2K"
        self.assertTrue(lines[0].startswith("\r" + esc_clear))
        self.assertEqual(lines[0][len("\r" + esc_clear) :].rstrip(), expected1)

        self.assertTrue(lines[1].startswith("\r" + esc_clear))
        # For the second line, we expect it to be padded to clear the previous line
        # but the meaningful content should match expected2
        actual_content = lines[1][len("\r" + esc_clear) :].rstrip()
        self.assertEqual(actual_content, expected2)

    def test_short_to_long_transition(self):
        """
        The bar should also work when a shorter line is followed by a longer one.
        No extra characters should appear after the second write.
        """
        self.bar = ProgressBar(total_files=3, width=40, silent=False)
        # 1st: short bar
        self.bar.update(file_index=1, elapsed_sec=5, filled_blocks=3)

        # 2nd: long bar
        self.bar.update(file_index=2, elapsed_sec=8, filled_blocks=25)

        lines = self.get_output_lines()
        self.assertEqual(len(lines), 2)

        pb = ProgressBar(total_files=1)  # dummy instance to call the helper
        expected1 = (
            f"File    {int(round(1 / 3 * 100))}% "
            f"{self.bar._build_bar(3)} Elapsed " + pb._format_elapsed(5)
        )
        expected2 = (
            f"File    {int(round(2 / 3 * 100))}% "
            f"{self.bar._build_bar(25)} Elapsed " + pb._format_elapsed(8)
        )

        esc_clear = "\x1b[2K"
        self.assertTrue(lines[0].startswith("\r" + esc_clear))
        self.assertEqual(lines[0][len("\r" + esc_clear) :].rstrip(), expected1)

        self.assertTrue(lines[1].startswith("\r" + esc_clear))
        self.assertEqual(lines[1][len("\r" + esc_clear) :].rstrip(), expected2)

    def test_multiple_updates_same_length(self):
        """
        When successive updates have the same length, the output should still be clean.
        """
        self.bar = ProgressBar(total_files=3, width=40, silent=False)
        for i in range(3):
            self.bar.update(file_index=i + 1, elapsed_sec=2 * i, filled_blocks=10)

        lines = self.get_output_lines()
        self.assertEqual(len(lines), 3)

        pb = ProgressBar(total_files=1)  # dummy instance to call the helper
        esc_clear = "\x1b[2K"
        for idx, line in enumerate(lines, start=1):
            pct = int(round(idx / 3 * 100))
            expected = (
                f"File    {pct}% "
                f"{self.bar._build_bar(10)} Elapsed "
                + pb._format_elapsed(2 * (idx - 1))
            )
            self.assertTrue(line.startswith("\r" + esc_clear))
            self.assertEqual(line[len("\r" + esc_clear) :].rstrip(), expected)


class TestCameraArchiverProgress(unittest.TestCase):
    """
    Verify that CameraArchiver.transcode_all() updates the ProgressBar correctly
    after each successful file and stops updating when a failure occurs.
    """

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    def test_progress_updates_on_success(self, mock_run):
        """All files succeed – progress bar should be updated twice."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "f1.mp4"
            f2 = tmp_dir / "f2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()  # type: ignore[arg-type]
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            # Patch ProgressBar.update so we can inspect the arguments
            with mock.patch.object(
                ProgressBar, "update", return_value=None
            ) as mock_update:
                archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 2)
            # Two successful files – two update calls
            self.assertEqual(mock_update.call_count, 2)

            # Determine the expected number of blocks for each file
            num_blocks = (
                30 - 2
            ) // ProgressBar.BLOCK_WIDTH  # default width used by transcode_all
            expected_calls = [
                # file_index=1 → 50% progress
                call(
                    file_index=1,
                    elapsed_sec=mock.ANY,
                    filled_blocks=num_blocks // 2,
                ),
                # file_index=2 → 100% progress
                call(
                    file_index=2,
                    elapsed_sec=mock.ANY,
                    filled_blocks=num_blocks,
                ),
            ]
            mock_update.assert_has_calls(expected_calls)
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(FFMpegTranscoder, "run", side_effect=[True, False])
    def test_progress_stops_on_failure(self, mock_run):
        """Second file fails – only one update should be issued."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            f1 = tmp_dir / "f1.mp4"
            f2 = tmp_dir / "f2.mp4"
            f1.touch()
            f2.touch()

            ts = datetime.now()  # type: ignore[arg-type]
            files_to_process = [(f1, ts, ts), (f2, ts, ts)]

            with mock.patch.object(
                ProgressBar, "update", return_value=None
            ) as mock_update:
                archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 1)
            # Only the first file succeeded → one update call
            self.assertEqual(mock_update.call_count, 1)

            num_blocks = (30 - 2) // ProgressBar.BLOCK_WIDTH
            mock_update.assert_called_once_with(
                file_index=1,
                elapsed_sec=mock.ANY,
                filled_blocks=num_blocks // 2,
            )
        finally:
            shutil.rmtree(tmp_dir)

    @patch.object(FFMpegTranscoder, "run", return_value=True)
    def test_single_file_progress(self, mock_run):
        """When only one file is processed, the progress bar updates once and finishes."""
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            # 1 MP4 file
            f1 = tmp_dir / "single.mp4"
            f1.touch()

            ts = datetime.now()
            files_to_process = [(f1, ts, ts)]

            with (
                mock.patch.object(
                    ProgressBar, "start", return_value=None
                ) as mock_start,
                mock.patch.object(
                    ProgressBar, "update", return_value=None
                ) as mock_update,
                mock.patch.object(
                    ProgressBar, "finish", return_value=None
                ) as mock_finish,
            ):
                archiver = CameraArchiver(tmp_dir, tmp_dir, MagicMock())
                result = archiver.transcode_all(files_to_process)

            self.assertEqual(len(result), 1)
            self.assertIn((f1, ts, ts), result)

            mock_start.assert_called_once()
            mock_finish.assert_called_once()
            mock_update.assert_called_once()

            # Default width used by transcode_all
            num_blocks = (30 - 2) // ProgressBar.BLOCK_WIDTH

            mock_update.assert_called_once_with(
                file_index=1,
                elapsed_sec=mock.ANY,
                filled_blocks=num_blocks,  # fully filled bar for the lone file
            )
        finally:
            shutil.rmtree(tmp_dir)


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
