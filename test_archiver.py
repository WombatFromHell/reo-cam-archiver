#!/usr/bin/env python3
"""
Test suite for archiver.py – rewritten to use only unittest.
"""

import os
import sys
import tempfile
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, call

# ----------------------------------------------------------------------
# Import the functions that we want to test
# ----------------------------------------------------------------------
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
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
)

# ----------------------------------------------------------------------
# Helper classes / fixtures
# ----------------------------------------------------------------------


class TempDirMixin:
    """
    A mix‑in that creates a temporary directory in setUp() and removes it in tearDown().
    Sub‑classes can use self.test_dir (or any other name you prefer).
    """

    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)


# ----------------------------------------------------------------------
# Individual test cases
# ----------------------------------------------------------------------


class TestTimestampParsing(unittest.TestCase):
    """Test timestamp extraction from camera filenames."""

    def test_valid_mp4_filenames(self):
        test_cases = [
            ("REO_DRIVEWAY_01_20250821211345.mp4", datetime(2025, 8, 21, 21, 13, 45)),
            ("REO_BACKYARD_CAM2_20240101000000.mp4", datetime(2024, 1, 1, 0, 0, 0)),
            ("REO_FRONT_20231231235959.mp4", datetime(2023, 12, 31, 23, 59, 59)),
            ("REO_A_B_C_20220630120000.mp4", datetime(2022, 6, 30, 12, 0, 0)),
        ]

        for filename, expected in test_cases:
            with self.subTest(filename=filename):
                result = parse_timestamp_from_filename(filename)
                self.assertEqual(result, expected)

    def test_valid_jpg_filenames(self):
        test_cases = [
            ("REO_DRIVEWAY_01_20250821211345.jpg", datetime(2025, 8, 21, 21, 13, 45)),
            ("REO_CAM_20240515143022.JPG", datetime(2024, 5, 15, 14, 30, 22)),
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
                datetime(2024, 8, 21, 12, 34, 56),
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
                datetime(2024, 8, 22, 11, 11, 11),
            ),
            # Valid JPG
            (
                self.test_dir / "2023" / "12" / "31" / "REO_CAM1_20231231235959.jpg",
                datetime(2023, 12, 31, 23, 59, 59),
            ),
            # Non‑camera file – should be ignored
            (self.test_dir / "2024" / "08" / "21" / "not_a_camera_file.mp4", None),
            # Wrong extension – ignore
            (self.test_dir / "2024" / "08" / "21" / "REO_CAM_invalid.txt", None),
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
        ts = datetime(2024, 8, 21, 12, 34, 56)
        expected = Path("/archive/2024/08/21/archived-20240821123456.mp4")
        self.assertEqual(get_output_path(inp, base, ts), expected)

    def test_get_output_path_diff_timestamp(self):
        inp = Path("/base/2024/01/15/REO_CAM_20240115000000.mp4")
        base = Path("/archive")
        ts = datetime(2024, 1, 15, 10, 30, 45)
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


class TestIntegration(unittest.TestCase):
    """Small integration tests that exercise the main workflow."""

    def setUp(self):
        self.input_dir = Path(tempfile.mkdtemp())
        self.output_dir = Path(tempfile.mkdtemp())

        day_folder = self.input_dir / "2024" / "08" / "21"
        day_folder.mkdir(parents=True, exist_ok=True)

        self.mp4 = day_folder / "REO_DRIVEWAY_01_20240821123456.mp4"
        self.jpg = day_folder / "REO_DRIVEWAY_01_20240821123456.jpg"

        self.mp4.write_bytes(b"fake mp4")
        self.jpg.write_bytes(b"fake jpg")

        old_ts = (datetime.now() - timedelta(days=35)).timestamp()
        os.utime(self.mp4, (old_ts, old_ts))
        os.utime(self.jpg, (old_ts, old_ts))

    def tearDown(self):
        shutil.rmtree(self.input_dir, ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def test_discovery_and_filtering(self):
        archiver = CameraArchiver(self.input_dir, self.output_dir, MagicMock())
        archiver.discover_files()
        self.assertEqual(len(archiver.all_files), 2)

        old = archiver.filter_old_files(30)
        self.assertEqual(len(old), 2)

        recent = archiver.filter_old_files(40)
        self.assertEqual(len(recent), 0)

    def test_output_path_generation(self):
        ts = datetime(2024, 8, 21, 12, 34, 56)
        out = get_output_path(self.mp4, self.output_dir, ts)
        expected = (
            self.output_dir / "2024" / "08" / "21" / "archived-20240821123456.mp4"
        )
        self.assertEqual(out, expected)


# ----------------------------------------------------------------------
# Test runner
# ----------------------------------------------------------------------


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
