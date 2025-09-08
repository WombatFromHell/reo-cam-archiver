#!/usr/bin/env python3
import io
import logging
import shutil
import sys
import tempfile
import subprocess
from datetime import datetime
from pathlib import Path
import unittest
from unittest.mock import patch

import archiver


class DummyStream:
    def __init__(self):
        self.written = []
        self.isatty_value = False

    def write(self, msg):
        self.written.append(msg)

    def flush(self):
        pass

    def isatty(self):
        return self.isatty_value


class DummyProgressBar:
    def start(self):  # pragma: no cover
        pass

    def finish(self):  # pragma: no cover
        pass

    def start_file(self):  # pragma: no cover
        pass

    def finish_file(self, idx):  # pragma: no cover
        pass

    def update_progress(self, idx, pct):  # pragma: no cover
        pass

    def start_processing(self):  # pragma: no cover
        pass


class TestTimestampParsing(unittest.TestCase):
    def test_parse_timestamp_from_filename_valid(self):
        name = "REO_cam_20231201010101.mp4"
        ts = archiver.parse_timestamp_from_filename(name)
        if ts:
            self.assertIsNotNone(ts)
            self.assertEqual(ts.year, 2023)
            self.assertEqual(ts.month, 12)
            self.assertEqual(ts.day, 1)

    def test_parse_timestamp_from_filename_out_of_range(self):
        name = "REO_cam_18991231235959.mp4"
        self.assertIsNone(archiver.parse_timestamp_from_filename(name))
        name2 = "REO_cam_21000101000000.mp4"
        self.assertIsNone(archiver.parse_timestamp_from_filename(name2))

    def test_parse_timestamp_from_filename_invalid(self):
        names = ["REO_cam_20231201.mp4", "something_else.txt"]
        for name in names:
            self.assertIsNone(archiver.parse_timestamp_from_filename(name))


class TestSafeRemove(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        self.logger = logging.getLogger(f"safe_remove_{self._testMethodName}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.addHandler(handler)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_safe_remove_dry_run(self):
        p = self.temp_dir / "test.mp4"
        p.write_text("data")
        archiver.safe_remove(p, self.logger, dry_run=True)
        self.assertTrue(p.exists())
        logs = self.log_capture.getvalue()
        self.assertIn("[DRY RUN] Would remove", logs)

    def test_safe_remove_actual(self):
        p = self.temp_dir / "test.mp4"
        p.write_text("data")
        archiver.safe_remove(p, self.logger, dry_run=False)
        self.assertFalse(p.exists())
        logs = self.log_capture.getvalue()
        self.assertIn("Removed:", logs)


class TestOutputPath(unittest.TestCase):
    def setUp(self):
        self.out_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.out_dir, ignore_errors=True)

    def test_output_path_with_parts(self):
        fp = Path("root/dir/sub1/sub2/file.mp4")
        ts = datetime(2023, 12, 1, 0, 0)
        y, m, d = fp.parts[-4:-1]
        expected = (
            self.out_dir / y / m / d / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        result = archiver.output_path(fp, ts, self.out_dir)
        self.assertEqual(result, expected)

    def test_output_path_without_parts(self):
        fp = Path("file.mp4")
        ts = datetime(2023, 12, 1, 0, 0)
        expected = (
            self.out_dir
            / "2023"
            / "12"
            / "01"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        result = archiver.output_path(fp, ts, self.out_dir)
        self.assertEqual(result, expected)


class TestProgressBar(unittest.TestCase):
    def test_progress_bar_non_silent(self):
        stream = DummyStream()
        stream.isatty_value = True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)
        self.assertTrue(any("Progress [1/1]:" in s for s in stream.written))
        before_len = len(stream.written)
        bar.update_progress(1, 50.0)
        after_len = len(stream.written)
        self.assertEqual(before_len, after_len)
        bar.finish()
        self.assertTrue(any("\x1b[999B\x1b[2K\r\n" in s for s in stream.written))

    def test_progress_bar_silent(self):
        stream = DummyStream()
        stream.isatty_value = True
        bar = archiver.ProgressBar(total_files=1, silent=True, out=stream)
        self.assertEqual(stream.written, [])
        bar.update_progress(1, 50.0)
        self.assertEqual(stream.written, [])
        bar.finish()
        self.assertEqual(stream.written, [])

    def test_non_tty_output(self):
        stream = DummyStream()
        stream.isatty_value = False  # simulate non‑TTY output
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)

        self.assertTrue(
            any(s.startswith("\r") and "Progress [1/1]:" in s for s in stream.written),
            msg=f"Expected a progress line, got {stream.written}",
        )

    def test_update_progress_no_change(self):
        stream = DummyStream()
        stream.isatty_value = True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)  # first write
        first_len = len(stream.written)
        bar.update_progress(1, 50.0)  # same pct – no new write
        self.assertEqual(len(stream.written), first_len)

    def test_finish_file_calls_update(self):
        stream = DummyStream()
        stream.isatty_value = True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.finish_file(1)  # should call update_progress with 100%
        self.assertTrue(any("100%" in s for s in stream.written))

    def test_start_file_sets_time(self):
        bar = archiver.ProgressBar(total_files=1, silent=True)
        self.assertIsNone(bar.file_start)
        bar.start_file()
        after = bar.file_start
        self.assertIsNotNone(after)


class TestLoggerSetup(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.log_file = self.temp_dir / "log.txt"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_setup_logging(self):
        import io
        from unittest.mock import patch

        with patch("archiver.sys.stdout", new=io.StringIO()):
            logger = archiver.setup_logging(self.log_file)
            logger.info("hello world")

        content = self.log_file.read_text()
        self.assertIn("hello world", content)


class TestGetVideoDuration(unittest.TestCase):
    @patch("shutil.which", return_value=None)
    def test_get_video_duration_no_ffprobe(self, mock_which):
        ts = archiver.get_video_duration(Path("dummy.mp4"))
        self.assertIsNone(ts)

    @patch("shutil.which")
    @patch("archiver.subprocess.check_output")
    def test_get_video_duration_success(self, mock_check_output, mock_which):
        mock_which.return_value = "/usr/bin/ffprobe"
        mock_check_output.return_value = "123.45\n"
        ts = archiver.get_video_duration(Path("dummy.mp4"))
        self.assertEqual(ts, 123.45)

    @patch("shutil.which")
    @patch(
        "archiver.subprocess.check_output",
        side_effect=subprocess.CalledProcessError(1, ["ffprobe"]),
    )
    def test_get_video_duration_error(self, mock_check_output, mock_which):
        mock_which.return_value = "/usr/bin/ffprobe"
        ts = archiver.get_video_duration(Path("dummy.mp4"))
        self.assertIsNone(ts)


class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        self.logger = logging.getLogger(f"process_{self._testMethodName}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.addHandler(handler)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=None)
    def test_process_files_skip_logic(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1, 0, 0)
        mp4_path = self.temp_dir / "video.mp4"
        mp4_path.write_text("data")
        jpg_path = self.temp_dir / "video.jpg"
        jpg_path.write_text("jpgdata")

        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()

        # existing archived file >1MB
        outp = archiver.output_path(mp4_path, ts, out_dir)
        outp.write_bytes(b"0" * (2_048_576))

        key = ts.strftime("%Y%m%d%H%M%S")
        mapping = {key: {".mp4": mp4_path, ".jpg": jpg_path}}

        mock_safe_remove.side_effect = (
            lambda p, logger, dry_run, **kw: p.unlink() if not dry_run else None
        )

        archiver.process_files(
            [(mp4_path, ts)],
            out_dir,
            self.logger,
            dry_run=False,
            no_skip=False,
            mapping=mapping,
            bar=DummyProgressBar(),
        )
        self.assertFalse(mp4_path.exists())
        self.assertFalse(jpg_path.exists())
        logs = self.log_capture.getvalue()
        self.assertIn("[SKIP] Existing archive large enough", logs)

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=True)
    def test_process_files_no_skip(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1, 0, 0)
        mp4_path = self.temp_dir / "video.mp4"
        mp4_path.write_text("data")
        jpg_path = self.temp_dir / "video.jpg"
        jpg_path.write_text("jpgdata")

        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()

        # existing archived file >1MB
        outp = archiver.output_path(mp4_path, ts, out_dir)
        outp.write_bytes(b"0" * (2_048_576))

        key = ts.strftime("%Y%m%d%H%M%S")
        mapping = {key: {".mp4": mp4_path, ".jpg": jpg_path}}

        mock_safe_remove.side_effect = (
            lambda p, logger, dry_run, **kw: p.unlink() if not dry_run else None
        )

        archiver.process_files(
            [(mp4_path, ts)],
            out_dir,
            self.logger,
            dry_run=False,
            no_skip=True,
            mapping=mapping,
            bar=DummyProgressBar(),
        )
        mock_transcode.assert_called_once()
        args = mock_transcode.call_args[0]
        self.assertEqual(args[0], mp4_path)
        self.assertEqual(args[1], outp)
        self.assertFalse(mp4_path.exists())
        self.assertFalse(jpg_path.exists())
        logs = self.log_capture.getvalue()
        self.assertIn("Transcoding", logs)

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=True)
    def test_process_files_dry_run(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1, 0, 0)
        mp4_path = self.temp_dir / "video.mp4"
        mp4_path.write_text("data")
        jpg_path = self.temp_dir / "video.jpg"
        jpg_path.write_text("jpgdata")

        out_dir = self.temp_dir / "archived"

        key = ts.strftime("%Y%m%d%H%M%S")
        mapping = {key: {".mp4": mp4_path, ".jpg": jpg_path}}

        archiver.process_files(
            [(mp4_path, ts)],
            out_dir,
            self.logger,
            dry_run=True,
            no_skip=False,
            mapping=mapping,
            bar=DummyProgressBar(),
        )
        mock_transcode.assert_not_called()
        mock_safe_remove.assert_not_called()
        logs = self.log_capture.getvalue()
        self.assertIn("[DRY RUN] Would transcode", logs)


class TestCleanupEmptyDirectories(unittest.TestCase):
    """Verify that only YYYY/MM/DD directories are removed."""

    def setUp(self):
        self.base_dir = Path(tempfile.mkdtemp())

        # Empty day directory – should be removed
        (self.base_dir / "2023" / "12" / "01").mkdir(parents=True)

        # Day with a nested empty sub‑directory – should *not* be removed
        (self.base_dir / "2023" / "12" / "02").mkdir(parents=True)
        (self.base_dir / "2023" / "12" / "02" / "sub").mkdir()

        # Random directory that does not match the pattern – should stay
        (self.base_dir / "random").mkdir()

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_remove_empty_day_dirs_only(self):
        archiver.clean_empty_directories(self.base_dir)
        self.assertFalse((self.base_dir / "2023" / "12" / "01").exists())

    def test_preserve_day_with_subdirs_and_files(self):
        archiver.clean_empty_directories(self.base_dir)
        # The day that had a sub‑directory should still exist
        self.assertTrue((self.base_dir / "2023" / "12" / "02").exists())
        self.assertTrue((self.base_dir / "2023" / "12" / "02" / "sub").exists())

    def test_preserve_random_directory(self):
        archiver.clean_empty_directories(self.base_dir)
        self.assertTrue((self.base_dir / "random").exists())


class TestCleanupEmptyDirectoriesOut(unittest.TestCase):
    """Same checks but for the archived output tree."""

    def setUp(self):
        self.out_dir = Path(tempfile.mkdtemp())

        # Empty day directory – should be removed
        (self.out_dir / "2023" / "12" / "01").mkdir(parents=True)

        # Day with a nested sub‑directory and a file – should stay
        day = self.out_dir / "2023" / "12" / "02"
        day.mkdir(parents=True)
        (day / "sub").mkdir()
        (day / "file.mp4").write_text("data")

    def tearDown(self):
        shutil.rmtree(self.out_dir, ignore_errors=True)

    def test_remove_empty_day_dirs_only(self):
        archiver.clean_empty_directories(self.out_dir)
        self.assertFalse((self.out_dir / "2023" / "12" / "01").exists())

    def test_preserve_nonempty_day(self):
        archiver.clean_empty_directories(self.out_dir)
        day = self.out_dir / "2023" / "12" / "02"
        self.assertTrue(day.exists())
        self.assertTrue((day / "sub").exists())
        self.assertTrue((day / "file.mp4").exists())


class TestMainCleanupRandomDirectories(unittest.TestCase):
    """Confirm that main() never deletes directories that do not match the pattern."""

    def setUp(self):
        self.input_dir = Path(tempfile.mkdtemp())
        self.output_dir = Path(tempfile.mkdtemp())

        # Random directory in the input tree
        (self.input_dir / "random").mkdir()
        (self.input_dir / "random" / "file.txt").write_text("data")

        # Random directory in the output tree
        (self.output_dir / "randombak").mkdir()
        (self.output_dir / "randombak" / "archive.mp4").write_text("data")

    def tearDown(self):
        shutil.rmtree(self.input_dir, ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    @patch("archiver.setup_logging")
    def test_cleanup_random_dirs_not_removed(
        self,
        mock_setup_logging,
        mock_remove_orphaned,
        mock_process_files,
        mock_scan_files,
    ):
        # Dummy logger that simply records messages
        class DummyLogger:
            def __init__(self):
                self.messages = []

            def info(self, msg):
                self.messages.append(msg)

        dummy_logger = DummyLogger()
        mock_setup_logging.return_value = dummy_logger

        original_argv = sys.argv
        try:
            sys.argv = [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
            ]
            archiver.main()
        finally:
            sys.argv = original_argv

        # Random directories should still exist after main()
        self.assertTrue((self.input_dir / "random").exists())
        self.assertTrue((self.input_dir / "random" / "file.txt").exists())

        self.assertTrue((self.output_dir / "randombak").exists())
        self.assertTrue((self.output_dir / "randombak" / "archive.mp4").exists())


class TestRemoveOrphanedJPGs(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.logger_capture = io.StringIO()
        handler = logging.StreamHandler(self.logger_capture)
        self.logger = logging.getLogger(f"remove_{self._testMethodName}")
        self.logger.handlers.clear()
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("archiver.safe_remove")
    def test_remove_orphaned_jpgs(self, mock_safe_remove):
        orphan_path = self.temp_dir / "orphan.jpg"
        orphan_path.write_text("jpgdata")

        mapping = {"20231201000000": {".jpg": orphan_path}}
        processed = set()

        archiver.remove_orphaned_jpgs(mapping, processed, self.logger, dry_run=False)
        mock_safe_remove.assert_called_once_with(
            orphan_path, self.logger, False, use_trash=False, trash_root=None
        )

        logs = self.logger_capture.getvalue()
        self.assertIn("Found orphaned JPG", logs)


class TestTrashBehavior(unittest.TestCase):
    """Verify that files and empty directories are moved to the trash directory."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        # Trash root inside the temporary directory for isolation
        self.trash_root = self.temp_dir / ".Deleted"
        self.trash_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_safe_remove_moves_file_to_trash(self):
        p = self.temp_dir / "file.txt"
        p.write_text("data")
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger(f"trash_test_{self._testMethodName}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(handler)

        archiver.safe_remove(
            p, logger, dry_run=False, use_trash=True, trash_root=self.trash_root
        )

        self.assertFalse(p.exists())
        dest = self.trash_root / "input" / p.name  # <trash_root>/input/<file>
        self.assertTrue(dest.exists())
        logs = log_capture.getvalue()
        self.assertIn("Moved to trash", logs)

    def test_cleanup_empty_directories_moves_dir_to_trash(self):
        empty_day = self.temp_dir / "2023" / "12" / "01"
        empty_day.mkdir(parents=True)
        archiver.clean_empty_directories(
            self.temp_dir, None, use_trash=True, trash_root=self.trash_root
        )
        # original day directory should be gone
        self.assertFalse(empty_day.exists())
        dest = self.trash_root / "01"
        self.assertTrue(dest.is_dir())

    def test_safe_remove_dry_run_when_use_trash(self):
        p = self.temp_dir / "file.txt"
        p.write_text("data")
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        logger = logging.getLogger(f"dryrun_test_{self._testMethodName}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.addHandler(handler)

        archiver.safe_remove(
            p, logger, dry_run=True, use_trash=True, trash_root=self.trash_root
        )

        self.assertTrue(p.exists())
        dest = self.trash_root / p.name
        self.assertFalse(dest.exists())
        logs = log_capture.getvalue()
        self.assertIn("[DRY RUN] Would remove", logs)


class TestMainFunction(unittest.TestCase):
    def setUp(self):
        self.input_dir = Path(tempfile.mkdtemp())
        self.output_dir = Path(tempfile.mkdtemp())

        for i in range(5):
            f = self.output_dir / f"archived-{i}.mp4"
            f.write_bytes(b"0" * (2_048_576))

    def tearDown(self):
        shutil.rmtree(self.input_dir, ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    @patch("archiver.setup_logging")
    def test_archive_size_limit_removal(
        self,
        mock_setup_logging,
        mock_remove_orphaned,
        mock_process_files,
        mock_scan_files,
    ):
        class DummyLogger:
            def __init__(self):
                self.messages = []

            def info(self, msg):
                self.messages.append(msg)

        dummy_logger = DummyLogger()
        mock_setup_logging.return_value = dummy_logger

        original_argv = sys.argv
        try:
            sys.argv = [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
                "--age",
                "0",
                "--max-size",
                "0",
            ]
            archiver.main()
        finally:
            sys.argv = original_argv

        self.assertFalse(any(self.output_dir.iterdir()))
        msgs = dummy_logger.messages
        self.assertTrue(any("Archive size exceeds limit" in m for m in msgs))
        self.assertTrue(any("Removed old archive:" in m for m in msgs))


if __name__ == "__main__":
    unittest.main(verbosity=2)
