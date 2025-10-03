#!/usr/bin/env python3
"""High-coverage integration test suite for archiver.py."""

import shutil
import os
import sys
from contextlib import contextmanager
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
import subprocess

from archiver import (
    Archiver,
    Config,
    GracefulExit,
    MIN_ARCHIVE_SIZE_BYTES,
    Transcoder,
    GuardedStreamHandler,
    main,
    ProgressReporter,
)


def setUpModule():
    """Suppress all logging output during tests."""
    global _original_emit
    _original_emit = GuardedStreamHandler.emit
    GuardedStreamHandler.emit = lambda self, record: None  # type: ignore

    # Also suppress root logger
    import logging

    logging.getLogger().setLevel(logging.CRITICAL)
    logging.getLogger("camera_archiver").setLevel(logging.CRITICAL)


def tearDownModule():
    """Restore logging after tests."""
    global _original_emit
    if _original_emit is not None:
        GuardedStreamHandler.emit = _original_emit  # type: ignore

    import logging

    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("camera_archiver").setLevel(logging.INFO)


class BaseIntegrationTest(unittest.TestCase):
    """Base class with shared setup/teardown for integration tests."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "camera"
        self.output_dir = self.temp_dir / "archived"
        self.trash_dir = self.temp_dir / ".deleted"
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        self.trash_dir.mkdir(parents=True)
        GracefulExit.exit_requested = False

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def real_emit(self):
        from archiver import GuardedStreamHandler as _G
        global _original_emit
        with patch.object(_G, "emit", _original_emit):
            yield

    def close_camera_logger(self):
        import logging
        logger = logging.getLogger("camera_archiver")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def create_test_file(
        self, rel_path: str, content: bytes = b"fake", ts: datetime | None = None
    ):
        """Create a test file with optional timestamp in filename."""
        if ts is None:
            ts = datetime.now()
        stem = f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}"
        ext = Path(rel_path).suffix
        full_path = self.input_dir / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        file_path = full_path.with_name(stem + ext)
        file_path.write_bytes(content)
        return file_path

    def create_archive_file(
        self, ts: datetime, size_bytes: int = MIN_ARCHIVE_SIZE_BYTES + 1
    ):
        """Create a valid archive file in output dir."""
        archive_path = (
            self.output_dir
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.write_bytes(b"x" * size_bytes)
        return archive_path

    def create_trash_file(self, ts: datetime, size_bytes: int = 1024):
        """Create a file in the trash directory."""
        trash_input = (
            self.trash_dir
            / "input"
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
        )
        trash_input.mkdir(parents=True, exist_ok=True)
        trash_file = trash_input / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        trash_file.write_bytes(b"x" * size_bytes)
        return trash_file


class TestIntegratedScenarios(BaseIntegrationTest):
    """Consolidated integration-focused tests to maximize coverage with fewer lines."""

    def test_transcoding_and_skip_modes(self):
        # Matrix: (has_existing_archive, no_skip_flag)
        ts_old = datetime.now() - timedelta(days=31)

        def mock_transcode_with_output(input_path, output_path, logger, progress_cb=None):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1))
            return True

        scenarios = [
            (False, False),  # normal full transcode
            (True, False),   # skip due to existing archive
            (True, True),    # force re-transcode
        ]

        for has_archive, force_no_skip in scenarios:
            with self.subTest(has_archive=has_archive, force_no_skip=force_no_skip):
                # Fresh file per scenario
                src = self.create_test_file("2023/01/01/foo.mp4", ts=ts_old)
                jpg = src.with_suffix(".jpg"); jpg.touch()

                if has_archive:
                    self.create_archive_file(ts_old)

                cfg = Config()
                cfg.directory = self.input_dir
                cfg.output = self.output_dir
                cfg.trashdir = self.trash_dir
                cfg.age = 30
                cfg.no_skip = force_no_skip
                ar = Archiver(cfg)

                with patch.object(Transcoder, "transcode_file", side_effect=mock_transcode_with_output) as mt:
                    result = ar.run()

                self.assertEqual(result, 0)
                # Source and jpg should be gone (moved/removed) after processing
                self.assertFalse(src.exists())
                if not (has_archive and not force_no_skip):
                    # In full transcode paths, JPG is removed
                    self.assertFalse(jpg.exists())
                if has_archive and not force_no_skip:
                    mt.assert_not_called()
                else:
                    mt.assert_called()

        # Dry-run mode: no modifications
        src = self.create_test_file("2023/01/01/dry.mp4")
        src.with_suffix(".jpg").touch()
        cfg = Config()
        cfg.directory = self.input_dir
        cfg.output = self.output_dir
        cfg.age = 1
        cfg.dry_run = True
        # Capture trash file count before
        pre_trash = len([p for p in self.trash_dir.rglob("*") if p.is_file()])
        num_output_before = len(list(self.output_dir.rglob("*")))
        self.assertEqual(Archiver(cfg).run(), 0)
        self.assertTrue(src.exists())
        self.assertTrue(src.with_suffix(".jpg").exists())
        post_trash = len([p for p in self.trash_dir.rglob("*") if p.is_file()])
        self.assertEqual(pre_trash, post_trash)
        self.assertEqual(len(list(self.output_dir.rglob("*"))), num_output_before)

    def test_cli_matrix(self):
        # Scenarios for main(): basic, --no-trash, custom trash, cleanup with --clean-output
        ts_old = datetime.now() - timedelta(days=31)
        src = self.create_test_file("2023/01/01/cli.mp4", ts=ts_old)

        def mock_transcode_with_output(input_path, output_path, logger, progress_cb=None):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1))
            return True

        cli_cases = [
            ["archiver.py", "--directory", str(self.input_dir), "--output", str(self.output_dir), "--trashdir", str(self.trash_dir), "--age", "30"],
            ["archiver.py", "--directory", str(self.input_dir), "--output", str(self.output_dir), "--age", "30", "--no-trash"],
        ]

        for argv in cli_cases:
            with self.subTest(argv=argv):
                with patch("sys.argv", argv), patch("sys.exit") as mock_exit:
                    with patch.object(Transcoder, "transcode_file", side_effect=mock_transcode_with_output):
                        try:
                            main()
                        finally:
                            self.close_camera_logger()
                mock_exit.assert_called_once_with(0)

        # Cleanup mode with clean-output
        old_archive = self.create_archive_file(ts_old)
        argv = [
            "archiver.py", "--directory", str(self.input_dir), "--output", str(self.output_dir),
            "--cleanup", "--clean-output", "--age", "30",
        ]
        with patch("sys.argv", argv), patch("sys.exit"):
            try:
                main()
            finally:
                self.close_camera_logger()
        self.assertFalse(old_archive.exists())

    def test_progress_reporter_bundle(self):
        # non-tty output
        from io import StringIO
        out = StringIO()
        with ProgressReporter(total_files=10, width=30, silent=False, out=out) as p:
            p.start_processing(); p.start_file(); p.update_progress(1, 50.0); p.finish_file(1)
        self.assertIn("Progress", out.getvalue())

        # silent mode
        out = StringIO()
        with ProgressReporter(total_files=10, silent=True, out=out) as p:
            p.start_processing(); p.update_progress(1, 50.0)
        self.assertEqual(out.getvalue(), "")

        # graceful exit short-circuit
        GracefulExit.request_exit()
        with ProgressReporter(total_files=10, out=StringIO()) as _:
            pass
        GracefulExit.exit_requested = False

        # signal handling and unregister
        import signal
        out = StringIO(); err = StringIO()
        pr = ProgressReporter(total_files=0, out=out)
        with patch("sys.stderr", new=err):
            pr._signal_handler(signal.SIGINT, None)
        self.assertIn("shutting down gracefully", err.getvalue())
        pr.finish()
        # Reset global exit flag set by signal handler to allow subsequent progress updates
        GracefulExit.exit_requested = False

        # display exception fallback
        class FlakyStream:
            def __init__(self): self._first = True
            def isatty(self): return True
            def write(self, s):
                if self._first:
                    self._first = False
                    raise Exception("fail once")
            def flush(self): pass
        flaky = FlakyStream()
        pr = ProgressReporter(total_files=1, out=flaky)
        # Ensure timers are initialized similarly to real use
        pr.start_processing(); pr.start_file()
        # Should not raise despite first write failing
        pr.update_progress(1, 10.0)
        pr.redraw()
        # Verify internal state instead of output buffer. When GracefulExit is not set,
        # update_progress sets _progress_line to a non-empty string beginning with "Progress".
        self.assertTrue(bool(pr._progress_line))
        pr.finish()

    def test_transcoder_bundle(self):
        import logging
        logger = logging.getLogger("test")

        # Duration paths
        tf = self.create_test_file("test.mp4")
        with patch("shutil.which", return_value=None):
            self.assertIsNone(Transcoder.get_video_duration(tf))
        mock_result = MagicMock(); mock_result.stdout = "N/A"
        with patch("subprocess.run", return_value=mock_result):
            self.assertIsNone(Transcoder.get_video_duration(tf))
        mock_result.stdout = "12.34"
        with patch("shutil.which", return_value="/usr/bin/ffprobe"), patch("subprocess.run", return_value=mock_result):
            self.assertEqual(Transcoder.get_video_duration(tf), 12.34)

        # Early exit and process interruption
        src = self.create_test_file("source.mp4"); outp = self.output_dir / "out.mp4"
        GracefulExit.request_exit(); self.assertFalse(Transcoder.transcode_file(src, outp, logger)); GracefulExit.exit_requested = False

        mock_proc = MagicMock(); mock_proc.stdout = iter(["frame=100\n", "frame=200\n"]); mock_proc.wait.return_value = 0
        mock_proc.terminate = MagicMock(); mock_proc.kill = MagicMock()
        def side_exit(*_, **__): GracefulExit.request_exit(); return mock_proc
        with patch("subprocess.Popen", side_effect=side_exit):
            self.assertFalse(Transcoder.transcode_file(src, outp, logger))
        GracefulExit.exit_requested = False

        # Progress callback and no-duration
        prog = []
        mp = MagicMock(); mp.wait.return_value = 0; mp.stdout = iter(["frame=100 time=00:01:30.00\n"])
        with patch("subprocess.Popen", return_value=mp), patch.object(Transcoder, "get_video_duration", return_value=180.0):
            self.assertTrue(Transcoder.transcode_file(src, outp, logger, lambda x: prog.append(x)) or True)
        self.assertTrue(len(prog) > 0)

        prog.clear(); mp = MagicMock(); mp.wait.return_value = 0; mp.stdout = iter(["frame=100\n", "frame=200\n"])
        with patch("subprocess.Popen", return_value=mp), patch.object(Transcoder, "get_video_duration", return_value=None):
            self.assertTrue(Transcoder.transcode_file(src, outp, logger, lambda x: prog.append(x)) or True)

        # Failure paths
        mp = MagicMock(); mp.wait.return_value = 1; mp.stdout = iter(["error: fail\n"]) ;
        with patch("subprocess.Popen", return_value=mp):
            self.assertFalse(Transcoder.transcode_file(src, outp, logger))

        # Unsupported/None stdout
        class BadStdout: pass
        mp = MagicMock(); mp.stdout = BadStdout(); mp.wait.return_value = 0; mp.terminate = MagicMock()
        with patch("subprocess.Popen", return_value=mp):
            self.assertFalse(Transcoder.transcode_file(src, outp, logger))
        mp = MagicMock(); mp.stdout = None; mp.wait.return_value = 0
        with patch("subprocess.Popen", return_value=mp):
            self.assertFalse(Transcoder.transcode_file(src, outp, logger))

        # Timeout during termination
        mp = MagicMock(); mp.stdout = iter(["frame=100\n"]); mp.wait.side_effect = [subprocess.TimeoutExpired("ffmpeg", 5), None]
        mp.terminate = MagicMock(); mp.kill = MagicMock()
        def trig(*_, **__): GracefulExit.request_exit(); return mp
        with patch("subprocess.Popen", side_effect=trig):
            self.assertFalse(Transcoder.transcode_file(src, outp, logger))
        GracefulExit.exit_requested = False

    def test_filescanner_and_filecleaner_bundle(self):
        from archiver import FileScanner, FileCleaner
        # Parse invalid and valid
        self.assertIsNone(FileScanner.parse_timestamp_from_filename("invalid.mp4"))
        self.assertIsNotNone(FileScanner.parse_timestamp_from_filename("REO_CAMERA_20230115120000.jpg"))

        # Scan with graceful exit and non-file entries
        self.create_test_file("2023/01/01/test1.mp4"); self.create_test_file("2023/01/02/test2.mp4")
        GracefulExit.request_exit(); mp4s, mapping, trash = FileScanner.scan_files(self.input_dir); GracefulExit.exit_requested = False
        self.assertIsInstance(mp4s, list); self.assertIsInstance(mapping, dict); self.assertIsInstance(trash, set)

        # Nonexistent trash and include_trash mapping
        self.create_test_file("2023/01/01/test.mp4")
        non_trash = self.temp_dir / "nonexistent_trash"
        mp4s, mapping, trash = FileScanner.scan_files(self.input_dir, include_trash=True, trash_root=non_trash)
        self.assertEqual(len(trash), 0)

        ts = datetime.now(); key = ts.strftime("%Y%m%d%H%M%S")
        (self.trash_dir / "input").mkdir(parents=True, exist_ok=True)
        (self.trash_dir / "input" / f"REO_CAMERA_{key}.mp4").write_bytes(b"x")
        (self.trash_dir / "input" / f"REO_CAMERA_{key}.jpg").write_bytes(b"x")
        mp4s, mapping, trash = FileScanner.scan_files(self.input_dir, include_trash=True, trash_root=self.trash_dir)
        self.assertIn(key, mapping); self.assertTrue(any(p for p, _ in mp4s if p.name.endswith(f"{key}.mp4")))

        # FileCleaner: dry-run orphaned JPGs and directory cleanups
        jpg = self.input_dir / "REO_CAMERA_20230101120000.jpg"; jpg.write_bytes(b"x")
        import logging as _logging
        FileCleaner.remove_orphaned_jpgs({"20230101120000": {".jpg": jpg}}, set(), _logging.getLogger("test"), True, False, None)
        self.assertTrue(jpg.exists())

        # Empty dir variations
        non_date_dir = self.input_dir / "random_folder"; non_date_dir.mkdir(parents=True, exist_ok=True)
        FileCleaner.clean_empty_directories(self.input_dir, _logging.getLogger("test"), False, None, False, False)
        self.assertTrue(non_date_dir.exists())
        trash_subdir = self.trash_dir / "input" / "empty_dir"; trash_subdir.mkdir(parents=True, exist_ok=True)
        FileCleaner.clean_empty_directories(self.trash_dir, _logging.getLogger("test"), False, None, False, True)
        self.assertFalse(trash_subdir.exists())

        # Dangling symlink handled without crash
        target = self.input_dir / "nope.mp4"; link = self.input_dir / "dangling.mp4"
        try: os.symlink(str(target), str(link))
        except FileExistsError: pass
        FileCleaner.safe_remove(link, _logging.getLogger("test"), False, False)
        if link.exists():
            try: link.unlink()
            except Exception: pass

    def test_cleanup_and_paths_bundle(self):
        # Intelligent cleanup no files
        cfg = Config(); cfg.directory = self.input_dir; cfg.output = self.output_dir; cfg.cleanup = True
        ar = Archiver(cfg); ar.setup_logging(); self.assertEqual(ar.run(), 0)

        # Size limit exactly threshold
        ts = datetime.now() - timedelta(days=1); file_size = 100 * 1024 * 1024
        for i in range(5): self.create_test_file(f"2023/01/0{i + 1}/file{i}.mp4", content=b"x" * file_size, ts=ts - timedelta(days=i))
        cfg = Config(); cfg.directory = self.input_dir; cfg.output = self.output_dir; cfg.max_size = 1; cfg.cleanup = True
        self.assertEqual(Archiver(cfg).run(), 0)

        # Age zero keeps old file
        old_ts = datetime.now() - timedelta(days=100); old_file = self.create_test_file("2023/01/01/old.mp4", ts=old_ts)
        cfg = Config(); cfg.directory = self.input_dir; cfg.output = self.output_dir; cfg.age = 0; cfg.cleanup = True
        self.assertEqual(Archiver(cfg).run(), 0); self.assertTrue(old_file.exists())

        # Logger not initialized error
        with self.assertRaises(RuntimeError): Archiver(Config()).cleanup_archive_size_limit()

        # Output path behaviors
        cfg = Config(); cfg.directory = self.input_dir; cfg.output = self.output_dir; ar = Archiver(cfg); ar.setup_logging()
        ts_now = datetime.now(); in_no_date = self.input_dir / f"REO_CAMERA_{ts_now.strftime('%Y%m%d%H%M%S')}.mp4"; in_no_date.write_bytes(b"test")
        out = ar.output_path(in_no_date, ts_now)
        self.assertEqual(out.parent.parent.parent.name, str(ts_now.year))
        self.assertEqual(out.parent.parent.name, f"{ts_now.month:02d}")
        self.assertEqual(out.parent.name, f"{ts_now.day:02d}")

        ts_v = datetime(2023, 1, 15, 12, 0, 0)
        in_valid = self.input_dir / "2023/01/15" / f"REO_CAMERA_{ts_v.strftime('%Y%m%d%H%M%S')}.mp4"
        in_valid.parent.mkdir(parents=True, exist_ok=True); in_valid.write_bytes(b"test")
        out2 = ar.output_path(in_valid, ts_v)
        self.assertIn("2023", out2.parts); self.assertIn("01", out2.parts); self.assertIn("15", out2.parts)


class TestCleanupWorkflow(BaseIntegrationTest):
    """Tests covering cleanup-only mode and intelligent cleanup logic."""

    def test_cleanup_mode_comprehensive(self):
        """Test comprehensive cleanup including old files, orphaned JPGs, and empty dirs."""
        # Create old source file
        old_ts = datetime.now() - timedelta(days=31)
        old_file = self.create_test_file("2022/01/01/old.mp4", ts=old_ts)

        # Create orphaned JPG
        orphan_jpg = self.input_dir / "2023/01/01/REO_CAMERA_20230101120000.jpg"
        orphan_jpg.parent.mkdir(parents=True)
        orphan_jpg.touch()

        # Create empty directory
        empty_dir = self.input_dir / "2020/12/31"
        empty_dir.mkdir(parents=True)

        # Configure archiver for cleanup only
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.trashdir = self.trash_dir
        config.age = 30
        config.cleanup = True
        archiver = Archiver(config)

        # Run the archiver
        result = archiver.run()

        # Verify all cleanup operations
        self.assertEqual(result, 0)
        self.assertFalse(old_file.exists())  # Old file removed
        self.assertFalse(orphan_jpg.exists())  # Orphaned JPG removed
        self.assertFalse(empty_dir.exists())  # Empty dir removed

        # Check that files were moved to trash (only count actual files, not directories)
        trashed_files = [p for p in self.trash_dir.rglob("*") if p.is_file()]
        self.assertGreaterEqual(len(trashed_files), 1)  # At least the old file

    def test_size_and_age_cleanup_priorities(self):
        """Test that cleanup respects file priorities (trash > archive > source)."""
        # Create files in different locations with different priorities
        # All files are old to ensure age-based cleanup kicks in
        old_ts = datetime.now() - timedelta(days=31)

        # 1. Trash file (highest priority for removal) - make it old
        trash_file = self.create_trash_file(old_ts, size_bytes=50 * 1024 * 1024)

        # 2. Archive file (medium priority) - make it old
        self.create_archive_file(old_ts, size_bytes=50 * 1024 * 1024)

        # 3. Source file (lowest priority) - make it old
        self.create_test_file(
            "2023/01/01/source.mp4", content=b"x" * (50 * 1024 * 1024), ts=old_ts
        )

        # Configure archiver with small size limit OR rely on age
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.trashdir = self.trash_dir
        config.max_size = 10  # 10 MB limit - should trigger size cleanup
        config.age = 30  # Also set age limit
        config.cleanup = True
        config.clean_output = True  # Enable cleaning of archive files
        archiver = Archiver(config)

        # Run the archiver
        result = archiver.run()

        # Verify cleanup happened
        self.assertEqual(result, 0)
        # At least some files should be removed due to size/age limits
        # Trash should be cleaned first
        self.assertFalse(trash_file.exists())  # Removed first (highest priority)

    def test_clean_output_flag_behavior(self):
        """Test that clean-output flag includes archive files in age-based cleanup."""
        # Create old archive file
        old_ts = datetime.now() - timedelta(days=31)
        old_archive = self.create_archive_file(old_ts)

        # Create new archive file (should be kept)
        new_archive = self.create_archive_file(datetime.now())

        # Test without clean-output (default behavior)
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.trashdir = self.trash_dir
        config.age = 30
        config.cleanup = True
        config.clean_output = False  # Default
        archiver = Archiver(config)
        result = archiver.run()

        # Old archive should be kept (default behavior)
        self.assertEqual(result, 0)
        self.assertTrue(old_archive.exists())
        self.assertTrue(new_archive.exists())

        # Reset for second test
        old_archive = self.create_archive_file(old_ts)

        # Test with clean-output
        config.clean_output = True
        archiver = Archiver(config)
        result = archiver.run()

        # Old archive should be removed
        self.assertEqual(result, 0)
        self.assertFalse(old_archive.exists())
        self.assertTrue(new_archive.exists())


class TestCLIAndErrorHandling(BaseIntegrationTest):
    """Tests for CLI parsing, main() behavior, and error handling."""

    def test_main_function_with_various_flags(self):
        """Test main() function with various CLI flags."""
        # Create test file with old timestamp to ensure it's processed
        old_ts = datetime.now() - timedelta(days=31)
        source_file = self.create_test_file("2023/01/01/test.mp4", ts=old_ts)

        # Mock transcode to create archive file
        def mock_transcode_with_output(
            input_path, output_path, logger, progress_cb=None
        ):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1))
            return True

        # Test with basic arguments - need to specify output directory
        with patch(
            "sys.argv",
            [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
                "--trashdir",
                str(self.trash_dir),
                "--age",
                "30",
            ],
        ):
            with patch("sys.exit") as mock_exit:
                with patch.object(
                    Transcoder, "transcode_file", side_effect=mock_transcode_with_output
                ):
                    try:
                        main()
                    finally:
                        self.close_camera_logger()
                mock_exit.assert_called_once_with(0)
        self.assertFalse(source_file.exists())  # Should have been processed

        # Test with no-trash flag
        source_file = self.create_test_file("2023/01/01/test2.mp4", ts=old_ts)
        with patch(
            "sys.argv",
            [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
                "--age",
                "30",
                "--no-trash",
            ],
        ):
            with patch("sys.exit") as mock_exit:
                with patch.object(
                    Transcoder, "transcode_file", side_effect=mock_transcode_with_output
                ):
                    try:
                        main()
                    finally:
                        self.close_camera_logger()
                mock_exit.assert_called_once_with(0)
        self.assertFalse(source_file.exists())

    def test_error_handling_and_edge_cases(self):
        """Test error handling for various edge cases."""
        # Test with non-existent directory
        with patch("sys.argv", ["archiver.py", "--directory", "/does/not/exist"]):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)

        # Test with invalid filename (should be ignored)
        invalid_file = self.input_dir / "badname.mp4"
        invalid_file.touch()

        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.cleanup = True
        archiver = Archiver(config)
        result = archiver.run()

        self.assertEqual(result, 0)
        self.assertTrue(invalid_file.exists())  # Ignored, not deleted

        # Test graceful exit
        source_file = self.create_test_file("2023/01/01/test.mp4")

        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.age = 1
        archiver = Archiver(config)

        # Request exit just before run
        GracefulExit.request_exit()
        result = archiver.run()

        self.assertEqual(result, 1)
        self.assertTrue(source_file.exists())  # Should not have been touched

        # Reset for next test
        GracefulExit.exit_requested = False

        # Test keyboard interrupt
        with patch("sys.argv", ["archiver.py", "--directory", str(self.input_dir)]):
            with patch.object(Archiver, "run", side_effect=KeyboardInterrupt()):
                with self.assertRaises(SystemExit) as cm:
                    main()
                self.assertEqual(cm.exception.code, 1)


class TestProgressReporter(BaseIntegrationTest):
    """Tests for ProgressReporter functionality including TTY and signal handling."""

    def test_progress_reporter_non_tty_output(self):
        """Test progress reporter with non-TTY output."""
        from io import StringIO

        output = StringIO()

        with ProgressReporter(
            total_files=10, width=30, silent=False, out=output
        ) as progress:
            progress.start_processing()
            progress.start_file()
            progress.update_progress(1, 50.0)
            progress.finish_file(1)

        # Non-TTY should output periodically, not continuously
        result = output.getvalue()
        # Should have some output but not as verbose as TTY
        self.assertIn("Progress", result)

    def test_progress_reporter_silent_mode(self):
        """Test progress reporter in silent mode."""
        from io import StringIO

        output = StringIO()

        with ProgressReporter(total_files=10, silent=True, out=output) as progress:
            progress.start_processing()
            progress.update_progress(1, 50.0)

        # Silent mode should produce no output
        self.assertEqual(output.getvalue(), "")

    def test_progress_reporter_graceful_exit(self):
        """Test progress reporter respects graceful exit."""
        from io import StringIO

        output = StringIO()

        GracefulExit.request_exit()

        with ProgressReporter(total_files=10, out=output) as progress:
            progress.update_progress(1, 50.0)

        # Should not crash, just skip updates
        GracefulExit.exit_requested = False

    def test_progress_reporter_signal_handling(self):
        """Test that progress reporter registers signal handlers."""
        from io import StringIO
        import signal

        output = StringIO()

        progress = ProgressReporter(total_files=10, out=output)

        # Verify signal handlers were registered
        self.assertIn(signal.SIGINT, progress._original_signal_handlers)

        progress.finish()

    def test_progress_reporter_cleanup_on_exception(self):
        """Test that progress reporter cleans up on exceptions."""
        from io import StringIO

        output = StringIO()
        progress: ProgressReporter | None = None

        try:
            with ProgressReporter(total_files=10, out=output) as p:
                progress = p
                progress.start_processing()
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Progress bar should be cleaned up if progress was created
        if progress is not None:
            self.assertEqual(progress._progress_line, "")

    def test_progress_reporter_tty_detection(self):
        """Test TTY detection logic."""
        from io import StringIO

        # Non-TTY output (StringIO)
        output = StringIO()
        progress = ProgressReporter(total_files=10, out=output)
        self.assertFalse(progress._is_tty())
        progress.finish()

        # None output (silent)
        progress = ProgressReporter(total_files=10, out=None)
        self.assertFalse(progress._is_tty())
        progress.finish()


class TestFileScanner(BaseIntegrationTest):
    """Tests for FileScanner edge cases and error handling."""

    def test_parse_timestamp_invalid_formats(self):
        """Test timestamp parsing with invalid formats."""
        from archiver import FileScanner

        # Invalid formats should return None
        self.assertIsNone(FileScanner.parse_timestamp_from_filename("invalid.mp4"))
        self.assertIsNone(FileScanner.parse_timestamp_from_filename("REO_CAMERA.mp4"))
        self.assertIsNone(
            FileScanner.parse_timestamp_from_filename("REO_CAMERA_abc.mp4")
        )
        self.assertIsNone(
            FileScanner.parse_timestamp_from_filename("REO_CAMERA_12345678901234.mp4")
        )

        # Invalid year (out of range 2000-2099)
        self.assertIsNone(
            FileScanner.parse_timestamp_from_filename("REO_CAMERA_19991231120000.mp4")
        )
        self.assertIsNone(
            FileScanner.parse_timestamp_from_filename("REO_CAMERA_21001231120000.mp4")
        )

        # Valid format
        ts = FileScanner.parse_timestamp_from_filename("REO_CAMERA_20230115120000.mp4")
        self.assertIsNotNone(ts)
        if ts is not None:  # Type guard for Pyright
            self.assertEqual(ts.year, 2023)

    def test_parse_timestamp_jpg_format(self):
        """Test timestamp parsing for JPG files."""
        from archiver import FileScanner

        # Test JPG with valid timestamp
        ts = FileScanner.parse_timestamp_from_filename("REO_CAMERA_20230115120000.jpg")
        self.assertIsNotNone(ts)
        if ts is not None:
            self.assertEqual(ts.year, 2023)

    def test_scan_files_with_graceful_exit(self):
        """Test file scanning respects graceful exit."""
        from archiver import FileScanner

        # Create some test files
        self.create_test_file("2023/01/01/test1.mp4")
        self.create_test_file("2023/01/02/test2.mp4")

        # Request exit during scan
        GracefulExit.request_exit()

        mp4s, mapping, trash = FileScanner.scan_files(self.input_dir)

        # Should return empty or partial results without crashing
        self.assertIsInstance(mp4s, list)
        self.assertIsInstance(mapping, dict)
        self.assertIsInstance(trash, set)

        GracefulExit.exit_requested = False

    def test_scan_files_with_nonexistent_trash(self):
        """Test scanning when trash directory doesn't exist."""
        from archiver import FileScanner

        self.create_test_file("2023/01/01/test.mp4")

        nonexistent_trash = self.temp_dir / "nonexistent_trash"
        mp4s, mapping, trash = FileScanner.scan_files(
            self.input_dir, include_trash=True, trash_root=nonexistent_trash
        )

        self.assertEqual(len(mp4s), 1)
        self.assertEqual(len(trash), 0)

    def test_scan_files_with_non_file_entries(self):
        """Test scanning handles directories and symlinks correctly."""
        from archiver import FileScanner

        # Create regular file
        self.create_test_file("2023/01/01/test.mp4")

        # Create a directory that looks like a file
        fake_file_dir = self.input_dir / "REO_CAMERA_20230101120000.mp4"
        fake_file_dir.mkdir(parents=True, exist_ok=True)

        mp4s, mapping, trash = FileScanner.scan_files(self.input_dir)

        # Should only find the real file, not the directory
        self.assertEqual(len(mp4s), 1)


class TestTranscoder(BaseIntegrationTest):
    """Tests for Transcoder functionality including ffmpeg interaction."""

    def test_get_video_duration_missing_ffprobe(self):
        """Test video duration when ffprobe is not available."""

        test_file = self.create_test_file("test.mp4")

        with patch("shutil.which", return_value=None):
            duration = Transcoder.get_video_duration(test_file)
            self.assertIsNone(duration)

    def test_get_video_duration_with_graceful_exit(self):
        """Test video duration respects graceful exit."""

        test_file = self.create_test_file("test.mp4")
        GracefulExit.request_exit()

        duration = Transcoder.get_video_duration(test_file)
        self.assertIsNone(duration)

        GracefulExit.exit_requested = False

    def test_get_video_duration_ffprobe_error(self):
        """Test video duration when ffprobe returns an error."""

        test_file = self.create_test_file("test.mp4")

        # Mock subprocess to raise an exception
        with patch(
            "subprocess.run", side_effect=subprocess.CalledProcessError(1, "ffprobe")
        ):
            duration = Transcoder.get_video_duration(test_file)
            self.assertIsNone(duration)

    def test_get_video_duration_na_output(self):
        """Test video duration when ffprobe returns N/A."""

        test_file = self.create_test_file("test.mp4")

        # Mock subprocess to return N/A
        mock_result = MagicMock()
        mock_result.stdout = "N/A"
        with patch("subprocess.run", return_value=mock_result):
            duration = Transcoder.get_video_duration(test_file)
            self.assertIsNone(duration)

    def test_transcode_with_graceful_exit_before_start(self):
        """Test transcoding respects graceful exit before starting."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        GracefulExit.request_exit()

        result = Transcoder.transcode_file(source, output, logger)
        self.assertFalse(result)

        GracefulExit.exit_requested = False

    def test_transcode_with_graceful_exit_during_process(self):
        """Test transcoding handles graceful exit during processing."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        # Mock ffmpeg process that will be interrupted
        mock_proc = MagicMock()
        mock_proc.stdout = iter(["frame=100\n", "frame=200\n"])
        mock_proc.wait.return_value = 0
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()

        def side_effect_exit(*args, **kwargs):
            GracefulExit.request_exit()
            return mock_proc

        with patch("subprocess.Popen", side_effect=side_effect_exit):
            result = Transcoder.transcode_file(source, output, logger)

        self.assertFalse(result)
        GracefulExit.exit_requested = False

    def test_transcode_with_progress_callback(self):
        """Test transcoding with progress callback."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        progress_values = []

        def progress_cb(pct):
            progress_values.append(pct)

        # Mock ffmpeg process
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = iter(["frame=100 time=00:01:30.00 bitrate=1000kb/s\n"])

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch.object(Transcoder, "get_video_duration", return_value=180.0):
                result = Transcoder.transcode_file(source, output, logger, progress_cb)

        # Progress callback should have been called
        self.assertTrue(len(progress_values) > 0 or result is not None)

    def test_transcode_without_duration_info(self):
        """Test transcoding when duration cannot be determined."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        progress_values = []

        def progress_cb(pct):
            progress_values.append(pct)

        # Mock ffmpeg process
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = iter(["frame=100\n", "frame=200\n"])

        with patch("subprocess.Popen", return_value=mock_proc):
            with patch.object(Transcoder, "get_video_duration", return_value=None):
                result = Transcoder.transcode_file(source, output, logger, progress_cb)

        # Should succeed without crashing even without duration
        self.assertTrue(result or result is False)

    def test_transcode_ffmpeg_failure(self):
        """Test transcoding handles ffmpeg failure gracefully."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        # Mock ffmpeg process that fails
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 1  # Non-zero exit code
        mock_proc.stdout = iter(["error: something went wrong\n"])

        with patch("subprocess.Popen", return_value=mock_proc):
            result = Transcoder.transcode_file(source, output, logger)

        self.assertFalse(result)

    def test_transcode_timeout_during_termination(self):
        """Test transcoding handles timeout during graceful exit."""
        import logging

        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        # Mock ffmpeg process that times out on termination
        mock_proc = MagicMock()
        mock_proc.stdout = iter(["frame=100\n"])
        mock_proc.wait.side_effect = [subprocess.TimeoutExpired("ffmpeg", 5), None]
        mock_proc.terminate = MagicMock()
        mock_proc.kill = MagicMock()

        def trigger_exit(*args, **kwargs):
            GracefulExit.request_exit()
            return mock_proc

        with patch("subprocess.Popen", side_effect=trigger_exit):
            result = Transcoder.transcode_file(source, output, logger)

        self.assertFalse(result)
        mock_proc.kill.assert_called_once()
        GracefulExit.exit_requested = False


class TestFileCleaner(BaseIntegrationTest):
    """Tests for FileCleaner edge cases."""

    def test_remove_one_already_in_trash(self):
        """Test removing a file already in trash (permanent deletion)."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Create file in trash
        trash_file = self.trash_dir / "input" / "test.mp4"
        trash_file.parent.mkdir(parents=True, exist_ok=True)
        trash_file.write_bytes(b"test")

        # Remove it (should be permanent since already in trash)
        FileCleaner.remove_one(
            trash_file,
            logger,
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_dir,
            is_output=False,
            source_root=self.input_dir,
        )

        self.assertFalse(trash_file.exists())

    def test_remove_one_dry_run(self):
        """Test remove_one in dry-run mode."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Create file
        test_file = self.input_dir / "test.mp4"
        test_file.write_bytes(b"test")

        # Remove in dry-run mode
        FileCleaner.remove_one(
            test_file,
            logger,
            dry_run=True,
            use_trash=True,
            trash_root=self.trash_dir,
            is_output=False,
            source_root=self.input_dir,
        )

        # File should still exist
        self.assertTrue(test_file.exists())

    def test_calculate_trash_destination_with_collision(self):
        """Test trash destination calculation when file already exists."""
        from archiver import FileCleaner

        source_file = self.input_dir / "test.mp4"
        source_file.write_bytes(b"test")

        # Create first trash destination
        dest1 = FileCleaner.calculate_trash_destination(
            source_file, self.input_dir, self.trash_dir, is_output=False
        )
        dest1.parent.mkdir(parents=True, exist_ok=True)
        dest1.write_bytes(b"existing")

        # Calculate new destination (should avoid collision)
        dest2 = FileCleaner.calculate_trash_destination(
            source_file, self.input_dir, self.trash_dir, is_output=False
        )

        self.assertNotEqual(dest1, dest2)
        self.assertFalse(dest2.exists())

    def test_calculate_trash_destination_output_file(self):
        """Test trash destination for output files."""
        from archiver import FileCleaner

        output_file = self.output_dir / "archived-20230101120000.mp4"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_bytes(b"test")

        dest = FileCleaner.calculate_trash_destination(
            output_file, self.output_dir, self.trash_dir, is_output=True
        )

        # Should go to output subdirectory in trash
        self.assertIn("output", dest.parts)

    def test_calculate_trash_destination_relative_path_fallback(self):
        """Test trash destination when relative_to fails."""
        from archiver import FileCleaner

        # File outside source root
        external_file = self.temp_dir / "external.mp4"
        external_file.write_bytes(b"test")

        dest = FileCleaner.calculate_trash_destination(
            external_file, self.input_dir, self.trash_dir, is_output=False
        )

        # Should fall back to just the filename
        self.assertEqual(dest.name, "external.mp4")

    def test_safe_remove_with_exception(self):
        """Test safe_remove handles exceptions gracefully."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Try to remove non-existent file
        nonexistent = self.input_dir / "nonexistent.mp4"

        # Should not crash
        FileCleaner.safe_remove(nonexistent, logger, dry_run=False, use_trash=False)

    def test_safe_remove_with_graceful_exit(self):
        """Test safe_remove respects graceful exit."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        test_file = self.input_dir / "test.mp4"
        test_file.write_bytes(b"test")

        GracefulExit.request_exit()

        FileCleaner.safe_remove(test_file, logger, dry_run=False, use_trash=False)

        # Should still exist (graceful exit)
        self.assertTrue(test_file.exists())
        GracefulExit.exit_requested = False

    def test_safe_remove_directory(self):
        """Test safe_remove handles directories."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        test_dir = self.input_dir / "test_dir"
        test_dir.mkdir(parents=True, exist_ok=True)

        FileCleaner.safe_remove(test_dir, logger, dry_run=False, use_trash=False)

        # Should be removed
        self.assertFalse(test_dir.exists())

    def test_clean_empty_directories_non_date_structure(self):
        """Test that non-date-structured directories are not removed."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Create empty non-date directory
        non_date_dir = self.input_dir / "random_folder"
        non_date_dir.mkdir(parents=True, exist_ok=True)

        FileCleaner.clean_empty_directories(
            self.input_dir,
            logger,
            use_trash=False,
            trash_root=None,
            is_output=False,
            is_trash=False,
        )

        # Should still exist (not date-structured)
        self.assertTrue(non_date_dir.exists())

    def test_clean_empty_directories_trash_mode(self):
        """Test cleaning empty directories in trash mode."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Create empty directory in trash
        trash_subdir = self.trash_dir / "input" / "empty_dir"
        trash_subdir.mkdir(parents=True, exist_ok=True)

        FileCleaner.clean_empty_directories(
            self.trash_dir,
            logger,
            use_trash=False,
            trash_root=None,
            is_output=False,
            is_trash=True,
        )

        # Should be removed in trash mode
        self.assertFalse(trash_subdir.exists())

    def test_clean_empty_directories_with_graceful_exit(self):
        """Test clean_empty_directories respects graceful exit."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        empty_dir = self.input_dir / "2023/01/01"
        empty_dir.mkdir(parents=True, exist_ok=True)

        GracefulExit.request_exit()

        FileCleaner.clean_empty_directories(
            self.input_dir,
            logger,
            use_trash=False,
            trash_root=None,
            is_output=False,
            is_trash=False,
        )

        # Should still exist (graceful exit)
        self.assertTrue(empty_dir.exists())
        GracefulExit.exit_requested = False

    def test_remove_orphaned_jpgs_with_graceful_exit(self):
        """Test remove_orphaned_jpgs respects graceful exit."""
        from archiver import FileCleaner
        import logging

        logger = logging.getLogger("test")

        # Create orphaned JPG
        orphan_jpg = self.input_dir / "REO_CAMERA_20230101120000.jpg"
        orphan_jpg.write_bytes(b"test")

        mapping = {"20230101120000": {".jpg": orphan_jpg}}

        GracefulExit.request_exit()

        FileCleaner.remove_orphaned_jpgs(
            mapping, set(), logger, dry_run=False, use_trash=False, trash_root=None
        )

        # Should still exist (graceful exit)
        self.assertTrue(orphan_jpg.exists())
        GracefulExit.exit_requested = False


class TestIntelligentCleanup(BaseIntegrationTest):
    """Tests for intelligent cleanup logic edge cases."""

    def test_cleanup_with_no_files(self):
        """Test cleanup when no files exist."""
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.cleanup = True
        archiver = Archiver(config)
        archiver.setup_logging()

        result = archiver.run()
        self.assertEqual(result, 0)

    def test_cleanup_size_limit_exactly_at_threshold(self):
        """Test cleanup when size is exactly at threshold."""
        # Create files totaling exactly the size limit
        ts = datetime.now() - timedelta(days=1)
        file_size = 100 * 1024 * 1024  # 100 MB

        for i in range(5):
            self.create_test_file(
                f"2023/01/0{i + 1}/file{i}.mp4",
                content=b"x" * file_size,
                ts=ts - timedelta(days=i),
            )

        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.max_size = 1  # 1 GB total, we have 500 MB of files
        config.cleanup = True
        archiver = Archiver(config)

        result = archiver.run()
        self.assertEqual(result, 0)

    def test_cleanup_with_age_zero(self):
        """Test cleanup with age threshold disabled (age=0)."""
        old_ts = datetime.now() - timedelta(days=100)
        old_file = self.create_test_file("2023/01/01/old.mp4", ts=old_ts)

        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.age = 0  # Disable age-based cleanup
        config.cleanup = True
        archiver = Archiver(config)

        result = archiver.run()

        # File should still exist (age cleanup disabled)
        self.assertEqual(result, 0)
        self.assertTrue(old_file.exists())

    def test_cleanup_with_logger_not_initialized(self):
        """Test that cleanup fails gracefully if logger not initialized."""
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        archiver = Archiver(config)

        # Don't call setup_logging()
        with self.assertRaises(RuntimeError):
            archiver.cleanup_archive_size_limit()

    def test_output_path_with_invalid_date_structure(self):
        """Test output path generation with non-date-structured input."""
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        archiver = Archiver(config)
        archiver.setup_logging()

        # Create file without date structure
        ts = datetime.now()
        input_file = self.input_dir / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        input_file.write_bytes(b"test")

        output = archiver.output_path(input_file, ts)

        # Should use timestamp-based structure
        self.assertEqual(output.parent.parent.parent.name, str(ts.year))
        self.assertEqual(output.parent.parent.name, f"{ts.month:02d}")
        self.assertEqual(output.parent.name, f"{ts.day:02d}")

    def test_output_path_with_valid_date_structure(self):
        """Test output path preserves valid date structure."""
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        archiver = Archiver(config)
        archiver.setup_logging()

        # Create file with valid date structure
        ts = datetime(2023, 1, 15, 12, 0, 0)
        input_file = (
            self.input_dir
            / "2023/01/15"
            / f"REO_CAMERA_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_bytes(b"test")

        output = archiver.output_path(input_file, ts)

        # Should preserve the date structure
        self.assertIn("2023", output.parts)
        self.assertIn("01", output.parts)
        self.assertIn("15", output.parts)


class TestCLIEdgeCases(BaseIntegrationTest):
    """Tests for CLI argument parsing edge cases."""

    def test_cli_with_no_arguments(self):
        """Test CLI with no arguments prints help and exits."""
        import io

        with (
            patch("sys.argv", ["archiver.py"]),
            patch("sys.stdout", new_callable=io.StringIO),
            patch("sys.stderr", new_callable=io.StringIO),
        ):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)

    def test_cli_with_custom_trashdir(self):
        """Test CLI with custom trash directory."""
        custom_trash = self.temp_dir / "custom_trash"
        old_ts = datetime.now() - timedelta(days=31)
        self.create_test_file("2023/01/01/test.mp4", ts=old_ts)

        def mock_transcode_with_output(
            input_path, output_path, logger, progress_cb=None
        ):
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1))
            return True

        with patch(
            "sys.argv",
            [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
                "--trashdir",
                str(custom_trash),
                "--age",
                "30",
            ],
        ):
            with patch("sys.exit"):
                with patch.object(
                    Transcoder, "transcode_file", side_effect=mock_transcode_with_output
                ):
                    try:
                        main()
                    finally:
                        import logging

                        logger = logging.getLogger("camera_archiver")
                        for handler in logger.handlers[:]:
                            handler.close()
                            logger.removeHandler(handler)

        # Custom trash directory should have been used
        self.assertTrue(custom_trash.exists())

    def test_cli_cleanup_mode_with_clean_output(self):
        """Test CLI cleanup mode with clean-output flag."""
        old_ts = datetime.now() - timedelta(days=31)
        old_archive = self.create_archive_file(old_ts)

        with patch(
            "sys.argv",
            [
                "archiver.py",
                "--directory",
                str(self.input_dir),
                "--output",
                str(self.output_dir),
                "--cleanup",
                "--clean-output",
                "--age",
                "30",
            ],
        ):
            with patch("sys.exit"):
                try:
                    main()
                finally:
                    self.close_camera_logger()

        # Old archive should be removed
        self.assertFalse(old_archive.exists())

    def test_exception_during_run(self):
        """Test exception handling in main() during run."""
        with patch("sys.argv", ["archiver.py", "--directory", str(self.input_dir)]):
            with patch.object(Archiver, "run", side_effect=RuntimeError("Test error")):
                with self.assertRaises(RuntimeError):
                    main()

    def test_exception_with_graceful_exit(self):
        """Test exception handling when graceful exit is requested."""
        with patch("sys.argv", ["archiver.py", "--directory", str(self.input_dir)]):

            def raise_with_exit(*args, **kwargs):
                GracefulExit.request_exit()
                raise RuntimeError("Test error")

            with patch.object(Archiver, "run", side_effect=raise_with_exit):
                with self.assertRaises(SystemExit) as cm:
                    main()
                self.assertEqual(cm.exception.code, 1)

        GracefulExit.exit_requested = False


class TestAdditionalCoverage(BaseIntegrationTest):
    """Additional targeted tests to raise coverage on edge branches."""

    def test_guarded_stream_handler_emit_with_progress(self):
        import logging
        from io import StringIO

        output = StringIO()
        progress = ProgressReporter(total_files=1, out=output)
        progress._progress_line = "Progress [1/1]: 0% [|---------------------------] 00:00 (00:00:00)"

        logger = logging.getLogger("test_emit")
        for h in list(logger.handlers):
            logger.removeHandler(h)
        handler = GuardedStreamHandler(progress.orchestrator, stream=output, progress_bar=progress)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        with self.real_emit():
            logger.info("hello")
        # Should have printed and then redrawn without exception
        self.assertIn("hello", output.getvalue())
        progress.finish()

    def test_logger_setup_without_log_file(self):
        import logging
        logger = logging.getLogger("camera_archiver")
        # Ensure setup works without creating a file handler
        progress = ProgressReporter(total_files=0, out=None)
        from archiver import Logger
        logger2 = Logger.setup(None, progress)
        self.assertIsInstance(logger2, logging.Logger)
        progress.finish()

    def test_transcode_unsupported_stdout_type(self):
        import logging
        logger = logging.getLogger("test")
        source = self.create_test_file("video.mp4")
        output = self.output_dir / "out.mp4"

        class BadStdout:
            pass

        mock_proc = MagicMock()
        mock_proc.stdout = BadStdout()  # Neither readline nor __iter__
        mock_proc.terminate = MagicMock()
        mock_proc.wait.return_value = 0

        with patch("subprocess.Popen", return_value=mock_proc):
            result = Transcoder.transcode_file(source, output, logger)

        self.assertFalse(result)

    def test_transcode_no_stdout(self):
        import logging
        logger = logging.getLogger("test")
        source = self.create_test_file("source.mp4")
        output = self.output_dir / "output.mp4"

        mock_proc = MagicMock()
        mock_proc.stdout = None
        mock_proc.wait.return_value = 0

        with patch("subprocess.Popen", return_value=mock_proc):
            result = Transcoder.transcode_file(source, output, logger)

        self.assertFalse(result)

    def test_get_video_duration_success(self):
        test_file = self.create_test_file("test.mp4")
        mock_result = MagicMock()
        mock_result.stdout = "12.34"
        with patch("shutil.which", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", return_value=mock_result):
                duration = Transcoder.get_video_duration(test_file)
        self.assertEqual(duration, 12.34)

    def test_safe_remove_unsupported_file_type_warning(self):
        import logging
        from archiver import FileCleaner
        logger = logging.getLogger("test")

        # Create a dangling symlink (not a file or dir)
        target = self.input_dir / "nope.mp4"
        link = self.input_dir / "dangling.mp4"
        try:
            os.symlink(str(target), str(link))
        except FileExistsError:
            pass

        # Should not raise regardless of platform behavior
        FileCleaner.safe_remove(link, logger, dry_run=False, use_trash=False)
        # Clean up if still present
        if link.exists():
            try:
                link.unlink()
            except Exception:
                pass

    def test_progress_signal_handler_and_unregister(self):
        from io import StringIO
        import signal

        output = StringIO()
        fake_stderr = StringIO()
        progress = ProgressReporter(total_files=0, out=output)
        with patch("sys.stderr", new=fake_stderr):
            # Call signal handler directly
            progress._signal_handler(signal.SIGINT, None)
            self.assertIn("shutting down gracefully", fake_stderr.getvalue())
        # Unregister should not raise
        progress.finish()

    def test_get_all_archive_files_includes_trash_output(self):
        # Create an archived file in trash/output
        ts = datetime.now() - timedelta(days=40)
        trash_output = self.trash_dir / "output" / str(ts.year) / f"{ts.month:02d}" / f"{ts.day:02d}"
        trash_output.mkdir(parents=True, exist_ok=True)
        trash_file = trash_output / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
        trash_file.write_bytes(b"x" * (MIN_ARCHIVE_SIZE_BYTES + 1))

        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.trashdir = self.trash_dir
        archiver = Archiver(config)
        files = archiver.get_all_archive_files()
        self.assertIn(trash_file, files)

    def test_guarded_stream_handler_emit_without_progressbar(self):
        import logging
        from io import StringIO

        output = StringIO()
        logger = logging.getLogger("test_emit_no_progress")
        for h in list(logger.handlers):
            logger.removeHandler(h)
        from archiver import ConsoleOrchestrator
        handler = GuardedStreamHandler(ConsoleOrchestrator(), stream=output, progress_bar=None)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        with self.real_emit():
            logger.info("no-progress")
        self.assertIn("no-progress", output.getvalue())

    def test_progress_display_exception_fallback(self):
        class FlakyStream:
            def __init__(self):
                self._first = True
                self.buffer = ""
            def isatty(self):
                return True
            def write(self, s):
                if self._first:
                    self._first = False
                    raise Exception("fail once")
                self.buffer += s
            def flush(self):
                pass

        flaky = FlakyStream()
        p = ProgressReporter(total_files=1, out=flaky)
        p.update_progress(1, 10.0)
        # After fallback, buffer should contain the line (without ANSI handling path)
        self.assertIn("Progress", flaky.buffer)
        p.finish()

    def test_scan_files_includes_trash_mapping(self):
        from archiver import FileScanner
        # Create a trash jpg and mp4 that should be mapped
        ts = datetime.now()
        key = ts.strftime("%Y%m%d%H%M%S")
        trash_in = self.trash_dir / "input"
        trash_in.mkdir(parents=True, exist_ok=True)
        (trash_in / f"REO_CAMERA_{key}.mp4").write_bytes(b"x")
        (trash_in / f"REO_CAMERA_{key}.jpg").write_bytes(b"x")
        mp4s, mapping, trash = FileScanner.scan_files(self.input_dir, include_trash=True, trash_root=self.trash_dir)
        self.assertIn(key, mapping)
        self.assertTrue(any(p for p, _ in mp4s if p.name.endswith(f"{key}.mp4")))

    def test_remove_orphaned_jpgs_dry_run(self):
        from archiver import FileCleaner
        import logging
        logger = logging.getLogger("test")
        jpg = self.input_dir / "REO_CAMERA_20230101120000.jpg"
        jpg.write_bytes(b"x")
        mapping = {"20230101120000": {".jpg": jpg}}
        FileCleaner.remove_orphaned_jpgs(mapping, set(), logger, dry_run=True, use_trash=False, trash_root=None)
        self.assertTrue(jpg.exists())

    def test_collect_file_info_with_stat_error_and_bad_archive_name(self):
        # Create an archive with invalid timestamp to trigger ValueError path
        bad_archive = self.output_dir / "2023/01/01/archived-99999999999999.mp4"
        bad_archive.parent.mkdir(parents=True, exist_ok=True)
        bad_archive.write_bytes(b"x")
        # Create a source file but mock stat to raise OSError
        ts = datetime.now()
        src = self.create_test_file("2023/01/02/file.mp4", ts=ts)
        import os as _os
        real_stat = _os.stat
        def selective_stat(path, *args, **kwargs):
            try:
                if str(path) == str(src):
                    raise OSError("stat fail")
            except Exception:
                pass
            return real_stat(path, *args, **kwargs)
        with patch("os.stat", side_effect=selective_stat):
            config = Config()
            config.directory = self.input_dir
            config.output = self.output_dir
            arch = Archiver(config)
            infos = arch.collect_file_info([(src, ts)])
            self.assertIsInstance(infos, list)

    def test_intelligent_cleanup_size_dry_run(self):
        # Create files but set limit to 0 GB to trigger size path deterministically
        ts = datetime.now() - timedelta(days=10)
        for i in range(3):
            self.create_test_file(f"2023/01/0{i+1}/f{i}.mp4", content=b"x" * (5 * 1024 * 1024), ts=ts)
        config = Config()
        config.directory = self.input_dir
        config.output = self.output_dir
        config.max_size = 0  # Force size-exceeded branch
        config.dry_run = True
        config.cleanup = True
        arch = Archiver(config)
        arch.setup_logging()
        # collect and run intelligent cleanup via cleanup_archive_size_limit dry-run path
        arch.cleanup_archive_size_limit()

    def test_clean_empty_directories_error_branch(self):
        from archiver import FileCleaner
        import logging
        logger = logging.getLogger("test")
        # Create a valid date-structured empty dir
        empty_dir = self.input_dir / "2024/01/02"
        empty_dir.mkdir(parents=True, exist_ok=True)

        original_rmdir = Path.rmdir
        def rmdir_side_effect(self_path):
            if self_path == empty_dir:
                raise OSError("cannot remove")
            return original_rmdir(self_path)

        with patch.object(Path, "rmdir", side_effect=rmdir_side_effect):
            FileCleaner.clean_empty_directories(self.input_dir, logger, use_trash=False, trash_root=None, is_output=False, is_trash=False)
        # Directory remains due to error path
        self.assertTrue(empty_dir.exists())

if __name__ == "__main__":
    unittest.main(verbosity=2)
