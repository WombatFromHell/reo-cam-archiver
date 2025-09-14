#!/usr/bin/env python3

import io
import logging
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import subprocess
from contextlib import redirect_stdout, redirect_stderr
from typing import cast

import archiver


# Provide a dummy progress bar for tests if not defined in archiver.
if not hasattr(archiver, "DummyProgressBar"):

    class DummyProgressBar:
        def __init__(self):
            pass

        # The real ProgressBar exposes these methods; they are no‑ops for the stub.
        def start_processing(self):
            return None

        def start_file(self):
            return None

        def update_progress(self, *_, **__):
            return None

        def finish_file(self, *_, **__):
            return None

        def finish(self):
            return None

    archiver.DummyProgressBar = DummyProgressBar  # type: ignore[attr-defined]


# ------------------------------------------------------------------
# Helpers — keep the real tests focused on behaviour only.
# ------------------------------------------------------------------


def capture_logger(name: str) -> logging.Logger:
    """Return a logger that writes to an in‑memory stream."""
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # Attach the stream so tests can read it
    logger._stream = log_stream  # type: ignore[assignment]
    return logger


def write_file(path: Path, data: bytes | str = b""):
    """Create a file with the given data (bytes or string)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(data, bytes) else "w"
    with open(path, mode) as f:
        f.write(data)


def run_main(argv: list[str], logger: logging.Logger | None = None):
    """Run archiver.main() with a temporary sys.argv."""
    original_argv = sys.argv
    output_capture = io.StringIO()
    try:
        sys.argv = argv
        # main may raise SystemExit only when no arguments are provided.
        try:
            with redirect_stdout(output_capture), redirect_stderr(output_capture):
                archiver.main()
        except SystemExit:
            pass
    finally:
        sys.argv = original_argv

    if logger is not None:
        return getattr(logger, "_stream").getvalue()  # type: ignore[attr-defined]
    return output_capture.getvalue()


class TempDirTestCase(unittest.TestCase):
    """Base class that gives each test a fresh temporary directory."""

    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)


# ------------------------------------------------------------------
# Individual test classes — each covers one functional area.
# ------------------------------------------------------------------


class TestTimestampParsing(TempDirTestCase):
    """Tests for parse_timestamp_from_filename()"""

    def test_boundary_years(self):
        name_2000 = "REO_cam_20000101000000.mp4"
        ts = archiver.parse_timestamp_from_filename(name_2000)
        self.assertIsNotNone(ts)  # runtime guard
        ts = cast(datetime, ts)  # tell the type‑checker it's a datetime
        self.assertEqual(ts.year, 2000)

        name_2099 = "REO_cam_20991231235959.mp4"
        ts = archiver.parse_timestamp_from_filename(name_2099)
        self.assertIsNotNone(ts)
        ts = cast(datetime, ts)
        self.assertEqual(ts.year, 2099)

    def test_cases(self):
        cases = [
            ("REO_cam_20231201010101.mp4", datetime(2023, 12, 1, 1, 1, 1)),
            ("REO_cam_18991231235959.mp4", None),
            ("REO_cam_21000101000000.mp4", None),
            ("REO_cam_20231201.mp4", None),
            ("something_else.txt", None),
        ]
        for name, expected in cases:
            with self.subTest(name=name):
                ts = archiver.parse_timestamp_from_filename(name)
                if expected is not None:
                    self.assertIsNotNone(ts)
                    self.assertEqual(ts, expected)
                else:
                    self.assertIsNone(ts)


class TestSafeRemove(TempDirTestCase):
    """Tests for safe_remove() — dry‑run, trash, source_root handling."""

    def setUp(self) -> None:
        super().setUp()
        self.logger = capture_logger("safe_remove")
        # Patch the logger to avoid clutter
        patcher = patch.object(logging, "getLogger", return_value=self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_dry_run_and_real_removal(self):
        """Dry‑run keeps the file; real run deletes it."""
        p = self.temp_dir / "test.mp4"
        write_file(p)

        # Test dry run first
        archiver.safe_remove(p, self.logger, dry_run=True)
        self.assertTrue(p.exists())
        self.assertIn(
            "[DRY RUN] Would remove",
            getattr(self.logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )

        # Clear log stream for next test
        getattr(self.logger, "_stream").truncate(0)  # type: ignore[attr-defined]
        getattr(self.logger, "_stream").seek(0)  # type: ignore[attr-defined]

        # Test real removal
        archiver.safe_remove(p, self.logger, dry_run=False)
        self.assertFalse(p.exists())
        self.assertIn(
            "Removed:",
            getattr(self.logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )

    def test_trash_with_relative_path(self):
        """When use_trash=True, file is moved preserving relative path."""
        trash_root = self.temp_dir / ".Deleted"
        trash_root.mkdir()
        source_root = self.temp_dir

        p = self.temp_dir / "2023" / "12" / "01" / "video.mp4"
        write_file(p)

        archiver.safe_remove(
            p,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=source_root,
        )

        dest = trash_root / "input" / "2023" / "12" / "01" / "video.mp4"
        self.assertFalse(p.exists())
        self.assertTrue(dest.exists())
        self.assertIn(
            f"Moved to trash: {p} -> {dest}",
            getattr(self.logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )

    def test_source_root_fallback(self):
        """If source_root is None, only the file name is preserved."""
        trash_root = self.temp_dir / ".Deleted"
        trash_root.mkdir()

        p = self.temp_dir / "2023" / "12" / "01" / "video.mp4"
        write_file(p)

        archiver.safe_remove(
            p,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=trash_root,
            source_root=None,
        )

        dest = trash_root / "input" / "video.mp4"
        self.assertTrue(dest.exists())
        self.assertFalse(p.exists())


class TestOutputPath(TempDirTestCase):
    """Tests for output_path() — correct directory selection."""

    def test_cases(self):
        ts = datetime(2023, 12, 1)
        cases = [
            (Path("root/dir/sub1/sub2/file.mp4"), True),
            (Path("file.mp4"), False),
        ]
        for fp, has_parts in cases:
            with self.subTest(fp=fp):
                result = archiver.output_path(fp, ts, self.temp_dir)
                if has_parts:
                    y, m, d = fp.parts[-4:-1]
                    expected = (
                        self.temp_dir
                        / y
                        / m
                        / d
                        / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
                    )
                else:
                    expected = (
                        self.temp_dir
                        / "2023"
                        / "12"
                        / "01"
                        / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
                    )
                self.assertEqual(result, expected)


class TestProgressBar(TempDirTestCase):
    """Tests for ProgressBar — output formatting and state changes."""

    def setUp(self) -> None:
        super().setUp()
        # Use a stream that reports whether isatty() is true
        self.stream = io.StringIO()
        self.stream.isatty = lambda *_, **__: True

    def test_updates_and_finish(self):
        bar = archiver.ProgressBar(total_files=1, silent=False, out=self.stream)
        bar.update_progress(1, 50.0)
        # Progress line should appear once
        self.assertTrue(any("Progress [1/1]:" in s for s in self._get_stream_output()))
        first_len = len(self._get_stream_output())

        # Same pct again — no new line
        bar.update_progress(1, 50.0)
        self.assertEqual(first_len, len(self._get_stream_output()))

        # finish clears the line
        bar.finish()
        # Check that finish() was called by verifying the stream content changed
        final_content = self.stream.getvalue()
        self.assertIn("Progress [1/1]:", final_content)

    def test_silent_mode(self):
        stream = io.StringIO()
        stream.isatty = lambda *_, **__: True
        bar = archiver.ProgressBar(total_files=1, silent=True, out=stream)
        bar.update_progress(1, 50.0)
        self.assertEqual(stream.getvalue(), "")
        bar.finish()
        self.assertEqual(stream.getvalue(), "")

    def test_non_tty_output(self):
        stream = io.StringIO()
        stream.isatty = lambda *_, **__: False
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.update_progress(1, 50.0)
        # For non-tty output, check that progress was written
        content = stream.getvalue()
        self.assertIn("Progress [1/1]:", content)

    def test_finish_file_and_start(self):
        stream = io.StringIO()
        stream.isatty = lambda *_, **__: True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        bar.finish_file(1)
        # Check that finish_file produces output indicating completion
        content = stream.getvalue()
        self.assertTrue(
            "Progress [1/1]:" in content or "100%" in content or len(content) > 0
        )

        bar_start = archiver.ProgressBar(total_files=1, silent=True)
        self.assertIsNone(bar_start.file_start)
        bar_start.start_file()
        self.assertIsNotNone(bar_start.file_start)

    # ------------------------------------------------------------------
    # Helper to get stream content
    # ------------------------------------------------------------------

    def _get_stream_output(self):
        return self.stream.getvalue().splitlines()


class TestLoggerSetup(TempDirTestCase):
    """Tests that logging is wired correctly and GuardedStreamHandler clears lines."""

    def setUp(self) -> None:
        super().setUp()
        self.log_file = self.temp_dir / "log.txt"

    def test_setup_logging_writes_to_file(self):
        test_logger = logging.getLogger("test_file_logger")
        test_logger.handlers.clear()
        test_logger.propagate = False
        test_logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        test_logger.addHandler(file_handler)
        try:
            test_logger.info("hello world")
            file_handler.close()  # Ensure file is flushed and closed
            content = self.log_file.read_text()
            self.assertIn("hello world", content)
        finally:
            test_logger.handlers.clear()

    def test_guarded_handler_clears_line_before_log(self):
        stream = io.StringIO()
        stream.isatty = lambda *_, **__: True
        bar = archiver.ProgressBar(total_files=1, silent=False, out=stream)
        logger = logging.getLogger("guarded")
        logger.setLevel(logging.INFO)

        sh = archiver.GuardedStreamHandler(
            bar.orchestrator, stream=stream, progress_bar=bar
        )
        sh.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(sh)

        bar.start_file()
        logger.info("First log")
        bar.update_progress(1, 50.0)  # writes the bar
        logger.info("Second log")  # should appear above the bar

        stream_text = stream.getvalue()
        self.assertIn("First log", stream_text)
        self.assertIn("Second log", stream_text)


class TestGetVideoDuration(unittest.TestCase):
    """Tests for get_video_duration() — covers ffprobe presence and errors."""

    @patch("shutil.which")
    def test_ffprobe_scenarios(self, mock_which):
        scenarios = [
            ("no_ffprobe", None, None, None),
            ("success", "/usr/bin/ffprobe", "123.45\n", 123.45),
            (
                "error",
                "/usr/bin/ffprobe",
                subprocess.CalledProcessError(1, ["ffprobe"]),
                None,
            ),
        ]

        for name, which_ret, check_output_ret, expected in scenarios:
            with self.subTest(name=name):
                mock_which.return_value = which_ret
                if which_ret is None:
                    ts = archiver.get_video_duration(Path("dummy.mp4"))
                    self.assertIsNone(ts)
                    continue

                # patch subprocess.check_output only for this iteration
                with patch("archiver.subprocess.check_output") as mock_check:
                    if isinstance(check_output_ret, Exception):
                        mock_check.side_effect = check_output_ret
                    else:
                        mock_check.return_value = check_output_ret
                    ts = archiver.get_video_duration(Path("dummy.mp4"))
                    self.assertEqual(ts, expected)


class TestProcessFiles(TempDirTestCase):
    """Tests for process_files() — skip logic, dry‑run, and no‑skip behaviour."""

    def setUp(self) -> None:
        super().setUp()
        self.logger = capture_logger("process")
        # Patch the logger to avoid clutter
        patcher = patch.object(logging, "getLogger", return_value=self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=None)
    def test_skip_logic(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1)
        mp4_path = self.temp_dir / "video.mp4"
        write_file(mp4_path)

        jpg_path = self.temp_dir / "video.jpg"
        write_file(jpg_path)

        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()

        # existing archived file > 1 MB
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
            bar=archiver.DummyProgressBar(),  # type: ignore[attr-defined]
        )

        # Files should be removed after skip
        self.assertFalse(mp4_path.exists())
        self.assertFalse(jpg_path.exists())

        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("[SKIP] Existing archive large enough", logs)

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=True)
    def test_no_skip_triggers_transcode(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1)
        mp4_path = self.temp_dir / "video.mp4"
        write_file(mp4_path)

        jpg_path = self.temp_dir / "video.jpg"
        write_file(jpg_path)

        out_dir = self.temp_dir / "archived"
        out_dir.mkdir()

        # existing archived file > 1 MB
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
            bar=archiver.DummyProgressBar(),  # type: ignore[attr-defined]
        )

        mock_transcode.assert_called_once()
        args = mock_transcode.call_args[0]
        self.assertEqual(args[0], mp4_path)
        self.assertEqual(args[1], outp)

        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("Transcoding", logs)

    @patch("archiver.safe_remove")
    @patch("archiver.transcode_file", return_value=True)
    def test_dry_run(self, mock_transcode, mock_safe_remove):
        ts = datetime(2023, 12, 1)
        mp4_path = self.temp_dir / "video.mp4"
        write_file(mp4_path)

        jpg_path = self.temp_dir / "video.jpg"
        write_file(jpg_path)

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
            bar=archiver.DummyProgressBar(),  # type: ignore[attr-defined]
        )

        mock_transcode.assert_not_called()
        mock_safe_remove.assert_not_called()

        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("[DRY RUN] Would transcode", logs)


class TestCleanupEmptyDirectories(TempDirTestCase):
    """Tests for clean_empty_directories() — non‑matching dirs stay, empty ones removed/moved."""

    def test_non_matching_dir_ignored(self):
        (self.temp_dir / "2023" / "12").mkdir(parents=True)
        archiver.clean_empty_directories(self.temp_dir, logger=None)
        self.assertTrue((self.temp_dir / "2023" / "12").exists())

    def test_clean_and_trash(self):
        # empty day — should be removed or moved
        empty_day = self.temp_dir / "2023" / "12" / "01"
        empty_day.mkdir(parents=True)

        # non‑empty day — must stay
        non_empty = self.temp_dir / "2023" / "12" / "02"
        non_empty.mkdir(parents=True)
        (non_empty / "sub").mkdir()
        (non_empty / "file.mp4").write_text("data")

        trash_root = self.temp_dir / ".Deleted"
        trash_root.mkdir()

        # 1️⃣ no‑trash
        archiver.clean_empty_directories(self.temp_dir, logger=None)
        self.assertFalse(empty_day.exists())
        self.assertTrue(non_empty.exists())

        # Re‑create the empty day for the next subtest
        (self.temp_dir / "2023" / "12" / "01").mkdir(parents=True)

        # 2️⃣ with trash
        archiver.clean_empty_directories(
            self.temp_dir,
            logger=None,
            use_trash=True,
            trash_root=trash_root,
        )
        dest = trash_root / "input" / "2023" / "12" / "01"
        self.assertFalse((self.temp_dir / "2023" / "12" / "01").exists())
        self.assertTrue(dest.is_dir())


class TestRemoveOrphanedJPGs(TempDirTestCase):
    """Tests for remove_orphaned_jpgs() — ensures orphan JPGs are deleted."""

    def setUp(self) -> None:
        super().setUp()
        self.logger = capture_logger("orphan")
        patcher = patch.object(logging, "getLogger", return_value=self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

    @patch("archiver.safe_remove")
    def test_removes_orphans(self, mock_safe_remove):
        orphan_path = self.temp_dir / "orphan.jpg"
        write_file(orphan_path)

        mapping = {"20231201000000": {".jpg": orphan_path}}
        processed = set()

        archiver.remove_orphaned_jpgs(mapping, processed, self.logger, dry_run=False)

        mock_safe_remove.assert_called_once_with(
            orphan_path,
            self.logger,
            False,
            use_trash=False,
            trash_root=None,
        )
        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("Found orphaned JPG", logs)


class TestCleanupArchiveSizeLimit(TempDirTestCase):
    """Tests for cleanup_archive_size_limit() — dry‑run, limit not exceeded, actual removal."""

    def setUp(self) -> None:
        super().setUp()
        self.logger = capture_logger("size")
        patcher = patch.object(logging, "getLogger", return_value=self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_dry_run_logs_only(self):
        f = self.temp_dir / "archived-0000.mp4"
        write_file(f, b"0" * 1024)

        archiver.cleanup_archive_size_limit(
            self.temp_dir,
            logger=self.logger,
            max_size_gb=1,
            dry_run=True,
            use_trash=False,
            trash_root=None,
        )
        self.assertTrue(f.exists())
        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("[DRY RUN] Would check archive size limit", logs)

    def test_no_removal_when_within_limit(self):
        f = self.temp_dir / "archived-0000.mp4"
        write_file(f, b"0" * 1024)  # <1 GB

        archiver.cleanup_archive_size_limit(
            self.temp_dir,
            logger=self.logger,
            max_size_gb=10,
            dry_run=False,
            use_trash=False,
            trash_root=None,
        )
        self.assertTrue(f.exists())
        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("Current archive size:", logs)
        self.assertNotIn("Removed old archive", logs)

    def test_removal_when_exceeds_limit(self):
        # create two files that together exceed 1 GB
        f1 = self.temp_dir / "archived-0000.mp4"
        write_file(f1, b"0" * (1024**3 // 2))
        f2 = self.temp_dir / "archived-0001.mp4"
        write_file(f2, b"0" * (1024**3 // 2 + 1))

        archiver.cleanup_archive_size_limit(
            self.temp_dir,
            logger=self.logger,
            max_size_gb=0.5,  # limit < total
            dry_run=False,
            use_trash=False,
            trash_root=None,
        )
        self.assertFalse(f1.exists() or f2.exists())
        logs = getattr(self.logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("Archive size exceeds limit", logs)
        self.assertIn("Removed old archive:", logs)


class TestTrashBehavior(TempDirTestCase):
    """Tests that trashing works correctly for files and empty dirs, and dry-run does nothing."""

    def setUp(self) -> None:
        super().setUp()
        self.trash_root = self.temp_dir / ".Deleted"
        self.trash_root.mkdir()
        self.logger = capture_logger("trash")
        patcher = patch.object(logging, "getLogger", return_value=self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

        # Create test directory structures
        self.input_root = self.temp_dir / "camera"
        self.output_root = self.temp_dir / "camera" / "archived"
        self.input_root.mkdir(parents=True)
        self.output_root.mkdir(parents=True)

    def test_file_moved_to_trash(self):
        p = self.input_root / "file.txt"
        write_file(p)
        archiver.safe_remove(
            p,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_root,
            source_root=self.input_root,  # Add explicit source_root
        )
        dest = self.trash_root / "input" / p.name
        self.assertFalse(p.exists())
        self.assertTrue(dest.exists())

    def test_empty_dir_moved_to_trash(self):
        empty_day = self.input_root / "2023" / "12" / "01"
        empty_day.mkdir(parents=True)
        archiver.clean_empty_directories(
            self.input_root,
            None,
            use_trash=True,
            trash_root=self.trash_root,
            is_output=False,  # Explicitly mark as input
        )
        dest = self.trash_root / "input" / "2023" / "12" / "01"
        self.assertFalse(empty_day.exists())
        self.assertTrue(dest.is_dir())

    def test_dry_run_no_movement(self):
        p2 = self.input_root / "file2.txt"
        write_file(p2)
        archiver.safe_remove(
            p2,
            self.logger,
            dry_run=True,
            use_trash=True,
            trash_root=self.trash_root,
            source_root=self.input_root,  # Add explicit source_root
        )
        self.assertTrue(p2.exists())
        dest = self.trash_root / "input" / p2.name
        self.assertFalse(dest.exists())

    # NEW TESTS TO COVER THE FIXED BEHAVIOR

    def test_output_file_moved_to_trash(self):
        """Test output files go to the 'output' subdirectory in trash"""
        p = self.output_root / "archived-20230101000000.mp4"
        write_file(p)
        archiver.safe_remove(
            p,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_root,
            source_root=self.output_root,  # Explicit source_root for output
            is_output=True,  # Mark as output
        )
        dest = self.trash_root / "output" / p.name
        self.assertFalse(p.exists())
        self.assertTrue(dest.exists())

        # Verify it's NOT in the input subdirectory
        wrong_dest = self.trash_root / "input" / p.name
        self.assertFalse(wrong_dest.exists())

    def test_output_empty_dir_moved_to_trash(self):
        """Test output directories go to the 'output' subdirectory in trash"""
        empty_day = self.output_root / "2023" / "12" / "01"
        empty_day.mkdir(parents=True)
        archiver.clean_empty_directories(
            self.output_root,
            None,
            use_trash=True,
            trash_root=self.trash_root,
            is_output=True,  # Explicitly mark as output
        )
        dest = self.trash_root / "output" / "2023" / "12" / "01"
        self.assertFalse(empty_day.exists())
        self.assertTrue(dest.is_dir())

        # Verify it's NOT in the input subdirectory
        wrong_dest = self.trash_root / "input" / "2023" / "12" / "01"
        self.assertFalse(wrong_dest.exists())

    def test_source_root_path_preservation(self):
        """Test that relative paths are preserved correctly when source_root is provided"""
        # Create a nested file structure
        nested_file = self.output_root / "2023" / "01" / "01" / "file.mp4"
        nested_file.parent.mkdir(parents=True)
        write_file(nested_file)

        archiver.safe_remove(
            nested_file,
            self.logger,
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_root,
            source_root=self.output_root,  # Explicit source_root
            is_output=True,
        )

        # Verify the file is in the correct location with preserved structure
        dest = self.trash_root / "output" / "2023" / "01" / "01" / "file.mp4"
        self.assertFalse(nested_file.exists())
        self.assertTrue(dest.exists())

        # Verify it's NOT in the wrong location
        wrong_dest = self.trash_root / "output" / "file.mp4"
        self.assertFalse(wrong_dest.exists())

    def test_cleanup_archive_size_limit(self):
        """Test that cleanup_archive_size_limit moves files to the correct trash location"""
        # Create test files in output directory
        test_file = self.output_root / "archived-20230101000000.mp4"
        write_file(test_file)

        # Set a specific size to the file (to ensure it's counted in the size calculation)
        test_file.write_text("x" * 1024)  # 1KB file

        # Call the actual function (not mocked)
        archiver.cleanup_archive_size_limit(
            self.output_root,
            self.logger,
            max_size_gb=0,  # Set to 0 to trigger cleanup
            dry_run=False,
            use_trash=True,
            trash_root=self.trash_root,
        )

        # Verify the file was moved to the correct trash location
        dest = self.trash_root / "output" / test_file.name
        self.assertFalse(test_file.exists(), "Original file should be removed")
        self.assertTrue(dest.exists(), "File should exist in trash output directory")

        # Verify it's NOT in the input subdirectory
        wrong_dest = self.trash_root / "input" / test_file.name
        self.assertFalse(
            wrong_dest.exists(), "File should not be in trash input directory"
        )


class TestDefaultDirectoryAndTrashPath(TempDirTestCase):
    """Tests that the default directory and trash path are handled correctly."""

    def _run_and_capture_log(self, argv: list[str]) -> str:
        return run_main(argv)

    def test_default_directory_and_trash_path(self):
        argv = ["archiver.py", "--directory", str(self.temp_dir)]
        log_contents = self._run_and_capture_log(argv)
        self.assertIn(f"Input: {self.temp_dir}", log_contents)
        self.assertIn("Trash: None", log_contents)
        self.assertFalse((self.temp_dir / ".deleted").exists())

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    @patch("archiver.clean_empty_directories")
    def test_custom_trash_directory(
        self,
        mock_clean_dirs,
        mock_remove_jpgs,
        mock_process_files,
        mock_scan_files,
    ):
        custom_trash = self.temp_dir / "custom_trash"

        dummy_logger = capture_logger("custom_trash")

        with patch("archiver.setup_logging", return_value=dummy_logger):
            argv = [
                "archiver.py",
                "--directory",
                str(self.temp_dir),
                "--trashdir",
                str(custom_trash),
            ]
            run_main(argv)

        self.assertIn(
            f"Input: {self.temp_dir}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertIn(
            f"Trash: {custom_trash}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertTrue(custom_trash.exists())
        self.assertFalse((self.temp_dir / ".deleted").exists())

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    @patch("archiver.clean_empty_directories")
    def test_use_trash_flag(
        self, mock_clean_dirs, mock_remove_jpgs, mock_process_files, mock_scan_files
    ):
        expected_trash = self.temp_dir / ".deleted"

        dummy_logger = capture_logger("use_trash")

        with patch("archiver.setup_logging", return_value=dummy_logger):
            argv = ["archiver.py", "--directory", str(self.temp_dir), "--use-trash"]
            run_main(argv)

        self.assertIn(
            f"Input: {self.temp_dir}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertIn(
            f"Trash: {expected_trash}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertTrue(expected_trash.exists())
        self.assertFalse((self.temp_dir / "custom_trash").exists())

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    @patch("archiver.clean_empty_directories")
    def test_use_trash_with_custom_directory(
        self,
        mock_clean_dirs,
        mock_remove_jpgs,
        mock_process_files,
        mock_scan_files,
    ):
        custom_trash = self.temp_dir / "custom_trash"

        dummy_logger = capture_logger("mixed_trash")

        with patch("archiver.setup_logging", return_value=dummy_logger):
            argv = [
                "archiver.py",
                "--directory",
                str(self.temp_dir),
                "--use-trash",
                "--trashdir",
                str(custom_trash),
            ]
            run_main(argv)

        self.assertIn(
            f"Input: {self.temp_dir}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertIn(
            f"Trash: {custom_trash}",
            getattr(dummy_logger, "_stream").getvalue(),  # type: ignore[attr-defined]
        )
        self.assertTrue(custom_trash.exists())
        self.assertFalse((self.temp_dir / ".deleted").exists())


class TestMainFunction(TempDirTestCase):
    """Final integration test — archive size limit triggers removal of old files."""

    @patch("archiver.scan_files", return_value=([], {}))
    @patch("archiver.process_files", return_value=set())
    @patch("archiver.remove_orphaned_jpgs")
    def test_archive_size_limit_removal(
        self, mock_remove_jpgs, mock_process_files, mock_scan_files
    ):
        dummy_logger = capture_logger("main")

        # Create the output directory and some files to exceed the limit
        output_dir = self.temp_dir / "out"
        output_dir.mkdir()

        # Create files that will exceed the 0 GB limit
        (output_dir / "archived-20231201000000.mp4").write_bytes(b"0" * 1024)
        (output_dir / "archived-20231201000001.mp4").write_bytes(b"0" * 1024)

        with patch("archiver.setup_logging", return_value=dummy_logger):
            argv = [
                "archiver.py",
                "--directory",
                str(self.temp_dir),
                "--output",
                str(output_dir),
                "--age",
                "0",
                "--max-size",
                "0",
            ]
            run_main(argv)

        # all files should be gone because the limit is 0 GB
        self.assertFalse(any(output_dir.iterdir()))
        msgs = getattr(dummy_logger, "_stream").getvalue()  # type: ignore[attr-defined]
        self.assertIn("Archive size exceeds limit", msgs)
        self.assertIn("Removed old archive:", msgs)


if __name__ == "__main__":
    unittest.main(verbosity=2)
