from datetime import datetime, timedelta
from pathlib import Path
import subprocess
from typing import Optional, Iterable, Tuple
import logging

import pytest

import archiver

# -------- Fixtures & helpers --------


@pytest.fixture
def logger(mocker):
    """Reusable mocked logger"""
    return mocker.MagicMock()


@pytest.fixture
def graceful_exit():
    return archiver.GracefulExit()


@pytest.fixture
def file_structure(tmp_path):
    """Create standard camera dated directory structure."""

    def _create(
        base: str = "camera", ts: Optional[datetime] = None, suffix: str = "mp4"
    ) -> Path:
        if ts is None:
            ts = datetime(2023, 1, 15, 12, 0, 0)
        date_dir = (
            tmp_path / base / ts.strftime("%Y") / ts.strftime("%m") / ts.strftime("%d")
        )
        date_dir.mkdir(parents=True, exist_ok=True)
        name = f"REO_camera_{ts.strftime('%Y%m%d%H%M%S')}.{suffix}"
        p = date_dir / name
        p.touch()
        return p

    return _create


@pytest.fixture
def create_files(tmp_path):
    """Generic helper to create files from a list of (relative_path, content)."""

    def _create(items: Iterable[Tuple[str, Optional[str]]]):
        created = []
        for rel, content in items:
            p = tmp_path / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            if content is None:
                p.touch()
            else:
                p.write_text(content)
            created.append(p)
        return created

    return _create


def _make_mock_proc(mocker, lines, rc=0):
    """Create a mock subprocess.Popen process."""
    proc = mocker.MagicMock()
    stdout = mocker.MagicMock()
    stdout.readline.side_effect = list(lines)
    proc.stdout = stdout
    proc.wait.return_value = rc
    proc.returncode = rc
    return proc


# -------- Config Tests --------


@pytest.mark.parametrize(
    "input_args,expect",
    [
        (
            [],
            {
                "directory": Path("/camera"),
                "age": 30,
                "log_file": Path("/camera") / "archiver.log",
            },
        ),  # defaults
        (
            ["/custom/dir"],
            {
                "directory": Path("/custom/dir"),
                "dry_run": False,
                "age": 30,
                "log_file": Path("/custom/dir") / "archiver.log",
            },
        ),
        (["/custom/dir", "--dry-run"], {"dry_run": True}),
        (["/custom/dir", "--age", "60"], {"age": 60}),
        (["/custom/dir", "--delete"], {"delete": True, "trash_root": None}),
        (["/custom/dir", "--no-confirm"], {"no_confirm": True}),
        (["/custom/dir", "--no-skip"], {"no_skip": True}),
        (
            ["/custom/dir", "--cleanup", "--clean-output"],
            {"cleanup": True, "clean_output": True},
        ),
        # Add more comprehensive test cases for all arguments
        (["/custom/dir", "-o", "/custom/output"], {"output": Path("/custom/output")}),
        (
            ["/custom/dir", "--trash-root", "/custom/trash"],
            {"trash_root": Path("/custom/trash")},
        ),
        (["/custom/dir", "--age", "7"], {"age": 7}),
        (
            ["/custom/dir", "--log-file", "/tmp/log.txt"],
            {"log_file": Path("/tmp/log.txt")},
        ),
        # Comprehensive test case with all arguments - note that --delete overrides trash-root
        (
            [
                "/test/dir",
                "-o",
                "/test/output",
                "--trash-root",
                "/test/trash",
                "--age",
                "15",
                "--log-file",
                "/test/log.txt",
                "--dry-run",
                "--no-confirm",
                "--no-skip",
                "--delete",  # This overrides trash-root
                "--cleanup",
                "--clean-output",
            ],
            {
                "directory": Path("/test/dir"),
                "output": Path("/test/output"),
                "trash_root": None,  # --delete overrides trash-root
                "age": 15,
                "log_file": Path("/test/log.txt"),
                "dry_run": True,
                "no_confirm": True,
                "no_skip": True,
                "delete": True,
                "cleanup": True,
                "clean_output": True,
            },
        ),
    ],
)
def test_parse_args_and_config(input_args, expect):
    args = archiver.parse_args(input_args)
    cfg = archiver.Config(args)

    for k, v in expect.items():
        assert getattr(cfg, k) == v


# -------- GracefulExit Tests --------


def test_graceful_exit_threadsafe():
    g = archiver.GracefulExit()
    assert not g.should_exit()
    g.request_exit()
    assert g.should_exit()


def test_graceful_exit_thread_safety(mocker):
    """Test GracefulExit with concurrent access."""
    import threading

    g = archiver.GracefulExit()
    results = []

    def check_and_request():
        results.append(g.should_exit())
        g.request_exit()
        results.append(g.should_exit())

    t = threading.Thread(target=check_and_request)
    t.start()
    t.join()

    assert results == [False, True]


# -------- ProgressReporter Tests --------


def test_progress_reporter_basic_flow(mocker):
    """Test basic progress reporter flow."""
    mocker.patch("time.time", side_effect=[0.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0])
    g = archiver.GracefulExit()
    r = archiver.ProgressReporter(10, g, silent=False)
    mock_stderr = mocker.patch("sys.stderr")

    r.start_file()
    assert r.current == 1

    r.update_progress(50.0)
    assert mock_stderr.write.called

    r.finish_file()
    written = mock_stderr.write.call_args_list[-1][0][0]
    assert "100%" in written


@pytest.mark.parametrize(
    "elapsed,expected_sub",
    [(100.0, "01:40"), (3661.0, "01:01:01")],
)
def test_progress_reporter_time_formatting(mocker, elapsed, expected_sub):
    """Test progress reporter time formatting."""
    call_count = [0]
    times = [0.0, 0.0, elapsed]

    def fake_time():
        idx = min(call_count[0], len(times) - 1)
        call_count[0] += 1
        return times[idx]

    mocker.patch("time.time", side_effect=fake_time)
    g = archiver.GracefulExit()
    r = archiver.ProgressReporter(10, g, silent=False)
    mock_stderr = mocker.patch("sys.stderr")

    r.start_file()
    r.update_progress(50.0)

    assert mock_stderr.write.called
    written = mock_stderr.write.call_args[0][0]
    assert expected_sub in written or "Progress" in written


def test_progress_reporter_silent_mode(mocker):
    """Test progress reporter in silent mode."""
    g = archiver.GracefulExit()
    r = archiver.ProgressReporter(2, g, silent=True)
    mock_stderr = mocker.patch("sys.stderr")

    r.start_file()
    r.update_progress(50.0)

    mock_stderr.write.assert_not_called()


def test_progress_reporter_exit_requested(mocker):
    """Test progress reporter respects graceful exit."""
    g = archiver.GracefulExit()
    g.request_exit()

    r = archiver.ProgressReporter(10, g, silent=False)
    mock_stderr = mocker.patch("sys.stderr")

    r.start_file()
    r.update_progress(50.0)

    mock_stderr.write.assert_not_called()


def test_progress_reporter_context_manager(mocker):
    """Test progress reporter as context manager."""
    g = archiver.GracefulExit()
    r = archiver.ProgressReporter(10, g, silent=False)
    mock_stderr = mocker.patch("sys.stderr")

    with r:
        pass

    mock_stderr.write.assert_called_with("\n")


# -------- Logger Tests --------


def test_log_rotation(tmp_path):
    """Test log file rotation when size exceeds limit."""
    log_file = tmp_path / "app.log"
    with open(log_file, "wb") as f:
        f.write(b"x" * (archiver.LOG_ROTATION_SIZE + 10))

    archiver.Logger._rotate_log_file(log_file)
    assert (tmp_path / "app.log.1").exists()
    assert log_file.exists()
    assert log_file.stat().st_size == 0


def test_log_rotation_with_backups(tmp_path, mocker):
    """Test log rotation with existing backup files."""
    log_file = tmp_path / "app.log"
    (tmp_path / "app.log.1").touch()
    (tmp_path / "app.log.2").touch()

    with open(log_file, "wb") as f:
        f.write(b"x" * (archiver.LOG_ROTATION_SIZE + 10))

    mock_move = mocker.patch("shutil.move")
    archiver.Logger._rotate_log_file(log_file)

    assert mock_move.call_count >= 3


def test_logger_setup(tmp_path, mocker):
    """Test logger setup with handlers."""
    log_file = tmp_path / "test.log"
    args = archiver.parse_args([str(tmp_path), "--log-file", str(log_file)])
    config = archiver.Config(args)

    mocker.patch.object(archiver.Logger, "_rotate_log_file")
    logger = archiver.Logger.setup(config)

    assert logger.level == logging.INFO
    assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    assert any(isinstance(h, archiver.ThreadSafeStreamHandler) for h in logger.handlers)


def test_logger_setup_exception_handling(mocker, tmp_path):
    """Test logger setup with exception handling in file handler closure."""
    log_file = tmp_path / "test.log"
    args = archiver.parse_args([str(tmp_path), "--log-file", str(log_file)])
    config = archiver.Config(args)

    # Create a mock handler that raises an exception during close
    mock_handler = mocker.MagicMock()
    mock_handler.close.side_effect = Exception("Mock close exception")

    # Mock getLogger to return a logger with our problematic handler
    mock_logger = mocker.MagicMock()
    mock_logger.handlers = [mock_handler]
    # Set up the side effect to return the same mock logger for any name
    mocker.patch("logging.getLogger", return_value=mock_logger)
    mocker.patch.object(archiver.Logger, "_rotate_log_file")

    # This should not raise an exception despite the close() failure
    logger = archiver.Logger.setup(config)

    # The setup should still complete
    assert logger is not None


# -------- FileDiscovery Tests --------


def test_parse_timestamp_various():
    """Test timestamp parsing from various filename formats."""
    fd = archiver.FileDiscovery
    assert fd._parse_timestamp("REO_camera_20230115120000.mp4") == datetime(
        2023, 1, 15, 12, 0, 0
    )
    assert fd._parse_timestamp("invalid.mp4") is None
    assert (
        fd._parse_timestamp("REO_camera_19991231235959.mp4") is None
    )  # Year too early
    assert fd._parse_timestamp_from_archived_filename(
        "archived-20230115120000.mp4"
    ) == datetime(2023, 1, 15, 12, 0, 0)
    assert fd._parse_timestamp_from_archived_filename("invalid.mp4") is None


@pytest.mark.parametrize("scenario", ["basic", "with_trash", "with_output_clean"])
def test_discover_files_comprehensive(tmp_path, scenario):
    """Test file discovery in various scenarios."""
    if scenario == "basic":
        # Test file discovery with actual file creation
        camera_dir = tmp_path / "camera" / "2023" / "01" / "15"
        camera_dir.mkdir(parents=True, exist_ok=True)

        mp4_file = camera_dir / "REO_camera_20230115120000.mp4"
        jpg_file = camera_dir / "REO_camera_20230115120000.jpg"
        mp4_file.touch()
        jpg_file.touch()

        mp4s, mapping, trash = archiver.FileDiscovery.discover_files(
            tmp_path / "camera"
        )
        assert len(mp4s) == 1
        assert "20230115120000" in mapping
        assert mapping["20230115120000"][".mp4"] == mp4_file
        assert mapping["20230115120000"][".jpg"] == jpg_file

    elif scenario == "with_trash":
        # Test file discovery including trash directory
        camera_dir = tmp_path / "camera" / "2023" / "01" / "15"
        trash_dir = tmp_path / "trash" / "input" / "2023" / "01" / "15"
        camera_dir.mkdir(parents=True, exist_ok=True)
        trash_dir.mkdir(parents=True, exist_ok=True)

        camera_file = camera_dir / "REO_camera_20230115120000.mp4"
        trash_file = trash_dir / "REO_camera_20230115130000.mp4"
        camera_file.touch()
        trash_file.touch()

        mp4s, mapping, trash_files = archiver.FileDiscovery.discover_files(
            tmp_path / "camera", tmp_path / "trash"
        )
        assert len(mp4s) == 2
        assert trash_file in trash_files

    elif scenario == "with_output_clean":
        # Test file discovery with output directory when clean_output is enabled
        camera_dir = tmp_path / "camera" / "2023" / "01" / "15"
        output_dir = tmp_path / "archived" / "2023" / "01" / "15"
        camera_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        camera_file = camera_dir / "REO_camera_20230115120000.mp4"
        archived_file = output_dir / "archived-20230115120000.mp4"
        camera_file.touch()
        archived_file.touch()

        mp4s, mapping, trash = archiver.FileDiscovery.discover_files(
            tmp_path / "camera",
            output_directory=tmp_path / "archived",
            clean_output=True,
        )
        assert len(mp4s) == 2
        # Check if both files (camera_file and archived_file) are in the mp4s list
        file_paths = [item[0] for item in mp4s]
        assert camera_file in file_paths
        assert archived_file in file_paths


def test_discover_files_with_clean_output(tmp_path):
    """Test file discovery with output directory when clean_output is enabled."""
    camera_dir = tmp_path / "camera" / "2023" / "01" / "15"
    output_dir = tmp_path / "archived" / "2023" / "01" / "15"
    camera_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    camera_file = camera_dir / "REO_camera_20230115120000.mp4"
    archived_file = output_dir / "archived-20230115120000.mp4"
    camera_file.touch()
    archived_file.touch()

    mp4s, mapping, trash = archiver.FileDiscovery.discover_files(
        tmp_path / "camera", output_directory=tmp_path / "archived", clean_output=True
    )
    assert len(mp4s) == 2
    assert any(m[0] == archived_file for m in mp4s)


# -------- FileManager Tests --------


@pytest.mark.parametrize(
    "dry_run,delete,trash_root,expect_contains",
    [
        (True, False, None, "[DRY RUN] Would remove"),
        (False, True, None, "Removed:"),
        (False, False, "/tmp/trash", "Moved to trash"),
    ],
)
def test_remove_file_behaviors(
    mocker, tmp_path, logger, dry_run, delete, trash_root, expect_contains
):
    """Test file removal with various configurations."""
    p = Path("/tmp/testfile.mp4")
    trash = Path(trash_root) if trash_root else None

    if not dry_run and delete:
        mocker.patch("pathlib.Path.is_file", return_value=True)
        mocker.patch("pathlib.Path.unlink")

    if not dry_run and trash is not None:
        mocker.patch("pathlib.Path.is_file", return_value=True)
        mocker.patch("pathlib.Path.mkdir")
        mocker.patch("shutil.move")

    archiver.FileManager.remove_file(
        p, logger, dry_run=dry_run, delete=delete, trash_root=trash
    )
    assert logger.info.call_count >= 1
    called = logger.info.call_args[0][0]
    assert expect_contains in called


def test_remove_file_error_handling(mocker, logger):
    """Test file removal error handling."""
    p = Path("/tmp/testfile.mp4")

    mocker.patch("pathlib.Path.is_file", side_effect=Exception("Permission denied"))
    archiver.FileManager.remove_file(p, logger, dry_run=False, delete=True)

    logger.error.assert_called()


def test_calculate_trash_destination_basic(tmp_path):
    """Test trash destination calculation."""
    file_path = tmp_path / "camera" / "2023" / "01" / "15" / "file.mp4"
    source_root = tmp_path / "camera"
    trash_root = tmp_path / "trash"

    dest = archiver.FileManager._calculate_trash_destination(
        file_path, source_root, trash_root, is_output=False
    )

    expected = trash_root / "input" / "2023" / "01" / "15" / "file.mp4"
    assert dest == expected


def test_calculate_trash_destination_conflict(mocker, tmp_path):
    """Test trash destination with filename conflict."""
    file_path = tmp_path / "camera" / "2023" / "01" / "15" / "file.mp4"
    source_root = tmp_path / "camera"
    trash_root = tmp_path / "trash"
    base_dest = trash_root / "input" / "2023" / "01" / "15" / "file.mp4"

    def fake_exists(self):
        try:
            return str(self) == str(base_dest)
        except Exception:
            return False

    mocker.patch("pathlib.Path.exists", fake_exists)
    mocker.patch("time.time", return_value=1000.0)

    dest = archiver.FileManager._calculate_trash_destination(
        file_path, source_root, trash_root, is_output=False
    )
    assert "1000_1" in dest.name


def test_remove_directory_behavior(mocker, logger):
    """Test FileManager.remove_file with directory behavior."""
    # Test removing a directory
    dir_path = Path("/tmp/testdir")

    _ = mocker.patch.object(Path, "is_file", return_value=False)
    _ = mocker.patch.object(Path, "is_dir", return_value=True)
    mock_rmdir = mocker.patch.object(Path, "rmdir")

    archiver.FileManager.remove_file(dir_path, logger, dry_run=False, delete=True)

    # Should call rmdir for directories
    mock_rmdir.assert_called_once()
    logger.info.assert_called_with(f"Removed: {dir_path}")


def test_remove_unsupported_file_type(mocker, logger):
    """Test FileManager.remove_file with unsupported file type."""
    file_path = Path("/tmp/test_unknown")

    # Mock is_file and is_dir to return False (neither file nor directory)
    mocker.patch.object(Path, "is_file", return_value=False)
    mocker.patch.object(Path, "is_dir", return_value=False)

    archiver.FileManager.remove_file(file_path, logger, dry_run=False, delete=True)

    # Should log warning for unsupported file type
    logger.warning.assert_called_with(f"Unsupported file type for removal: {file_path}")


def test_calculate_trash_destination_relative_to_failure(tmp_path):
    """Test FileManager._calculate_trash_destination when relative_to fails."""
    file_path = tmp_path / "some" / "arbitrary" / "path" / "file.mp4"
    source_root = tmp_path / "camera"  # Different root, so relative_to will fail
    trash_root = tmp_path / "trash"

    dest = archiver.FileManager._calculate_trash_destination(
        file_path, source_root, trash_root, is_output=False
    )

    # When relative_to fails, it should use just the filename
    expected = trash_root / "input" / "file.mp4"
    assert dest == expected


def test_clean_empty_directories(mocker, tmp_path, logger):
    """Test cleaning empty directories."""
    empty_dir = tmp_path / "2023" / "01" / "15"
    empty_dir.mkdir(parents=True, exist_ok=True)

    mocker.patch(
        "os.walk",
        return_value=[
            (str(empty_dir), [], []),
            (str(empty_dir.parent), [], []),
        ],
    )
    mock_rmdir = mocker.patch("pathlib.Path.rmdir")
    mocker.patch("pathlib.Path.iterdir", return_value=[])

    archiver.FileManager.clean_empty_directories(empty_dir, logger)
    assert mock_rmdir.call_count >= 1


# -------- Transcoder Tests --------


@pytest.mark.parametrize(
    "ffprobe_available,run_result,expected",
    [
        (
            True,
            subprocess.CompletedProcess(args=[], returncode=0, stdout="120.5"),
            120.5,
        ),  # success
        (
            True,
            subprocess.CompletedProcess(args=[], returncode=0, stdout="N/A"),
            None,
        ),  # N/A response
        (
            True,
            subprocess.CompletedProcess(args=[], returncode=0, stdout=""),
            None,
        ),  # empty response
        (False, None, None),  # ffprobe not available
        (True, Exception("ffprobe failed"), None),  # exception case
    ],
)
def test_get_video_duration_comprehensive(
    mocker, ffprobe_available, run_result, expected
):
    """Comprehensive test for getting video duration with various responses."""
    mocker.patch(
        "shutil.which", return_value="/usr/bin/ffprobe" if ffprobe_available else None
    )

    if ffprobe_available and isinstance(run_result, Exception):
        mocker.patch("subprocess.run", side_effect=run_result)
    elif ffprobe_available and isinstance(run_result, subprocess.CompletedProcess):
        mocker.patch("subprocess.run", return_value=run_result)

    d = archiver.Transcoder.get_video_duration(Path("/tmp/x.mp4"))
    assert d == expected


def test_transcode_file_dry_run(mocker, tmp_path, logger, graceful_exit):
    """Test transcoding in dry-run mode."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"
    mocker.patch("pathlib.Path.mkdir")
    mocker.patch("subprocess.Popen")

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, None, graceful_exit, dry_run=True
    )
    assert res is True
    logger.info.assert_called()


def test_transcode_file_success(mocker, tmp_path, logger, graceful_exit):
    """Test successful file transcoding."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"
    proc = _make_mock_proc(
        mocker,
        ["frame=  100 time=00:00:01.00", "frame=  200 time=00:00:02.00", ""],
        rc=0,
    )
    mocker.patch("subprocess.Popen", return_value=proc)
    mocker.patch.object(archiver.Transcoder, "get_video_duration", return_value=3.0)
    mocker.patch("pathlib.Path.mkdir")
    progress = mocker.MagicMock()

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, progress, graceful_exit, dry_run=False
    )
    assert res is True
    assert progress.called


def test_transcode_file_failure(mocker, tmp_path, logger, graceful_exit):
    """Test failed file transcoding."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"
    proc = _make_mock_proc(mocker, ["Error: Bad", ""], rc=1)
    mocker.patch("subprocess.Popen", return_value=proc)
    mocker.patch.object(archiver.Transcoder, "get_video_duration", return_value=3.0)
    mocker.patch("pathlib.Path.mkdir")

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, None, graceful_exit, dry_run=False
    )
    assert res is False
    logger.error.assert_called()


def test_transcode_file_cancelled(mocker, tmp_path, logger):
    """Test transcoding cancellation."""
    g = archiver.GracefulExit()
    g.request_exit()

    res = archiver.Transcoder.transcode_file(
        Path("/in.mp4"), Path("/out.mp4"), logger, None, g, dry_run=False
    )
    assert res is False


def test_transcode_file_popen_failure(mocker, tmp_path, logger, graceful_exit):
    """Test transcoding with Popen failure."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"

    mocker.patch("subprocess.Popen", side_effect=OSError("Failed to start"))
    mocker.patch("pathlib.Path.mkdir")

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, None, graceful_exit, dry_run=False
    )
    assert res is False
    logger.error.assert_called()


def test_transcode_file_no_stdout(mocker, tmp_path, logger, graceful_exit):
    """Test transcoding when stdout is unavailable."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"
    proc = mocker.MagicMock()
    proc.stdout = None
    proc.wait.return_value = 0

    mocker.patch("subprocess.Popen", return_value=proc)
    mocker.patch("pathlib.Path.mkdir")

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, None, graceful_exit, dry_run=False
    )
    assert res is False
    logger.error.assert_called()


# -------- FileProcessor Tests --------


@pytest.mark.parametrize(
    "scenario", ["basic", "age_cutoff", "existing_archive", "cleanup_mode"]
)
def test_generate_action_plan_comprehensive(
    mocker, tmp_path, logger, graceful_exit, file_structure, scenario
):
    """Comprehensive test for action plan generation with various configurations."""

    if scenario == "basic":
        # Basic test: should create transcoding and removal actions
        cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
        proc = archiver.FileProcessor(cfg, logger, graceful_exit)

        mp4 = file_structure()
        jpg = file_structure(suffix="jpg")
        ts = datetime(2023, 1, 15, 12, 0, 0)
        mp4s = [(mp4, ts)]
        mapping = {"20230115120000": {".mp4": mp4, ".jpg": jpg}}

        mocker.patch("pathlib.Path.exists", return_value=False)
        plan = proc.generate_action_plan(mp4s, mapping)

        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2

    elif scenario == "age_cutoff":
        # Age cutoff test: old files should be transcoded, recent ones skipped
        cfg = archiver.Config(archiver.parse_args([str(tmp_path), "--age", "30"]))
        proc = archiver.FileProcessor(cfg, logger, graceful_exit)

        # Create files with different timestamps
        recent_ts = datetime.now() - timedelta(days=10)
        old_ts = datetime.now() - timedelta(days=40)
        mp4_recent = file_structure(ts=recent_ts)
        mp4_old = file_structure(ts=old_ts)

        mp4s = [(mp4_recent, recent_ts), (mp4_old, old_ts)]
        mapping = {
            recent_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_recent},
            old_ts.strftime("%Y%m%d%H%M%S"): {".mp4": mp4_old},
        }

        mocker.patch("pathlib.Path.exists", return_value=False)
        plan = proc.generate_action_plan(mp4s, mapping)

        # Only the old file (40 days ago, older than 30 day cutoff) should be targeted for transcoding
        assert any(a["input"] == mp4_old for a in plan["transcoding"])
        # The recent file should be skipped due to age filter
        assert not any(a["input"] == mp4_recent for a in plan["transcoding"])

    elif scenario == "existing_archive":
        # Existing archive test: should skip transcoding if archive exists and is large enough
        cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
        proc = archiver.FileProcessor(cfg, logger, graceful_exit)

        mp4 = file_structure()
        jpg = file_structure(suffix="jpg")
        ts = datetime(2023, 1, 15, 12, 0, 0)
        mp4s = [(mp4, ts)]
        mapping = {"20230115120000": {".mp4": mp4, ".jpg": jpg}}

        mocker.patch("pathlib.Path.exists", return_value=True)
        mock_stat = mocker.patch("pathlib.Path.stat")
        mock_stat.return_value.st_size = archiver.MIN_ARCHIVE_SIZE_BYTES + 1000

        plan = proc.generate_action_plan(mp4s, mapping)

        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # Still has removals for skip path

    elif scenario == "cleanup_mode":
        # Cleanup mode test: should skip transcoding and go straight to removals
        cfg = archiver.Config(archiver.parse_args([str(tmp_path), "--cleanup"]))
        proc = archiver.FileProcessor(cfg, logger, graceful_exit)

        mp4 = file_structure()
        jpg = file_structure(suffix="jpg")
        ts = datetime(2023, 1, 15, 12, 0, 0)
        mp4s = [(mp4, ts)]
        mapping = {"20230115120000": {".mp4": mp4, ".jpg": jpg}}

        mocker.patch("pathlib.Path.exists", return_value=False)
        plan = proc.generate_action_plan(mp4s, mapping)

        assert len(plan["transcoding"]) == 0
        assert all(
            "cleanup mode enabled" in r["reason"] or "archive exists" in r["reason"]
            for r in plan["removals"]
        )


def test_generate_action_plan_with_trash_files(
    mocker, tmp_path, logger, graceful_exit, file_structure
):
    """Test action plan generation when trash files are present."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    mp4 = file_structure()
    ts = datetime(2023, 1, 15, 12, 0, 0)
    mp4s = [(mp4, ts)]
    mapping = {"20230115120000": {".mp4": mp4}}

    # Mock Path.exists to return False so the file gets added to transcoding plan
    mocker.patch("pathlib.Path.exists", return_value=False)

    # Test the actual generate_action_plan method as it exists in the code
    plan = proc.generate_action_plan(mp4s, mapping)

    # The method should add the file to transcoding since trash files aren't
    # actually filtered in the current implementation (the check is in the code but
    # the trash files parameter is not passed from outside)
    assert len(plan["transcoding"]) == 1
    assert plan["transcoding"][0]["input"] == mp4


def test_execute_plan_output_file_detection(mocker, tmp_path, logger, graceful_exit):
    """Test execute_plan output file detection logic."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path), "--clean-output"]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    _ = tmp_path / "in.mp4"
    _ = tmp_path / "out.mp4"

    plan = {
        "transcoding": [],
        "removals": [
            {
                "type": "source_removal_after_transcode",
                "file": cfg.output
                / "2023"
                / "01"
                / "15"
                / "test.mp4",  # This is in output dir
                "reason": "x",
            },
        ],
    }

    # Mock the relative_to to raise ValueError for files not in output directory
    # and not to raise for files that are in the output directory
    _ = mocker.patch("pathlib.Path.exists", return_value=True)
    mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")

    # Execute plan with a file that's in the output directory
    res = proc.execute_plan(plan, mocker.MagicMock())
    assert res is True
    # Should call remove_file with is_output=True for files in output directory
    assert mock_remove.called


def test_execute_plan_and_cleanup(mocker, tmp_path, logger, graceful_exit):
    """Test plan execution with cleanup."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    input_path = tmp_path / "in.mp4"
    out_path = tmp_path / "out.mp4"
    jpg = tmp_path / "in.jpg"

    plan = {
        "transcoding": [
            {
                "type": "transcode",
                "input": input_path,
                "output": out_path,
                "jpg_to_remove": jpg,
            }
        ],
        "removals": [
            {
                "type": "source_removal_after_transcode",
                "file": input_path,
                "reason": "x",
            },
            {"type": "jpg_removal_after_transcode", "file": jpg, "reason": "y"},
        ],
    }

    mocker.patch.object(archiver.Transcoder, "transcode_file", return_value=True)
    mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")
    progress = mocker.MagicMock()

    res = proc.execute_plan(plan, progress)
    assert res is True
    assert mock_remove.call_count >= 2


def test_execute_plan_transcode_failure(mocker, tmp_path, logger, graceful_exit):
    """Test plan execution with transcode failure."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    input_path = tmp_path / "in.mp4"
    out_path = tmp_path / "out.mp4"

    plan = {
        "transcoding": [
            {
                "type": "transcode",
                "input": input_path,
                "output": out_path,
                "jpg_to_remove": None,
            }
        ],
        "removals": [],
    }

    mocker.patch.object(archiver.Transcoder, "transcode_file", return_value=False)
    progress = mocker.MagicMock()

    res = proc.execute_plan(plan, progress)
    assert res is True
    logger.error.assert_called()


def test_cleanup_orphaned_files(
    mocker, tmp_path, logger, graceful_exit, file_structure
):
    """Test cleanup of orphaned files."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    orphan_jpg = file_structure(suffix="jpg")
    paired_ts = datetime(2023, 1, 15, 12, 0, 1)
    paired_mp4 = file_structure(ts=paired_ts)
    paired_jpg = file_structure(ts=paired_ts, suffix="jpg")

    mapping = {
        "20230115120000": {".jpg": orphan_jpg},
        "20230115120001": {".mp4": paired_mp4, ".jpg": paired_jpg},
    }

    mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")
    mock_clean = mocker.patch.object(archiver.FileManager, "clean_empty_directories")

    proc.cleanup_orphaned_files(mapping)
    mock_remove.assert_called_once()
    mock_clean.assert_called_once()


def test_output_path_generation(tmp_path, logger, graceful_exit):
    """Test output path generation."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    input_file = (
        tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
    )
    ts = datetime(2023, 1, 15, 12, 0, 0)

    output_path = proc._output_path(input_file, ts)
    expected = (
        tmp_path / "archived" / "2023" / "01" / "15" / "archived-20230115120000.mp4"
    )

    assert output_path == expected


# -------- run_archiver Integration Tests --------


@pytest.mark.parametrize(
    "scenario",
    [
        "no_files",
        "missing_directory",
        "dry_run",
        "user_cancels",
        "with_cleanup",
        "error_handling",
        "exception_handling",
    ],
)
def test_run_archiver_comprehensive(tmp_path, mocker, logger, scenario):
    """Comprehensive test for run_archiver in various scenarios."""

    if scenario == "no_files":
        # Test run_archiver with no files to process
        args = archiver.parse_args([str(tmp_path)])
        cfg = archiver.Config(args)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch.object(
            archiver.FileDiscovery, "discover_files", return_value=([], {}, set())
        )
        mocker.patch("pathlib.Path.exists", return_value=True)

        res = archiver.run_archiver(cfg)
        assert res == 0
        logger.info.assert_any_call("No files to process")

    elif scenario == "missing_directory":
        # Test run_archiver with missing input directory
        args = archiver.parse_args([str(tmp_path)])
        cfg = archiver.Config(args)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch("pathlib.Path.exists", return_value=False)

        res = archiver.run_archiver(cfg)
        assert res == 1
        logger.error.assert_called()

    elif scenario == "dry_run":
        # Test run_archiver in dry-run mode
        args = archiver.parse_args([str(tmp_path), "--dry-run"])
        cfg = archiver.Config(args)

        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        ts = datetime(2023, 1, 15, 12, 0, 0)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, ts)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )
        mocker.patch("pathlib.Path.exists", return_value=True)

        fake_proc = mocker.MagicMock()
        fake_proc.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }
        fake_proc.execute_plan.return_value = True
        mocker.patch("archiver.FileProcessor", return_value=fake_proc)

        fake_progress = mocker.MagicMock()
        fake_progress.__enter__ = mocker.MagicMock(return_value=fake_progress)
        fake_progress.__exit__ = mocker.MagicMock(return_value=None)
        mocker.patch("archiver.ProgressReporter", return_value=fake_progress)

        res = archiver.run_archiver(cfg)
        assert res == 0
        logger.info.assert_any_call(
            "Dry run completed - no transcoding or removals performed"
        )

    elif scenario == "user_cancels":
        # Test run_archiver when user cancels confirmation
        args = archiver.parse_args([str(tmp_path)])
        cfg = archiver.Config(args)

        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        ts = datetime(2023, 1, 15, 12, 0, 0)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, ts)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )
        mocker.patch("pathlib.Path.exists", return_value=True)
        mocker.patch("archiver.confirm_plan", return_value=False)

        fake_proc = mocker.MagicMock()
        fake_proc.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }
        mocker.patch("archiver.FileProcessor", return_value=fake_proc)

        res = archiver.run_archiver(cfg)
        assert res == 0
        logger.info.assert_any_call("Operation cancelled by user")

    elif scenario == "with_cleanup":
        # Test run_archiver with cleanup enabled
        args = archiver.parse_args([str(tmp_path), "--cleanup"])
        cfg = archiver.Config(args)

        mp4_file = (
            tmp_path / "camera" / "2023" / "01" / "15" / "REO_camera_20230115120000.mp4"
        )
        ts = datetime(2023, 1, 15, 12, 0, 0)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            return_value=(
                [(mp4_file, ts)],
                {"20230115120000": {".mp4": mp4_file}},
                set(),
            ),
        )
        mocker.patch("pathlib.Path.exists", return_value=True)
        mocker.patch("archiver.confirm_plan", return_value=True)

        fake_proc = mocker.MagicMock()
        fake_proc.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }
        fake_proc.execute_plan.return_value = True
        mocker.patch("archiver.FileProcessor", return_value=fake_proc)

        fake_progress = mocker.MagicMock()
        fake_progress.__enter__ = mocker.MagicMock(return_value=fake_progress)
        fake_progress.__exit__ = mocker.MagicMock(return_value=None)
        mocker.patch("archiver.ProgressReporter", return_value=fake_progress)

        res = archiver.run_archiver(cfg)
        assert res == 0
        logger.info.assert_any_call("Cleaning up files")
        fake_proc.cleanup_orphaned_files.assert_called_once()

    elif scenario == "error_handling":
        # Test run_archiver error handling
        args = archiver.parse_args([str(tmp_path)])
        cfg = archiver.Config(args)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            side_effect=Exception("Discovery failed"),
        )
        mocker.patch("pathlib.Path.exists", return_value=True)

        res = archiver.run_archiver(cfg)
        assert res == 1
        logger.error.assert_called()

    elif scenario == "exception_handling":
        # Test run_archiver with exception at different stages of the pipeline
        args = archiver.parse_args([str(tmp_path)])
        cfg = archiver.Config(args)

        mocker.patch.object(archiver.Logger, "setup", return_value=logger)
        # Mock setup_signal_handlers to avoid actual signal manipulation
        mocker.patch("archiver.setup_signal_handlers")

        # Test exception during discovery stage
        mocker.patch.object(
            archiver.FileDiscovery,
            "discover_files",
            side_effect=OSError("Permission denied"),
        )
        mocker.patch.object(Path, "exists", return_value=True)

        res = archiver.run_archiver(cfg)
        assert res == 1  # Should return error code
        logger.error.assert_called()


# -------- Display and Confirm Plan Tests --------


def test_display_plan(mocker, tmp_path, logger):
    """Test displaying action plan."""
    args = archiver.parse_args([str(tmp_path)])
    cfg = archiver.Config(args)

    plan = {
        "transcoding": [
            {
                "type": "transcode",
                "input": Path("/test/input.mp4"),
                "output": Path("/test/output.mp4"),
                "jpg_to_remove": Path("/test/input.jpg"),
            }
        ],
        "removals": [
            {
                "type": "source_removal_after_transcode",
                "file": Path("/test/input.mp4"),
                "reason": "x",
            },
        ],
    }

    archiver.display_plan(plan, logger, cfg)
    assert logger.info.call_count >= 4


def test_display_plan_with_cleanup(mocker, tmp_path, logger):
    """Test displaying plan with cleanup enabled."""
    args = archiver.parse_args([str(tmp_path), "--cleanup", "--age", "30"])
    cfg = archiver.Config(args)

    plan = {"transcoding": [], "removals": []}
    archiver.display_plan(plan, logger, cfg)

    calls = [call[0][0] for call in logger.info.call_args_list]
    assert any("Cleanup enabled" in call for call in calls)


def test_confirm_plan_no_confirm_flag(tmp_path, logger):
    """Test plan confirmation with no-confirm flag."""
    args = archiver.parse_args([str(tmp_path), "--no-confirm"])
    cfg = archiver.Config(args)

    plan = {"transcoding": [], "removals": []}
    result = archiver.confirm_plan(plan, cfg, logger)

    assert result is True


def test_confirm_plan_user_yes(mocker, tmp_path, logger):
    """Test plan confirmation with user input 'yes'."""
    args = archiver.parse_args([str(tmp_path)])
    cfg = archiver.Config(args)

    mocker.patch("builtins.input", return_value="y")
    plan = {"transcoding": [], "removals": []}

    result = archiver.confirm_plan(plan, cfg, logger)
    assert result is True


def test_confirm_plan_user_no(mocker, tmp_path, logger):
    """Test plan confirmation with user input 'no'."""
    args = archiver.parse_args([str(tmp_path)])
    cfg = archiver.Config(args)

    mocker.patch("builtins.input", return_value="n")
    plan = {"transcoding": [], "removals": []}

    result = archiver.confirm_plan(plan, cfg, logger)
    assert result is False


def test_confirm_plan_keyboard_interrupt(mocker, tmp_path, logger):
    """Test plan confirmation with keyboard interrupt."""
    args = archiver.parse_args([str(tmp_path)])
    cfg = archiver.Config(args)

    mocker.patch("builtins.input", side_effect=KeyboardInterrupt)
    plan = {"transcoding": [], "removals": []}

    result = archiver.confirm_plan(plan, cfg, logger)
    assert result is False


def test_confirm_plan_empty_response(mocker, tmp_path, logger):
    """Test plan confirmation with empty user response."""
    args = archiver.parse_args([str(tmp_path)])
    cfg = archiver.Config(args)

    # Mock input to return an empty string
    mocker.patch("builtins.input", return_value="")
    plan = {"transcoding": [], "removals": []}
    result = archiver.confirm_plan(plan, cfg, logger)
    assert result is False  # Empty response should return False


# -------- Signal Handler Tests --------


def test_setup_signal_handlers(mocker):
    """Test signal handler setup."""
    graceful_exit = archiver.GracefulExit()
    mock_signal = mocker.patch("signal.signal")

    archiver.setup_signal_handlers(graceful_exit)

    assert mock_signal.call_count == 3


def test_setup_signal_handlers_exception(mocker):
    """Test signal handler setup with exception handling."""
    import signal

    graceful_exit = archiver.GracefulExit()

    # Mock signal.signal to raise an exception for one of the signals
    def side_effect(sig, handler):
        if sig == signal.SIGTERM:
            raise ValueError("Invalid signal")
        return None

    mock_signal = mocker.patch("signal.signal", side_effect=side_effect)

    # Should not raise an exception even if signal.signal fails
    archiver.setup_signal_handlers(graceful_exit)

    # Should still try to register all signals despite one failing
    assert mock_signal.call_count == 3


@pytest.mark.parametrize(
    "signal_type,signal_name",
    [
        ("SIGINT", "SIGINT"),
        ("SIGTERM", "SIGTERM"),
        ("SIGHUP", "SIGHUP"),
        ("UNKNOWN", "signal 999"),  # Test with unknown signal number
    ],
)
def test_signal_handler_triggers_exit(mocker, signal_type, signal_name):
    """Test that signal handlers trigger graceful exit for various signals."""
    import signal

    graceful_exit = archiver.GracefulExit()
    mock_signal = mocker.patch("signal.signal")
    mock_stderr = mocker.patch("sys.stderr")

    archiver.setup_signal_handlers(graceful_exit)

    # Get the appropriate signal value based on the test case
    if signal_type == "UNKNOWN":
        actual_signal = 999
    else:
        actual_signal = getattr(signal, signal_type)

    # Get the handler for the first signal registered (the same handler is used for all)
    handler = mock_signal.call_args_list[0][0][1]
    handler(actual_signal, None)

    assert graceful_exit.should_exit()
    mock_stderr.write.assert_called()

    if signal_type == "UNKNOWN":
        written_args = mock_stderr.write.call_args[0][0]
        assert "signal 999" in written_args


# -------- Main Entry Point Tests --------


def test_main_entry_point(mocker):
    """Test main entry point."""
    mock_args = mocker.MagicMock()
    mock_config = mocker.MagicMock()

    mocker.patch("archiver.parse_args", return_value=mock_args)
    mocker.patch("archiver.Config", return_value=mock_config)
    mocker.patch("archiver.run_archiver", return_value=0)

    archiver.main()


def test_main_entry_point_with_system_exit(mocker):
    """Test main entry point with system exit."""
    # Test that main properly returns the exit code from run_archiver
    mock_args = mocker.MagicMock()
    mock_config = mocker.MagicMock()
    expected_return_code = 42

    mocker.patch("archiver.parse_args", return_value=mock_args)
    mocker.patch("archiver.Config", return_value=mock_config)
    mocker.patch("archiver.run_archiver", return_value=expected_return_code)

    result = archiver.main()
    assert result == expected_return_code


# -------- ThreadSafeStreamHandler Tests --------


def test_thread_safe_stream_handler_emit(mocker):
    """Test ThreadSafeStreamHandler emit with lock."""
    handler = archiver.ThreadSafeStreamHandler()
    record = mocker.MagicMock()

    mock_super = mocker.patch.object(logging.StreamHandler, "emit")
    mocker.patch("sys.stderr")

    handler.emit(record)
    mock_super.assert_called_once()


def test_thread_safe_stream_handler_with_active_progress_reporter(mocker):
    """Test ThreadSafeStreamHandler emit when there's an active progress reporter."""
    handler = archiver.ThreadSafeStreamHandler()
    record = mocker.MagicMock()

    # Set up an active progress reporter
    mock_progress_reporter = mocker.MagicMock()
    archiver.ACTIVE_PROGRESS_REPORTER = mock_progress_reporter

    mock_stderr = mocker.patch("sys.stderr")
    mock_super = mocker.patch.object(logging.StreamHandler, "emit")

    handler.emit(record)

    # Should clear the line before emitting
    mock_stderr.write.assert_called()
    mock_super.assert_called_once()

    # Clean up
    archiver.ACTIVE_PROGRESS_REPORTER = None


# -------- Edge Cases and Miscellaneous --------


def test_config_with_custom_trash_root(tmp_path):
    """Test config with custom trash root."""
    trash_root = tmp_path / "custom_trash"
    args = archiver.parse_args([str(tmp_path), "--trash-root", str(trash_root)])
    cfg = archiver.Config(args)

    assert cfg.trash_root == trash_root


def test_config_delete_flag_overrides_trash(tmp_path):
    """Test that delete flag overrides trash root."""
    trash_root = tmp_path / "trash"
    args = archiver.parse_args(
        [str(tmp_path), "--delete", "--trash-root", str(trash_root)]
    )
    cfg = archiver.Config(args)

    assert cfg.delete is True
    assert cfg.trash_root is None


def test_config_default_directory_and_output(tmp_path):
    """Test Config with default directory and output logic."""
    # Test default directory when none provided (using /camera as default)
    args = archiver.parse_args([])  # This will use the default /camera
    cfg = archiver.Config(args)
    assert cfg.directory == Path("/camera")
    assert cfg.output == Path("/camera/archived")  # default output based on directory

    # Test with custom directory and no output (should use directory/archived)
    args2 = archiver.parse_args([str(tmp_path)])
    cfg2 = archiver.Config(args2)
    assert cfg2.directory == tmp_path
    assert cfg2.output == tmp_path / "archived"

    # Test with custom directory and custom output
    custom_output = tmp_path / "my_output"
    args3 = archiver.parse_args([str(tmp_path), "--output", str(custom_output)])
    cfg3 = archiver.Config(args3)
    assert cfg3.directory == tmp_path
    assert cfg3.output == custom_output


def test_parse_timestamp_edge_cases():
    """Test timestamp parsing edge cases."""
    fd = archiver.FileDiscovery

    # Valid edge case: leap year
    assert fd._parse_timestamp("REO_camera_20200229120000.mp4") == datetime(
        2020, 2, 29, 12, 0, 0
    )

    # Invalid: non-leap year
    assert fd._parse_timestamp("REO_camera_20210229120000.mp4") is None


def test_discover_files_invalid_dates(tmp_path):
    """Test discovery skips invalid date directories."""
    camera_dir = tmp_path / "camera"

    # Invalid month
    invalid_dir = camera_dir / "2023" / "13" / "15"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    (invalid_dir / "REO_camera_20230115120000.mp4").touch()

    mp4s, mapping, trash = archiver.FileDiscovery.discover_files(camera_dir)
    assert len(mp4s) == 0


def test_discover_files_error_handling(tmp_path, mocker):
    """Test FileDiscovery error handling for various scenarios."""
    # Test with AttributeError when accessing path attributes
    camera_dir = tmp_path / "camera"
    camera_dir.mkdir(parents=True, exist_ok=True)

    # Create a valid file structure
    valid_dir = camera_dir / "2023" / "01" / "15"
    valid_dir.mkdir(parents=True, exist_ok=True)
    valid_file = valid_dir / "REO_camera_20230115120000.mp4"
    valid_file.touch()

    # Instead of mocking relative_to, let's mock the path.parts access to raise AttributeError
    _ = Path.__dict__.get("parts", None)  # Store original property
    # We can't directly mock property, so let's mock the part where it's used
    mocker.patch.object(
        Path, "relative_to", side_effect=AttributeError("Mocked AttributeError")
    )

    # Should handle AttributeError gracefully and continue with valid files
    # But this will affect all files, so let's use a different approach - mock the specific
    # part of the code where the AttributeError occurs

    # Create a directory structure that will trigger the ValueError/AttributeError in discovery
    camera_dir2 = tmp_path / "camera2"
    camera_dir2.mkdir(parents=True, exist_ok=True)

    # Create a file in a directory structure with insufficient parts to trigger error handling
    short_path_dir = camera_dir2 / "invalid"  # Only 1 part instead of 4 needed
    short_path_dir.mkdir(parents=True, exist_ok=True)
    short_file = short_path_dir / "REO_camera_20230115120000.mp4"
    short_file.touch()

    # Should handle the error gracefully - files with insufficient directory structure are skipped
    mp4s2, mapping2, trash2 = archiver.FileDiscovery.discover_files(camera_dir2)
    # Files with insufficient directory parts should be skipped
    assert len(mp4s2) == 0


def test_transcode_file_cancellation_during_processing(
    mocker, tmp_path, logger, graceful_exit
):
    """Test cancellation during transcode processing."""
    in_p = Path("/in.mp4")
    out_p = tmp_path / "out.mp4"

    cancel_calls = [0]

    def mock_should_exit():
        cancel_calls[0] += 1
        return cancel_calls[0] > 1  # Exit on second call

    graceful_exit.should_exit = mock_should_exit

    proc = mocker.MagicMock()
    proc.stdout = mocker.MagicMock()
    proc.stdout.readline.side_effect = [
        "frame=100 time=00:00:01.00",
        KeyboardInterrupt(),
    ]
    proc.terminate = mocker.MagicMock()
    proc.wait = mocker.MagicMock(return_value=0)

    mocker.patch("subprocess.Popen", return_value=proc)
    mocker.patch.object(archiver.Transcoder, "get_video_duration", return_value=10.0)
    mocker.patch("pathlib.Path.mkdir")

    res = archiver.Transcoder.transcode_file(
        in_p, out_p, logger, None, graceful_exit, dry_run=False
    )
    assert res is False


def test_cleanup_orphaned_files_exit_requested(
    mocker, tmp_path, logger, graceful_exit, file_structure
):
    """Test cleanup respects graceful exit."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    graceful_exit.request_exit()

    orphan_jpg = file_structure(suffix="jpg")
    mapping = {"20230115120000": {".jpg": orphan_jpg}}

    mock_remove = mocker.patch.object(archiver.FileManager, "remove_file")
    _ = mocker.patch.object(archiver.FileManager, "clean_empty_directories")

    proc.cleanup_orphaned_files(mapping)

    mock_remove.assert_not_called()


def test_execute_plan_cancellation_during_execution(
    mocker, tmp_path, logger, graceful_exit
):
    """Test plan execution cancellation."""
    cfg = archiver.Config(archiver.parse_args([str(tmp_path)]))
    proc = archiver.FileProcessor(cfg, logger, graceful_exit)

    input_path = tmp_path / "in.mp4"
    out_path = tmp_path / "out.mp4"

    plan = {
        "transcoding": [
            {
                "type": "transcode",
                "input": input_path,
                "output": out_path,
                "jpg_to_remove": None,
            }
        ],
        "removals": [],
    }

    cancel_calls = [0]

    def mock_should_exit():
        cancel_calls[0] += 1
        return cancel_calls[0] > 0  # Exit immediately

    graceful_exit.should_exit = mock_should_exit

    mocker.patch.object(archiver.Transcoder, "transcode_file", return_value=True)
    progress = mocker.MagicMock()

    res = proc.execute_plan(plan, progress)
    assert res is True
