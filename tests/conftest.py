"""
Fixtures and utility functions for the Camera Archiver test suite.
"""

import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
from archiver import (  # noqa: E402
    Config,
    FileDiscovery,
    FileManager,
    GracefulExit,
    Logger,
    Transcoder,
)


@pytest.fixture(scope="module")
def persistent_camera_dir():
    """Module-scoped camera directory for read-only tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def camera_dir(temp_dir):
    """Create a temporary camera directory structure."""
    camera_path = temp_dir / "camera"
    camera_path.mkdir()
    yield camera_path


@pytest.fixture
def archived_dir(temp_dir):
    """Create a temporary archived directory structure."""
    archived_path = temp_dir / "archived"
    archived_path.mkdir()
    yield archived_path


@pytest.fixture
def trash_dir(temp_dir):
    """Create a temporary trash directory structure."""
    trash_path = temp_dir / ".deleted"
    trash_path.mkdir()
    yield trash_path


@pytest.fixture
def logger(temp_dir):
    """Create a test logger."""
    # Create a mock config with a temp log file to avoid directory issues
    mock_args = MagicMock()
    mock_args.directory = str(temp_dir)
    mock_args.log_file = str(temp_dir / "test.log")
    config = Config(mock_args)
    return Logger.setup(config)


@pytest.fixture
def graceful_exit():
    """Create a GracefulExit instance for tests."""
    return GracefulExit()


@pytest.fixture
def make_camera_file(temp_dir):
    """Factory fixture for creating camera files with timestamps."""

    def _make(timestamp: datetime, file_type: str = "mp4", archived: bool = False):
        year_dir = temp_dir / str(timestamp.year)
        month_dir = year_dir / f"{timestamp.month:02d}"
        day_dir = month_dir / f"{timestamp.day:02d}"
        day_dir.mkdir(parents=True, exist_ok=True)

        if archived:
            filename = f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.{file_type}"
        else:
            filename = f"REO_camera_{timestamp.strftime('%Y%m%d%H%M%S')}.{file_type}"

        file_path = day_dir / filename
        file_path.touch()
        return file_path

    return _make


@pytest.fixture
def make_file_set(make_camera_file):
    """Factory for creating MP4/JPG file pairs."""

    def _make(timestamps: list[datetime], archived: bool = False):
        files = []
        for ts in timestamps:
            mp4 = make_camera_file(ts, "mp4", archived)
            jpg = make_camera_file(ts, "jpg", archived)
            files.append({"mp4": mp4, "jpg": jpg, "timestamp": ts})
        return files

    return _make


class FileSetBuilder:
    """Builder pattern for complex file structures."""

    def __init__(self, temp_dir):
        self.temp_dir = temp_dir
        self.timestamps = []
        self.include_archives = False
        self.include_orphaned_jpgs = False

    def with_timestamps(self, *timestamps):
        """Add timestamps to the file set."""
        self.timestamps.extend(timestamps)
        return self

    def with_archives(self):
        """Include archive files in the build."""
        self.include_archives = True
        return self

    def with_orphaned_jpgs(self):
        """Include orphaned JPG files in the build."""
        self.include_orphaned_jpgs = True
        return self

    def build(self):
        """Build the file set according to specifications."""
        from datetime import datetime
        from pathlib import Path

        result = {"files": [], "archived_files": [], "orphaned_jpgs": []}

        # Create regular file sets
        for ts in self.timestamps:
            year_dir = self.temp_dir / str(ts.year)
            month_dir = year_dir / f"{ts.month:02d}"
            day_dir = month_dir / f"{ts.day:02d}"
            day_dir.mkdir(parents=True, exist_ok=True)

            # Create MP4 and JPG files
            mp4_path = day_dir / f"REO_camera_{ts.strftime('%Y%m%d%H%M%S')}.mp4"
            jpg_path = day_dir / f"REO_camera_{ts.strftime('%Y%m%d%H%M%S')}.jpg"
            mp4_path.touch()
            jpg_path.touch()

            result["files"].append({"mp4": mp4_path, "jpg": jpg_path, "timestamp": ts})

            # Create archive files if requested
            if self.include_archives:
                archived_path = day_dir / f"archived-{ts.strftime('%Y%m%d%H%M%S')}.mp4"
                archived_path.touch()
                result["archived_files"].append(archived_path)

        # Create orphaned JPG files if requested
        if self.include_orphaned_jpgs:
            orphaned_ts = datetime.now()
            orphaned_jpg = (
                self.temp_dir / f"REO_camera_{orphaned_ts.strftime('%Y%m%d%H%M%S')}.jpg"
            )
            orphaned_jpg.touch()
            result["orphaned_jpgs"].append(orphaned_jpg)

        return result


@pytest.fixture
def file_set_builder(temp_dir):
    """Builder pattern for complex file structures."""
    return FileSetBuilder(temp_dir)


@pytest.fixture
def sample_files(camera_dir):
    """Create sample camera files for testing."""
    # Create directory structure
    date = datetime(2023, 1, 15, 12, 0, 0)
    year_dir = camera_dir / str(date.year)
    month_dir = year_dir / f"{date.month:02d}"
    day_dir = month_dir / f"{date.day:02d}"
    day_dir.mkdir(parents=True)

    # Create sample files
    mp4_file = day_dir / f"REO_camera_{date.strftime('%Y%m%d%H%M%S')}.mp4"
    jpg_file = day_dir / f"REO_camera_{date.strftime('%Y%m%d%H%M%S')}.jpg"

    mp4_file.touch()
    jpg_file.touch()

    return {"mp4": mp4_file, "jpg": jpg_file, "timestamp": date}


@pytest.fixture
def mock_args():
    """Create mock command-line arguments."""
    args = MagicMock()
    args.directory = "/camera"
    args.output = None
    args.dry_run = False
    args.no_confirm = False
    args.no_skip = False
    args.delete = False
    args.trash_root = None
    args.cleanup = False
    args.clean_output = False
    args.age = 30
    args.log_file = None
    return args


@pytest.fixture
def config(mock_args):
    """Create a Config instance from mock arguments."""
    return Config(mock_args)


@pytest.fixture
def complete_test_environment(camera_dir, archived_dir, trash_dir, logger, config):
    """Provides complete test environment in one fixture."""
    return {
        "camera_dir": camera_dir,
        "archived_dir": archived_dir,
        "trash_dir": trash_dir,
        "logger": logger,
        "config": config,
    }


@pytest.fixture
def mock_subprocess_patterns(mocker):
    """Provides common subprocess mocking patterns."""
    return {
        "success": lambda: mocker.patch(
            "archiver.subprocess.run",
            return_value=mocker.Mock(stdout="120.5\n", returncode=0),
        ),
        "failure": lambda: mocker.patch(
            "archiver.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "cmd"),
        ),
        "not_found": lambda: mocker.patch("shutil.which", return_value=None),
    }


@pytest.fixture
def mock_file_operations(mocker):
    """Provides common file operation mocks."""
    return {
        "remove_success": lambda: mocker.patch.object(FileManager, "remove_file"),
        "remove_failure": lambda: mocker.patch.object(
            FileManager, "remove_file", side_effect=OSError("Permission denied")
        ),
        "discovery": lambda files: mocker.patch.object(
            FileDiscovery, "discover_files", return_value=files
        ),
    }


@pytest.fixture(params=["success", "failure", "interrupt"])
def transcoder_behavior(
    request,
    mocker,
    mock_transcode_success,
    mock_transcode_fail,
    mock_transcode_interrupt,
):
    """Parametrized fixture for different transcoder behaviors."""
    behaviors = {
        "success": mock_transcode_success,
        "failure": mock_transcode_fail,
        "interrupt": mock_transcode_interrupt,
    }

    return behaviors[request.param]


@pytest.fixture(scope="session")
def mock_transcode_success():
    """
    A deterministic mock for Transcoder.transcode_file that:
    - creates the output path (including parent dirs)
    - calls the progress callback at 25 / 50 / 75 / 100 %
    - returns True (success)
    """

    def _mock(
        input_path,
        output_path,
        logger,
        progress_cb=None,
        graceful_exit=None,
        dry_run=False,
    ):
        if not dry_run:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.touch()
        if progress_cb:
            for pct in (25.0, 50.0, 75.0, 100.0):
                progress_cb(pct)
        return True

    return _mock


@pytest.fixture(scope="session")
def mock_transcode_fail():
    """
    Mock that always returns False (ffmpeg error).
    """

    def _mock(*args, **kwargs):
        return False

    return _mock


@pytest.fixture(scope="session")
def mock_transcode_interrupt():
    """
    Mock that requests a graceful exit after 25 % progress.
    """

    def _mock(
        input_path,
        output_path,
        logger,
        progress_cb=None,
        graceful_exit=None,
        dry_run=False,
    ):
        if progress_cb:
            progress_cb(25.0)
            if graceful_exit:
                graceful_exit.request_exit()
            progress_cb(50.0)
        return False

    return _mock


class FileAssertions:
    """Helper class for file-related assertions."""

    @staticmethod
    def assert_file_moved_to_trash(source_file, trash_dir, is_output=False):
        """Assert file was moved to trash correctly."""
        assert not source_file.exists(), f"Source {source_file} still exists"

        trash_sub = "output" if is_output else "input"
        # Calculate expected trash path
        expected = trash_dir / trash_sub / source_file.name
        assert expected.exists(), f"Expected trash file {expected} not found"

    @staticmethod
    def assert_archive_created(timestamp, output_dir):
        """Assert archive file was created with correct structure."""
        expected = (
            output_dir
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        assert expected.exists(), f"Archive {expected} not created"
        return expected


@pytest.fixture
def file_assertions():
    return FileAssertions()


class PlanAssertions:
    """Helper class for action plan assertions."""

    @staticmethod
    def assert_plan_structure(plan, expected_transcodes, expected_removals):
        """Validate plan structure and counts."""
        assert "transcoding" in plan
        assert "removals" in plan
        assert len(plan["transcoding"]) == expected_transcodes
        assert len(plan["removals"]) == expected_removals

    @staticmethod
    def assert_plan_includes_file(plan, file_path, action_type):
        """Assert plan contains action for specific file."""
        if action_type == "transcode":
            assert any(a["input"] == file_path for a in plan["transcoding"])
        else:
            assert any(a["file"] == file_path for a in plan["removals"])


@pytest.fixture
def plan_assertions():
    return PlanAssertions()


@pytest.fixture(params=["basic_transcode", "failed_transcode", "with_interrupt"])
def integration_scenario(
    request,
    make_file_set,
    mocker,
    mock_transcode_success,
    mock_transcode_fail,
    mock_transcode_interrupt,
):
    """Parametrized fixture for different integration test scenarios."""
    # Return scenario configuration based on param
    scenarios = {
        "basic_transcode": {
            "files": make_file_set([datetime(2023, 1, 15, 12, 0)]),
            "mocks": {"transcode": "success"},
            "expected": {"archives": 1, "trash": 2},
        },
        "failed_transcode": {
            "files": make_file_set([datetime(2023, 1, 15, 12, 0)]),
            "mocks": {"transcode": "failure"},
            "expected": {"archives": 0, "trash": 0},
        },
        "with_interrupt": {
            "files": make_file_set([datetime(2023, 1, 15, 12, 0)]),
            "mocks": {"transcode": "interrupt"},
            "expected": {"archives": 0, "trash": 0},
        },
    }

    scenario_name = request.param
    scenario = scenarios[scenario_name]

    # Apply the appropriate mock for transcoding based on scenario
    transcode_mocks = {
        "success": mock_transcode_success,
        "failure": mock_transcode_fail,
        "interrupt": mock_transcode_interrupt,
    }

    mocker.patch.object(
        Transcoder,
        "transcode_file",
        side_effect=transcode_mocks[scenario["mocks"]["transcode"]],
    )

    return scenario


@pytest.fixture
def ffmpeg_mock(mocker):
    """Auto-configured ffmpeg process mock."""

    def _configure(success=True, progress_points=None):
        if progress_points is None:
            progress_points = [
                "frame=100 fps=100 time=00:00:01.00",
                "frame=200 fps=200 time=00:00:02.00",
            ]

        returncode = 0 if success else 1
        mock_process = mocker.Mock()
        mock_process.stdout = mocker.Mock()
        mock_process.stdout.readline = mocker.Mock(side_effect=progress_points + [""])
        mock_process.wait.return_value = returncode

        def popen_side_effect(*args, **kwargs):
            if args and len(args[0]) > 0:
                output_path = Path(args[0][-1])  # Last argument is usually output path
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()
            return mock_process

        return mocker.patch("archiver.subprocess.Popen", side_effect=popen_side_effect)

    return _configure


@pytest.fixture(
    params=[
        "complete_workflow",
        "with_cleanup",
        "dry_run",
        "existing_archive",
        "multiple_files",
        "signal_interrupt",
        "transcode_failure",
    ]
)
def e2e_scenario(request):
    """Provides e2e test scenario configuration."""
    return {
        "complete_workflow": {
            "config_mods": {"no_confirm": True},
            "file_count": 1,
            "transcode_behavior": "success",
            "expected_outcomes": {
                "return_code": 0,
                "files_archived": 1,
                "files_in_trash": 2,
            },
        },
        "with_cleanup": {
            "config_mods": {"no_confirm": True, "cleanup": True},
            "file_count": 1,
            "transcode_behavior": "success",
            "expected_outcomes": {
                "return_code": 0,
                "files_archived": 0,  # No transcoding in cleanup mode
                "files_in_trash": 2,
            },
        },
        "dry_run": {
            "config_mods": {"dry_run": True},
            "file_count": 1,
            "transcode_behavior": "success",
            "expected_outcomes": {
                "return_code": 0,
                "files_archived": 0,
                "files_in_trash": 0,  # Files should remain in source
            },
        },
        "existing_archive": {
            "config_mods": {"no_confirm": True, "no_skip": False},
            "file_count": 1,
            "transcode_behavior": "success",
            "expected_outcomes": {
                "return_code": 0,
                "files_archived": 0,  # Should skip transcoding since archive exists
                "files_in_trash": 2,  # But still remove source files
            },
        },
        "multiple_files": {
            "config_mods": {"no_confirm": True},
            "file_count": 3,
            "transcode_behavior": "success",
            "expected_outcomes": {
                "return_code": 0,
                "files_archived": 3,
                "files_in_trash": 6,  # 3 mp4 + 3 jpg
            },
        },
        "signal_interrupt": {
            "config_mods": {"no_confirm": True},
            "file_count": 1,
            "transcode_behavior": "interrupt",
            "expected_outcomes": {
                "return_code": 0,  # Should still return 0 on interruption
                "files_archived": 0,
                "files_in_trash": 0,  # Files should remain due to interruption
            },
        },
        "transcode_failure": {
            "config_mods": {"no_confirm": True},
            "file_count": 1,
            "transcode_behavior": "failure",
            "expected_outcomes": {
                "return_code": 0,  # Should still return 0 on failure
                "files_archived": 0,
                "files_in_trash": 0,  # Files should remain due to failure
            },
        },
    }[request.param]


class E2EOutcomeValidator:
    """Validates e2e test outcomes."""

    def __init__(self, camera_dir, archived_dir, trash_dir):
        self.camera_dir = camera_dir
        self.archived_dir = archived_dir
        self.trash_dir = trash_dir

    def validate(self, expected_outcomes, sample_files_list=None):
        """Validate all expected outcomes."""
        results = {}

        if "return_code" in expected_outcomes:
            # Validation for return code would happen externally
            results["return_code_validated"] = True

        if "files_archived" in expected_outcomes:
            archived_count = len(list(self.archived_dir.rglob("archived-*.mp4")))
            results["files_archived"] = {
                "expected": expected_outcomes["files_archived"],
                "actual": archived_count,
                "passed": archived_count == expected_outcomes["files_archived"],
            }

        if "files_in_trash" in expected_outcomes:
            trash_files = list((self.trash_dir / "input").rglob("*.*"))
            trash_count = len(trash_files)
            results["files_in_trash"] = {
                "expected": expected_outcomes["files_in_trash"],
                "actual": trash_count,
                "passed": trash_count == expected_outcomes["files_in_trash"],
            }

        return results


@pytest.fixture
def e2e_outcome_validator(camera_dir, archived_dir, trash_dir):
    """Provides E2E outcome validation helper."""
    return E2EOutcomeValidator(camera_dir, archived_dir, trash_dir)


@pytest.fixture(autouse=True)
def reset_globals():
    """Reset global state before each test."""
    import archiver

    archiver.ACTIVE_PROGRESS_REPORTER = None
    yield
