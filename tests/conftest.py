"""
Fixtures and utility functions for the Camera Archiver test suite.
"""

import shutil
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
    GracefulExit,
    Logger,
)


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
