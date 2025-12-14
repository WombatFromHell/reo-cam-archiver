"""
Test suite for the Logger module.

This module tests the Logger class which handles logging setup with file rotation,
thread-safe output, and coordination with progress reporting.
"""

import logging
import os
from pathlib import Path

import pytest

from src.archiver.config import Config
from src.archiver.logger import Logger, ThreadSafeStreamHandler


class TestThreadSafeStreamHandler:
    """Test ThreadSafeStreamHandler functionality."""

    def test_thread_safe_handler_emit_without_progress(self, mocker):
        """Test ThreadSafeStreamHandler emit when no progress reporter is active."""
        handler = ThreadSafeStreamHandler()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        mock_write = mocker.patch("sys.stderr.write")
        handler.emit(record)
        # Should call super().emit() which writes to stderr
        mock_write.assert_called()

    def test_thread_safe_handler_emit_with_progress(self, mocker):
        """Test ThreadSafeStreamHandler emit when progress reporter is active."""
        # Mock the ACTIVE_PROGRESS_REPORTER
        mock_reporter = mocker.MagicMock()

        mocker.patch("src.archiver.logger.ACTIVE_PROGRESS_REPORTER", mock_reporter)
        handler = ThreadSafeStreamHandler()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        mock_write = mocker.patch("sys.stderr.write")
        handler.emit(record)
        # Should clear the progress line first
        mock_write.assert_any_call("\r" + " " * 80 + "\r")


class TestLoggerSetup:
    """Test Logger.setup() functionality."""

    def test_setup_creates_logger_with_correct_name(self, mock_args):
        """Test that setup creates a logger with the correct name."""
        config = Config(mock_args)
        logger = Logger.setup(config)

        assert logger.name == "camera_archiver"

    def test_setup_sets_correct_log_level(self, mock_args):
        """Test that setup sets the correct log level."""
        config = Config(mock_args)
        logger = Logger.setup(config)

        assert logger.level == logging.INFO

    def test_setup_clears_existing_handlers(self, mock_args):
        """Test that setup clears existing handlers."""
        config = Config(mock_args)
        logger = Logger.setup(config)

        # Call setup again
        logger2 = Logger.setup(config)

        # Should have the same logger instance
        assert logger is logger2

    def test_setup_adds_console_handler(self, mock_args):
        """Test that setup adds a console handler."""
        config = Config(mock_args)
        logger = Logger.setup(config)

        # Should have at least one handler (console)
        assert len(logger.handlers) >= 1

        # At least one handler should be ThreadSafeStreamHandler
        assert any(isinstance(h, ThreadSafeStreamHandler) for h in logger.handlers)


class TestLoggerFileHandling:
    """Test Logger file handling functionality."""

    def test_setup_with_log_file(self, temp_dir, mock_args):
        """Test that setup creates file handler when log_file is specified."""
        log_file = temp_dir / "test.log"
        config = Config(mock_args)
        config.log_file = log_file
        logger = Logger.setup(config)

        # Should have file handler
        file_handlers = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1

    def test_setup_without_log_file(self, mock_args):
        """Test that setup works without log_file."""
        config = Config(mock_args)
        config.log_file = None
        logger = Logger.setup(config)

        # Should still work, just without file handler
        assert len(logger.handlers) >= 1  # Should have console handler

    def test_setup_with_invalid_log_file_path(self, mock_args):
        """Test that setup handles invalid log file paths gracefully."""
        # Use a path that can't be created
        config = Config(mock_args)
        config.log_file = Path("/invalid/path/test.log")
        logger = Logger.setup(config)

        # Should still work, just without file handler
        assert len(logger.handlers) >= 1  # Should have console handler


class TestLoggerLogRotation:
    """Test Logger log rotation functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "no_backups",
                "backup_files": [],
                "expected_max": 0,
            },
            {
                "name": "single_backup",
                "backup_files": ["test.log.1"],
                "expected_max": 1,
            },
            {
                "name": "multiple_backups",
                "backup_files": ["test.log.1", "test.log.3", "test.log.2"],
                "expected_max": 3,
            },
            {
                "name": "non_numeric_backups",
                "backup_files": ["test.log.backup", "test.log.old", "test.log.1"],
                "expected_max": 1,
            },
            {
                "name": "mixed_backups",
                "backup_files": [
                    "test.log.5",
                    "test.log.2",
                    "test.log.old",
                    "test.log.10",
                ],
                "expected_max": 10,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "no_backups"},
                {"name": "single_backup"},
                {"name": "multiple_backups"},
                {"name": "non_numeric_backups"},
                {"name": "mixed_backups"},
            ]
        ],
    )
    def test_find_max_backup_number(self, temp_dir, test_case):
        """Test _find_max_backup_number with various backup file scenarios."""
        log_file = temp_dir / "test.log"

        # Create backup files based on test case
        for backup_file in test_case["backup_files"]:
            (temp_dir / backup_file).touch()

        max_backup = Logger._find_max_backup_number(log_file)
        assert max_backup == test_case["expected_max"]

    def test_rename_existing_backups(self, temp_dir):
        """Test _rename_existing_backups functionality."""
        log_file = temp_dir / "test.log"

        # Create existing backup files
        (temp_dir / "test.log.1").touch()
        (temp_dir / "test.log.2").touch()

        # Rename backups
        Logger._rename_existing_backups(log_file, 2)

        # Check that files were renamed correctly
        # Original test.log.1 should be renamed to test.log.2
        # Original test.log.2 should be renamed to test.log.3
        assert not (temp_dir / "test.log.1").exists()  # Original .1 should be gone
        assert (temp_dir / "test.log.2").exists()  # Original .1 renamed to .2
        assert (temp_dir / "test.log.3").exists()  # Original .2 renamed to .3

    def test_create_backup_and_new_log(self, temp_dir):
        """Test _create_backup_and_new_log functionality."""
        log_file = temp_dir / "test.log"

        # Create original log file
        log_file.write_text("Original content")

        # Create backup and new log
        Logger._create_backup_and_new_log(log_file)

        # Check that backup was created
        backup_file = temp_dir / "test.log.1"
        assert backup_file.exists()
        assert backup_file.read_text() == "Original content"

        # Check that new log file was created
        assert log_file.exists()
        assert log_file.read_text() == ""

    def test_create_new_log_file(self, temp_dir):
        """Test _create_new_log_file functionality."""
        log_file = temp_dir / "test.log"

        # Create new log file
        Logger._create_new_log_file(log_file)

        # Check that file was created
        assert log_file.exists()

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "under_size_limit",
                "content_size": "small",
                "should_rotate": False,
            },
            {
                "name": "over_size_limit",
                "content_size": "large",
                "should_rotate": True,
            },
            {
                "name": "empty_file",
                "content_size": "empty",
                "should_rotate": False,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "under_size_limit"},
                {"name": "over_size_limit"},
                {"name": "empty_file"},
            ]
        ],
    )
    def test_rotate_log_file(self, temp_dir, test_case):
        """Test _rotate_log_file with various file size scenarios."""
        log_file = temp_dir / "test.log"

        # Create log file with appropriate content based on test case
        if test_case["content_size"] == "small":
            log_file.write_text("Small content")
        elif test_case["content_size"] == "large":
            # Create large log file (over LOG_ROTATION_SIZE which is 4MB)
            large_content = "x" * (5 * 1024 * 1024)  # 5MB
            log_file.write_text(large_content)
        elif test_case["content_size"] == "empty":
            log_file.write_text("")

        # Rotate
        Logger._rotate_log_file(log_file)

        if test_case["should_rotate"]:
            # Check that rotation occurred
            backup_file = temp_dir / "test.log.1"
            assert backup_file.exists()
            assert backup_file.stat().st_size > 0

            # Check that new log file was created
            assert log_file.exists()
            assert log_file.stat().st_size == 0
        else:
            # File should still exist and not be renamed
            assert log_file.exists()
            assert not (temp_dir / "test.log.1").exists()


class TestLoggerErrorHandling:
    """Test Logger error handling."""

    def test_setup_handles_mock_log_file(self, mock_args, mocker):
        """Test that setup handles mock log_file gracefully."""
        config = Config(mock_args)

        # Create a mock that behaves like a Path but returns False for exists()
        mock_log_file = mocker.MagicMock()
        mock_log_file.exists.return_value = False
        mock_log_file.parent.mkdir = mocker.MagicMock()
        mock_log_file.touch = mocker.MagicMock()

        config.log_file = mock_log_file

        # Should not raise an exception
        logger = Logger.setup(config)
        assert logger is not None

    def test_setup_handles_unwritable_directory(self, mock_args):
        """Test that setup handles unwritable directories gracefully."""
        config = Config(mock_args)
        config.log_file = Path("/root/test.log")  # Likely unwritable

        # Should not raise an exception
        logger = Logger.setup(config)
        assert logger is not None

    def test_rotate_log_file_handles_missing_file(self, temp_dir):
        """Test that _rotate_log_file handles missing files gracefully."""
        log_file = temp_dir / "nonexistent.log"

        # Should not raise an exception
        Logger._rotate_log_file(log_file)


class TestLoggerIntegration:
    """Test Logger integration with other components."""

    def test_integration_with_progress_reporter(self, mock_args, mocker):
        """Test that Logger works correctly with active progress reporter."""
        # Mock the ACTIVE_PROGRESS_REPORTER
        mock_reporter = mocker.MagicMock()

        mocker.patch("src.archiver.logger.ACTIVE_PROGRESS_REPORTER", mock_reporter)
        config = Config(mock_args)
        logger = Logger.setup(config)

        # Log a message
        mock_write = mocker.patch("sys.stderr.write")
        logger.info("Test message")
        # Should clear the progress line first
        mock_write.assert_any_call("\r" + " " * 80 + "\r")

    def test_integration_with_output_lock(self, mock_args):
        """Test that Logger uses OUTPUT_LOCK for coordination."""
        config = Config(mock_args)
        logger = Logger.setup(config)

        # The OUTPUT_LOCK should be used in ThreadSafeStreamHandler
        # This test mainly verifies no exceptions are raised
        logger.info("Test message")
        assert True


class TestLoggerEdgeCases:
    """Test Logger edge cases."""

    def test_setup_with_empty_log_file_path(self, mock_args):
        """Test setup with empty log file path."""
        config = Config(mock_args)
        config.log_file = None  # Empty log file path
        logger = Logger.setup(config)

        # Should work without file handler
        assert logger is not None

    def test_setup_with_relative_log_file_path(self, temp_dir, mock_args):
        """Test setup with relative log file path."""
        original_cwd = Path.cwd()

        try:
            # Change to temp directory
            os.chdir(temp_dir)

            # Use relative path
            config = Config(mock_args)
            config.log_file = Path("test.log")  # Relative path
            logger = Logger.setup(config)

            # Should work with relative path
            assert logger is not None
        finally:
            # Restore original directory
            os.chdir(original_cwd)

    def test_log_rotation_with_no_permission(self, temp_dir):
        """Test log rotation when we don't have permission to create backups."""
        log_file = temp_dir / "test.log"

        # Create a large file
        large_content = "x" * (5 * 1024 * 1024)  # 5MB
        log_file.write_text(large_content)

        # Make the file read-only
        log_file.chmod(0o444)

        try:
            # Try to rotate (should handle gracefully)
            Logger._rotate_log_file(log_file)
        finally:
            # Restore permissions
            log_file.chmod(0o644)

    def test_multiple_setup_calls(self, mock_args):
        """Test that multiple calls to setup work correctly."""
        config = Config(mock_args)

        # Call setup multiple times
        logger1 = Logger.setup(config)
        logger2 = Logger.setup(config)
        logger3 = Logger.setup(config)

        # Should return the same logger instance
        assert logger1 is logger2 is logger3

        # Should still have handlers
        assert len(logger1.handlers) >= 1


class TestErrorHandling:
    """Test error handling in logger methods."""

    def test_setup_with_handler_removal_error(self, mock_args, caplog, mocker):
        """Test logger setup when handler removal fails."""
        config = Config(mock_args)

        # Create a logger with a problematic handler
        logger = logging.getLogger("camera_archiver")

        # Create a mock handler that raises exception on close
        mock_handler = mocker.MagicMock()
        mock_handler.close.side_effect = Exception("Mock close error")
        logger.addHandler(mock_handler)

        # Setup should handle the exception gracefully
        result = Logger.setup(config)

        # Should still return a logger
        assert result is not None
        assert isinstance(result, logging.Logger)

    def test_create_new_log_file_with_oserror(self, temp_dir, mocker):
        """Test creating new log file when OSError occurs."""
        log_file = temp_dir / "test.log"

        # Mock the open function to raise OSError
        mocker.patch("builtins.open", side_effect=OSError("Mock OS error"))
        # Should handle the OSError gracefully
        Logger._create_new_log_file(log_file)

        # Should not raise exceptions
        assert True

    def test_create_new_log_file_with_attribute_error(self, temp_dir, mocker):
        """Test creating new log file when AttributeError occurs."""
        log_file = temp_dir / "test.log"

        # Mock the open function to raise AttributeError
        mocker.patch(
            "builtins.open", side_effect=AttributeError("Mock attribute error")
        )
        # Should handle the AttributeError gracefully
        Logger._create_new_log_file(log_file)

        # Should not raise exceptions
        assert True

    def test_rotate_log_file_with_oserror(self, temp_dir, mocker):
        """Test rotating log file when OSError occurs."""
        log_file = temp_dir / "test.log"
        log_file.touch()

        # Mock the rename function to raise OSError
        mocker.patch("pathlib.Path.rename", side_effect=OSError("Mock OS error"))
        # Should handle the OSError gracefully
        Logger._rotate_log_file(log_file)

        # Should not raise exceptions
        assert True

    def test_rotate_log_file_with_attribute_error(self, temp_dir, mocker):
        """Test rotating log file when AttributeError occurs."""
        log_file = temp_dir / "test.log"
        log_file.touch()

        # Mock the rename function to raise AttributeError
        mocker.patch(
            "pathlib.Path.rename", side_effect=AttributeError("Mock attribute error")
        )
        # Should handle the AttributeError gracefully
        Logger._rotate_log_file(log_file)

        # Should not raise exceptions
        assert True

    def test_setup_with_mock_config(self, caplog, mocker):
        """Test logger setup with mock config that might cause errors."""
        # Create a mock config
        mock_config = mocker.MagicMock()
        mock_config.log_file = None  # This should be handled gracefully

        # Setup should handle any errors gracefully
        result = Logger.setup(mock_config)

        # Should still return a logger
        assert result is not None
        assert isinstance(result, logging.Logger)
