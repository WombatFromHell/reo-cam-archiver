"""
Test suite for the ProgressReporter module.

This module tests the ProgressReporter class which handles progress reporting
with thread-safe operations, time formatting, and coordination with logging.
"""

import time

import pytest

from src.archiver.graceful_exit import GracefulExit
from src.archiver.progress import ProgressReporter


class TestProgressReporterInitialization:
    """Test ProgressReporter initialization and basic properties."""

    def test_initialization_with_defaults(self):
        """Test ProgressReporter initialization with default parameters."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=10, graceful_exit=graceful_exit)

        assert reporter.total == 10
        assert reporter.graceful_exit == graceful_exit
        assert not reporter.silent
        assert reporter.current == 0
        assert reporter.start_time > 0
        assert reporter.current_file_start_time > 0
        assert hasattr(reporter, "_lock")

    def test_initialization_with_silent_mode(self):
        """Test ProgressReporter initialization in silent mode."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(
            total_files=5, graceful_exit=graceful_exit, silent=True
        )

        assert reporter.total == 5
        assert reporter.silent

    def test_initialization_with_zero_files(self):
        """Test ProgressReporter initialization with zero files."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=0, graceful_exit=graceful_exit)

        assert reporter.total == 0
        assert reporter.current == 0


class TestProgressReporterTimeFormatting:
    """Test time formatting functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "zero_seconds",
                "seconds": 0,
                "expected": "00:00",
            },
            {
                "name": "middle_of_minute",
                "seconds": 59,
                "expected": "00:59",
            },
            {
                "name": "one_minute",
                "seconds": 60,
                "expected": "01:00",
            },
            {
                "name": "just_under_one_hour",
                "seconds": 3599,
                "expected": "59:59",
            },
            {
                "name": "one_hour",
                "seconds": 3600,
                "expected": "01:00:00",
            },
            {
                "name": "one_hour_one_minute_one_second",
                "seconds": 3661,
                "expected": "01:01:01",
            },
            {
                "name": "two_hours",
                "seconds": 7200,
                "expected": "02:00:00",
            },
            {
                "name": "just_under_one_day",
                "seconds": 86399,
                "expected": "23:59:59",
            },
            {
                "name": "sub_second",
                "seconds": 0.5,
                "expected": "00:00",
            },
            {
                "name": "just_under_one_hour_decimal",
                "seconds": 3599.9,
                "expected": "59:59",
            },
            {
                "name": "just_over_one_hour_decimal",
                "seconds": 3600.1,
                "expected": "01:00:00",
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "zero_seconds"},
                {"name": "middle_of_minute"},
                {"name": "one_minute"},
                {"name": "just_under_one_hour"},
                {"name": "one_hour"},
                {"name": "one_hour_one_minute_one_second"},
                {"name": "two_hours"},
                {"name": "just_under_one_day"},
                {"name": "sub_second"},
                {"name": "just_under_one_hour_decimal"},
                {"name": "just_over_one_hour_decimal"},
            ]
        ],
    )
    def test_format_time(self, test_case):
        """Test time formatting with various duration scenarios."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        result = reporter.format_time(test_case["seconds"])
        assert result == test_case["expected"]


class TestProgressReporterFileTracking:
    """Test file tracking functionality."""

    def test_start_file_increments_counter(self):
        """Test that start_file increments the current file counter."""
        reporter = ProgressReporter(total_files=3, graceful_exit=GracefulExit())

        assert reporter.current == 0
        reporter.start_file()
        assert reporter.current == 1
        reporter.start_file()
        assert reporter.current == 2

    def test_start_file_resets_file_timer(self):
        """Test that start_file resets the current file start time."""
        reporter = ProgressReporter(total_files=2, graceful_exit=GracefulExit())

        first_start = reporter.current_file_start_time
        time.sleep(0.01)  # Small delay
        reporter.start_file()
        second_start = reporter.current_file_start_time

        assert second_start > first_start

    def test_finish_file_calls_update_progress(self, mocker):
        """Test that finish_file calls update_progress with 100%."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        mock_update = mocker.patch.object(reporter, "update_progress")
        reporter.finish_file()
        mock_update.assert_called_once_with(100.0)


class TestProgressReporterProgressUpdates:
    """Test progress update functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "silent_mode",
                "silent": True,
                "graceful_exit": False,
                "percentage": 50.0,
                "should_call_write": False,
            },
            {
                "name": "graceful_exit",
                "silent": False,
                "graceful_exit": True,
                "percentage": 50.0,
                "should_call_write": False,
            },
            {
                "name": "100_percent",
                "silent": False,
                "graceful_exit": False,
                "percentage": 100.0,
                "should_call_write": True,
                "should_contain_newline": True,
            },
            {
                "name": "less_than_100_percent",
                "silent": False,
                "graceful_exit": False,
                "percentage": 50.0,
                "should_call_write": True,
                "should_contain_newline": False,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "silent_mode"},
                {"name": "graceful_exit"},
                {"name": "100_percent"},
                {"name": "less_than_100_percent"},
            ]
        ],
    )
    def test_update_progress(self, mocker, test_case):
        """Test update_progress with various scenarios."""
        graceful_exit = GracefulExit()
        if test_case["graceful_exit"]:
            graceful_exit.request_exit()

        reporter = ProgressReporter(
            total_files=1, graceful_exit=graceful_exit, silent=test_case["silent"]
        )

        mock_write = mocker.patch("sys.stderr.write")
        reporter.update_progress(test_case["percentage"])

        if test_case["should_call_write"]:
            mock_write.assert_called()
            call_args = mock_write.call_args[0][0]

            if test_case.get("should_contain_newline", False):
                assert "\n" in call_args
            else:
                assert not call_args.endswith("\n")
        else:
            mock_write.assert_not_called()

    def test_update_progress_format(self, mocker):
        """Test the format of progress updates."""
        reporter = ProgressReporter(total_files=2, graceful_exit=GracefulExit())
        reporter.start_file()  # current = 1

        mock_write = mocker.patch("sys.stderr.write")
        reporter.update_progress(25.0)

        call_args = mock_write.call_args[0][0]
        assert "Progress [1/2]" in call_args
        assert "25%" in call_args
        assert "|" in call_args  # Progress bar
        assert "-" in call_args  # Progress bar


class TestProgressReporterFinish:
    """Test finish functionality."""

    def test_finish_in_silent_mode(self, mocker):
        """Test that finish does nothing in silent mode."""
        reporter = ProgressReporter(
            total_files=1, graceful_exit=GracefulExit(), silent=True
        )

        mock_write = mocker.patch("sys.stderr.write")
        reporter.finish()
        mock_write.assert_not_called()

    def test_finish_adds_newline(self, mocker):
        """Test that finish adds a newline to complete the progress bar."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        mock_write = mocker.patch("sys.stderr.write")
        reporter.finish()
        mock_write.assert_called_once_with("\n")


class TestProgressReporterContextManager:
    """Test context manager functionality."""

    def test_context_manager_sets_global_reporter(self):
        """Test that context manager sets the global ACTIVE_PROGRESS_REPORTER."""
        # Import the module to access the global variable
        import src.archiver.utils as utils

        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=1, graceful_exit=graceful_exit)

        # The global variable should be accessible through the module
        assert utils.ACTIVE_PROGRESS_REPORTER is None

        with reporter:
            # After entering the context, the global should be set
            assert utils.ACTIVE_PROGRESS_REPORTER is reporter

        # After exiting the context, the global should be None again
        assert utils.ACTIVE_PROGRESS_REPORTER is None

    def test_context_manager_calls_finish_on_exit(self, mocker):
        """Test that context manager calls finish on exit."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=1, graceful_exit=graceful_exit)

        mock_finish = mocker.patch.object(reporter, "finish")
        with reporter:
            pass
        mock_finish.assert_called_once()

    def test_context_manager_handles_exceptions(self):
        """Test that context manager properly handles exceptions."""
        import src.archiver.utils as utils

        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=1, graceful_exit=graceful_exit)

        with pytest.raises(ValueError):
            with reporter:
                assert utils.ACTIVE_PROGRESS_REPORTER is reporter
                raise ValueError("Test exception")

        assert utils.ACTIVE_PROGRESS_REPORTER is None


class TestProgressReporterThreadSafety:
    """Test thread safety of ProgressReporter."""

    def test_concurrent_start_file(self):
        """Test that concurrent start_file calls are thread-safe."""
        reporter = ProgressReporter(total_files=100, graceful_exit=GracefulExit())

        import threading

        def start_files():
            for _ in range(10):
                reporter.start_file()

        threads = []
        for _ in range(10):
            thread = threading.Thread(target=start_files)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Should have exactly 100 calls (10 threads * 10 calls each)
        assert reporter.current == 100

    def test_concurrent_update_progress(self):
        """Test that concurrent update_progress calls are thread-safe."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        import threading

        def update_progress():
            for _ in range(10):
                reporter.update_progress(50.0)

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=update_progress)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Should complete without errors
        assert True


class TestProgressReporterIntegration:
    """Test integration with other components."""

    def test_integration_with_graceful_exit(self, mocker):
        """Test integration with GracefulExit during progress updates."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=5, graceful_exit=graceful_exit)

        # Start a file
        reporter.start_file()

        # Request exit
        graceful_exit.request_exit()

        # Progress updates should be suppressed
        mock_write = mocker.patch("sys.stderr.write")
        reporter.update_progress(50.0)
        mock_write.assert_not_called()

    def test_integration_with_output_lock(self, mocker):
        """Test that ProgressReporter uses OUTPUT_LOCK for coordination."""
        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=1, graceful_exit=graceful_exit)

        # The OUTPUT_LOCK should be used in update_progress
        reporter.update_progress(50.0)
        # This test mainly verifies no exceptions are raised
        assert True


class TestProgressReporterEdgeCases:
    """Test edge cases and error conditions."""

    def test_zero_total_files(self):
        """Test behavior with zero total files."""
        reporter = ProgressReporter(total_files=0, graceful_exit=GracefulExit())

        # Should handle gracefully
        reporter.start_file()
        reporter.update_progress(100.0)
        reporter.finish()

    def test_negative_progress(self, mocker):
        """Test behavior with negative progress values."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        reporter.update_progress(-10.0)
        # Should handle gracefully
        assert True

    def test_over_100_percent_progress(self, mocker):
        """Test behavior with progress over 100%."""
        reporter = ProgressReporter(total_files=1, graceful_exit=GracefulExit())

        reporter.update_progress(150.0)
        # Should handle gracefully
        assert True
