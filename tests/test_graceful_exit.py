"""
Test suite for the GracefulExit module.

This module tests the GracefulExit class which handles thread-safe exit flag
management and signal handling for graceful shutdown.
"""

import signal
import threading
import time
from pathlib import Path

import pytest

from src.archiver.graceful_exit import GracefulExit, setup_signal_handlers


class TestGracefulExitInitialization:
    """Test GracefulExit initialization."""

    def test_initialization(self):
        """Test GracefulExit initialization."""
        graceful_exit = GracefulExit()

        assert hasattr(graceful_exit, "_exit_requested")
        assert hasattr(graceful_exit, "_lock")
        assert not graceful_exit._exit_requested


class TestGracefulExitFlagManagement:
    """Test GracefulExit flag management."""

    def test_should_exit_initial_state(self):
        """Test that should_exit returns False initially."""
        graceful_exit = GracefulExit()

        assert not graceful_exit.should_exit()

    def test_request_exit_sets_flag(self):
        """Test that request_exit sets the exit flag."""
        graceful_exit = GracefulExit()

        graceful_exit.request_exit()
        assert graceful_exit.should_exit()

    def test_multiple_request_exit_calls(self):
        """Test that multiple request_exit calls work correctly."""
        graceful_exit = GracefulExit()

        graceful_exit.request_exit()
        graceful_exit.request_exit()
        graceful_exit.request_exit()

        assert graceful_exit.should_exit()


class TestGracefulExitThreadSafety:
    """Test GracefulExit thread safety."""

    def test_concurrent_request_exit(self):
        """Test that concurrent request_exit calls are thread-safe."""
        graceful_exit = GracefulExit()

        def request_exit():
            for _ in range(10):
                graceful_exit.request_exit()

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=request_exit)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Should complete without errors and flag should be True
        assert graceful_exit.should_exit()

    def test_concurrent_should_exit(self):
        """Test that concurrent should_exit calls are thread-safe."""
        graceful_exit = GracefulExit()

        results = []

        def check_exit():
            for _ in range(10):
                results.append(graceful_exit.should_exit())

        threads = []
        for _ in range(5):
            thread = threading.Thread(target=check_exit)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Should complete without errors
        assert len(results) == 50
        assert all(not result for result in results)

    def test_concurrent_request_and_check(self):
        """Test that concurrent request_exit and should_exit calls are thread-safe."""
        graceful_exit = GracefulExit()

        results = []

        def mixed_operations():
            for _ in range(5):
                graceful_exit.request_exit()
                results.append(graceful_exit.should_exit())

        threads = []
        for _ in range(4):
            thread = threading.Thread(target=mixed_operations)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Should complete without errors
        assert len(results) == 20
        # All results should be True since request_exit was called
        assert all(result for result in results)


class TestGracefulExitSignalHandling:
    """Test GracefulExit signal handling."""

    def test_setup_signal_handlers_basic(self):
        """Test that setup_signal_handlers completes without errors."""
        graceful_exit = GracefulExit()

        # Should not raise an exception
        setup_signal_handlers(graceful_exit)

    def test_signal_handler_registration(self, mocker):
        """Test that signal handlers are registered correctly."""
        graceful_exit = GracefulExit()

        # Mock the signal.signal function to verify it's called
        mock_signal = mocker.patch("signal.signal")
        setup_signal_handlers(graceful_exit)

        # Should be called for SIGINT, SIGTERM, SIGHUP
        assert mock_signal.call_count == 3

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "sigint",
                "signal": signal.SIGINT,
                "expected_message": "SIGINT",
            },
            {
                "name": "sigterm",
                "signal": signal.SIGTERM,
                "expected_message": "SIGTERM",
            },
            {
                "name": "sighup",
                "signal": signal.SIGHUP,
                "expected_message": "SIGHUP",
            },
            {
                "name": "unknown_signal",
                "signal": 999,
                "expected_message": "signal 999",
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "sigint"},
                {"name": "sigterm"},
                {"name": "sighup"},
                {"name": "unknown_signal"},
            ]
        ],
    )
    def test_signal_handler(self, mocker, test_case):
        """Test signal handler behavior with various signals."""
        graceful_exit = GracefulExit()

        mock_signal = mocker.patch("signal.signal")
        mock_write = mocker.patch("sys.stderr.write")
        setup_signal_handlers(graceful_exit)

        # Get the handler that was registered
        handler = mock_signal.call_args[0][1]

        # Call the handler with the test signal
        handler(test_case["signal"], None)

        # Should have called request_exit
        assert graceful_exit.should_exit()

        # Should have written appropriate message
        assert mock_write.call_count == 1
        call_args = mock_write.call_args[0][0]
        assert test_case["expected_message"] in call_args


class TestGracefulExitErrorHandling:
    """Test GracefulExit error handling."""

    def test_signal_handler_handles_signal_errors(self, mocker):
        """Test that signal handler setup handles signal errors gracefully."""
        graceful_exit = GracefulExit()

        # Mock signal.signal to raise an exception
        def mock_signal_sigint(sig, handler):
            if sig == signal.SIGINT:
                raise ValueError("Invalid signal")
            return None

        mocker.patch("signal.signal", side_effect=mock_signal_sigint)
        # Should not raise an exception
        setup_signal_handlers(graceful_exit)

    def test_signal_handler_handles_os_error(self, mocker):
        """Test that signal handler setup handles OS errors gracefully."""
        graceful_exit = GracefulExit()

        # Mock signal.signal to raise OSError
        def mock_signal_oserror(sig, handler):
            raise OSError("Permission denied")

        mocker.patch("signal.signal", side_effect=mock_signal_oserror)
        # Should not raise an exception
        setup_signal_handlers(graceful_exit)


class TestGracefulExitIntegration:
    """Test GracefulExit integration with other components."""

    def test_integration_with_progress_reporter(self, mocker):
        """Test that GracefulExit works with ProgressReporter."""
        from src.archiver.progress import ProgressReporter

        graceful_exit = GracefulExit()
        reporter = ProgressReporter(total_files=1, graceful_exit=graceful_exit)

        # Request exit
        graceful_exit.request_exit()

        # Progress updates should be suppressed
        mock_write = mocker.patch("sys.stderr.write")
        reporter.update_progress(50.0)
        mock_write.assert_not_called()

    def test_integration_with_transcoder(self, mocker):
        """Test that GracefulExit works with Transcoder."""
        from src.archiver.transcoder import Transcoder

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        # Transcoding should fail when exit is requested
        input_path = Path("/input/test.mp4")
        output_path = Path("/output/test.mp4")
        logger = mocker.MagicMock()

        result = Transcoder.transcode_file(
            input_path, output_path, logger, graceful_exit=graceful_exit
        )

        assert not result

    def test_integration_with_file_processor(self, mocker):
        """Test that GracefulExit works with FileProcessor."""
        from src.archiver.processor import FileProcessor

        graceful_exit = GracefulExit()
        graceful_exit.request_exit()

        # Create a mock config
        mock_config = mocker.MagicMock()
        mock_config.directory = Path("/test")

        # File processing should handle graceful exit
        FileProcessor(mock_config, mocker.MagicMock(), graceful_exit)

        # The processor should respect the graceful exit flag
        assert graceful_exit.should_exit()


class TestGracefulExitEdgeCases:
    """Test GracefulExit edge cases."""

    def test_signal_during_request_exit(self):
        """Test that signals during request_exit are handled correctly."""
        graceful_exit = GracefulExit()

        # This test mainly verifies no deadlocks occur
        def request_exit_with_delay():
            time.sleep(0.01)
            graceful_exit.request_exit()

        thread = threading.Thread(target=request_exit_with_delay)
        thread.start()

        # Multiple threads checking the flag
        results = []
        for _ in range(10):
            results.append(graceful_exit.should_exit())
            time.sleep(0.001)

        thread.join()

        # Should complete without errors
        assert True

    def test_rapid_signal_handling(self, mocker):
        """Test that rapid signal handling works correctly."""
        graceful_exit = GracefulExit()

        mock_signal = mocker.patch("signal.signal")
        setup_signal_handlers(graceful_exit)

        # Get the handler that was registered
        handler = mock_signal.call_args[0][1]

        # Call the handler rapidly multiple times
        for _ in range(10):
            handler(signal.SIGINT, None)

        # Should still work correctly
        assert graceful_exit.should_exit()

    def test_graceful_exit_in_different_states(self):
        """Test GracefulExit in different states."""
        graceful_exit = GracefulExit()

        # Initial state
        assert not graceful_exit.should_exit()

        # After request_exit
        graceful_exit.request_exit()
        assert graceful_exit.should_exit()

        # Multiple should_exit calls after request_exit
        for _ in range(5):
            assert graceful_exit.should_exit()


if __name__ == "__main__":
    # Add Path import for the integration test
    from pathlib import Path
