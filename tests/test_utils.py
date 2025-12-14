"""
Comprehensive tests for the utils module.
"""

from datetime import datetime
from pathlib import Path

import pytest

from src.archiver.utils import (
    _display_and_handle_plan,
    _execute_archiver_pipeline,
    _execute_processing_pipeline,
    _handle_archiver_error,
    _handle_dry_run_mode,
    _handle_real_execution,
    _handle_real_execution_if_confirmed,
    _is_user_confirmation_required,
    _perform_discovery,
    _perform_environment_setup,
    _setup_environment,
    _should_skip_processing,
    confirm_plan,
    display_plan,
    main,
    parse_size,
    run_archiver,
)


class TestParseSize:
    """Test parse_size function."""

    def test_parse_size_bytes(self):
        """Test parsing size in bytes."""
        result = parse_size("1024B")
        assert result == 1024

    def test_parse_size_kilobytes(self):
        """Test parsing size in kilobytes."""
        result = parse_size("1KB")
        assert result == 1024

    def test_parse_size_megabytes(self):
        """Test parsing size in megabytes."""
        result = parse_size("1MB")
        assert result == 1024 * 1024

    def test_parse_size_gigabytes(self):
        """Test parsing size in gigabytes."""
        result = parse_size("1GB")
        assert result == 1024 * 1024 * 1024

    def test_parse_size_terabytes(self):
        """Test parsing size in terabytes."""
        result = parse_size("1TB")
        assert result == 1024 * 1024 * 1024 * 1024

    def test_parse_size_with_decimal(self):
        """Test parsing size with decimal values."""
        result = parse_size("1.5GB")
        expected = int(1.5 * 1024 * 1024 * 1024)
        assert result == expected

    def test_parse_size_with_spaces(self):
        """Test parsing size with spaces."""
        result = parse_size("  500 GB  ")
        assert result == 500 * 1024 * 1024 * 1024

    def test_parse_size_case_insensitive(self):
        """Test parsing size case insensitivity."""
        result = parse_size("500gb")
        assert result == 500 * 1024 * 1024 * 1024

    def test_parse_size_short_units(self):
        """Test parsing size with short units (K, M, G, T)."""
        result = parse_size("1G")
        assert result == 1024 * 1024 * 1024

    def test_parse_size_invalid_format(self):
        """Test parsing invalid size format."""
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("invalid")

    def test_parse_size_unknown_unit(self):
        """Test parsing size with unknown unit."""
        with pytest.raises(ValueError, match="Unknown size unit"):
            parse_size("500PB")

    def test_parse_size_zero_value(self):
        """Test parsing size with zero value."""
        result = parse_size("0GB")
        assert result == 0

    def test_parse_size_large_value(self):
        """Test parsing large size value."""
        result = parse_size("10TB")
        expected = 10 * 1024 * 1024 * 1024 * 1024
        assert result == expected


class TestMainFunction:
    """Test main function."""

    def test_main_function(self, caplog, mocker):
        """Test main function execution."""
        # Mock sys.argv to avoid argument parsing issues
        mocker.patch("sys.argv", ["archiver"])
        # Mock the main function to avoid actual execution
        mock_run = mocker.patch("src.archiver.utils.run_archiver")
        mock_run.return_value = 0

        # This should not raise exceptions
        main()
        # We can't easily test the actual return value due to argument parsing
        # but we can verify it doesn't crash


class TestParseSizeEdgeCases:
    """Test edge cases for parse_size function."""

    def test_parse_size_early_exit_invalid_format(self):
        """Test parse_size with invalid format (early exit)."""
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("invalid")

    def test_parse_size_early_exit_unknown_unit(self):
        """Test parse_size with unknown unit (early exit)."""
        with pytest.raises(ValueError, match="Unknown size unit"):
            parse_size("500PB")

    def test_parse_size_boundary_conditions(self):
        """Test parse_size with boundary conditions."""
        # Test with minimum values
        assert parse_size("0B") == 0
        assert parse_size("0KB") == 0
        assert parse_size("0MB") == 0
        assert parse_size("0GB") == 0
        assert parse_size("0TB") == 0

    def test_parse_size_with_whitespace(self):
        """Test parse_size with various whitespace scenarios."""
        # Test with leading/trailing whitespace
        assert parse_size("  500 GB  ") == 500 * 1024 * 1024 * 1024
        assert parse_size("\t1TB\n") == 1 * 1024 * 1024 * 1024 * 1024

    def test_parse_size_case_sensitivity(self):
        """Test parse_size case sensitivity."""
        # Test various case combinations
        assert parse_size("1gb") == 1 * 1024 * 1024 * 1024
        assert parse_size("1GB") == 1 * 1024 * 1024 * 1024
        assert parse_size("1Gb") == 1 * 1024 * 1024 * 1024
        assert parse_size("1gB") == 1 * 1024 * 1024 * 1024

    def test_parse_size_with_decimal_values(self):
        """Test parse_size with decimal values."""
        # Test with various decimal values
        assert parse_size("1.5GB") == int(1.5 * 1024 * 1024 * 1024)
        assert parse_size("2.75TB") == int(2.75 * 1024 * 1024 * 1024 * 1024)
        assert parse_size("0.5MB") == int(0.5 * 1024 * 1024)

    def test_parse_size_large_values(self):
        """Test parse_size with large values."""
        # Test with large values
        assert parse_size("10TB") == 10 * 1024 * 1024 * 1024 * 1024
        assert parse_size("100TB") == 100 * 1024 * 1024 * 1024 * 1024

    def test_parse_size_short_units(self):
        """Test parse_size with short units (K, M, G, T)."""
        # Test with short units
        assert parse_size("1K") == 1024
        assert parse_size("1M") == 1024 * 1024
        assert parse_size("1G") == 1024 * 1024 * 1024
        assert parse_size("1T") == 1024 * 1024 * 1024 * 1024


class TestDisplayFunctions:
    """Test display functions with various scenarios."""

    def test_display_plan_with_empty_plan(self, caplog, mocker):
        """Test display_plan with empty plan."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_config.age = 30  # Set age to avoid TypeError
        mock_config.cleanup = False

        plan = {"transcoding": [], "removals": []}

        # Should not raise exceptions with empty plan
        display_plan(plan, mock_logger, mock_config)

        # Verify that header and footer were called
        assert mock_logger.info.call_count >= 2  # At least header and footer

    def test_display_plan_with_transcoding_actions(self, caplog, mocker):
        """Test display_plan with transcoding actions."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_config.age = 30  # Set age to avoid TypeError
        mock_config.cleanup = False

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": "/path/to/input.mp4",
                    "output": "/path/to/output.mp4",
                    "jpg_to_remove": "/path/to/input.jpg",
                }
            ],
            "removals": [],
        }

        display_plan(plan, mock_logger, mock_config)

        # Verify that transcoding action was logged
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Transcoding 1 files:" in call for call in calls)
        assert any(
            "/path/to/input.mp4 -> /path/to/output.mp4" in call for call in calls
        )

    def test_display_plan_with_removal_actions(self, caplog, mocker):
        """Test display_plan with removal actions."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_config.age = 30  # Set age to avoid TypeError
        mock_config.cleanup = False

        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/path/to/source.mp4",
                    "reason": "Source file for transcoded archive",
                }
            ],
        }

        display_plan(plan, mock_logger, mock_config)

        # Verify that removal action was logged
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Removing 1 files:" in call for call in calls)
        assert any(
            "/path/to/source.mp4 - Source file for transcoded archive" in call
            for call in calls
        )

    def test_display_plan_with_mixed_actions(self, caplog, mocker):
        """Test display_plan with mixed action types."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_config.age = 30  # Set age to avoid TypeError
        mock_config.cleanup = False

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": "/path/to/input1.mp4",
                    "output": "/path/to/output1.mp4",
                    "jpg_to_remove": "/path/to/input1.jpg",
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_skip",
                    "file": "/path/to/source2.mp4",
                    "reason": "Skipping transcoding: archive exists",
                },
                {
                    "type": "jpg_removal_after_skip",
                    "file": "/path/to/source2.jpg",
                    "reason": "Skipping transcoding: archive exists",
                },
            ],
        }

        display_plan(plan, mock_logger, mock_config)

        # Verify that both transcoding and removal actions were logged
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Transcoding 1 files:" in call for call in calls)
        assert any("Removing 2 files:" in call for call in calls)

    def test_display_plan_with_cleanup_mode(self, caplog, mocker):
        """Test display_plan in cleanup mode."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()
        mock_config.age = 30  # Set age to avoid TypeError
        mock_config.cleanup = True
        mock_config.clean_output = False
        mock_config.max_size = None

        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_skip",
                    "file": "/path/to/source.mp4",
                    "reason": "Skipping transcoding: cleanup mode enabled",
                }
            ],
        }

        display_plan(plan, mock_logger, mock_config)

        # Verify that cleanup information is displayed
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Cleanup enabled" in call for call in calls)
        assert any("Cleanup scope" in call for call in calls)

    def test_display_helper_functions(self, caplog, mocker):
        """Test display helper functions (lines 188, 198, 203-212 coverage)."""
        mock_logger = mocker.Mock()
        mock_config = mocker.Mock()

        # Test _display_cleanup_scope
        mock_config.clean_output = True
        from src.archiver.utils import _display_cleanup_scope

        _display_cleanup_scope(mock_config, mock_logger)

        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any(
            "Source files, archive files, and trash files" in call for call in calls
        )

        # Test with clean_output=False
        mock_logger.reset_mock()
        mock_config.clean_output = False
        _display_cleanup_scope(mock_config, mock_logger)

        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any(
            "Source files and trash files (archive files excluded)" in call
            for call in calls
        )

        # Test _display_size_limit_info
        mock_logger.reset_mock()
        mock_config.max_size = "1GB"
        from src.archiver.utils import _display_size_limit_info

        _display_size_limit_info(mock_config, mock_logger)

        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Size limit:" in call for call in calls)
        assert any("Size-based cleanup priority:" in call for call in calls)

        # Test _display_size_limit_with_error_handling
        mock_logger.reset_mock()
        mock_config.max_size = "invalid_size"
        from src.archiver.utils import _display_size_limit_with_error_handling

        _display_size_limit_with_error_handling(mock_config, mock_logger)

        calls = [call[0][0] for call in mock_logger.warning.call_args_list]
        assert any("Invalid max-size value:" in call for call in calls)

        # Test _display_plan_footer
        mock_logger.reset_mock()
        from src.archiver.utils import _display_plan_footer

        _display_plan_footer(mock_logger)

        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("=== END PLAN ===" in call for call in calls)


class TestConfirmPlan:
    """Test confirm_plan function with various scenarios."""

    def test_confirm_plan_with_no_confirm(self, caplog, mocker):
        """Test confirm_plan when no_confirm is True."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = True
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        result = confirm_plan(plan, mock_config, mock_logger)
        assert result is True

    def test_confirm_plan_with_empty_plan(self, caplog, mocker):
        """Test confirm_plan with empty plan."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        # Mock input to return 'y'
        mocker.patch("builtins.input", return_value="y")
        result = confirm_plan(plan, mock_config, mock_logger)
        assert result is True

    def test_confirm_plan_with_non_empty_plan(self, caplog, mocker):
        """Test confirm_plan with non-empty plan."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {
            "transcoding": [
                {"type": "transcode", "input": "/input.mp4", "output": "/output.mp4"}
            ],
            "removals": [],
        }

        # Mock input to return 'n'
        mocker.patch("builtins.input", return_value="n")
        result = confirm_plan(plan, mock_config, mock_logger)
        assert result is False

    def test_confirm_plan_with_keyboard_interrupt(self, caplog, mocker):
        """Test confirm_plan with keyboard interrupt."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        # Mock input to raise KeyboardInterrupt
        mocker.patch("builtins.input", side_effect=KeyboardInterrupt())
        result = confirm_plan(plan, mock_config, mock_logger)
        assert result is False

    def test_confirm_plan_with_invalid_input(self, caplog, mocker):
        """Test confirm_plan with invalid input."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        # Mock input to return invalid input (should return False)
        mocker.patch("builtins.input", return_value="invalid")
        result = confirm_plan(plan, mock_config, mock_logger)
        assert result is False

    def test_confirm_plan_with_case_insensitive_input(self, caplog, mocker):
        """Test confirm_plan with case insensitive input."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        # Test various case combinations
        for input_value in ["Y", "y", "YES", "yes", "Yes"]:
            mocker.patch("builtins.input", return_value=input_value)
            result = confirm_plan(plan, mock_config, mock_logger)
            assert result is True

    def test_confirm_plan_with_negative_input(self, caplog, mocker):
        """Test confirm_plan with negative input."""
        mock_config = mocker.Mock()
        mock_config.no_confirm = False
        mock_logger = mocker.Mock()

        plan = {"transcoding": [], "removals": []}

        # Test various negative input combinations
        for input_value in ["N", "n", "NO", "no", "No"]:
            mocker.patch("builtins.input", return_value=input_value)
            result = confirm_plan(plan, mock_config, mock_logger)
            assert result is False


class TestEnvironmentSetup:
    """Test environment setup functions."""

    def test_setup_environment_success(self, temp_dir, mocker):
        """Test _setup_environment with successful setup."""
        mock_config = mocker.Mock()
        mock_config.directory = temp_dir
        mock_config.output = temp_dir / "output"
        mock_logger = mocker.Mock()

        # Ensure directory exists
        mock_config.directory.mkdir(parents=True, exist_ok=True)

        result = _setup_environment(mock_config, mock_logger)
        assert result == 0

    def test_setup_environment_nonexistent_directory(self, mocker):
        """Test _setup_environment with nonexistent directory."""
        mock_config = mocker.Mock()
        mock_config.directory = Path("/nonexistent/directory")
        mock_logger = mocker.Mock()

        result = _setup_environment(mock_config, mock_logger)
        assert result == 1

        # Verify that error was logged
        mock_logger.error.assert_called()

    def test_setup_environment_output_directory_creation(self, temp_dir, mocker):
        """Test _setup_environment with output directory creation."""
        mock_config = mocker.Mock()
        mock_config.directory = temp_dir
        mock_config.output = temp_dir / "output"
        mock_logger = mocker.Mock()

        # Ensure input directory exists but output doesn't
        mock_config.directory.mkdir(parents=True, exist_ok=True)

        result = _setup_environment(mock_config, mock_logger)
        assert result == 0

        # Verify that output directory was created
        assert mock_config.output.exists()

        # Verify that creation was logged
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Created output directory" in call for call in calls)

    def test_setup_environment_existing_output_directory(self, temp_dir, mocker):
        """Test _setup_environment with existing output directory."""
        mock_config = mocker.Mock()
        mock_config.directory = temp_dir
        mock_config.output = temp_dir / "output"
        mock_logger = mocker.Mock()

        # Ensure both directories exist
        mock_config.directory.mkdir(parents=True, exist_ok=True)
        mock_config.output.mkdir(parents=True, exist_ok=True)

        result = _setup_environment(mock_config, mock_logger)
        assert result == 0

    def test_setup_environment_permission_error(self, mocker):
        """Test _setup_environment with permission error."""
        mock_config = mocker.Mock()
        mock_config.directory = Path("/protected/directory")
        mock_logger = mocker.Mock()

        # Mock exists to raise PermissionError
        def mock_exists():
            raise PermissionError("Permission denied")

        # Mock the exists method by patching Path.exists
        mocker.patch(
            "pathlib.Path.exists", side_effect=PermissionError("Permission denied")
        )

        # The function should raise PermissionError since it doesn't have error handling
        with pytest.raises(PermissionError, match="Permission denied"):
            _setup_environment(mock_config, mock_logger)

    def test_perform_discovery(self, temp_dir, mocker):
        """Test _perform_discovery function (lines 256-265 coverage)."""
        mock_config = mocker.Mock()
        mock_logger = mocker.Mock()

        # Configure directories
        mock_config.directory = temp_dir
        mock_config.trash_root = temp_dir / ".deleted"
        mock_config.output = temp_dir / "output"
        mock_config.clean_output = True

        # Create some test files
        test_file = temp_dir / "REO_01_20230115120000.mp4"
        test_file.touch()

        # Mock the FileDiscovery.discover_files method
        from src.archiver.discovery import FileDiscovery

        mock_discover = mocker.patch.object(
            FileDiscovery,
            "discover_files",
            return_value=([(test_file, datetime(2023, 1, 15, 12, 0, 0))], {}, set()),
        )

        # Call the function
        mp4s, mapping, trash_files = _perform_discovery(mock_config, mock_logger)

        # Verify the results
        assert len(mp4s) == 1
        assert len(mapping) == 0
        assert len(trash_files) == 0

        # Verify that the discovery was called with correct parameters
        mock_discover.assert_called_once_with(
            temp_dir, temp_dir / ".deleted", temp_dir / "output", True
        )

        # Verify that logging was called
        calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Discovering files" in call for call in calls)
        assert any("Discovered 1 MP4 files" in call for call in calls)

    def test_handle_dry_run_mode(self, config, graceful_exit, logger, mocker, caplog):
        """Test _handle_dry_run_mode function (lines 270-293 coverage)."""
        # Create a simple plan
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": "/path/to/input.mp4",
                    "output": "/path/to/output.mp4",
                    "jpg_to_remove": "/path/to/input.jpg",
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/path/to/source.mp4",
                    "reason": "Source file for transcoded archive",
                }
            ],
        }

        # Create a simple mapping
        mapping = {}

        # Mock the processor
        mock_processor = mocker.Mock()

        # Mock the ProgressReporter
        mock_progress_reporter = mocker.Mock()
        mock_progress_reporter.__enter__ = mocker.Mock(
            return_value=mock_progress_reporter
        )
        mock_progress_reporter.__exit__ = mocker.Mock(return_value=None)
        mock_progress_reporter_class = mocker.patch(
            "src.archiver.progress.ProgressReporter"
        )
        mock_progress_reporter_class.return_value = mock_progress_reporter

        # Test without cleanup
        config.cleanup = False
        config.max_size = None

        result = _handle_dry_run_mode(
            config, logger, graceful_exit, mock_processor, plan, mapping
        )

        # Should return 0 for successful dry run
        assert result == 0

        # Should have called processor.execute_plan
        mock_processor.execute_plan.assert_called_once()

        # Should have logged dry run information (check via caplog)
        assert "Processing files (dry run" in caplog.text
        assert "Dry run completed" in caplog.text

        # Test with cleanup enabled
        caplog.clear()
        mock_processor.reset_mock()
        config.cleanup = True
        config.max_size = "1GB"

        result = _handle_dry_run_mode(
            config, logger, graceful_exit, mock_processor, plan, mapping
        )

        # Should return 0 for successful dry run
        assert result == 0

        # Should have called cleanup methods
        mock_processor.cleanup_orphaned_files.assert_called_once()
        mock_processor.size_based_cleanup.assert_called_once()

        # Should have logged cleanup information (check via caplog)
        assert "Cleaning up files (dry run" in caplog.text


class TestRunArchiver:
    """Test run_archiver function."""

    def test_run_archiver_basic(self, caplog, mocker):
        """Test basic run_archiver execution."""
        mock_config = mocker.Mock()

        # Mock all the internal functions to avoid complex setup
        mock_setup_logging = mocker.patch("src.archiver.utils._setup_logging")
        mock_setup_graceful_exit = mocker.patch(
            "src.archiver.utils._setup_graceful_exit"
        )
        mock_execute = mocker.patch("src.archiver.utils._execute_archiver_pipeline")

        mock_setup_logging.return_value = mocker.Mock()
        mock_setup_graceful_exit.return_value = mocker.Mock()
        mock_execute.return_value = 0

        result = run_archiver(mock_config)
        assert result == 0

        # Verify that all setup functions were called
        mock_setup_logging.assert_called_once_with(mock_config)
        mock_setup_graceful_exit.assert_called_once()
        mock_execute.assert_called_once()


class TestConfirmPlanEdgeCases:
    """Test confirm_plan function edge cases for line 233 coverage."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("", False),  # Empty string
            ("   ", False),  # Whitespace only
            ("\t", False),  # Tab only
            ("\n", False),  # Newline only
        ],
    )
    def test_confirm_plan_empty_whitespace_input(self, input_value, expected, mocker):
        """Test confirm_plan with empty and whitespace inputs."""
        mocker.patch("builtins.input", return_value=input_value)
        config = mocker.Mock(no_confirm=False)
        plan = {"transcoding": [], "removals": []}
        logger = mocker.Mock()

        result = confirm_plan(plan, config, logger)
        assert result == expected

    def test_confirm_plan_no_input_enter_key(self, mocker):
        """Test confirm_plan when user presses Enter without typing."""
        mocker.patch("builtins.input", return_value="")
        config = mocker.Mock(no_confirm=False)
        plan = {"transcoding": [], "removals": []}
        logger = mocker.Mock()

        result = confirm_plan(plan, config, logger)
        assert result is False


class TestPerformDiscovery:
    """Test _perform_discovery function for lines 298-321 coverage."""

    def test_perform_discovery_valid_structure(self, temp_dir, mocker):
        """Test _perform_discovery with valid directory structure."""
        # Setup test data
        camera_dir = temp_dir / "camera"
        camera_dir.mkdir()

        # Mock FileDiscovery.discover_files
        mock_discovery = mocker.patch(
            "src.archiver.discovery.FileDiscovery.discover_files"
        )
        mock_discovery.return_value = ([], {}, set())

        # Create config
        config = mocker.Mock()
        config.directory = camera_dir
        config.trash_root = None
        config.output = None
        config.clean_output = False

        logger = mocker.Mock()

        # Call function
        mp4s, mapping, trash_files = _perform_discovery(config, logger)

        # Assertions
        mock_discovery.assert_called_once_with(camera_dir, None, None, False)
        assert mp4s == []
        assert mapping == {}
        assert trash_files == set()
        logger.info.assert_any_call("Discovering files")
        logger.info.assert_any_call("Discovered 0 MP4 files")

    def test_perform_discovery_with_cleanup(self, temp_dir, mocker):
        """Test _perform_discovery with cleanup enabled."""
        camera_dir = temp_dir / "camera"
        camera_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        mock_discovery = mocker.patch(
            "src.archiver.discovery.FileDiscovery.discover_files"
        )
        mock_discovery.return_value = ([], {}, set())

        config = mocker.Mock()
        config.directory = camera_dir
        config.trash_root = None
        config.output = output_dir
        config.clean_output = True

        logger = mocker.Mock()

        mp4s, mapping, trash_files = _perform_discovery(config, logger)

        mock_discovery.assert_called_once_with(camera_dir, None, output_dir, True)

    def test_perform_discovery_with_files(self, temp_dir, mocker):
        """Test _perform_discovery with actual files discovered."""
        camera_dir = temp_dir / "camera"
        camera_dir.mkdir()

        # Mock discovery with actual files
        mock_discovery = mocker.patch(
            "src.archiver.discovery.FileDiscovery.discover_files"
        )
        mock_files = [
            ("file1.mp4", datetime(2023, 1, 1)),
            ("file2.mp4", datetime(2023, 1, 2)),
        ]
        mock_mapping = {
            "20230101": {".mp4": "file1.mp4"},
            "20230102": {".mp4": "file2.mp4"},
        }
        mock_trash = {"trash1.mp4", "trash2.mp4"}
        mock_discovery.return_value = (mock_files, mock_mapping, mock_trash)

        config = mocker.Mock()
        config.directory = camera_dir
        config.trash_root = None
        config.output = None
        config.clean_output = False

        logger = mocker.Mock()

        mp4s, mapping, trash_files = _perform_discovery(config, logger)

        assert len(mp4s) == 2
        assert len(mapping) == 2
        assert len(trash_files) == 2
        logger.info.assert_any_call("Discovered 2 MP4 files")


class TestHandleRealExecution:
    """Test _handle_real_execution function for lines 343-358 coverage."""

    def test_handle_real_execution_cleanup_disabled(self, mocker, caplog):
        """Test _handle_real_execution with cleanup disabled."""
        # Setup mocks
        config = mocker.Mock()
        config.cleanup = False
        config.max_size = None

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        # Mock ProgressReporter
        mock_progress = mocker.patch("src.archiver.progress.ProgressReporter")
        mock_progress_instance = mocker.Mock()
        mock_progress_instance.__enter__ = mocker.Mock(
            return_value=mock_progress_instance
        )
        mock_progress_instance.__exit__ = mocker.Mock(return_value=None)
        mock_progress.return_value = mock_progress_instance

        # Call function
        result = _handle_real_execution(
            config, logger, graceful_exit, processor, plan, mapping, trash_files
        )

        # Assertions
        processor.execute_plan.assert_called_once()
        processor.cleanup_orphaned_files.assert_not_called()
        processor.size_based_cleanup.assert_not_called()
        logger.info.assert_any_call("Processing files")
        logger.info.assert_any_call("Archiving completed successfully")
        assert result == 0

    def test_handle_real_execution_cleanup_enabled_no_max_size(self, mocker, caplog):
        """Test _handle_real_execution with cleanup enabled but no max_size."""
        config = mocker.Mock()
        config.cleanup = True
        config.max_size = None

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        mock_progress = mocker.patch("src.archiver.progress.ProgressReporter")
        mock_progress_instance = mocker.Mock()
        mock_progress_instance.__enter__ = mocker.Mock(
            return_value=mock_progress_instance
        )
        mock_progress_instance.__exit__ = mocker.Mock(return_value=None)
        mock_progress.return_value = mock_progress_instance

        _handle_real_execution(
            config, logger, graceful_exit, processor, plan, mapping, trash_files
        )

        processor.cleanup_orphaned_files.assert_called_once_with(mapping)
        processor.size_based_cleanup.assert_not_called()

    def test_handle_real_execution_with_max_size(self, mocker, caplog):
        """Test _handle_real_execution with max_size specified."""
        config = mocker.Mock()
        config.cleanup = True
        config.max_size = "500GB"

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        mock_progress = mocker.patch("src.archiver.progress.ProgressReporter")
        mock_progress_instance = mocker.Mock()
        mock_progress_instance.__enter__ = mocker.Mock(
            return_value=mock_progress_instance
        )
        mock_progress_instance.__exit__ = mocker.Mock(return_value=None)
        mock_progress.return_value = mock_progress_instance

        _handle_real_execution(
            config, logger, graceful_exit, processor, plan, mapping, trash_files
        )

        processor.cleanup_orphaned_files.assert_called_once_with(mapping)
        processor.size_based_cleanup.assert_called_once_with(trash_files)

    def test_handle_real_execution_with_invalid_max_size(self, mocker, caplog):
        """Test _handle_real_execution with invalid max_size format."""
        config = mocker.Mock()
        config.cleanup = True
        config.max_size = 12345  # Invalid format (not a string)

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        mock_progress = mocker.patch("src.archiver.progress.ProgressReporter")
        mock_progress_instance = mocker.Mock()
        mock_progress_instance.__enter__ = mocker.Mock(
            return_value=mock_progress_instance
        )
        mock_progress_instance.__exit__ = mocker.Mock(return_value=None)
        mock_progress.return_value = mock_progress_instance

        _handle_real_execution(
            config, logger, graceful_exit, processor, plan, mapping, trash_files
        )

        # Should only call cleanup_orphaned_files, not size_based_cleanup
        processor.cleanup_orphaned_files.assert_called_once_with(mapping)
        processor.size_based_cleanup.assert_not_called()


class TestPipelineExecution:
    """Test pipeline execution functions for lines 363-384 coverage."""

    def test_execute_processing_pipeline_basic(self, mocker, caplog):
        """Test _execute_processing_pipeline with basic configuration."""
        config = mocker.Mock()
        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        mp4s = []
        mapping = {}
        trash_files = set()

        # Mock processor
        mock_processor = mocker.patch("src.archiver.processor.FileProcessor")
        processor_instance = mocker.Mock()
        mock_processor.return_value = processor_instance
        processor_instance.generate_action_plan.return_value = {
            "transcoding": [],
            "removals": [],
        }

        # Mock display and handle plan
        mocker.patch("src.archiver.utils._display_and_handle_plan")

        # Call function
        result = _execute_processing_pipeline(
            config, logger, graceful_exit, mp4s, mapping, trash_files
        )

        # Assertions
        processor_instance.generate_action_plan.assert_called_once_with(mp4s, mapping)
        assert result == 0

    def test_display_and_handle_plan_dry_run(self, mocker, caplog):
        """Test _display_and_handle_plan with dry run mode."""
        config = mocker.Mock()
        config.dry_run = True

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        # Mock display_plan and _handle_dry_run_mode
        mocker.patch("src.archiver.utils.display_plan")
        mocker.patch("src.archiver.utils._handle_dry_run_mode")

        # Call function
        _display_and_handle_plan(
            plan, logger, config, graceful_exit, processor, mapping, trash_files
        )

        # Assertions
        from src.archiver.utils import _handle_dry_run_mode as handle_dry_run

        handle_dry_run.assert_called_once()  # type: ignore

    def test_display_and_handle_plan_real_execution(self, mocker, caplog):
        """Test _display_and_handle_plan with real execution mode."""
        config = mocker.Mock()
        config.dry_run = False

        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        # Mock confirm_plan to return True
        mocker.patch("src.archiver.utils.confirm_plan", return_value=True)
        mocker.patch("src.archiver.utils.display_plan")
        mocker.patch("src.archiver.utils._handle_real_execution_if_confirmed")

        # Call function
        _display_and_handle_plan(
            plan, logger, config, graceful_exit, processor, mapping, trash_files
        )

        # Assertions
        from src.archiver.utils import (
            _handle_real_execution_if_confirmed as handle_real_exec_if_confirmed,
        )

        handle_real_exec_if_confirmed.assert_called_once()  # type: ignore

    def test_handle_real_execution_if_confirmed_user_declines(self, mocker, caplog):
        """Test _handle_real_execution_if_confirmed when user declines."""
        config = mocker.Mock()
        logger = mocker.Mock()
        graceful_exit = mocker.Mock()
        processor = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        mapping = {}
        trash_files = set()

        # Mock confirm_plan to return False and _handle_real_execution
        mocker.patch("src.archiver.utils.confirm_plan", return_value=False)
        mock_handle_real = mocker.patch("src.archiver.utils._handle_real_execution")

        # Call function
        _handle_real_execution_if_confirmed(
            plan, config, logger, graceful_exit, processor, mapping, trash_files
        )

        # Should not call _handle_real_execution
        mock_handle_real.assert_not_called()
        logger.info.assert_any_call("Operation cancelled by user")

    def test_is_user_confirmation_required(self, mocker, caplog):
        """Test _is_user_confirmation_required function."""
        config = mocker.Mock()
        plan = {"transcoding": [], "removals": []}
        logger = mocker.Mock()

        # Mock confirm_plan to return True
        mocker.patch("src.archiver.utils.confirm_plan", return_value=True)

        result = _is_user_confirmation_required(config, plan, logger)
        assert result is True


class TestErrorHandling:
    """Test error handling functions for lines 389-447 coverage."""

    def test_handle_archiver_error_basic(self, mocker, caplog):
        """Test _handle_archiver_error with basic error."""
        logger = mocker.Mock()
        error = ValueError("Test error")

        result = _handle_archiver_error(error, logger)

        logger.error.assert_called_once_with("Error: Test error")
        assert result == 1

    def test_execute_archiver_pipeline_error(self, mocker, caplog):
        """Test _execute_archiver_pipeline with error handling."""
        config = mocker.Mock()
        logger = mocker.Mock()
        graceful_exit = mocker.Mock()

        # Mock _perform_environment_setup to return error code
        mocker.patch("src.archiver.utils._perform_environment_setup", return_value=1)

        result = _execute_archiver_pipeline(config, logger, graceful_exit)

        assert result == 1

    def test_perform_environment_setup_error(self, mocker, caplog):
        """Test _perform_environment_setup with error conditions."""
        config = mocker.Mock()
        logger = mocker.Mock()

        # Mock _setup_environment to return error code
        mocker.patch("src.archiver.utils._setup_environment", return_value=1)

        result = _perform_environment_setup(config, logger)

        assert result == 1

    def test_should_skip_processing_various_scenarios(self, mocker):
        """Test _should_skip_processing with various scenarios."""
        logger = mocker.Mock()

        # Test with empty mp4s list
        result = _should_skip_processing([], logger)
        assert result is True
        logger.info.assert_called_with("No files to process")

        # Test with non-empty mp4s list
        result = _should_skip_processing(["file1.mp4"], logger)
        assert result is False

    def test_execute_archiver_pipeline_success(self, mocker, caplog):
        """Test _execute_archiver_pipeline with successful execution."""
        config = mocker.Mock()
        logger = mocker.Mock()
        graceful_exit = mocker.Mock()

        # Mock all internal functions for successful path
        mocker.patch("src.archiver.utils._perform_environment_setup", return_value=0)
        mocker.patch(
            "src.archiver.utils._perform_discovery", return_value=([], {}, set())
        )
        mocker.patch("src.archiver.utils._should_skip_processing", return_value=False)
        mocker.patch("src.archiver.utils._execute_processing_pipeline", return_value=0)

        result = _execute_archiver_pipeline(config, logger, graceful_exit)

        assert result == 0
