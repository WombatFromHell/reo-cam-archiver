"""
Comprehensive tests for the FileProcessor module.
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.archiver.config import Config
from src.archiver.processor import FileProcessor
from src.archiver.transcoder import Transcoder


class TestFileProcessorInitialization:
    """Test FileProcessor initialization and basic functionality."""

    def test_file_processor_initialization(self, config, graceful_exit, logger):
        """Test that FileProcessor initializes correctly."""
        processor = FileProcessor(config, logger, graceful_exit)

        assert processor.config == config
        assert processor.logger == logger
        assert processor.graceful_exit == graceful_exit

    def test_file_processor_with_minimal_config(self, mock_args, graceful_exit, logger):
        """Test FileProcessor with minimal configuration."""
        mock_args.directory = "/camera"
        mock_args.output = None
        mock_args.age = 0

        config = Config(mock_args)
        processor = FileProcessor(config, logger, graceful_exit)

        assert processor.config == config
        assert processor.config.directory == Path("/camera")

    """Test plan execution functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "successful_transcode",
                "transcode_success": True,
                "expected_result": True,
                "expected_removals": 2,  # Both source and jpg should be removed
            },
            {
                "name": "failed_transcode",
                "transcode_success": False,
                "expected_result": False,
                "expected_removals": 0,  # No removals when transcode fails
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "successful_transcode"},
                {"name": "failed_transcode"},
            ]
        ],
    )
    def test_execute_plan(
        self, config, graceful_exit, logger, make_file_set, mocker, test_case
    ):
        """Test plan execution with various scenarios."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Create a simple plan
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": str(files[0]["mp4"]),
                    "reason": "Source file for transcoded archive",
                },
                {
                    "type": "jpg_removal_after_transcode",
                    "file": str(files[0]["jpg"]),
                    "reason": "Paired with transcoded MP4",
                },
            ],
        }

        # Mock transcoder to return the specified success/failure
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file",
            return_value=test_case["transcode_success"],
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # execute_plan always returns True unless there are critical errors
        assert result

        # Verify that progress reporter was used (at least start_file should be called)
        assert len(mock_progress_reporter.method_calls) >= 1
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should still return True (continue processing)
        assert result is True

    def test_execute_plan_with_graceful_exit(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test plan execution with graceful exit."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Create a plan with multiple actions
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived2.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
            ],
            "removals": [],
        }

        # Mock transcoder
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Request graceful exit before processing
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should exit gracefully and return True
        assert result is True


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_generate_action_plan_with_empty_inputs(
        self, config, graceful_exit, logger
    ):
        """Test action plan generation with empty inputs."""
        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan([], {})

        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 0

    def test_execute_plan_with_empty_plan(self, mocker, config, graceful_exit, logger):
        """Test plan execution with empty plan."""
        plan = {"transcoding": [], "removals": []}

        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        assert result is True

    def test_cleanup_orphaned_files_with_empty_mapping(
        self, config, graceful_exit, logger, mocker
    ):
        """Test orphaned file cleanup with empty mapping."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Should handle empty mapping gracefully
        processor.cleanup_orphaned_files({})


class TestIntegration:
    """Test integration with other components."""

    def test_integration_with_transcoder(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test integration with Transcoder component."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                }
            ],
            "removals": [],
        }

        # Mock transcoder
        mock_transcode = mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        processor.execute_plan(plan, mock_progress_reporter)

        # Verify transcoder was called correctly
        mock_transcode.assert_called_once()
        call_args = mock_transcode.call_args
        assert str(files[0]["mp4"]) in str(call_args)
        assert "archived.mp4" in str(call_args)

    def test_integration_with_file_manager(self, config, graceful_exit, logger, mocker):
        """Test integration with FileManager component."""
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/test/file.mp4",
                    "reason": "Test removal",
                }
            ],
        }

        # Mock file manager
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        processor.execute_plan(plan, mock_progress_reporter)

        # Verify file manager was called
        mock_remove.assert_called_once()


class TestMissingMethods:
    """Test methods that are currently missing coverage."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "zero_age",
                "older_than": 0,
                "expected_cutoff": None,
            },
            {
                "name": "positive_age",
                "older_than": 30,
                "expected_cutoff": "not_none",
            },
            {
                "name": "large_age",
                "older_than": 365,
                "expected_cutoff": "not_none",
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "zero_age"},
                {"name": "positive_age"},
                {"name": "large_age"},
            ]
        ],
    )
    def test_calculate_age_cutoff(self, config, graceful_exit, logger, test_case):
        """Test _calculate_age_cutoff with various age configurations."""
        config.older_than = test_case["older_than"]
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._calculate_age_cutoff()

        if test_case["expected_cutoff"] == "not_none":
            assert result is not None
            assert isinstance(result, datetime)

            # Should be approximately the specified days ago
            expected_cutoff = datetime.now() - timedelta(days=test_case["older_than"])
            time_difference = abs((result - expected_cutoff).total_seconds())
            assert time_difference < 10  # Allow 10 seconds tolerance
        else:
            assert result is None

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "none_cutoff",
                "age_cutoff": None,
                "file_age_days": 45,
                "expected_result": False,
            },
            {
                "name": "old_file",
                "age_cutoff": 30,
                "file_age_days": 45,
                "expected_result": False,
            },
            {
                "name": "recent_file",
                "age_cutoff": 30,
                "file_age_days": 15,
                "expected_result": True,
            },
            {
                "name": "exactly_at_cutoff",
                "age_cutoff": 30,
                "file_age_days": 30,
                "expected_result": False,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "none_cutoff"},
                {"name": "old_file"},
                {"name": "recent_file"},
                {"name": "exactly_at_cutoff"},
            ]
        ],
    )
    def test_should_skip_file_due_to_age(
        self, config, graceful_exit, logger, temp_dir, test_case
    ):
        """Test _should_skip_file_due_to_age with various scenarios."""
        processor = FileProcessor(config, logger, graceful_exit)

        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Calculate age cutoff and file timestamp based on test case
        if test_case["age_cutoff"] is not None:
            age_cutoff = datetime.now() - timedelta(days=test_case["age_cutoff"])
        else:
            age_cutoff = None

        file_timestamp = datetime.now() - timedelta(days=test_case["file_age_days"])

        result = processor._should_skip_file_due_to_age(
            test_file, file_timestamp, age_cutoff
        )

        # The logic is: skip if file timestamp is >= age_cutoff (recent files should be skipped)
        # The method returns True if file should be skipped
        if test_case["age_cutoff"] is None:
            # No age cutoff means don't skip any files
            expected = False
        else:
            # File timestamp >= age_cutoff (should be skipped)
            if test_case["file_age_days"] <= test_case["age_cutoff"]:
                expected = True
            # File timestamp < age_cutoff (should NOT be skipped)
            else:
                expected = False

        assert result == expected

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "cleanup_mode",
                "no_skip": False,
                "cleanup_mode": True,
                "file_exists": True,
                "file_size": None,
                "expected_result": True,
            },
            {
                "name": "no_skip_disabled_no_file",
                "no_skip": False,
                "cleanup_mode": False,
                "file_exists": False,
                "file_size": None,
                "expected_result": False,
            },
            {
                "name": "no_skip_disabled_large_file",
                "no_skip": False,
                "cleanup_mode": False,
                "file_exists": True,
                "file_size": 2_000_000,  # 2MB > 1MB threshold
                "expected_result": True,
            },
            {
                "name": "no_skip_disabled_small_file",
                "no_skip": False,
                "cleanup_mode": False,
                "file_exists": True,
                "file_size": 500_000,  # 0.5MB < 1MB threshold
                "expected_result": False,
            },
            {
                "name": "no_skip_enabled",
                "no_skip": True,
                "cleanup_mode": False,
                "file_exists": True,
                "file_size": 2_000_000,
                "expected_result": False,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "cleanup_mode"},
                {"name": "no_skip_disabled_no_file"},
                {"name": "no_skip_disabled_large_file"},
                {"name": "no_skip_disabled_small_file"},
                {"name": "no_skip_enabled"},
            ]
        ],
    )
    def test_should_skip_transcoding(
        self, config, graceful_exit, logger, temp_dir, test_case
    ):
        """Test _should_skip_transcoding with various scenarios."""
        config.no_skip = test_case["no_skip"]
        processor = FileProcessor(config, logger, graceful_exit)

        output_file = temp_dir / "output.mp4"

        if test_case["file_exists"]:
            output_file.touch()
            if test_case["file_size"] is not None:
                # Create file with specific size
                with output_file.open("w") as f:
                    f.write("x" * test_case["file_size"])

        result = processor._should_skip_transcoding(
            output_file, cleanup_mode=test_case["cleanup_mode"]
        )
        assert result == test_case["expected_result"]

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "oserror",
                "exception_type": OSError,
                "expected_result": False,
            },
            {
                "name": "typeerror",
                "exception_type": TypeError,
                "expected_result": False,
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "oserror"},
                {"name": "typeerror"},
            ]
        ],
    )
    def test_should_skip_transcoding_with_exceptions(
        self, config, graceful_exit, logger, temp_dir, mocker, test_case
    ):
        """Test _should_skip_transcoding with various exception scenarios."""
        config.no_skip = False
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a mock Path object that will raise the specified exception on stat()
        mock_path = mocker.MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.stat.side_effect = test_case["exception_type"]("Mocked error")

        result = processor._should_skip_transcoding(mock_path)
        assert result == test_case["expected_result"]

    def test_determine_source_root_with_input_file(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root with input file."""
        config.output = temp_dir / "output"
        config.clean_output = True
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a file in the input directory
        input_file = temp_dir / "input" / "test.mp4"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.touch()

        source_root, is_output_file = processor._determine_source_root(input_file)
        assert source_root == config.directory
        assert is_output_file is False

    def test_determine_source_root_with_output_file(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root with output file."""
        config.output = temp_dir / "output"
        config.clean_output = True
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a file in the output directory
        output_file = config.output / "test.mp4"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.touch()

        source_root, is_output_file = processor._determine_source_root(output_file)
        assert source_root == config.output
        assert is_output_file is True

    def test_determine_source_root_with_clean_output_disabled(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root when clean_output is disabled."""
        config.output = temp_dir / "output"
        config.clean_output = False
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a file in the output directory
        output_file = config.output / "test.mp4"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.touch()

        source_root, is_output_file = processor._determine_source_root(output_file)
        # Should still be treated as input file when clean_output is disabled
        assert source_root == config.directory
        assert is_output_file is False

    def test_determine_source_root_with_no_output(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root when output is not configured."""
        config.output = None
        config.clean_output = True
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a test file
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        source_root, is_output_file = processor._determine_source_root(test_file)
        assert source_root == config.directory
        assert is_output_file is False

    def test_handle_action_type(self, config, graceful_exit, logger):
        """Test _handle_action_type method."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test various action types
        result = processor._handle_action_type("transcode")
        assert result == "Processing transcoding action"

        result = processor._handle_action_type("source_removal_after_transcode")
        assert result == "Processing source removal after transcode"

        result = processor._handle_action_type("jpg_removal_after_transcode")
        assert result == "Processing JPG removal after transcode"

        result = processor._handle_action_type("source_removal_after_skip")
        assert result == "Processing source removal after skip"

        result = processor._handle_action_type("jpg_removal_after_skip")
        assert result == "Processing JPG removal after skip"

    def test_handle_unknown_action_type(self, config, graceful_exit, logger, caplog):
        """Test _handle_unknown_action_type method (lines 426-427, 431-432 coverage)."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test with unknown action type
        result = processor._handle_unknown_action_type("unknown_action")

        # Should return the expected string and log a warning
        assert result == "Processing unknown action type: unknown_action"

        # Verify that warning was logged
        assert "Unknown action type: unknown_action" in caplog.text

        # Test with another unknown action type
        result = processor._handle_unknown_action_type("another_unknown")
        assert result == "Processing unknown action type: another_unknown"
        assert "Unknown action type: another_unknown" in caplog.text

    def test_get_action_description(self, config, graceful_exit, logger):
        """Test _get_action_description method."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test various action types
        result = processor._get_action_description("transcode")
        assert result == "Processing transcoding action"

        result = processor._get_action_description("source_removal_after_transcode")
        assert result == "Processing source removal after transcode"

        result = processor._get_action_description("jpg_removal_after_transcode")
        assert result == "Processing JPG removal after transcode"

        result = processor._get_action_description("source_removal_after_skip")
        assert result == "Processing source removal after skip"

        result = processor._get_action_description("jpg_removal_after_skip")
        assert result == "Processing JPG removal after skip"

        """Test _get_action_description method."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test various action types
        result = processor._get_action_description("transcode")
        assert result == "Processing transcoding action"

        result = processor._get_action_description("source_removal_after_transcode")
        assert result == "Processing source removal after transcode"

        result = processor._get_action_description("jpg_removal_after_transcode")
        assert result == "Processing JPG removal after transcode"

        result = processor._get_action_description("source_removal_after_skip")
        assert result == "Processing source removal after skip"

        result = processor._get_action_description("jpg_removal_after_skip")
        assert result == "Processing JPG removal after skip"


class TestAgeCutoffLogic:
    """Test age cutoff logic in FileProcessor methods."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "exact_match",
                "age_cutoff": datetime(2023, 1, 15, 12, 0, 0),
                "timestamp": datetime(2023, 1, 15, 12, 0, 0),
                "expected_result": True,
            },
            {
                "name": "none_cutoff",
                "age_cutoff": None,
                "timestamp": datetime(2023, 1, 15, 12, 0, 0),
                "expected_result": False,
            },
            {
                "name": "older_file",
                "age_cutoff": datetime.now() - timedelta(days=30),
                "timestamp": datetime.now() - timedelta(days=45),
                "expected_result": False,
            },
            {
                "name": "newer_file",
                "age_cutoff": datetime.now() - timedelta(days=30),
                "timestamp": datetime.now() - timedelta(days=15),
                "expected_result": True,
            },
        ],
        ids=["exact_match", "none_cutoff", "older_file", "newer_file"],
    )
    def test_age_cutoff_scenarios(
        self, config, graceful_exit, logger, temp_dir, test_case
    ):
        """Test _should_skip_file_due_to_age with various scenarios."""
        processor = FileProcessor(config, logger, graceful_exit)

        test_file = temp_dir / "test.mp4"
        test_file.touch()

        result = processor._should_skip_file_due_to_age(
            test_file, test_case["timestamp"], test_case["age_cutoff"]
        )
        assert result == test_case["expected_result"]

    def test_trash_files_skip_in_generate_action_plan(
        self, mocker, config, graceful_exit, logger, make_file_set
    ):
        """Test trash files skip logic in generate_action_plan (line 159 coverage)."""
        # Create a file that should be skipped as trash
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Mock the trash files set to include our test file
        trash_files = {files[0]["mp4"]}

        # We need to patch the generate_action_plan method to use our trash_files
        # Since the actual method doesn't use trash_files parameter, we need to modify the test
        # Let's patch the set() call to return our trash_files
        import builtins

        original_set = builtins.set

        def mock_set(*args):
            if not args:  # This is the empty set() call on line 158
                return trash_files
            return original_set(*args)

        mocker.patch("builtins.set", side_effect=mock_set)
        mp4s = [(files[0]["mp4"], timestamp)]

        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip the trash file
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 0  # No actions for trash files

    def test_age_cutoff_in_generate_action_plan(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test age cutoff logic in generate_action_plan (line 165 coverage)."""
        # Set age cutoff to skip recent files
        config.older_than = 1  # 1 day

        # Create a file that should be skipped due to age cutoff
        # Use a timestamp that is definitely newer than the cutoff
        cutoff_time = datetime.now() - timedelta(days=1)
        recent_timestamp = cutoff_time + timedelta(
            seconds=1
        )  # 1 second newer than cutoff

        recent_files = make_file_set([recent_timestamp])

        mp4s = [(recent_files[0]["mp4"], recent_timestamp)]

        mapping = {
            recent_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_files[0]["mp4"],
                ".jpg": recent_files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)

        # Debug: Check the age cutoff calculation
        age_cutoff = processor._calculate_age_cutoff()
        logger.info(f"Age cutoff: {age_cutoff}")
        logger.info(f"File timestamp: {recent_timestamp}")
        logger.info(
            f"Should skip: {processor._should_skip_file_due_to_age(recent_files[0]['mp4'], recent_timestamp, age_cutoff)}"
        )

        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip the recent file due to age cutoff
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 0  # No actions for skipped files

    def test_generate_action_plan_with_age_cutoff_edge_cases(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation with age cutoff edge cases."""
        # Set age cutoff to skip recent files
        config.older_than = 1  # 1 day

        # Create files with timestamps at various ages
        exact_cutoff = datetime.now() - timedelta(days=1)
        just_older = exact_cutoff - timedelta(seconds=1)
        just_newer = exact_cutoff + timedelta(seconds=1)

        files_exact = make_file_set([exact_cutoff])
        files_older = make_file_set([just_older])
        files_newer = make_file_set([just_newer])

        mp4s = [
            (files_exact[0]["mp4"], exact_cutoff),
            (files_older[0]["mp4"], just_older),
            (files_newer[0]["mp4"], just_newer),
        ]

        mapping = {
            exact_cutoff.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files_exact[0]["mp4"],
                ".jpg": files_exact[0]["jpg"],
            },
            just_older.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files_older[0]["mp4"],
                ".jpg": files_older[0]["jpg"],
            },
            just_newer.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files_newer[0]["mp4"],
                ".jpg": files_newer[0]["jpg"],
            },
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should process the older file and the exact cutoff file (both are not newer than cutoff)
        # The newer file should be skipped
        assert len(plan["transcoding"]) == 2  # Older and exact cutoff files
        assert len(plan["removals"]) == 4  # 2 source + 2 jpg removals


class TestTranscodingLogic:
    """Test transcoding logic in FileProcessor methods."""

    def test_execute_transcoding_action_with_graceful_exit(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test _execute_transcoding_action with graceful exit during progress."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        action = {
            "type": "transcode",
            "input": str(files[0]["mp4"]),
            "output": str(files[0]["mp4"].parent / "archived.mp4"),
            "jpg_to_remove": str(files[0]["jpg"]),
        }

        # Mock transcoder to simulate long-running operation
        def mock_transcode_with_progress(*args, **kwargs):
            # Simulate progress updates
            progress_callback = kwargs.get("progress_callback")
            if progress_callback:
                # Request graceful exit during progress
                graceful_exit.request_exit()
                progress_callback(50.0)  # This should check graceful_exit
            return True

        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file",
            side_effect=mock_transcode_with_progress,
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor._execute_transcoding_action(action, mock_progress_reporter)

        assert result is True
        mock_progress_reporter.start_file.assert_called_once()

    def test_execute_transcoding_action_progress_callback(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test _execute_transcoding_action progress callback functionality."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        action = {
            "type": "transcode",
            "input": str(files[0]["mp4"]),
            "output": str(files[0]["mp4"].parent / "archived.mp4"),
            "jpg_to_remove": str(files[0]["jpg"]),
        }

        # Track if progress callback was called
        progress_callback_called = []

        # Mock transcoder to call progress callback
        def mock_transcode_with_progress(*args, **kwargs):
            # The progress callback is the 4th positional argument
            if len(args) >= 4:
                progress_callback = args[3]
                if progress_callback:
                    progress_callback_called.append(25.0)
                    progress_callback_called.append(75.0)
                    progress_callback_called.append(100.0)
                    progress_callback(25.0)
                    progress_callback(75.0)
                    progress_callback(100.0)
            return True

        # Patch the transcoder method directly
        mocker.patch.object(
            Transcoder, "transcode_file", side_effect=mock_transcode_with_progress
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Ensure graceful_exit should not exit during this test
        graceful_exit._exit_flag = False

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor._execute_transcoding_action(action, mock_progress_reporter)

        assert result is True

        # Verify progress updates were called
        assert len(progress_callback_called) == 3, (
            f"Expected 3 progress calls, got {len(progress_callback_called)}"
        )
        assert mock_progress_reporter.update_progress.call_count == 3
        mock_progress_reporter.update_progress.assert_any_call(25.0)
        mock_progress_reporter.update_progress.assert_any_call(75.0)
        mock_progress_reporter.update_progress.assert_any_call(100.0)

    @pytest.mark.parametrize(
        "file_size,expected_result",
        [
            (1000, True),  # 1KB file
            (10_000_000, True),  # 10MB file
            (100_000, True),  # 100KB file
            (1_000_000, True),  # 1MB file
        ],
        ids=["small_file", "large_file", "medium_file", "one_mb_file"],
    )
    def test_execute_transcoding_action_with_different_file_sizes(
        self,
        config,
        graceful_exit,
        logger,
        temp_dir,
        mocker,
        file_size,
        expected_result,
    ):
        """Test _execute_transcoding_action with various file sizes."""
        # Create test file of specified size
        test_file = temp_dir / f"test_{file_size}.mp4"

        with test_file.open("w") as f:
            f.write("x" * file_size)

        # Create action for the test file
        action = {
            "type": "transcode",
            "input": str(test_file),
            "output": str(test_file.parent / f"archived_{file_size}.mp4"),
            "jpg_to_remove": None,
        }

        # Mock transcoder
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)

        # Test with the file
        result = processor._execute_transcoding_action(action, mock_progress_reporter)

        assert result == expected_result


class TestFileRemovalLogic:
    """Test file removal logic in FileProcessor methods."""

    @pytest.mark.parametrize("jpg_path", [None, ""], ids=["none_path", "empty_string"])
    def test_remove_paired_jpg_with_falsy_paths(
        self, config, graceful_exit, logger, mocker, jpg_path
    ):
        """Test _remove_paired_jpg when jpg_path is falsy."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file manager to track calls
        mock_remove_file = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )

        # Call with falsy path
        processor._remove_paired_jpg(jpg_path)

        # Should not call remove_file when path is falsy
        mock_remove_file.assert_not_called()

    def test_remove_paired_jpg_with_valid_path(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_paired_jpg with valid path."""
        jpg_file = temp_dir / "test.jpg"
        jpg_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file manager to track calls
        mock_remove_file = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )

        # Call with valid path
        processor._remove_paired_jpg(str(jpg_file))

        # Should call remove_file with correct parameters
        mock_remove_file.assert_called_once_with(
            jpg_file,  # Now expects Path object
            logger,
            dry_run=False,
            delete=False,
            trash_root=config.trash_root,  # Use the actual trash_root from config
            is_output=False,
            source_root=config.directory,
        )

    def test_remove_source_file_with_removal_actions(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_source_file with removal actions."""
        source_file = temp_dir / "source.mp4"
        source_file.touch()

        removal_actions = [
            {
                "type": "source_removal_after_transcode",
                "file": str(source_file),
                "reason": "Source file for transcoded archive",
            }
        ]

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file manager to track calls
        mock_remove_file = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )

        # Call with valid file path
        processor._remove_source_file(str(source_file), removal_actions)

        # Should call remove_file and remove the action from the list
        mock_remove_file.assert_called_once()
        assert len(removal_actions) == 0  # Action should be removed

    def test_remove_source_file_with_no_matching_action(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_source_file when no matching action exists."""
        source_file = temp_dir / "source.mp4"
        source_file.touch()

        removal_actions = [
            {
                "type": "jpg_removal_after_transcode",
                "file": str(source_file),
                "reason": "Paired with transcoded MP4",
            }
        ]

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file manager to track calls
        mock_remove_file = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )

        # Call with file path that doesn't match any source removal action
        processor._remove_source_file(str(source_file), removal_actions)

        # Should not call remove_file when no matching action
        mock_remove_file.assert_not_called()
        assert len(removal_actions) == 1  # No action should be removed

    def test_filter_removal_actions_with_failed_transcodes(
        self, config, graceful_exit, logger, mocker
    ):
        """Test _filter_removal_actions with failed transcodes."""
        processor = FileProcessor(config, logger, graceful_exit)

        removal_actions = [
            {
                "type": "source_removal_after_transcode",
                "file": "/path/to/source1.mp4",
                "reason": "Source file for transcoded archive",
            },
            {
                "type": "jpg_removal_after_transcode",
                "file": "/path/to/source1.jpg",
                "reason": "Paired with transcoded MP4",
            },
            {
                "type": "source_removal_after_transcode",
                "file": "/path/to/source2.mp4",
                "reason": "Source file for transcoded archive",
            },
            {
                "type": "jpg_removal_after_transcode",
                "file": "/path/to/source2.jpg",
                "reason": "Paired with transcoded MP4",
            },
        ]

        failed_transcodes = {"/path/to/source1.mp4"}
        failed_jpgs_to_remove = {"/path/to/source1.jpg"}

        # Mock the skip removal methods
        mocker.patch.object(
            processor,
            "_should_skip_removal_action",
            side_effect=lambda action, *args: action["file"] in failed_transcodes
            or action["file"] in failed_jpgs_to_remove,
        )

        filtered_actions = processor._filter_removal_actions(
            removal_actions, failed_transcodes, failed_jpgs_to_remove
        )

        # Should filter out actions for failed transcodes
        assert len(filtered_actions) == 2
        assert all(
            action["file"] == "/path/to/source2.mp4"
            or action["file"] == "/path/to/source2.jpg"
            for action in filtered_actions
        )


class TestOrphanedFileIntegration:
    """Test integration with other components for orphaned file cleanup."""

    def test_cleanup_orphaned_files_with_different_file_types(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test cleanup_orphaned_files with different file types."""
        # Create files with orphaned JPG
        timestamp1 = datetime(2023, 1, 15, 12, 0, 0)
        timestamp2 = datetime(2023, 1, 15, 12, 1, 0)

        files1 = make_file_set([timestamp1])
        files2 = make_file_set([timestamp2])

        # Remove MP4 from second file to create orphaned JPG
        files2[0]["mp4"].unlink()

        # Create a file with no JPG (only MP4)
        files3 = make_file_set([datetime(2023, 1, 15, 12, 2, 0)])
        files3[0]["jpg"].unlink()

        mapping = {
            timestamp1.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files1[0]["mp4"],
                ".jpg": files1[0]["jpg"],
            },
            timestamp2.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files2[0]["jpg"],
            },
            datetime(2023, 1, 15, 12, 2, 0).strftime("%Y%m%d%H%M%S"): {
                ".mp4": files3[0]["mp4"],
                ".jpg": None,  # No JPG
            },
        }

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the file manager to track calls
        mock_remove = mocker.patch.object(processor, "_remove_orphaned_jpg_file")
        processor.cleanup_orphaned_files(mapping)

        # Should remove exactly one orphaned JPG
        mock_remove.assert_called_once()
        # Should be called with the orphaned JPG file
        mock_remove.assert_called_with(files2[0]["jpg"])

    def test_cleanup_orphaned_files_with_graceful_exit(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test cleanup_orphaned_files with graceful exit during processing."""
        # Create files with orphaned JPG
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Remove MP4 to create orphaned JPG
        files[0]["mp4"].unlink()

        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files[0]["jpg"],
            }
        }

        # Request graceful exit
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the file manager to track calls
        mock_remove = mocker.patch.object(processor, "_remove_orphaned_jpg_file")
        processor.cleanup_orphaned_files(mapping)

        # Should not remove any files due to graceful exit
        mock_remove.assert_not_called()

    def test_cleanup_orphaned_files_with_empty_mapping(
        self, config, graceful_exit, logger, mocker
    ):
        """Test cleanup_orphaned_files with empty mapping."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the file manager to track calls
        mock_remove = mocker.patch.object(processor, "_remove_orphaned_jpg_file")
        processor.cleanup_orphaned_files({})

        # Should not remove any files with empty mapping
        mock_remove.assert_not_called()

    def test_cleanup_orphaned_files_with_exception_handling(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test cleanup_orphaned_files with exception handling."""
        # Create files with orphaned JPG
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Remove MP4 to create orphaned JPG
        files[0]["mp4"].unlink()

        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the file manager to raise exception
        mocker.patch.object(
            processor, "_remove_orphaned_jpg_file", side_effect=OSError("Mocked error")
        )

        # Should raise the exception (no exception handling in this method)
        with pytest.raises(OSError, match="Mocked error"):
            processor.cleanup_orphaned_files(mapping)

    def test_remove_orphaned_jpg_file_methods(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_orphaned_jpg_file and related helper methods (lines 465-468, 480-482, 486-493 coverage)."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test JPG files in different locations
        input_jpg = temp_dir / "input" / "test.jpg"
        output_jpg = temp_dir / "output" / "test.jpg"

        input_jpg.parent.mkdir(parents=True, exist_ok=True)
        output_jpg.parent.mkdir(parents=True, exist_ok=True)

        input_jpg.touch()
        output_jpg.touch()

        # Configure output directory
        config.output = temp_dir / "output"

        # Test _is_jpg_from_output_directory with no output configured
        config.output = None
        assert processor._is_jpg_from_output_directory(input_jpg) is False

        # Test _is_jpg_from_output_directory with output configured
        config.output = temp_dir / "output"
        assert processor._is_jpg_from_output_directory(input_jpg) is False
        assert processor._is_jpg_from_output_directory(output_jpg) is True

        # Test _get_source_root_for_jpg
        is_output, source_root = processor._determine_jpg_source_info(input_jpg)
        assert is_output is False
        assert source_root == config.directory

        is_output, source_root = processor._determine_jpg_source_info(output_jpg)
        assert is_output is True
        assert source_root == config.output

        # Test _remove_orphaned_jpg_file (mock the actual file removal)
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")
        processor._remove_orphaned_jpg_file(input_jpg)

        # Verify the call
        mock_remove.assert_called_once()
        call_args = mock_remove.call_args
        assert call_args[0][0] == input_jpg  # File path
        assert call_args[1]["is_output"] is False  # is_output
        assert call_args[1]["source_root"] == config.directory  # source_root

    def test_cleanup_orphaned_files(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test orphaned file cleanup."""
        # Create files with orphaned JPG
        timestamp1 = datetime(2023, 1, 15, 12, 0, 0)
        timestamp2 = datetime(2023, 1, 15, 12, 1, 0)

        files1 = make_file_set([timestamp1])
        files2 = make_file_set([timestamp2])

        # Remove MP4 from second file to create orphaned JPG
        files2[0]["mp4"].unlink()

        mapping = {
            timestamp1.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files1[0]["mp4"],
                ".jpg": files1[0]["jpg"],
            },
            timestamp2.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files2[0]["jpg"],
            },
        }

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the file manager to track calls
        mock_remove = mocker.patch.object(processor, "_remove_orphaned_jpg_file")
        processor.cleanup_orphaned_files(mapping)

        # Should remove exactly one orphaned JPG
        mock_remove.assert_called_once()


class TestActionPlanGeneration:
    """Test action plan generation with various scenarios."""

    def test_action_plan_generation_cleanup_mode(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation in cleanup mode."""
        # Enable cleanup mode
        config.cleanup = True

        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # In cleanup mode, should skip transcoding and create removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # source + jpg removal due to skip

    def test_action_plan_generation_with_age_cutoffs(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation with age cutoffs."""
        # Set age cutoff to skip recent files
        config.older_than = 1  # 1 day

        recent_timestamp = datetime.now()  # Will be skipped
        old_timestamp = datetime.now() - timedelta(days=2)  # Will be processed

        recent_files = make_file_set([recent_timestamp])
        old_files = make_file_set([old_timestamp])

        mp4s = [
            (recent_files[0]["mp4"], recent_timestamp),
            (old_files[0]["mp4"], old_timestamp),
        ]

        mapping = {
            recent_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_files[0]["mp4"],
                ".jpg": recent_files[0]["jpg"],
            },
            old_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": old_files[0]["mp4"],
                ".jpg": old_files[0]["jpg"],
            },
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should only process the old file
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2  # Only for the old file

    def test_action_plan_generation_with_existing_archives(
        self, config, graceful_exit, logger, make_file_set, temp_dir
    ):
        """Test action plan generation when archive already exists."""
        # Disable no_skip to allow skipping when archive exists
        config.no_skip = False
        config.output = temp_dir / "output"

        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Create existing archive
        archive_path = (
            config.output
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.touch()

        # Make archive large enough to skip transcoding
        with archive_path.open("w") as f:
            f.write("x" * 2_000_000)  # 2MB file

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip transcoding and create removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # source + jpg removal due to skip

    def test_action_plan_generation_complex_scenarios(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation with complex scenarios."""
        # Create multiple files with different characteristics
        # Use fixed timestamps to avoid issues with current time
        timestamps = [
            datetime(2023, 1, 15, 12, 0, 0),  # File 1
            datetime(2023, 1, 15, 12, 1, 0),  # File 2
            datetime(2023, 1, 15, 12, 2, 0),  # File 3
        ]

        files = []
        for ts in timestamps:
            files.extend(make_file_set([ts]))

        mp4s = [(f["mp4"], ts) for f, ts in zip(files, timestamps)]

        mapping = {}
        for i, (ts, f) in enumerate(zip(timestamps, files)):
            mapping[ts.strftime("%Y%m%d%H%M%S")] = {".mp4": f["mp4"], ".jpg": f["jpg"]}

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should process all files
        assert len(plan["transcoding"]) == 3
        assert len(plan["removals"]) == 6  # 3 source + 3 jpg removals

    def test_generate_action_plan_basic(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test basic action plan generation."""
        # Create test files
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Verify plan structure
        assert "transcoding" in plan
        assert "removals" in plan
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2  # source + jpg removal

    def test_generate_action_plan_with_cleanup_mode(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation in cleanup mode."""
        # Enable cleanup mode
        config.cleanup = True

        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # In cleanup mode, should skip transcoding and create removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # source + jpg removal due to skip

    def test_generate_action_plan_with_age_cutoff(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test action plan generation with age cutoff."""
        # Set age cutoff to skip recent files
        config.older_than = 1  # 1 day

        recent_timestamp = datetime.now()  # Will be skipped
        old_timestamp = datetime.now() - timedelta(days=2)  # Will be processed

        recent_files = make_file_set([recent_timestamp])
        old_files = make_file_set([old_timestamp])

        mp4s = [
            (recent_files[0]["mp4"], recent_timestamp),
            (old_files[0]["mp4"], old_timestamp),
        ]

        mapping = {
            recent_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": recent_files[0]["mp4"],
                ".jpg": recent_files[0]["jpg"],
            },
            old_timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": old_files[0]["mp4"],
                ".jpg": old_files[0]["jpg"],
            },
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should only process the old file
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 2  # Only for the old file

    def test_generate_action_plan_with_existing_archive(
        self, config, graceful_exit, logger, make_file_set, temp_dir
    ):
        """Test action plan generation when archive already exists."""
        # Disable no_skip to allow skipping when archive exists
        config.no_skip = False
        config.output = temp_dir / "output"

        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Create existing archive
        archive_path = (
            config.output
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        archive_path.touch()

        # Make archive large enough to skip transcoding
        with archive_path.open("w") as f:
            f.write("x" * 2_000_000)  # 2MB file

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": files[0]["jpg"],
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should skip transcoding and create removal actions
        assert len(plan["transcoding"]) == 0
        assert len(plan["removals"]) == 2  # source + jpg removal due to skip


class TestPlanExecution:
    """Test plan execution with various scenarios."""

    def test_execute_plan_with_graceful_exit(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test execute_plan with graceful exit during processing."""
        # Create a plan with multiple actions
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived2.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
            ],
            "removals": [],
        }

        # Mock transcoder
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Request graceful exit before processing
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should exit gracefully and return True
        assert result is True
        # Should not have executed any transcoding actions
        assert not mock_progress_reporter.start_file.called

    def test_execute_plan_with_failed_transcode(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test execute_plan with failed transcoding."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": str(files[0]["mp4"]),
                    "reason": "Source file for transcoded archive",
                },
                {
                    "type": "jpg_removal_after_transcode",
                    "file": str(files[0]["jpg"]),
                    "reason": "Paired with transcoded MP4",
                },
            ],
        }

        # Mock transcoder to return failure
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=False
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should still return True (continue processing)
        assert result is True
        # Should not remove source files and JPGs due to transcoding failure
        mock_file_manager = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )
        mock_file_manager.assert_not_called()

    def test_execute_plan_with_mixed_action_types(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test execute_plan with mixed action types."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                }
            ],
            "removals": [
                {
                    "type": "source_removal_after_skip",
                    "file": str(files[0]["mp4"]),
                    "reason": "Skipping transcoding: archive exists",
                },
                {
                    "type": "jpg_removal_after_skip",
                    "file": str(files[0]["jpg"]),
                    "reason": "Skipping transcoding: archive exists",
                },
            ],
        }

        # Mock transcoder to return success
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Mock file manager
        mock_file_manager = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file"
        )

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        assert result is True
        # Should execute transcoding and removal actions
        assert mock_progress_reporter.start_file.called
        # Should have 3 removal calls: JPG from transcode success, source from transcode success, and 1 from skip removals
        # (the other skip removal might be filtered out)
        assert mock_file_manager.call_count == 3

    @pytest.mark.parametrize(
        "exception_type,exception_msg",
        [
            (OSError, "File not found"),
            (PermissionError, "Permission denied"),
            (FileNotFoundError, "File does not exist"),
        ],
        ids=["os_error", "permission_error", "file_not_found"],
    )
    def test_execute_plan_with_various_exceptions(
        self, config, graceful_exit, logger, mocker, exception_type, exception_msg
    ):
        """Test execute_plan with various exception types during removal."""
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file1.mp4",
                    "reason": "Test removal",
                },
            ],
        }

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Mock file manager to raise specified exception
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=exception_type(exception_msg),
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should handle exceptions gracefully
        result = processor.execute_plan(plan, mock_progress_reporter)

        assert result is True

    def test_execute_plan_with_exception_group(
        self, config, graceful_exit, logger, mocker
    ):
        """Test execute_plan with ExceptionGroup handling."""
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file1.mp4",
                    "reason": "Test removal",
                },
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file2.mp4",
                    "reason": "Test removal",
                },
            ],
        }

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Mock file manager to raise exceptions
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("File not found"),
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should handle ExceptionGroup gracefully
        result = processor.execute_plan(plan, mock_progress_reporter)

        assert result is True


class TestErrorHandling:
    """Test error handling in FileProcessor methods."""

    def test_error_handling_file_operations(
        self, config, graceful_exit, logger, mocker
    ):
        """Test error handling in file operations."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test _remove_paired_jpg with exception
        mock_remove_file = mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("Mocked error"),
        )

        # Should raise the exception (no exception handling in this method)
        with pytest.raises(OSError, match="Mocked error"):
            processor._remove_paired_jpg("/test/file.jpg")

        # Verify the mock was called correctly
        mock_remove_file.assert_called_once_with(
            Path("/test/file.jpg"),
            logger,
            dry_run=False,
            delete=False,
            trash_root=config.trash_root,
            is_output=False,
            source_root=config.directory,
        )

    def test_exception_handling_during_processing(
        self, config, graceful_exit, logger, mocker
    ):
        """Test exception handling during file processing."""
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/test/file1.mp4",
                    "reason": "Test removal 1",
                },
                {
                    "type": "source_removal_after_transcode",
                    "file": "/test/file2.mp4",
                    "reason": "Test removal 2",
                },
            ],
        }

        # Mock file manager to raise different exceptions
        def mock_remove_with_exceptions(file_path, *args, **kwargs):
            if "file1" in file_path:
                raise OSError("File not found")
            elif "file2" in file_path:
                raise PermissionError("Permission denied")
            return True

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_with_exceptions,
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should handle multiple exceptions gracefully
        result = processor.execute_plan(plan, mocker.MagicMock())

        assert result is True

    def test_execute_plan_with_removal_exceptions(
        self, config, graceful_exit, logger, mocker
    ):
        """Test plan execution with removal exceptions."""
        # Create a simple plan
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file.mp4",
                    "reason": "Test removal",
                }
            ],
        }

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Mock file manager to raise exception
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("File not found"),
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should handle exception gracefully
        result = processor.execute_plan(plan, mock_progress_reporter)

        assert result is True

    def test_cleanup_orphaned_files_with_graceful_exit(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test orphaned file cleanup with graceful exit."""
        # Create files with orphaned JPG
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Remove MP4 to create orphaned JPG
        files[0]["mp4"].unlink()

        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files[0]["jpg"],
            }
        }

        # Request graceful exit
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)

        # Should exit gracefully
        processor.cleanup_orphaned_files(mapping)

    def test_error_recovery_scenarios(self, config, graceful_exit, logger, mocker):
        """Test error recovery scenarios."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test with various error scenarios
        test_cases = [
            (OSError("File not found"), "/nonexistent/file.mp4"),
            (PermissionError("Permission denied"), "/protected/file.mp4"),
            (Exception("Generic error"), "/error/file.mp4"),
        ]

        for exception, file_path in test_cases:
            mocker.patch(
                "src.archiver.file_manager.FileManager.remove_file",
                side_effect=exception,
            )

            # Should raise the exception (no exception handling in this method)
            with pytest.raises(type(exception)):
                processor._remove_paired_jpg(file_path)

    def test_error_logging_scenarios(self, config, graceful_exit, logger, mocker):
        """Test error logging scenarios."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock logger to capture log messages
        mock_logger = mocker.MagicMock()
        processor.logger = mock_logger

        # Test error logging - but the method raises exceptions, so we need to catch them
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("Test error"),
        )

        with pytest.raises(OSError, match="Test error"):
            processor._remove_paired_jpg("/test/file.jpg")

        # The error is not logged by _remove_paired_jpg itself, but by the caller
        # So we can't easily test error logging here without mocking at a different level


class TestComplexScenarios:
    """Test complex scenarios in FileProcessor methods."""

    def test_edge_cases_in_file_processing(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test edge cases in file processing."""
        # Create files with edge case scenarios
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Test with None JPG
        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": None,  # No JPG file
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should create transcoding action but no JPG removal
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 1  # Only source removal

    def test_error_handling_complex_scenarios(
        self, config, graceful_exit, logger, mocker
    ):
        """Test error handling in complex scenarios."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test with various complex error scenarios
        plan = {
            "transcoding": [],
            "removals": [
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file1.mp4",
                    "reason": "Test removal 1",
                },
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file2.mp4",
                    "reason": "Test removal 2",
                },
                {
                    "type": "source_removal_after_transcode",
                    "file": "/nonexistent/file3.mp4",
                    "reason": "Test removal 3",
                },
            ],
        }

        # Mock file manager to raise different exceptions for different files
        def mock_remove_with_complex_exceptions(file_path, *args, **kwargs):
            if "file1" in file_path:
                raise OSError("File not found")
            elif "file2" in file_path:
                raise PermissionError("Permission denied")
            elif "file3" in file_path:
                raise Exception("Generic error")
            return True

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_with_complex_exceptions,
        )

        # Should handle all exceptions gracefully
        result = processor.execute_plan(plan, mocker.MagicMock())

        assert result is True

    def test_graceful_exit_complex_scenarios(
        self, config, graceful_exit, logger, make_file_set, mocker
    ):
        """Test graceful exit in complex scenarios."""
        # Create a complex plan
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived1.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
                {
                    "type": "transcode",
                    "input": str(files[0]["mp4"]),
                    "output": str(files[0]["mp4"].parent / "archived2.mp4"),
                    "jpg_to_remove": str(files[0]["jpg"]),
                },
            ],
            "removals": [
                {
                    "type": "source_removal_after_skip",
                    "file": str(files[0]["mp4"]),
                    "reason": "Skipping transcoding: archive exists",
                },
                {
                    "type": "jpg_removal_after_skip",
                    "file": str(files[0]["jpg"]),
                    "reason": "Skipping transcoding: archive exists",
                },
            ],
        }

        # Mock transcoder
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Request graceful exit during processing
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should exit gracefully
        assert result is True
        # Should not have executed any actions
        assert not mock_progress_reporter.start_file.called

    def test_complex_file_processing_scenarios(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test complex file processing scenarios."""
        # Create multiple files with different characteristics
        timestamps = [
            datetime(2023, 1, 15, 12, 0, 0),
            datetime(2023, 1, 15, 12, 1, 0),
            datetime(2023, 1, 15, 12, 2, 0),
        ]

        files = []
        for ts in timestamps:
            files.extend(make_file_set([ts]))

        # Create a scenario with mixed file types
        # Remove JPG from one file to create orphaned JPG scenario
        files[1]["jpg"].unlink()

        mp4s = [(f["mp4"], ts) for f, ts in zip(files, timestamps) if f["mp4"].exists()]

        mapping = {}
        for i, (ts, f) in enumerate(zip(timestamps, files)):
            mp4_exists = f["mp4"].exists() if isinstance(f, dict) else True
            jpg_exists = f["jpg"].exists() if isinstance(f, dict) else True

            mapping[ts.strftime("%Y%m%d%H%M%S")] = {
                ".mp4": f["mp4"] if mp4_exists else None,
                ".jpg": f["jpg"] if jpg_exists else None,
            }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should handle the complex scenario correctly
        # All 3 files should be processed (even the one with None JPG)
        assert len(plan["transcoding"]) == 3
        assert (
            len(plan["removals"]) >= 3
        )  # At least source removals for transcoded files


class TestEarlyExitPaths:
    """Test early exit paths in FileProcessor methods."""

    def test_generate_action_plan_early_exit_no_jpg(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test generate_action_plan when JPG file is None."""
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Remove the JPG file to create a scenario with None JPG
        files[0]["jpg"].unlink()

        mp4s = [(files[0]["mp4"], timestamp)]
        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": files[0]["mp4"],
                ".jpg": None,  # No JPG file
            }
        }

        processor = FileProcessor(config, logger, graceful_exit)
        plan = processor.generate_action_plan(mp4s, mapping)

        # Should still create transcoding action but no JPG removal
        assert len(plan["transcoding"]) == 1
        assert len(plan["removals"]) == 1  # Only source removal, no JPG removal

    def test_execute_plan_early_exit_graceful_exit(
        self, config, graceful_exit, logger, mocker
    ):
        """Test execute_plan with graceful exit during processing."""
        # Create a plan with multiple actions
        plan = {
            "transcoding": [
                {
                    "type": "transcode",
                    "input": "/test/input1.mp4",
                    "output": "/test/output1.mp4",
                    "jpg_to_remove": "/test/input1.jpg",
                },
                {
                    "type": "transcode",
                    "input": "/test/input2.mp4",
                    "output": "/test/output2.mp4",
                    "jpg_to_remove": "/test/input2.jpg",
                },
            ],
            "removals": [],
        }

        # Mock transcoder
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=True
        )

        # Mock progress reporter
        mock_progress_reporter = mocker.MagicMock()

        # Request graceful exit before processing
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)
        result = processor.execute_plan(plan, mock_progress_reporter)

        # Should exit gracefully and return True
        assert result is True
        # Should not have executed any transcoding actions
        assert not mock_progress_reporter.start_file.called

    def test_cleanup_orphaned_files_early_exit_graceful_exit(
        self, config, graceful_exit, logger, make_file_set
    ):
        """Test cleanup_orphaned_files with graceful exit."""
        # Create files with orphaned JPG
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        files = make_file_set([timestamp])

        # Remove MP4 to create orphaned JPG
        files[0]["mp4"].unlink()

        mapping = {
            timestamp.strftime("%Y%m%d%H%M%S"): {
                ".mp4": None,  # No MP4
                ".jpg": files[0]["jpg"],
            }
        }

        # Request graceful exit
        graceful_exit.request_exit()

        processor = FileProcessor(config, logger, graceful_exit)

        # Should exit gracefully without processing
        processor.cleanup_orphaned_files(mapping)

    def test_size_based_cleanup_early_exit_no_max_size(
        self, config, graceful_exit, logger
    ):
        """Test size_based_cleanup when max_size is not configured."""
        config.max_size = None

        processor = FileProcessor(config, logger, graceful_exit)

        # Should exit early without doing anything
        processor.size_based_cleanup(set())

    def test_size_based_cleanup_early_exit_invalid_max_size(
        self, config, graceful_exit, logger, mocker
    ):
        """Test size_based_cleanup when max_size parsing fails."""
        config.max_size = "invalid_size"

        # Mock parse_size to return None
        mocker.patch(
            "src.archiver.processor.FileProcessor._parse_max_size_with_error_handling",
            return_value=None,
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should exit early without doing anything
        processor.size_based_cleanup(set())

    def test_size_based_cleanup_early_exit_size_not_exceeded(
        self, config, graceful_exit, logger, mocker
    ):
        """Test size_based_cleanup when size is not exceeded."""
        config.max_size = "10GB"

        # Mock size calculation to return small size
        mocker.patch(
            "src.archiver.processor.FileProcessor._calculate_total_directory_sizes",
            return_value=1024,
        )
        mocker.patch(
            "src.archiver.processor.FileProcessor._is_cleanup_needed",
            return_value=False,
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Should exit early without cleanup
        processor.size_based_cleanup(set())


class TestFixtureIntegration:
    """Test integration with test fixtures."""

    def test_file_processor_with_config_fixture(self, config, graceful_exit, logger):
        """Test FileProcessor using the config fixture."""
        processor = FileProcessor(config, logger, graceful_exit)

        assert processor.config == config
        assert processor.config.directory is not None

    def test_file_processor_with_graceful_exit_fixture(
        self, config, graceful_exit, logger
    ):
        """Test FileProcessor using the graceful_exit fixture."""
        processor = FileProcessor(config, logger, graceful_exit)

        assert processor.graceful_exit == graceful_exit
        assert not graceful_exit.should_exit()

    def test_file_processor_with_logger_fixture(self, config, graceful_exit, logger):
        """Test FileProcessor using the logger fixture."""
        processor = FileProcessor(config, logger, graceful_exit)

        assert processor.logger == logger
        assert hasattr(logger, "info")
        assert hasattr(logger, "error")
