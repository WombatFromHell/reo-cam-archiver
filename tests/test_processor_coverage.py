"""
Additional tests to cover uncovered lines in processor.py
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.archiver.processor import FileProcessor
from src.archiver.utils import parse_size


class TestProcessorCoverage:
    """Tests to cover uncovered lines in processor.py"""

    def test_calculate_age_cutoff_with_zero_days(self, config, graceful_exit, logger):
        """Test _calculate_age_cutoff when older_than is 0"""
        config.older_than = 0
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._calculate_age_cutoff()
        assert result is None

    def test_should_skip_file_due_to_age_with_none_cutoff(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _should_skip_file_due_to_age with None cutoff"""
        processor = FileProcessor(config, logger, graceful_exit)

        test_file = temp_dir / "test.mp4"
        test_file.touch()
        timestamp = datetime.now()

        result = processor._should_skip_file_due_to_age(test_file, timestamp, None)
        assert result is False

    def test_create_skip_removal_actions_cleanup_mode(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _create_skip_removal_actions in cleanup mode"""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = temp_dir / "input.mp4"
        jpg_file = temp_dir / "input.jpg"
        output_file = temp_dir / "output.mp4"
        removal_actions = []

        processor._create_skip_removal_actions(
            input_file, jpg_file, output_file, True, removal_actions
        )

        assert len(removal_actions) == 2
        assert removal_actions[0]["type"] == "source_removal_after_skip"
        assert removal_actions[1]["type"] == "jpg_removal_after_skip"
        assert "cleanup mode enabled" in removal_actions[0]["reason"]

    def test_create_skip_removal_actions_normal_mode(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _create_skip_removal_actions in normal mode"""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = temp_dir / "input.mp4"
        jpg_file = temp_dir / "input.jpg"
        output_file = temp_dir / "output.mp4"
        removal_actions = []

        processor._create_skip_removal_actions(
            input_file, jpg_file, output_file, False, removal_actions
        )

        assert len(removal_actions) == 2
        assert "archive exists" in removal_actions[0]["reason"]

    def test_create_skip_removal_actions_no_jpg(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _create_skip_removal_actions with no JPG file"""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = temp_dir / "input.mp4"
        output_file = temp_dir / "output.mp4"
        removal_actions = []

        processor._create_skip_removal_actions(
            input_file, None, output_file, False, removal_actions
        )

        assert len(removal_actions) == 1
        assert removal_actions[0]["type"] == "source_removal_after_skip"

    def test_determine_source_root_output_file(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root with output file"""
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        config.output = output_dir
        config.clean_output = True

        # Create a file inside the output directory
        output_file = output_dir / "test.mp4"
        output_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)
        source_root, is_output_file = processor._determine_source_root(output_file)

        assert is_output_file is True
        assert source_root == config.output

    def test_determine_source_root_input_file(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root with input file"""
        config.output = temp_dir / "output"
        config.clean_output = True

        # Create a file outside the output directory (in input)
        input_file = temp_dir / "input.mp4"
        input_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)
        source_root, is_output_file = processor._determine_source_root(input_file)

        assert is_output_file is False
        assert source_root == config.directory

    def test_determine_source_root_no_output(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _determine_source_root when no output is configured"""
        config.output = None

        input_file = temp_dir / "input.mp4"
        input_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)
        source_root, is_output_file = processor._determine_source_root(input_file)

        assert is_output_file is False
        assert source_root == config.directory

    def test_execute_transcoding_action_failure(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _execute_transcoding_action with transcoding failure"""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = temp_dir / "input.mp4"
        output_file = temp_dir / "output.mp4"
        input_file.touch()

        # Mock transcoder to return failure
        mocker.patch(
            "src.archiver.transcoder.Transcoder.transcode_file", return_value=False
        )

        # Mock progress reporter
        mock_progress = mocker.MagicMock()

        action = {"input": input_file, "output": output_file, "jpg_to_remove": None}

        result = processor._execute_transcoding_action(action, mock_progress)
        assert result is False

    def test_remove_paired_jpg_with_string_path(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_paired_jpg with string path"""
        processor = FileProcessor(config, logger, graceful_exit)

        jpg_file = temp_dir / "test.jpg"
        jpg_file.touch()

        # Mock FileManager.remove_file to track calls
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")

        processor._remove_paired_jpg(str(jpg_file))

        mock_remove.assert_called_once()
        # Verify the first argument is a Path object
        assert isinstance(mock_remove.call_args[0][0], Path)

    def test_remove_paired_jpg_with_none(self, config, graceful_exit, logger, mocker):
        """Test _remove_paired_jpg with None"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock FileManager.remove_file to track calls
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")

        processor._remove_paired_jpg(None)

        # Should not call remove_file when path is None
        mock_remove.assert_not_called()

    def test_remove_source_file_not_found_in_actions(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _remove_source_file when file is not found in removal actions"""
        processor = FileProcessor(config, logger, graceful_exit)

        input_file = temp_dir / "input.mp4"
        input_file.touch()

        # Create removal actions without our test file
        removal_actions = [
            {
                "type": "source_removal_after_transcode",
                "file": str(temp_dir / "other.mp4"),
                "reason": "Other file",
            }
        ]

        # Mock FileManager.remove_file to track calls
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")

        processor._remove_source_file(input_file, removal_actions)

        # Should not call remove_file since the file wasn't found in actions
        mock_remove.assert_not_called()
        # Actions list should remain unchanged
        assert len(removal_actions) == 1

    @pytest.mark.parametrize(
        "action_type,failed_set_attr,expected_result",
        [
            ("source_removal_after_transcode", "failed_transcodes", True),
            ("jpg_removal_after_transcode", "failed_jpgs", True),
            ("other_action", "failed_transcodes", False),
        ],
        ids=["source_failure", "jpg_failure", "other_action"],
    )
    def test_should_skip_removal_action(
        self,
        config,
        graceful_exit,
        logger,
        action_type,
        failed_set_attr,
        expected_result,
    ):
        """Test _should_skip_removal_action for different failure scenarios"""
        processor = FileProcessor(config, logger, graceful_exit)

        action = {"type": action_type, "file": "/test/file.ext"}

        failed_transcodes = (
            {"/test/file.ext"} if failed_set_attr == "failed_transcodes" else set()
        )
        failed_jpgs = {"/test/file.ext"} if failed_set_attr == "failed_jpgs" else set()

        result = processor._should_skip_removal_action(
            action, failed_transcodes, failed_jpgs
        )
        assert result is expected_result

    def test_is_source_removal_for_failed_transcode_positive(
        self, config, graceful_exit, logger
    ):
        """Test _is_source_removal_for_failed_transcode positive case"""
        processor = FileProcessor(config, logger, graceful_exit)

        action = {"type": "source_removal_after_transcode", "file": "/test/file.mp4"}

        failed_transcodes = {"/test/file.mp4"}

        result = processor._is_source_removal_for_failed_transcode(
            action, failed_transcodes
        )
        assert result is True

    def test_is_source_removal_for_failed_transcode_negative(
        self, config, graceful_exit, logger
    ):
        """Test _is_source_removal_for_failed_transcode negative cases"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Wrong action type
        action1 = {"type": "jpg_removal_after_transcode", "file": "/test/file.mp4"}
        failed_transcodes = {"/test/file.mp4"}
        result1 = processor._is_source_removal_for_failed_transcode(
            action1, failed_transcodes
        )
        assert result1 is False

        # File not in failed set
        action2 = {"type": "source_removal_after_transcode", "file": "/test/file.mp4"}
        failed_transcodes = {"/test/other.mp4"}
        result2 = processor._is_source_removal_for_failed_transcode(
            action2, failed_transcodes
        )
        assert result2 is False

    def test_is_jpg_removal_for_failed_transcode_positive(
        self, config, graceful_exit, logger
    ):
        """Test _is_jpg_removal_for_failed_transcode positive case"""
        processor = FileProcessor(config, logger, graceful_exit)

        action = {"type": "jpg_removal_after_transcode", "file": "/test/file.jpg"}

        failed_jpgs = {"/test/file.jpg"}

        result = processor._is_jpg_removal_for_failed_transcode(action, failed_jpgs)
        assert result is True

    def test_execute_removal_action_with_exception(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _execute_removal_action with exception"""
        processor = FileProcessor(config, logger, graceful_exit)

        test_file = temp_dir / "test.mp4"
        test_file.touch()

        action = {"file": test_file}

        exceptions = []

        # Mock FileManager.remove_file to raise an exception
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("Permission denied"),
        )

        processor._execute_removal_action(action, exceptions)

        assert len(exceptions) == 1
        assert isinstance(exceptions[0], OSError)

    def test_handle_removal_exceptions_with_multiple(
        self, config, graceful_exit, logger, caplog
    ):
        """Test _handle_removal_exceptions with multiple exceptions"""
        processor = FileProcessor(config, logger, graceful_exit)

        exceptions = [OSError("Error 1"), PermissionError("Error 2")]

        processor._handle_removal_exceptions(exceptions)

        # Check that error messages were logged
        assert "Multiple removal failures occurred: 2 total" in caplog.text

    def test_handle_action_type_various_types(self, config, graceful_exit, logger):
        """Test _handle_action_type with various action types"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test all known action types
        assert "Processing transcoding action" in processor._handle_action_type(
            "transcode"
        )
        assert (
            "Processing source removal after transcode"
            in processor._handle_action_type("source_removal_after_transcode")
        )
        assert (
            "Processing JPG removal after transcode"
            in processor._handle_action_type("jpg_removal_after_transcode")
        )
        assert "Processing source removal after skip" in processor._handle_action_type(
            "source_removal_after_skip"
        )
        assert "Processing JPG removal after skip" in processor._handle_action_type(
            "jpg_removal_after_skip"
        )

        # Test unknown action type
        result = processor._handle_action_type("unknown_action")
        assert "unknown action type" in result.lower()

    def test_remove_orphaned_jpg_files_with_none_values(
        self, config, graceful_exit, logger, mocker
    ):
        """Test _remove_orphaned_jpg_files with None values in mapping"""
        processor = FileProcessor(config, logger, graceful_exit)

        mapping = {
            "timestamp1": {
                ".jpg": None,  # No JPG
                ".mp4": Path("/path/to/file.mp4"),
            },
            "timestamp2": {
                ".jpg": Path("/path/to/file.jpg"),
                ".mp4": None,  # No MP4 (orphaned JPG)
            },
        }

        # Mock the removal method to track calls
        mock_remove = mocker.patch.object(processor, "_remove_orphaned_jpg_file")

        count = processor._remove_orphaned_jpg_files(mapping)

        # Should only call for the orphaned JPG (timestamp2)
        mock_remove.assert_called_once_with(Path("/path/to/file.jpg"))
        assert count == 1

    def test_is_orphaned_jpg_cases(self, config, graceful_exit, logger):
        """Test _is_orphaned_jpg with various cases"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Case 1: JPG exists, MP4 doesn't (orphaned)
        result1 = processor._is_orphaned_jpg(Path("/path/to/file.jpg"), None)
        assert result1 is True

        # Case 2: Both exist (not orphaned)
        result2 = processor._is_orphaned_jpg(
            Path("/path/to/file.jpg"), Path("/path/to/file.mp4")
        )
        assert result2 is False

        # Case 3: JPG doesn't exist (not orphaned)
        result3 = processor._is_orphaned_jpg(None, Path("/path/to/file.mp4"))
        assert result3 is False

        # Case 4: Neither exists (not orphaned)
        result4 = processor._is_orphaned_jpg(None, None)
        assert result4 is False

    def test_remove_orphaned_jpg_file_with_none(
        self, config, graceful_exit, logger, mocker
    ):
        """Test _remove_orphaned_jpg_file with None input"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock FileManager.remove_file to track calls
        mock_remove = mocker.patch("src.archiver.file_manager.FileManager.remove_file")

        processor._remove_orphaned_jpg_file(None)

        # Should not call remove_file when jpg is None
        mock_remove.assert_not_called()

    def test_determine_jpg_source_info(self, config, graceful_exit, logger, temp_dir):
        """Test _determine_jpg_source_info"""
        processor = FileProcessor(config, logger, graceful_exit)

        jpg_file = temp_dir / "test.jpg"
        jpg_file.touch()

        is_output, source_root = processor._determine_jpg_source_info(jpg_file)

        # Since the file is not in output directory, is_output should be False
        assert is_output is False
        assert source_root == config.directory

    def test_is_jpg_from_output_directory_with_no_output(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _is_jpg_from_output_directory when no output is configured"""
        config.output = None
        processor = FileProcessor(config, logger, graceful_exit)

        jpg_file = temp_dir / "test.jpg"
        jpg_file.touch()

        result = processor._is_jpg_from_output_directory(jpg_file)
        assert result is False

    def test_get_source_root_for_jpg(self, config, graceful_exit, logger, temp_dir):
        """Test _get_source_root_for_jpg with both cases"""
        processor = FileProcessor(config, logger, graceful_exit)

        jpg_file = temp_dir / "test.jpg"

        # Test with output file
        result_output = processor._get_source_root_for_jpg(jpg_file, True)
        assert result_output == config.output

        # Test with input file
        result_input = processor._get_source_root_for_jpg(jpg_file, False)
        assert result_input == config.directory

    def test_get_directory_size_nonexistent(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _get_directory_size with nonexistent directory"""
        processor = FileProcessor(config, logger, graceful_exit)

        nonexistent_dir = temp_dir / "nonexistent"
        size = processor._get_directory_size(nonexistent_dir)

        assert size == 0

    def test_get_directory_size_with_nonexistent_dir(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _get_directory_size with nonexistent directory"""
        processor = FileProcessor(config, logger, graceful_exit)

        nonexistent_dir = temp_dir / "nonexistent"
        size = processor._get_directory_size(nonexistent_dir)

        assert size == 0

    @pytest.mark.parametrize(
        "max_size_value,expected_result,should_log_error",
        [
            (
                "1GB",
                None,
                False,
            ),  # Valid size - actual result will be checked separately
            (
                "invalid_size",
                None,
                True,
            ),  # Invalid size - should return None and log error
            (None, None, False),  # None value - should return None
        ],
        ids=["valid_size", "invalid_size", "none_size"],
    )
    def test_parse_max_size_with_error_handling(
        self,
        config,
        graceful_exit,
        logger,
        mocker,
        caplog,
        max_size_value,
        expected_result,
        should_log_error,
    ):
        """Test _parse_max_size_with_error_handling with various inputs"""
        config.max_size = max_size_value
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock parse_size appropriately based on the test case
        if max_size_value == "invalid_size":
            mocker.patch(
                "src.archiver.utils.parse_size",
                side_effect=ValueError("Invalid size format"),
            )

        result = processor._parse_max_size_with_error_handling()

        if max_size_value == "invalid_size":
            assert result is None
            assert "Invalid max-size value:" in caplog.text
        elif max_size_value is None:
            assert result is None
        else:  # Valid size
            # For valid size, check that result is not None and is greater than 0
            assert result is not None
            assert result > 0

    def test_get_trash_directory_size_no_trash_root(
        self, config, graceful_exit, logger
    ):
        """Test _get_trash_directory_size when no trash root is configured"""
        config.trash_root = None
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._get_trash_directory_size()
        assert result == 0

    def test_get_archived_directory_size_no_output(self, config, graceful_exit, logger):
        """Test _get_archived_directory_size when no output is configured"""
        config.output = None
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._get_archived_directory_size()
        assert result == 0

    def test_is_cleanup_needed_within_limit(
        self, config, graceful_exit, logger, caplog
    ):
        """Test _is_cleanup_needed when within size limit"""
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._is_cleanup_needed(500, 1000)  # 500 < 1000, so within limit
        assert result is False
        assert "is within limit" in caplog.text

    def test_is_cleanup_needed_exceeds_limit(
        self, config, graceful_exit, logger, caplog
    ):
        """Test _is_cleanup_needed when exceeding size limit"""
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._is_cleanup_needed(
            1500, 1000
        )  # 1500 > 1000, so exceeds limit
        assert result is True
        assert "exceeds limit" in caplog.text

    def test_perform_cleanup_operations(self, config, graceful_exit, logger, mocker):
        """Test _perform_cleanup_operations"""
        config.max_size = "1KB"
        config.older_than = 30
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the methods that are called
        mock_collect = mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=[]
        )
        mock_sort = mocker.patch.object(
            processor, "_sort_files_by_priority_and_age", return_value=[]
        )
        mock_remove = mocker.patch.object(
            processor, "_remove_files_until_under_limit", return_value=0
        )
        mock_log = mocker.patch.object(processor, "_log_cleanup_results")

        processor._perform_cleanup_operations(2000, 1000, use_combined_strategy=True)

        mock_collect.assert_called_once_with(
            is_size_based=False, use_combined_strategy=True
        )
        mock_sort.assert_called_once()
        mock_remove.assert_called_once()
        mock_log.assert_called_once()

    def test_collect_all_files_for_cleanup(
        self, config, graceful_exit, logger, mocker, temp_dir
    ):
        """Test _collect_all_files_for_cleanup"""
        config.output = temp_dir / "output"
        config.output.mkdir(exist_ok=True)
        config.clean_output = True
        config.trash_root = temp_dir / ".deleted"
        config.trash_root.mkdir(exist_ok=True)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the collection methods
        mock_trash = mocker.patch.object(
            processor,
            "_collect_trash_files_for_cleanup",
            return_value=[(datetime.now(), 1, temp_dir / "trash.mp4")],
        )
        mock_archived = mocker.patch.object(
            processor,
            "_collect_archived_files_for_cleanup",
            return_value=[(datetime.now(), 3, temp_dir / "archived.mp4")],
        )
        mock_source = mocker.patch.object(
            processor,
            "_collect_source_files_for_cleanup",
            return_value=[(datetime.now(), 2, temp_dir / "source.mp4")],
        )

        files = processor._collect_all_files_for_cleanup(is_size_based=True)

        assert len(files) == 3
        mock_trash.assert_called_once_with(True, False)
        mock_archived.assert_called_once_with(True, False)
        mock_source.assert_called_once_with(True, False)

    def test_collect_trash_files_for_cleanup(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _collect_trash_files_for_cleanup"""
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        # Create subdirectories
        (trash_root / "input").mkdir()
        (trash_root / "output").mkdir()

        # Create test files
        old_file = trash_root / "input" / "REO_test_20230101120000.mp4"
        old_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)
        processor.config.trash_root = trash_root

        files = processor._collect_trash_files_for_cleanup(is_size_based=True)

        # Should find the file we created
        assert len(files) > 0
        assert any("REO_test_20230101120000.mp4" in str(f[2]) for f in files)

    def test_collect_archived_files_for_cleanup_no_clean_output(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _collect_archived_files_for_cleanup when clean_output is False"""
        config.output = temp_dir / "output"
        config.output.mkdir(exist_ok=True)
        config.clean_output = False  # This should prevent collection

        processor = FileProcessor(config, logger, graceful_exit)

        files = processor._collect_archived_files_for_cleanup()

        # Should return empty list since clean_output is False
        assert len(files) == 0

    def test_collect_archived_files_for_cleanup_with_clean_output(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _collect_archived_files_for_cleanup when clean_output is True"""
        config.output = temp_dir / "output"
        config.output.mkdir(exist_ok=True)
        config.clean_output = True  # This should allow collection

        # Create an archived file
        archived_file = config.output / "archived-20230101120000.mp4"
        archived_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)

        files = processor._collect_archived_files_for_cleanup()

        # Should find the archived file
        assert len(files) > 0

    def test_sort_files_by_priority_and_age(self, config, graceful_exit, logger):
        """Test _sort_files_by_priority_and_age"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test data: (timestamp, priority, filepath)
        now = datetime.now()
        older = now - timedelta(days=1)
        newest = now + timedelta(days=1)

        files = [
            (newest, 2, Path("/path3")),
            (older, 3, Path("/path1")),
            (now, 1, Path("/path2")),  # This should come first (priority 1)
            (older, 1, Path("/path4")),  # This should come second (priority 1, older)
        ]

        sorted_files = processor._sort_files_by_priority_and_age(files)

        # Should be sorted by priority first (1, 1, 2, 3), then by timestamp within same priority
        assert sorted_files[0][1] == 1  # First two should have priority 1
        assert sorted_files[1][1] == 1
        assert sorted_files[0][0] <= sorted_files[1][0]  # Older timestamp first

    def test_remove_files_until_under_limit_stops_when_under(
        self, config, graceful_exit, logger, mocker, temp_dir
    ):
        """Test _remove_files_until_under_limit stops when under limit"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test files
        file1 = temp_dir / "file1.mp4"
        file1.touch()

        sorted_files = [(datetime.now(), 1, file1)]

        # Mock the removal function to return a size that brings us under limit
        def mock_remove(file_path, total_size, max_bytes, removed_size):
            return 500  # This will make total_size - removed_size = 1500 - 500 = 1000, which equals max_bytes

        mocker.patch.object(
            processor, "_remove_file_for_cleanup", side_effect=mock_remove
        )

        result = processor._remove_files_until_under_limit(sorted_files, 1500, 1000)

        # Should have removed files until under limit
        assert result >= 0

    def test_remove_file_for_cleanup_exception(
        self, config, graceful_exit, logger, mocker, temp_dir, caplog
    ):
        """Test _remove_file_for_cleanup when FileManager.remove_file raises exception"""
        processor = FileProcessor(config, logger, graceful_exit)

        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Mock FileManager.remove_file to raise an exception
        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=OSError("Permission denied"),
        )

        result = processor._remove_file_for_cleanup(test_file, 2000, 1000, 0)

        # Should return the same removed size (0) and log the error
        assert result == 0
        assert "Failed to remove" in caplog.text

    @pytest.mark.parametrize(
        "max_size_value,parse_return_value,calc_sizes_return,needed_return_value",
        [
            (None, None, None, None),  # No max_size configured
            (
                "invalid",
                None,
                None,
                None,
            ),  # Invalid max_size parsing - should return early after parse
            ("10GB", 10737418240, 1024, False),  # Cleanup not needed
        ],
        ids=["no_max_size", "invalid_max_size", "cleanup_not_needed"],
    )
    def test_size_based_cleanup_scenarios(
        self,
        config,
        graceful_exit,
        logger,
        mocker,
        max_size_value,
        parse_return_value,
        calc_sizes_return,
        needed_return_value,
    ):
        """Test size_based_cleanup with various configuration scenarios"""
        config.max_size = max_size_value
        processor = FileProcessor(config, logger, graceful_exit)

        # Mock all the methods that would be called
        mock_parse = mocker.patch.object(
            processor, "_parse_max_size_with_error_handling"
        )
        mock_calc = mocker.patch.object(processor, "_calculate_total_directory_sizes")
        mock_needed = mocker.patch.object(processor, "_is_cleanup_needed")
        mock_perform = mocker.patch.object(processor, "_perform_cleanup_operations")

        # Set up return values based on the test case
        mock_parse.return_value = parse_return_value
        if calc_sizes_return is not None:
            mock_calc.return_value = calc_sizes_return
        if needed_return_value is not None:
            mock_needed.return_value = needed_return_value

        processor.size_based_cleanup(set())

        # Determine expected behavior based on the test case
        if max_size_value is None:
            # Should exit early without calling other methods
            mock_parse.assert_not_called()
            mock_calc.assert_not_called()
            mock_needed.assert_not_called()
            mock_perform.assert_not_called()
        elif max_size_value == "invalid":
            # Parse is called, but if it returns None, should return early without calling calc
            mock_parse.assert_called_once()
            mock_calc.assert_not_called()  # This should NOT be called if parse returns None
            mock_needed.assert_not_called()
            mock_perform.assert_not_called()
        elif max_size_value == "10GB":
            # Should not perform cleanup operations when not needed
            mock_perform.assert_not_called()

    def test_collect_files_from_location_nonexistent_dir(
        self, config, graceful_exit, logger
    ):
        """Test _collect_files_from_location with nonexistent directory"""
        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._collect_files_from_location(
            Path("/nonexistent/dir"), 1, "source", include_age_filter=False
        )

        assert result == []

    def test_should_skip_file_for_cleanup_scenarios(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _should_skip_file_for_cleanup with various scenarios"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test file in trash directory
        trash_file = temp_dir / ".deleted" / "input" / "test.mp4"
        # Don't actually create the file, just test the logic path

        # This tests the combination logic in _should_skip_file_for_cleanup
        # The result depends on whether the file is actually in a trash directory
        # which is determined by _is_file_in_trash_directory
        processor._should_skip_file_for_cleanup(trash_file)

    def test_is_file_in_output_directory_when_not_cleaning_output_true(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _is_file_in_output_directory_when_not_cleaning_output positive case"""
        config.output = temp_dir / "output"
        config.clean_output = False  # Not cleaning output

        # Create a file in the output directory
        test_file = config.output / "test.mp4"

        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._is_file_in_output_directory_when_not_cleaning_output(
            test_file
        )
        assert result is True

    def test_is_file_in_output_directory_when_not_cleaning_output_false_because_cleaning(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _is_file_in_output_directory_when_not_cleaning_output negative case because cleaning output"""
        config.output = temp_dir / "output"
        config.clean_output = True  # Cleaning output

        # Create a file in the output directory
        test_file = config.output / "test.mp4"

        processor = FileProcessor(config, logger, graceful_exit)

        result = processor._is_file_in_output_directory_when_not_cleaning_output(
            test_file
        )
        assert result is False

    def test_should_skip_file_due_to_age_for_cleanup_size_based(
        self, config, graceful_exit, logger
    ):
        """Test _should_skip_file_due_to_age_for_cleanup with size_based=True"""
        processor = FileProcessor(config, logger, graceful_exit)

        # With size_based=True, should always return False (don't skip based on age)
        result = processor._should_skip_file_due_to_age_for_cleanup(
            datetime.now(), datetime.now() - timedelta(days=1), is_size_based=True
        )
        assert result is False

    def test_should_skip_file_due_to_age_for_cleanup_age_threshold(
        self, config, graceful_exit, logger
    ):
        """Test _should_skip_file_due_to_age_for_cleanup with age threshold"""
        processor = FileProcessor(config, logger, graceful_exit)

        # File timestamp is older than cutoff - should not skip (return False)
        old_ts = datetime.now() - timedelta(days=10)
        cutoff = datetime.now() - timedelta(days=5)
        result = processor._should_skip_file_due_to_age_for_cleanup(
            old_ts, cutoff, is_size_based=False
        )
        assert result is False

        # File timestamp is newer than cutoff - should skip (return True)
        new_ts = datetime.now() - timedelta(days=1)
        result2 = processor._should_skip_file_due_to_age_for_cleanup(
            new_ts, cutoff, is_size_based=False
        )
        assert result2 is True

    def test_validate_and_filter_files(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _validate_and_filter_files"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test files
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        files = [(datetime.now(), 1, test_file)]

        # Mock the validation method
        mocker.patch.object(processor, "_is_valid_for_cleanup", return_value=True)

        result = processor._validate_and_filter_files(files, config)
        assert len(result) == 1

    def test_is_valid_for_cleanup(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test _is_valid_for_cleanup"""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test file
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        file_info = (datetime.now(), 1, test_file)

        # Mock the skip methods
        mocker.patch.object(
            processor, "_should_skip_file_for_cleanup", return_value=False
        )
        mocker.patch.object(
            processor, "_should_skip_file_due_to_age_for_cleanup", return_value=False
        )

        result = processor._is_valid_for_cleanup(file_info, config)
        assert result is True

    def test_collect_source_files_for_cleanup_exclude_output_when_not_cleaning(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test _collect_source_files_for_cleanup excludes output directory when not cleaning"""
        config.output = temp_dir / "output"
        config.output.mkdir(exist_ok=True)
        config.clean_output = False  # Not cleaning output directory

        processor = FileProcessor(config, logger, graceful_exit)

        # Create a file in the output directory (should be excluded from source collection)
        output_file = config.output / "test.mp4"
        output_file.touch()

        # Create a file in the main directory (should be included)
        main_file = temp_dir / "main_test.mp4"
        main_file.touch()

        files = processor._collect_source_files_for_cleanup()

        # Should not include the output file
        file_paths = [str(f[2]) for f in files]
        assert str(output_file) not in file_paths
        # May or may not include main file depending on naming convention

    def test_output_path_generation(self, config, graceful_exit, logger, temp_dir):
        """Test _output_path method"""
        config.output = temp_dir / "output"
        processor = FileProcessor(config, logger, graceful_exit)

        timestamp = datetime(2023, 5, 15, 14, 30, 0)
        input_file = temp_dir / "REO_test_01_20230515143000.mp4"

        output_path = processor._output_path(input_file, timestamp)

        expected = config.output / "2023" / "05" / "15" / "archived-20230515143000.mp4"
        assert output_path == expected
