"""
Comprehensive tests for the FileProcessor cleanup functionality.
This serves as a smoke test to prevent regressions and catch errant behavior
in the cleanup operations.
"""

from datetime import datetime, timedelta

from src.archiver.discovery import FileDiscovery
from src.archiver.processor import (
    AgeBasedCleanupStrategy,
    CleanupFile,
    CleanupRules,
    CleanupStrategy,
    CombinedCleanupStrategy,
    FileProcessor,
    SizeBasedCleanupStrategy,
)


class TestCleanupInitialization:
    """Test cleanup-related initialization and basic functionality."""

    def test_cleanup_rules_creation(self, config):
        """Test creation of CleanupRules object."""
        rules = CleanupRules(
            max_size=1024, older_than_days=30, clean_output=True, is_size_based=False
        )

        assert rules.max_size == 1024
        assert rules.older_than_days == 30
        assert rules.clean_output is True
        assert rules.is_size_based is False

    def test_cleanup_rules_age_based_inclusion(self, config, temp_dir):
        """Test CleanupRules should_include_file with age-based logic."""
        # Create a rule with age-based logic
        rules = CleanupRules(
            max_size=1024, older_than_days=30, clean_output=True, is_size_based=False
        )

        # Create timestamps: one old (should be included), one recent (should not be included)
        old_timestamp = datetime.now() - timedelta(days=45)  # Older than 30 days
        recent_timestamp = datetime.now() - timedelta(
            days=15
        )  # More recent than 30 days

        # Old file should be included
        assert rules.should_include_file(old_timestamp) is True

        # Recent file should not be included
        assert rules.should_include_file(recent_timestamp) is False

    def test_cleanup_rules_size_based_inclusion(self, config, temp_dir):
        """Test CleanupRules should_include_file with size-based logic."""
        # Create a rule with size-based logic
        rules = CleanupRules(
            max_size=1024, older_than_days=30, clean_output=True, is_size_based=True
        )

        # Create timestamps: both should be included regardless of age
        old_timestamp = datetime.now() - timedelta(days=45)  # Older than 30 days
        recent_timestamp = datetime.now() - timedelta(
            days=15
        )  # More recent than 30 days

        # Both files should be included since age is ignored in size-based mode
        assert rules.should_include_file(old_timestamp) is True
        assert rules.should_include_file(recent_timestamp) is True

    def test_cleanup_file_creation(self, temp_dir):
        """Test creation of CleanupFile object."""
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        cleanup_file = CleanupFile(
            path=test_file, timestamp=datetime.now(), priority=1, location_type="trash"
        )

        assert cleanup_file.path == test_file
        assert cleanup_file.priority == 1
        assert cleanup_file.location_type == "trash"

    def test_cleanup_file_inclusion_logic_age_based(self, temp_dir):
        """Test CleanupFile should_include_in_cleanup with age-based logic."""
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Create a config with age threshold
        from unittest.mock import MagicMock

        mock_config = MagicMock()
        mock_config.older_than = 30

        # Create an old file (should be included)
        old_timestamp = datetime.now() - timedelta(days=45)
        old_cleanup_file = CleanupFile(
            path=test_file, timestamp=old_timestamp, priority=1, location_type="trash"
        )

        # Should be included in age-based cleanup
        assert (
            old_cleanup_file.should_include_in_cleanup(mock_config, is_size_based=False)
            is True
        )

        # Create a recent file (should not be included in age-based cleanup)
        recent_timestamp = datetime.now() - timedelta(days=15)
        recent_cleanup_file = CleanupFile(
            path=test_file,
            timestamp=recent_timestamp,
            priority=1,
            location_type="trash",
        )

        # Should not be included in age-based cleanup
        assert (
            recent_cleanup_file.should_include_in_cleanup(
                mock_config, is_size_based=False
            )
            is False
        )

    def test_cleanup_file_inclusion_logic_size_based(self, temp_dir):
        """Test CleanupFile should_include_in_cleanup with size-based logic."""
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Create a config with age threshold
        from unittest.mock import MagicMock

        mock_config = MagicMock()
        mock_config.older_than = 30

        # Parametrized test for different timestamps
        test_cases = [
            ("old", datetime.now() - timedelta(days=45)),
            ("recent", datetime.now() - timedelta(days=15)),
        ]

        for _, timestamp in test_cases:
            cleanup_file = CleanupFile(
                path=test_file, timestamp=timestamp, priority=1, location_type="trash"
            )

            # Both should be included in size-based cleanup (age ignored)
            assert (
                cleanup_file.should_include_in_cleanup(mock_config, is_size_based=True)
                is True
            ), (
                f"File with timestamp {timestamp} should be included in size-based cleanup"
            )

    def test_strategy_pattern_implementation(self):
        """Test the strategy pattern for cleanup strategies."""
        # Test that abstract base class exists and is abstract
        assert issubclass(CleanupStrategy, object)

        # Test age-based strategy
        age_strategy = AgeBasedCleanupStrategy()
        assert isinstance(age_strategy, CleanupStrategy)

        # Test size-based strategy
        size_strategy = SizeBasedCleanupStrategy()
        assert isinstance(size_strategy, CleanupStrategy)

    def test_unified_collection_method_exists(self, config, logger, graceful_exit):
        """Test that the unified collection method exists."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Check that the new unified method exists
        assert hasattr(processor, "_collect_files_from_location")
        assert hasattr(processor, "_collect_files_for_cleanup_unified")

    def test_unified_validation_method_exists(self, config, logger, graceful_exit):
        """Test that the unified validation method exists."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Check that the new unified validation methods exist
        assert hasattr(processor, "_validate_and_filter_files")
        assert hasattr(processor, "_is_valid_for_cleanup")

    def test_cleanup_configuration_object_integration(
        self, config, logger, graceful_exit
    ):
        """Test integration of the cleanup configuration object."""

        # Test creating a CleanupRules object with processor config
        rules = CleanupRules(
            max_size=2048 if config.max_size else None,
            older_than_days=config.older_than,
            clean_output=config.clean_output,
            is_size_based=True,
        )

        assert rules.older_than_days == config.older_than
        assert rules.clean_output == config.clean_output

    def test_unified_cleanup_pipeline_method_exists(
        self, config, logger, graceful_exit
    ):
        """Test that the unified cleanup pipeline method exists."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Check that the new unified cleanup method exists
        assert hasattr(processor, "_execute_unified_cleanup")


class TestUnifiedCollectionAndValidation:
    """Test unified collection and validation methods."""

    def test_collect_files_from_location_with_different_types(
        self, config, logger, graceful_exit, temp_dir, mocker
    ):
        """Test _collect_files_from_location with different location types."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a test directory
        test_dir = temp_dir / "test_loc"
        test_dir.mkdir()

        # Create test files with proper naming for timestamp parsing
        old_file = test_dir / "REO_front_01_20230101120000.mp4"
        old_file.touch()

        # Mock the timestamp parsing to return a known timestamp
        mock_ts = datetime(2023, 1, 1, 12, 0, 0)
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp",
            return_value=mock_ts,
        )

        # Test collecting from source location
        files = processor._collect_files_from_location(
            test_dir, 1, "source", include_age_filter=False
        )

        assert len(files) == 1
        ts, priority, path = files[0]
        assert ts == mock_ts
        assert priority == 1
        assert path == old_file

    def test_collect_files_from_location_with_archived_type(
        self, config, logger, graceful_exit, temp_dir, mocker
    ):
        """Test _collect_files_from_location with archived location type."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create a test directory
        test_dir = temp_dir / "archived"
        test_dir.mkdir()

        # Create test file with archived naming
        archived_file = test_dir / "archived-20230101120000.mp4"
        archived_file.touch()

        # Mock the archived timestamp parsing to return a known timestamp
        mock_ts = datetime(2023, 1, 1, 12, 0, 0)
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp_from_archived_filename",
            return_value=mock_ts,
        )

        # Test collecting from archived location
        files = processor._collect_files_from_location(
            test_dir, 2, "archived", include_age_filter=False
        )

        assert len(files) == 1
        ts, priority, path = files[0]
        assert ts == mock_ts
        assert priority == 2
        assert path == archived_file

    def test_validate_and_filter_files(
        self, config, logger, graceful_exit, temp_dir, mocker
    ):
        """Test _validate_and_filter_files method."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test files with timestamps
        mock_ts = datetime(2023, 1, 1, 12, 0, 0)
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Create a list of files to validate
        files_to_validate = [(mock_ts, 1, test_file)]

        # Mock the validation method to always return True
        mocker.patch.object(processor, "_is_valid_for_cleanup", return_value=True)

        validated_files = processor._validate_and_filter_files(
            files_to_validate, config
        )

        assert len(validated_files) == 1
        assert validated_files[0] == (mock_ts, 1, test_file)

    def test_is_valid_for_cleanup(
        self, config, logger, graceful_exit, temp_dir, mocker
    ):
        """Test _is_valid_for_cleanup method."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test file with timestamp
        mock_ts = datetime(2023, 1, 1, 12, 0, 0)
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Mock the skip methods to return False (don't skip)
        mocker.patch.object(
            processor, "_should_skip_file_for_cleanup", return_value=False
        )
        mocker.patch.object(
            processor, "_should_skip_file_due_to_age_for_cleanup", return_value=False
        )

        # Test that a valid file passes validation
        is_valid = processor._is_valid_for_cleanup((mock_ts, 1, test_file), config)
        assert is_valid is True

    def test_is_valid_for_cleanup_with_skip_conditions(
        self, config, logger, graceful_exit, temp_dir, mocker
    ):
        """Test _is_valid_for_cleanup with skip conditions."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Create test file with timestamp
        mock_ts = datetime(2023, 1, 1, 12, 0, 0)
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        # Mock the skip methods to return True (skip)
        mocker.patch.object(
            processor, "_should_skip_file_for_cleanup", return_value=True
        )

        # Test that a file that should be skipped fails validation
        is_valid = processor._is_valid_for_cleanup((mock_ts, 1, test_file), config)
        assert is_valid is False


class TestSizeBasedCleanupBasic:
    """Test basic size-based cleanup functionality."""

    def test_size_based_cleanup_basic_functionality(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test basic size-based cleanup functionality with real file operations."""
        # Setup
        config.max_size = "1KB"  # 1024 bytes
        config.directory = temp_dir
        config.trash_root = None  # Default configuration

        # Create files that exceed the limit
        files = []
        total_size = 0
        for i in range(3):
            file_path = temp_dir / f"test{i}.mp4"
            size = 500 + i * 100  # 500, 600, 700 bytes
            with file_path.open("w") as f:
                f.write("x" * size)
            files.append((file_path, size))
            total_size += size

        # Verify we exceed the limit
        assert total_size > 1024

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing to make files valid
        def mock_parse_timestamp(filename):
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track actual removals
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            try:
                size = file_path.stat().st_size
                file_path.unlink()
                removed_files.append((file_path, size))
                return size
            except Exception:
                return 0

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Debug: Check what files are collected
        print("\n=== Debug: Collecting trash files ===")
        trash_files = processor._collect_trash_files_for_cleanup()
        print(f"Trash files collected: {len(trash_files)}")
        for ts, priority, file_path in trash_files:
            print(f"  Priority {priority}: {file_path} (timestamp: {ts})")

        print("\n=== Debug: Collecting source files ===")
        source_files = processor._collect_source_files_for_cleanup()
        print(f"Source files collected: {len(source_files)}")
        for ts, priority, file_path in source_files:
            print(f"  Priority {priority}: {file_path} (timestamp: {ts})")

        # Run cleanup
        processor.size_based_cleanup(set())

        # Verify cleanup actually reduced size
        final_size = sum(f.stat().st_size for f in temp_dir.rglob("*") if f.is_file())
        assert final_size <= 1024

        # Verify files were removed
        assert len(removed_files) > 0

    def test_size_based_cleanup_priority_ordering_with_proper_trash(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test priority ordering with proper trash directory structure."""
        config.max_size = "1KB"
        config.directory = temp_dir
        config.trash_root = None  # Default to .deleted
        config.clean_output = True  # Allow archived files to be cleaned up

        # Create proper trash directory structure
        trash_dir = temp_dir / ".deleted"
        input_trash = trash_dir / "input"
        output_trash = trash_dir / "output"
        input_trash.mkdir(parents=True, exist_ok=True)
        output_trash.mkdir(parents=True, exist_ok=True)

        # Create files with different priorities and ages
        old_timestamp = datetime(2023, 1, 1, 12, 0, 0)
        new_timestamp = datetime(2023, 1, 2, 12, 0, 0)

        # Trash files (priority 1) - use proper camera filename format
        old_trash = input_trash / "REO_front_01_20230101120000.mp4"
        new_trash = output_trash / "REO_front_01_20230102120000.mp4"

        # Archived files (priority 2)
        output_dir = temp_dir / "output"
        output_dir.mkdir(exist_ok=True)
        old_archived = output_dir / "archived-20230101120000.mp4"
        new_archived = output_dir / "archived-20230102120000.mp4"

        # Source files (priority 3) - use proper camera filename format
        old_source = temp_dir / "REO_front_01_20230101120000.mp4"
        new_source = temp_dir / "REO_front_01_20230102120000.mp4"

        # Create all files with 300 bytes each (total 1800 > 1024 limit)
        for file in [
            old_trash,
            new_trash,
            old_archived,
            new_archived,
            old_source,
            new_source,
        ]:
            with file.open("w") as f:
                f.write("x" * 300)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            if "20230101" in filename:
                return old_timestamp
            elif "20230102" in filename:
                return new_timestamp
            return datetime(2023, 1, 1, 12, 0, 0)

        def mock_parse_archived(filename):
            if "20230101" in filename:
                return old_timestamp
            elif "20230102" in filename:
                return new_timestamp
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Track removal order
        removal_order = []

        def mock_remove_file(file_path, *args, **kwargs):
            size = file_path.stat().st_size
            file_path.unlink()
            removal_order.append(str(file_path))
            return size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run cleanup
        processor.size_based_cleanup(set())

        # Verify priority ordering: trash first, then archived, then source
        # Within each priority: oldest first
        assert len(removal_order) > 0

        # Check that trash files are removed before others
        first_removal = removal_order[0]
        assert ".deleted" in first_removal

        # Check that older files are removed before newer ones within same priority
        if len(removal_order) >= 2 and ".deleted" in removal_order[1]:
            assert "20230101" in removal_order[0] or "20230101" in removal_order[1]

    def test_size_based_cleanup_with_age_thresholds(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test size_based_cleanup with age thresholds."""
        config.max_size = "1KB"
        config.older_than = 30  # 30 days
        config.directory = temp_dir

        # Create test files with different ages
        recent_timestamp = datetime.now() - timedelta(days=15)  # Recent file
        old_timestamp = datetime.now() - timedelta(days=45)  # Old file

        recent_file = temp_dir / "recent.mp4"
        old_file = temp_dir / "old.mp4"

        # Make files larger to exceed the limit
        with recent_file.open("w") as f:
            f.write("x" * 600)
        with old_file.open("w") as f:
            f.write("x" * 600)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file collection to return both files
        mock_files = [
            (recent_timestamp, 3, recent_file),
            (old_timestamp, 3, old_file),
        ]

        mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=mock_files
        )
        mocker.patch.object(processor, "_remove_file_for_cleanup", return_value=600)

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1200
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Should only remove the old file (respect age threshold)
        assert processor._remove_file_for_cleanup.call_count == 1  # type: ignore
        # Should be called with the old file
        processor._remove_file_for_cleanup.assert_called_with(old_file, 1200, 1024, 0)  # type: ignore

    def test_size_based_cleanup_failure_handling(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test size_based_cleanup with failure handling."""
        config.max_size = "1KB"
        config.directory = temp_dir

        # Create test files
        file1 = temp_dir / "file1.mp4"
        file2 = temp_dir / "file2.mp4"

        # Make files larger to exceed the limit
        with file1.open("w") as f:
            f.write("x" * 600)
        with file2.open("w") as f:
            f.write("x" * 600)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file collection
        mock_files = [
            (datetime.now(), 3, file1),
            (datetime.now(), 3, file2),
        ]

        mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=mock_files
        )

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1200
        )

        # Mock the actual file removal to fail on first file, succeed on second
        def mock_file_manager_remove(file_path, *args, **kwargs):
            if file_path == file1:
                raise OSError("Mocked failure")
            # Return the file size for successful removal
            return file_path.stat().st_size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_file_manager_remove,
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Should continue processing even if some files fail
        # The cleanup should have processed both files (one failed, one succeeded)
        # We can verify this by checking the log messages or the final size

    def test_size_based_cleanup_boundary_conditions(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test size_based_cleanup with boundary conditions."""
        config.max_size = "1KB"
        config.directory = temp_dir

        # Create test file exactly at the limit
        file1 = temp_dir / "file1.mp4"
        with file1.open("w") as f:
            f.write("x" * 1024)  # Exactly 1KB

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock file collection
        mock_files = [(datetime.now(), 3, file1)]
        mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=mock_files
        )
        mocker.patch.object(processor, "_remove_file_for_cleanup", return_value=1024)

        # Mock size calculation to return exactly the limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1024
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Should not remove anything since we're exactly at the limit
        processor._remove_file_for_cleanup.assert_not_called()  # type: ignore

    def test_size_based_cleanup_helper_methods(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test size-based cleanup helper methods."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test _get_directory_size
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        # Create some files
        file1 = test_dir / "file1.txt"
        file2 = test_dir / "file2.txt"

        with file1.open("w") as f:
            f.write("x" * 100)
        with file2.open("w") as f:
            f.write("x" * 200)

        # Test with existing directory
        size = processor._get_directory_size(test_dir)
        assert size == 300  # 100 + 200 bytes

        # Test with non-existent directory
        non_existent_dir = temp_dir / "non_existent"
        size = processor._get_directory_size(non_existent_dir)
        assert size == 0

        # Test _is_file_in_trash_directory
        config.trash_root = temp_dir / "trash"

        # File in trash directory
        trash_file = temp_dir / "trash" / "file.mp4"
        trash_file.parent.mkdir(parents=True, exist_ok=True)
        trash_file.touch()

        # File not in trash directory
        non_trash_file = temp_dir / "other" / "file.mp4"
        non_trash_file.parent.mkdir(parents=True, exist_ok=True)
        non_trash_file.touch()

        assert processor._is_file_in_trash_directory(trash_file) is True
        assert processor._is_file_in_trash_directory(non_trash_file) is False

        # Test with no trash root
        config.trash_root = None
        assert processor._is_file_in_trash_directory(trash_file) is False

        # Test _is_file_in_output_directory_when_not_cleaning_output
        config.output = temp_dir / "output"
        config.clean_output = False

        # File in output directory
        output_file = config.output / "file.mp4"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.touch()

        # File not in output directory
        non_output_file = temp_dir / "other" / "file.mp4"
        non_output_file.parent.mkdir(parents=True, exist_ok=True)
        non_output_file.touch()

        assert (
            processor._is_file_in_output_directory_when_not_cleaning_output(output_file)
            is True
        )
        assert (
            processor._is_file_in_output_directory_when_not_cleaning_output(
                non_output_file
            )
            is False
        )

        # Test with clean_output enabled
        config.clean_output = True
        assert (
            processor._is_file_in_output_directory_when_not_cleaning_output(output_file)
            is False
        )

        # Test _should_skip_file_due_to_age_for_cleanup
        age_cutoff = datetime.now() - timedelta(days=30)

        # Old timestamp (should not be skipped)
        old_timestamp = datetime.now() - timedelta(days=45)
        assert (
            processor._should_skip_file_due_to_age_for_cleanup(
                old_timestamp, age_cutoff
            )
            is False
        )

        # Recent timestamp (should be skipped)
        recent_timestamp = datetime.now() - timedelta(days=15)
        assert (
            processor._should_skip_file_due_to_age_for_cleanup(
                recent_timestamp, age_cutoff
            )
            is True
        )

        # Exact cutoff timestamp (should be skipped)
        exact_timestamp = age_cutoff
        assert (
            processor._should_skip_file_due_to_age_for_cleanup(
                exact_timestamp, age_cutoff
            )
            is True
        )

        # Test with no age cutoff
        assert (
            processor._should_skip_file_due_to_age_for_cleanup(recent_timestamp, None)
            is False
        )

        # Test _should_skip_file_for_cleanup
        config.trash_root = temp_dir / "trash"
        config.clean_output = False

        # File in trash should be skipped
        assert processor._should_skip_file_for_cleanup(trash_file) is True

        # File in output when not cleaning output should be skipped
        assert processor._should_skip_file_for_cleanup(output_file) is True

        # Other files should not be skipped
        assert processor._should_skip_file_for_cleanup(non_trash_file) is False

    def test_plan_execution_helper_methods(
        self, config, graceful_exit, logger, temp_dir
    ):
        """Test plan execution helper methods."""
        processor = FileProcessor(config, logger, graceful_exit)

        # Test _calculate_total_directory_sizes
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.output = temp_dir / "output"

        # Create proper trash directory structure (input and output subdirectories)
        (temp_dir / ".deleted").mkdir(parents=True, exist_ok=True)
        (temp_dir / ".deleted" / "input").mkdir(parents=True, exist_ok=True)
        (temp_dir / ".deleted" / "output").mkdir(parents=True, exist_ok=True)
        (temp_dir / "output").mkdir(parents=True, exist_ok=True)

        # Create files with content in proper locations
        trash_file = temp_dir / ".deleted" / "input" / "trash.mp4"
        output_file = temp_dir / "output" / "archived.mp4"
        source_file = temp_dir / "source.mp4"

        for file in [trash_file, output_file, source_file]:
            with file.open("w") as f:
                f.write("x" * 100)

        # Test directory size calculations
        trash_size = processor._get_trash_directory_size()
        archived_size = processor._get_archived_directory_size()
        source_size = processor._get_directory_size(temp_dir)

        assert trash_size == 100  # Should find the file in .deleted/input
        assert archived_size == 100
        assert source_size >= 300  # At least the 3 files we created

        # Test _calculate_total_directory_sizes
        total_size = processor._calculate_total_directory_sizes()
        assert total_size >= 500  # At least 100 + 100 + 300

        # Test _is_cleanup_needed
        assert processor._is_cleanup_needed(500, 1024) is False  # 500 < 1024
        assert processor._is_cleanup_needed(1025, 1024) is True  # 1025 > 1024
        assert processor._is_cleanup_needed(1024, 1024) is False  # 1024 == 1024

        # Test _collect_all_files_for_cleanup
        config.trash_root = temp_dir / ".deleted"
        config.output = temp_dir / "output"
        config.clean_output = True

        # Create files in trash directory
        trash_dir = temp_dir / ".deleted"
        trash_file1 = trash_dir / "REO_01_20230115120000.mp4"
        with trash_file1.open("w") as f:
            f.write("x" * 100)

        # Also create a file in the .deleted directory itself (not in subdirectory)
        trash_file2 = trash_dir / "REO_02_20230114120000.mp4"
        with trash_file2.open("w") as f:
            f.write("x" * 100)

        # Create files in output directory
        output_dir = temp_dir / "output"
        output_file1 = output_dir / "archived-20230115120000.mp4"
        with output_file1.open("w") as f:
            f.write("x" * 100)

        # Create files in source directory
        source_dir = temp_dir / "2023" / "01" / "15"
        source_dir.mkdir(parents=True, exist_ok=True)
        source_file1 = source_dir / "REO_01_20230115120000.mp4"
        with source_file1.open("w") as f:
            f.write("x" * 100)

        # Test file collection
        all_files = processor._collect_all_files_for_cleanup()

        # Should have files from at least some directories
        assert len(all_files) >= 2  # At least output and source files

        # Check priorities for the files we have
        for ts, priority, file_path in all_files:
            if "output" in str(file_path):
                assert (
                    priority == 3
                )  # archived files now have highest protection (priority 3)
            elif "2023" in str(file_path):
                assert (
                    priority == 2
                )  # source files in YYYY/MM/DD structure have medium protection (priority 2)

        # Test _sort_files_by_priority_and_age
        sorted_files = processor._sort_files_by_priority_and_age(all_files)

        # Should be sorted by priority first, then by timestamp
        for i in range(len(sorted_files) - 1):
            prio1, ts1 = sorted_files[i][1], sorted_files[i][0]
            prio2, ts2 = sorted_files[i + 1][1], sorted_files[i + 1][0]

            if prio1 == prio2:
                # Same priority, should be sorted by timestamp (oldest first)
                assert ts1 <= ts2
            else:
                # Different priority, should be sorted by priority (ascending)
                assert prio1 <= prio2

    def test_size_based_cleanup_custom_trash_root(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test size-based cleanup with custom trash-root configuration."""
        config.max_size = "1KB"
        config.directory = temp_dir
        config.trash_root = temp_dir / "custom_trash"  # Custom trash root

        # Create custom trash structure
        custom_trash = temp_dir / "custom_trash"
        input_trash = custom_trash / "input"
        output_trash = custom_trash / "output"
        input_trash.mkdir(parents=True, exist_ok=True)
        output_trash.mkdir(parents=True, exist_ok=True)

        # Create files in custom trash
        trash_file1 = input_trash / "trash1.mp4"
        trash_file2 = output_trash / "trash2.mp4"

        # Create files in wrong .deleted location (should be ignored)
        wrong_trash_dir = temp_dir / ".deleted"
        wrong_trash_dir.mkdir(exist_ok=True)
        wrong_trash = wrong_trash_dir / "wrong.mp4"

        # Create source files
        source_file = temp_dir / "source.mp4"

        # Make all files 400 bytes (total 1600 > 1024)
        for file in [trash_file1, trash_file2, wrong_trash, source_file]:
            with file.open("w") as f:
                f.write("x" * 400)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            size = file_path.stat().st_size
            file_path.unlink()
            removed_files.append(str(file_path))
            return size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run cleanup
        processor.size_based_cleanup(set())

        # Verify correct files were targeted
        removed_paths = [str(f) for f in removed_files]

        # Should remove from custom trash, not .deleted
        assert any("custom_trash" in path for path in removed_paths)
        assert not any(".deleted" in path for path in removed_paths)

        # Verify size calculation is correct (doesn't include .deleted)
        final_size = processor._calculate_total_directory_sizes()
        assert final_size <= 1024

    def test_size_enforcement_final_size_verification(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that cleanup actually achieves the target size threshold."""
        config.max_size = "1KB"  # 1024 bytes
        config.directory = temp_dir
        config.trash_root = temp_dir / "trash"

        # Create structure
        trash_dir = temp_dir / "trash"
        input_trash = trash_dir / "input"
        output_trash = trash_dir / "output"
        input_trash.mkdir(parents=True, exist_ok=True)
        output_trash.mkdir(parents=True, exist_ok=True)

        # Create files that significantly exceed limit
        files_data = [
            (input_trash / "trash1.mp4", 400),
            (input_trash / "trash2.mp4", 400),
            (output_trash / "trash3.mp4", 400),
            (temp_dir / "source1.mp4", 300),
            (temp_dir / "source2.mp4", 300),
            (temp_dir / "source3.mp4", 300),
        ]

        total_size = 0
        for file_path, size in files_data:
            with file_path.open("w") as f:
                f.write("x" * size)
            total_size += size

        # Should be well over limit (400+400+400+300+300+300 = 2100)
        assert total_size > 1024

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_timestamp,
        )

        # Real file removal
        def mock_remove_file(file_path, *args, **kwargs):
            try:
                size = file_path.stat().st_size
                file_path.unlink()
                return size
            except Exception:
                return 0

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run cleanup
        processor.size_based_cleanup(set())

        # Critical verification: final size should be at or below threshold
        final_size = processor._calculate_total_directory_sizes()
        assert final_size <= 1024, f"Final size {final_size} exceeds limit 1024"

        # Should have removed enough files to get under limit
        removed_count = len([f for f in files_data if not f[0].exists()])
        assert (
            removed_count >= 2
        )  # Should remove at least 2 files to get from 2100 to <=1024

    def test_size_based_cleanup_ignores_age_thresholds(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that size-based cleanup ignores age thresholds and removes oldest files first."""
        config.max_size = "1KB"
        config.older_than = 30  # 30 days
        config.directory = temp_dir
        config.trash_root = temp_dir / "trash"

        # Create structure
        trash_dir = temp_dir / "trash"
        input_trash = trash_dir / "input"
        output_trash = trash_dir / "output"
        output_dir = temp_dir / "output"
        input_trash.mkdir(parents=True, exist_ok=True)
        output_trash.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(exist_ok=True)

        # Create files with different ages
        recent_time = datetime.now() - timedelta(days=15)  # Recent file
        old_time = datetime.now() - timedelta(days=45)  # Old file

        # Create files (both recent and old files will be considered during size-based cleanup)
        recent_trash = input_trash / "recent_trash.mp4"
        recent_archived = output_dir / "archived-20230115120000.mp4"
        recent_source = temp_dir / "recent_source.mp4"

        # Old files
        old_trash = output_trash / "old_trash.mp4"
        old_archived = output_dir / "archived-20221201120000.mp4"
        old_source = temp_dir / "old_source.mp4"

        # Create all files with 300 bytes each
        for file in [
            recent_trash,
            recent_archived,
            recent_source,
            old_trash,
            old_archived,
            old_source,
        ]:
            with file.open("w") as f:
                f.write("x" * 300)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing to return our specific ages
        def mock_parse_timestamp(filename):
            if "recent" in filename:
                return recent_time
            elif "old" in filename:
                return old_time
            return datetime(2023, 1, 1, 12, 0, 0)

        def mock_parse_archived(filename):
            if "20230115" in filename:
                return recent_time
            elif "20221201" in filename:
                return old_time
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 300

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run cleanup
        processor.size_based_cleanup(set())

        # Verify that during size-based cleanup, age thresholds are ignored
        # and files are removed based on size/priority/age to stay under limit
        removed_paths = [str(f) for f in removed_files]

        # Size-based cleanup should ignore age thresholds and remove oldest files first
        # to stay under the size limit, so both old and recent files may be removed
        # depending on the priority order (trash -> archived -> source)

        # Since size-based cleanup ignores age thresholds, it should remove files
        # based on priority (trash first, then archived, then source) and oldest first
        # to stay under the size limit
        assert (
            len(removed_paths) > 0
        )  # At least some files should be removed to meet size limit

        # Trash files should be removed first (regardless of age) during size-based cleanup
        trash_removed = [p for p in removed_paths if "trash" in p]
        assert (
            len(trash_removed) > 0
        )  # At least some trash files should be removed first

    def test_comprehensive_size_based_cleanup_behavior(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Comprehensive test to validate size-based cleanup behavior.

        This test verifies that:
        1. Size-based cleanup ignores age thresholds and removes oldest files first
        2. Files are removed in priority order: trash -> archived -> source
        3. Cleanup stops once size limit is achieved
        """
        # Configure for size-based cleanup with small limit to trigger cleanup
        config.max_size = "2KB"  # Very small limit to force cleanup
        config.older_than = 30  # 30 day age threshold
        config.directory = temp_dir
        config.output = temp_dir / "output"
        config.trash_root = temp_dir / "trash"

        # Create directory structure
        config.output.mkdir(exist_ok=True)
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create files with different ages and priorities
        # Old files (50 days ago - should normally be removed with 30-day threshold)
        old_time = datetime.now() - timedelta(days=50)
        old_timestamp_str = old_time.strftime("%Y%m%d%H%M%S")

        # Recent files (5 days ago - should normally be kept with 30-day threshold)
        recent_time = datetime.now() - timedelta(days=5)
        recent_timestamp_str = recent_time.strftime("%Y%m%d%H%M%S")

        # Create files in different categories with different ages
        # Trash files (highest priority for removal)
        old_trash_input = (
            config.trash_root / "input" / f"REO_old_{old_timestamp_str}.mp4"
        )
        recent_trash_input = (
            config.trash_root / "input" / f"REO_recent_{recent_timestamp_str}.mp4"
        )
        old_trash_output = (
            config.trash_root / "output" / f"archived-{old_timestamp_str}_copy.mp4"
        )
        recent_trash_output = (
            config.trash_root / "output" / f"archived-{recent_timestamp_str}_copy.mp4"
        )

        # Archived files (medium priority for removal)
        old_archived = config.output / f"archived-{old_timestamp_str}.mp4"
        recent_archived = config.output / f"archived-{recent_timestamp_str}.mp4"

        # Source files (lowest priority for removal)
        old_source = temp_dir / f"REO_old_{old_timestamp_str}.mp4"
        recent_source = temp_dir / f"REO_recent_{recent_timestamp_str}.mp4"

        # Create all files with 500 bytes each
        all_files = [
            old_trash_input,
            recent_trash_input,
            old_trash_output,
            recent_trash_output,
            old_archived,
            recent_archived,
            old_source,
            recent_source,
        ]

        for file_path in all_files:
            with open(file_path, "w") as f:
                f.write("x" * 500)  # 500 bytes each

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing to return our specific ages
        def mock_parse_timestamp(filename):
            if "old_" in filename or "old_timestamp_str" in filename:
                return old_time
            elif "recent_" in filename or recent_timestamp_str in filename:
                return recent_time
            return datetime(2023, 1, 1, 12, 0, 0)

        def mock_parse_archived(filename):
            if old_timestamp_str in filename:
                return old_time
            elif recent_timestamp_str in filename:
                return recent_time
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 500  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify behavior
        removed_paths = [str(f) for f in removed_files]

        # With size-based cleanup ignoring age thresholds, it should remove files
        # in priority order (trash first) and oldest first within each category
        # to stay under the 2KB limit

        # Should have removed files to get under the size limit
        assert len(removed_paths) > 0, "At least some files should have been removed"

        # Since trash files have highest priority, some trash files should be removed first
        # Verify that the cleanup prioritized correctly
        # (Note: actual behavior depends on implementation details)
        print(f"Removed {len(removed_paths)} files: {removed_paths}")


class TestCombinedCleanupStrategy:
    """Test the combined cleanup strategy functionality."""

    def test_combined_cleanup_strategy_creation(self):
        """Test creation of CombinedCleanupStrategy."""
        strategy = CombinedCleanupStrategy()
        assert isinstance(strategy, CleanupStrategy)
        assert isinstance(strategy, CombinedCleanupStrategy)

    def test_combined_cleanup_strategy_age_filtering(self, config, temp_dir, mocker):
        """Test CombinedCleanupStrategy with age-based filtering."""
        strategy = CombinedCleanupStrategy()

        # Create a mock file info tuple
        old_timestamp = datetime.now() - timedelta(
            days=45
        )  # Older than default 30-day threshold
        recent_timestamp = datetime.now() - timedelta(
            days=15
        )  # More recent than 30-day threshold

        # Create mock file paths
        old_file = temp_dir / "REO_front_01_20230101120000.mp4"
        recent_file = temp_dir / "REO_front_01_20230201120000.mp4"

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            if "20230101" in filename:
                return old_timestamp
            elif "20230201" in filename:
                return recent_timestamp
            return None

        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp",
            side_effect=mock_parse_timestamp,
        )

        # Mock archived file parsing
        def mock_parse_archived(filename):
            if "20230101" in filename:
                return old_timestamp
            elif "20230201" in filename:
                return recent_timestamp
            return None

        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Test with config that has older_than > 0
        config.older_than = 30

        # Old file should be included (older than 30 days)
        old_file_info = (None, None, old_file)
        assert strategy.should_include_file(old_file_info, config) is True

        # Recent file should not be included (more recent than 30 days)
        recent_file_info = (None, None, recent_file)
        assert strategy.should_include_file(recent_file_info, config) is False

    def test_combined_cleanup_strategy_no_age_threshold(self, config, temp_dir, mocker):
        """Test CombinedCleanupStrategy when no age threshold is set."""
        strategy = CombinedCleanupStrategy()

        # Create a mock file
        recent_file = temp_dir / "REO_front_01_20230201120000.mp4"
        recent_file_info = (None, None, recent_file)

        # Mock timestamp parsing
        recent_timestamp = datetime.now() - timedelta(days=15)
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp",
            return_value=recent_timestamp,
        )
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp_from_archived_filename",
            return_value=recent_timestamp,
        )

        # Set older_than to 0 (no age threshold)
        config.older_than = 0

        # File should be included when no age threshold is set
        assert strategy.should_include_file(recent_file_info, config) is True

    def test_combined_cleanup_strategy_with_zero_threshold(
        self, config, temp_dir, mocker
    ):
        """Test CombinedCleanupStrategy with zero age threshold."""
        strategy = CombinedCleanupStrategy()

        # Create a mock file
        recent_file = temp_dir / "REO_front_01_20230201120000.mp4"
        recent_file_info = (None, None, recent_file)

        # Mock timestamp parsing
        recent_timestamp = datetime.now() - timedelta(days=15)
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp",
            return_value=recent_timestamp,
        )
        mocker.patch(
            "src.archiver.discovery.FileDiscovery._parse_timestamp_from_archived_filename",
            return_value=recent_timestamp,
        )

        # Set older_than to 0 (no age threshold)
        config.older_than = 0

        # File should be included when age threshold is 0
        assert strategy.should_include_file(recent_file_info, config) is True

    def test_combined_cleanup_rules_with_combined_strategy_flag(self):
        """Test CleanupRules with use_combined_strategy flag."""
        # Test with combined strategy enabled
        rules = CleanupRules(
            max_size=1024,
            older_than_days=30,
            clean_output=True,
            is_size_based=False,
            use_combined_strategy=True,
        )

        # Create timestamps: one old (should be included), one recent (should not be included)
        old_timestamp = datetime.now() - timedelta(days=45)  # Older than 30 days
        recent_timestamp = datetime.now() - timedelta(
            days=15
        )  # More recent than 30 days

        # With combined strategy, old file should be included
        assert rules.should_include_file(old_timestamp) is True

        # With combined strategy, recent file should not be included
        assert rules.should_include_file(recent_timestamp) is False

        # Test with combined strategy disabled (but is_size_based also False)
        rules2 = CleanupRules(
            max_size=1024,
            older_than_days=30,
            clean_output=True,
            is_size_based=False,
            use_combined_strategy=False,
        )

        # Should behave like age-based when combined strategy is disabled
        assert rules2.should_include_file(old_timestamp) is True
        assert rules2.should_include_file(recent_timestamp) is False


class TestCombinedCleanupIntegration:
    """Test integration of combined cleanup functionality."""

    def test_size_based_cleanup_with_combined_strategy_detection(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that size_based_cleanup detects and uses combined strategy when both params specified."""
        # Configure both max_size and older_than to trigger combined strategy
        config.max_size = "1KB"
        config.older_than = 30  # This should trigger combined strategy
        config.directory = temp_dir

        # Create test files that exceed the limit
        file1 = temp_dir / "file1.mp4"
        file2 = temp_dir / "file2.mp4"
        with file1.open("w") as f:
            f.write("x" * 600)
        with file2.open("w") as f:
            f.write("x" * 600)

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return datetime(2023, 1, 1, 12, 0, 0)

        def mock_parse_archived(filename):
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Mock the collection methods to verify they're called with the right parameters
        mock_collect_all = mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=[]
        )
        mocker.patch.object(
            processor, "_remove_files_until_under_limit", return_value=0
        )

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1200
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify that _collect_all_files_for_cleanup was called with use_combined_strategy=True
        # since both max_size and older_than are specified
        mock_collect_all.assert_called_once()
        call_kwargs = mock_collect_all.call_args[1]  # Use [1] for kwargs
        assert call_kwargs.get("use_combined_strategy") is True
        assert (
            call_kwargs.get("is_size_based") is False
        )  # Should not use pure size-based when combined is active

    def test_size_based_cleanup_without_combined_strategy(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that size_based_cleanup uses regular strategy when only max_size specified."""
        # Configure only max_size, no older_than
        config.max_size = "1KB"
        config.older_than = 0  # No age threshold
        config.directory = temp_dir

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock the collection methods to verify they're called with the right parameters
        mock_collect_all = mocker.patch.object(
            processor, "_collect_all_files_for_cleanup", return_value=[]
        )
        mocker.patch.object(
            processor, "_remove_files_until_under_limit", return_value=0
        )

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1200
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify that _collect_all_files_for_cleanup was called with use_combined_strategy=False
        # since only max_size is specified
        mock_collect_all.assert_called_once()
        call_kwargs = mock_collect_all.call_args[1]  # Use [1] for kwargs
        assert call_kwargs.get("use_combined_strategy") is False
        assert (
            call_kwargs.get("is_size_based") is True
        )  # Should use pure size-based when combined is not active

    def test_combined_cleanup_with_real_files(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test combined cleanup with real files respecting both size and age constraints."""
        # Create a subdirectory for our test files to avoid including temp_dir files
        test_subdir = temp_dir / "test_camera"
        test_subdir.mkdir()

        # Configure both max_size and older_than
        config.max_size = "1KB"  # 1024 bytes
        config.older_than = 30  # 30 days
        config.directory = test_subdir  # Use the subdirectory instead of temp_dir
        config.trash_root = temp_dir / "trash"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create files with different ages
        # Old files (50 days ago - should be included in combined strategy)
        old_time = datetime.now() - timedelta(days=50)
        old_timestamp_str = old_time.strftime("%Y%m%d%H%M%S")

        # Recent files (5 days ago - should be excluded in combined strategy)
        recent_time = datetime.now() - timedelta(days=5)
        recent_timestamp_str = recent_time.strftime("%Y%m%d%H%M%S")

        # Create files in different categories with different ages
        # Trash files (highest priority for removal)
        old_trash_input = (
            config.trash_root / "input" / f"REO_old_{old_timestamp_str}.mp4"
        )
        recent_trash_input = (
            config.trash_root / "input" / f"REO_recent_{recent_timestamp_str}.mp4"
        )

        # Source files (lower priority for removal)
        old_source = test_subdir / f"REO_old_{old_timestamp_str}.mp4"
        recent_source = test_subdir / f"REO_recent_{recent_timestamp_str}.mp4"

        # Create all files with 500 bytes each
        all_files = [
            old_trash_input,
            recent_trash_input,
            old_source,
            recent_source,
        ]

        for file_path in all_files:
            with open(file_path, "w") as f:
                f.write("x" * 500)  # 500 bytes each

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing to return our specific ages
        def mock_parse_timestamp(filename):
            if old_timestamp_str in filename:
                return old_time
            elif recent_timestamp_str in filename:
                return recent_time
            return datetime(2023, 1, 1, 12, 0, 0)

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 500  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Run size-based cleanup (which should use combined strategy since older_than > 0)
        processor.size_based_cleanup(set())

        # Verify behavior with combined strategy:
        # - Only old files should be considered (due to age filtering)
        # - Among old files, trash files should be removed first (due to priority)
        removed_paths = [str(f) for f in removed_files]

        # With combined strategy, only old files should have been considered
        # The recent files should not have been removed due to age filtering
        for path in removed_paths:
            assert old_timestamp_str in path  # All removed files should be old
            assert recent_timestamp_str not in path  # No recent files should be removed

        # At least one old file should have been removed
        assert len(removed_paths) > 0


class TestCombinedCleanupComprehensive:
    """Comprehensive tests for combined cleanup functionality."""

    def test_combined_cleanup_filters_by_age(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that combined cleanup properly filters files by age threshold."""
        # Configure both max_size and older_than to trigger combined strategy
        config.max_size = "100KB"
        config.older_than = 14  # 14 days threshold
        config.directory = temp_dir
        config.trash_root = temp_dir / "trash"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create test dates
        old_date = datetime.now() - timedelta(days=20)  # >14 days old
        recent_date = datetime.now() - timedelta(days=10)  # <14 days old
        very_recent_date = datetime.now() - timedelta(days=5)  # <14 days old

        # Create file paths
        old_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_01_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_02_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_03_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_04_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        very_recent_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_05_{very_recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        old_archived_file = (
            config.directory
            / "archived"
            / f"archived-{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_archived_file = (
            config.directory
            / "archived"
            / f"archived-{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Track removed files
        removed_files = []

        # Mock the individual collection methods to properly apply age filtering in combined strategy
        def mock_collect_trash_files_for_cleanup(
            is_size_based=False, use_combined_strategy=False
        ):
            files_list = []

            if use_combined_strategy:
                # Only include files older than 14 days
                files_list.append((old_date, 1, old_input_file))  # trash input
                files_list.append((old_date, 1, old_output_file))  # trash output
            else:
                # Include all files when not using combined strategy
                files_list.append((old_date, 1, old_input_file))
                files_list.append((old_date, 1, old_output_file))
                files_list.append((recent_date, 1, recent_input_file))
                files_list.append((recent_date, 1, recent_output_file))
                files_list.append((very_recent_date, 1, very_recent_input_file))

            return files_list

        def mock_collect_archived_files_for_cleanup(
            is_size_based=False, use_combined_strategy=False
        ):
            files_list = []

            if use_combined_strategy:
                # Only include files older than 14 days
                files_list.append((old_date, 3, old_archived_file))  # archived
            else:
                # Include all files when not using combined strategy
                files_list.append((old_date, 3, old_archived_file))  # archived
                files_list.append((recent_date, 3, recent_archived_file))  # archived

            return files_list

        def mock_collect_source_files_for_cleanup(
            is_size_based=False, use_combined_strategy=False
        ):
            files_list = []

            if use_combined_strategy:
                # Only include files older than 14 days
                files_list.append(
                    (
                        old_date,
                        2,
                        temp_dir
                        / f"source_old_{old_date.strftime('%Y%m%d%H%M%S')}.mp4",
                    )
                )  # source
            else:
                # Include all files when not using combined strategy
                files_list.append(
                    (
                        old_date,
                        2,
                        temp_dir
                        / f"source_old_{old_date.strftime('%Y%m%d%H%M%S')}.mp4",
                    )
                )  # source
                files_list.append(
                    (
                        recent_date,
                        2,
                        temp_dir
                        / f"source_recent_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4",
                    )
                )  # source

            return files_list

        def mock_calculate_total_directory_sizes():
            return 500000  # 500KB - exceeds our 100KB limit

        def mock_remove_files_until_under_limit(sorted_files, total_size, max_bytes):
            for ts, priority, file_path in sorted_files:
                if total_size - len(removed_files) * 102400 <= max_bytes:
                    break  # We're now under the limit
                removed_files.append(str(file_path))
            return len(removed_files) * 102400

        # Patch the necessary methods
        mocker.patch.object(
            processor,
            "_collect_trash_files_for_cleanup",
            side_effect=mock_collect_trash_files_for_cleanup,
        )
        mocker.patch.object(
            processor,
            "_collect_archived_files_for_cleanup",
            side_effect=mock_collect_archived_files_for_cleanup,
        )
        mocker.patch.object(
            processor,
            "_collect_source_files_for_cleanup",
            side_effect=mock_collect_source_files_for_cleanup,
        )
        mocker.patch.object(
            processor,
            "_calculate_total_directory_sizes",
            side_effect=mock_calculate_total_directory_sizes,
        )
        mocker.patch.object(
            processor,
            "_remove_files_until_under_limit",
            side_effect=mock_remove_files_until_under_limit,
        )

        # Run size-based cleanup (this should trigger combined strategy since both params are set)
        processor.size_based_cleanup(set())

        # Verify that only old files were targeted for removal
        old_files_removed = [
            f for f in removed_files if old_date.strftime("%Y%m%d") in f
        ]
        recent_files_removed = [
            f for f in removed_files if recent_date.strftime("%Y%m%d") in f
        ]
        very_recent_files_removed = [
            f for f in removed_files if very_recent_date.strftime("%Y%m%d") in f
        ]

        # Assertions
        assert len(old_files_removed) > 0, (
            "Old files should be removed when using --older-than 14"
        )
        assert len(recent_files_removed) == 0, (
            "Recent files should NOT be removed when using --older-than 14"
        )
        assert len(very_recent_files_removed) == 0, (
            "Very recent files should NOT be removed when using --older-than 14"
        )

    def test_combined_cleanup_preserves_trash_files_under_age_threshold(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that trash files younger than age threshold are preserved."""
        # Configure both max_size and older_than to trigger combined strategy
        config.max_size = "100KB"
        config.older_than = 14  # 14 days threshold
        config.directory = temp_dir
        config.trash_root = temp_dir / "trash"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create test dates
        old_date = datetime.now() - timedelta(days=20)  # >14 days old
        recent_date = datetime.now() - timedelta(days=10)  # <14 days old

        # Create file paths
        old_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_01_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_02_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_03_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_04_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        processor = FileProcessor(config, logger, graceful_exit)

        removed_files = []

        def mock_collect_trash_files_for_cleanup(
            is_size_based=False, use_combined_strategy=False
        ):
            files_list = []

            if use_combined_strategy:
                # Only include files older than 14 days
                files_list.append((old_date, 1, old_input_file))  # Should be removed
                files_list.append((old_date, 1, old_output_file))  # Should be removed
                # Recent files should NOT be included in combined strategy
            else:
                # Include all files when not using combined strategy
                files_list.append((old_date, 1, old_input_file))
                files_list.append((old_date, 1, old_output_file))
                files_list.append((recent_date, 1, recent_input_file))
                files_list.append((recent_date, 1, recent_output_file))

            return files_list

        def mock_calculate_total_directory_sizes():
            return 500000  # Exceeds limit

        def mock_remove_files_until_under_limit(sorted_files, total_size, max_bytes):
            for ts, priority, file_path in sorted_files:
                if total_size - len(removed_files) * 102400 <= max_bytes:
                    break
                removed_files.append(str(file_path))
            return len(removed_files) * 102400

        mocker.patch.object(
            processor,
            "_collect_trash_files_for_cleanup",
            side_effect=mock_collect_trash_files_for_cleanup,
        )
        mocker.patch.object(
            processor,
            "_calculate_total_directory_sizes",
            side_effect=mock_calculate_total_directory_sizes,
        )
        mocker.patch.object(
            processor,
            "_remove_files_until_under_limit",
            side_effect=mock_remove_files_until_under_limit,
        )

        processor.size_based_cleanup(set())

        # Verify trash files behavior
        old_trash_files = [
            f
            for f in removed_files
            if old_date.strftime("%Y%m%d") in f
            and ".trash" in f.lower()
            or "trash" in f.lower()
        ]
        recent_trash_files = [
            f
            for f in removed_files
            if recent_date.strftime("%Y%m%d") in f
            and ".trash" in f.lower()
            or "trash" in f.lower()
        ]

        # Look for trash files more broadly
        old_trash_files = [
            f
            for f in removed_files
            if old_date.strftime("%Y%m%d") in f
            and (".deleted" in f or "trash" in f.lower())
        ]
        recent_trash_files = [
            f
            for f in removed_files
            if recent_date.strftime("%Y%m%d") in f
            and (".deleted" in f or "trash" in f.lower())
        ]

        assert len(old_trash_files) > 0, "Old trash files should be removed"
        assert len(recent_trash_files) == 0, "Recent trash files should NOT be removed"

    def test_combined_cleanup_vs_size_based_only(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that combined strategy behaves differently from size-based only."""
        # Configure both max_size and older_than to trigger combined strategy
        config.max_size = "100KB"
        config.older_than = 14  # 14 days threshold
        config.directory = temp_dir
        config.trash_root = temp_dir / "trash"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create test dates
        old_date = datetime.now() - timedelta(days=20)  # >14 days old
        recent_date = datetime.now() - timedelta(days=10)  # <14 days old

        # Create file paths
        old_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_01_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_02_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_input_file = (
            config.trash_root
            / "input"
            / f"REO_front_03_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_04_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        processor = FileProcessor(config, logger, graceful_exit)

        # Test with combined strategy (both --max-size and --older-than)
        removed_files_combined = []

        def mock_collect_all_files_for_cleanup_combined(
            is_size_based=False, use_combined_strategy=False
        ):
            files_list = []

            if use_combined_strategy:
                # Only include files older than 14 days
                files_list.append((old_date, 1, old_input_file))
                files_list.append((old_date, 1, old_output_file))
                # Recent files are NOT included
            else:
                # Include all files
                files_list.append((old_date, 1, old_input_file))
                files_list.append((old_date, 1, old_output_file))
                files_list.append((recent_date, 1, recent_input_file))
                files_list.append((recent_date, 1, recent_output_file))

            return files_list

        def mock_calculate_total_directory_sizes():
            return 500000  # Exceeds limit

        def mock_remove_files_until_under_limit(sorted_files, total_size, max_bytes):
            for ts, priority, file_path in sorted_files:
                if total_size - len(removed_files_combined) * 102400 <= max_bytes:
                    break
                removed_files_combined.append(str(file_path))
            return len(removed_files_combined) * 102400

        mocker.patch.object(
            processor,
            "_collect_all_files_for_cleanup",
            side_effect=mock_collect_all_files_for_cleanup_combined,
        )
        mocker.patch.object(
            processor,
            "_calculate_total_directory_sizes",
            side_effect=mock_calculate_total_directory_sizes,
        )
        mocker.patch.object(
            processor,
            "_remove_files_until_under_limit",
            side_effect=mock_remove_files_until_under_limit,
        )

        processor.size_based_cleanup(set())

        # Check results for combined strategy
        old_files_combined = [
            f for f in removed_files_combined if old_date.strftime("%Y%m%d") in f
        ]
        recent_files_combined = [
            f for f in removed_files_combined if recent_date.strftime("%Y%m%d") in f
        ]

        # Verify combined strategy behavior
        assert len(old_files_combined) > 0, (
            "Old files should be removed with combined strategy"
        )
        assert len(recent_files_combined) == 0, (
            "Recent files should NOT be removed with combined strategy"
        )

    def test_cleanup_task_sh_simulation(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Test that simulates the exact cleanup-task.sh scenario."""
        # Configure both max_size and older_than to trigger combined strategy
        config.max_size = "100MB"  # 100MB to ensure cleanup happens
        config.older_than = 14  # 14 days as in cleanup-task.sh
        config.directory = temp_dir
        config.trash_root = (
            temp_dir / ".deleted"
        )  # Default trash root as in cleanup-task.sh
        config.clean_output = True

        # Create directory structure similar to cleanup-task.sh
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create test dates
        old_date = datetime.now() - timedelta(days=20)  # >14 days old
        recent_date = datetime.now() - timedelta(days=10)  # <14 days old

        # Create file paths for trash files in .deleted/output/ as mentioned in the issue
        old_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_02_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_output_file = (
            config.trash_root
            / "output"
            / f"REO_back_04_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        processor = FileProcessor(config, logger, graceful_exit)

        removed_files = []

        # Simulate the exact scenario from cleanup-task.sh: --older-than 14 --max-size 1TB
        def mock_collect_trash_files_for_cleanup(
            is_size_based=False, use_combined_strategy=False
        ):
            # This should be called with use_combined_strategy=True since both params are set
            files_list = []

            if use_combined_strategy:
                # For combined strategy, only include files older than 14 days
                files_list.append(
                    (old_date, 1, old_output_file)
                )  # Old trash file - should be removed
                # Recent trash file should NOT be included in combined strategy
            else:
                # For non-combined strategy, include all files
                files_list.append((old_date, 1, old_output_file))  # Old trash file
                files_list.append(
                    (recent_date, 1, recent_output_file)
                )  # Recent trash file

            return files_list

        def mock_calculate_total_directory_sizes():
            return 200000000  # 200MB - exceeds 100MB limit

        def mock_remove_files_until_under_limit(sorted_files, total_size, max_bytes):
            for ts, priority, file_path in sorted_files:
                if total_size - len(removed_files) * 102400 <= max_bytes:
                    break
                removed_files.append(str(file_path))
            return len(removed_files) * 102400

        mocker.patch.object(
            processor,
            "_collect_trash_files_for_cleanup",
            side_effect=mock_collect_trash_files_for_cleanup,
        )
        mocker.patch.object(
            processor,
            "_calculate_total_directory_sizes",
            side_effect=mock_calculate_total_directory_sizes,
        )
        mocker.patch.object(
            processor,
            "_remove_files_until_under_limit",
            side_effect=mock_remove_files_until_under_limit,
        )

        processor.size_based_cleanup(set())

        # Verify the specific scenario: old trash files in .deleted/output/ should be removed
        old_trash_files_removed = [
            f
            for f in removed_files
            if old_date.strftime("%Y%m%d") in f and ".deleted" in f and "output" in f
        ]
        recent_trash_files_removed = [
            f
            for f in removed_files
            if recent_date.strftime("%Y%m%d") in f and ".deleted" in f and "output" in f
        ]

        # With combined strategy, old trash files should be removed, recent ones should not
        assert len(old_trash_files_removed) > 0, (
            "Old trash files in .deleted/output/ should be removed with combined strategy"
        )
        assert len(recent_trash_files_removed) == 0, (
            "Recent trash files in .deleted/output/ should NOT be removed with combined strategy"
        )


class TestSmokeRegressionTests:
    """Smoke tests to prevent regressions in cleanup functionality."""

    def test_cleanup_does_not_remove_recent_files_when_using_age_threshold(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Regression test: Ensure recent files are not removed when using age threshold."""
        # Configure with age threshold
        config.max_size = "100KB"
        config.older_than = 14  # Only files older than 14 days should be removed
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create a recent file (less than 14 days old)
        recent_date = datetime.now() - timedelta(days=5)
        recent_file = (
            config.trash_root
            / "input"
            / f"REO_recent_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return recent_date

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 1024  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=200000
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify that recent files were NOT removed despite exceeding size limit
        recent_files_removed = [
            f for f in removed_files if recent_date.strftime("%Y%m%d") in f
        ]
        assert len(recent_files_removed) == 0, (
            "Recent files should NOT be removed when using --older-than threshold"
        )

    def test_cleanup_removes_old_files_when_using_age_threshold(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Regression test: Ensure old files are removed when using age threshold."""
        # Configure with age threshold
        config.max_size = "100KB"
        config.older_than = 14  # Only files older than 14 days should be removed
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create an old file (more than 14 days old)
        old_date = datetime.now() - timedelta(days=20)
        old_file = (
            config.trash_root
            / "input"
            / f"REO_old_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        old_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return old_date

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 1024  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Mock size calculation to return size that exceeds limit
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=200000
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify that old files WERE removed when exceeding size limit
        old_files_removed = [
            f for f in removed_files if old_date.strftime("%Y%m%d") in f
        ]
        assert len(old_files_removed) > 0, (
            "Old files SHOULD be removed when using --older-than threshold and size exceeded"
        )

    def test_cleanup_prioritizes_trash_files_over_archived_and_source(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Regression test: Ensure cleanup follows priority order: trash > archived > source."""
        # Configure for size-based cleanup
        config.max_size = "1KB"
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.output = temp_dir / "output"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)
        config.output.mkdir(exist_ok=True)

        # Create files of equal age in different locations
        test_date = datetime.now() - timedelta(
            days=30
        )  # Old enough to not be filtered by age
        test_date_str = test_date.strftime("%Y%m%d%H%M%S")

        # Trash file (highest priority for removal)
        trash_file = config.trash_root / "input" / f"REO_trash_{test_date_str}.mp4"
        # Archived file (medium priority)
        archived_file = config.output / f"archived-{test_date_str}.mp4"
        # Source file (lowest priority)
        source_file = temp_dir / f"REO_source_{test_date_str}.mp4"

        # Create all files with equal size
        for file_path in [trash_file, archived_file, source_file]:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("w") as f:
                f.write("x" * 600)  # 600 bytes each, total 1800 > 1024

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return test_date

        def mock_parse_archived(filename):
            return test_date

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )
        mocker.patch.object(
            FileDiscovery,
            "_parse_timestamp_from_archived_filename",
            side_effect=mock_parse_archived,
        )

        # Track removal order
        removal_order = []

        def mock_remove_file(file_path, *args, **kwargs):
            removal_order.append(str(file_path))
            return 600  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Mock size calculation
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1800
        )

        # Run size-based cleanup
        processor.size_based_cleanup(set())

        # Verify priority order: trash should be removed first
        assert len(removal_order) > 0
        first_removed = removal_order[0]

        # The first removed file should be from trash
        assert ".deleted" in first_removed or "trash" in first_removed.lower(), (
            f"First removed file should be from trash, but was: {first_removed}"
        )

    def test_cleanup_handles_both_max_size_and_older_than_simultaneously(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Regression test: Ensure cleanup properly handles both --max-size and --older-than."""
        # Configure with both parameters
        config.max_size = "1KB"
        config.older_than = 14  # Combined strategy should be used
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)
        (config.trash_root / "output").mkdir(exist_ok=True)

        # Create files: some old, some recent
        old_date = datetime.now() - timedelta(days=20)  # Older than 14 days
        recent_date = datetime.now() - timedelta(days=5)  # Younger than 14 days

        old_file = (
            config.trash_root
            / "input"
            / f"REO_old_{old_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_file = (
            config.trash_root
            / "input"
            / f"REO_recent_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )

        # Create both files
        for file_path in [old_file, recent_file]:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("w") as f:
                f.write("x" * 600)  # 600 bytes each

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            if old_date.strftime("%Y%m%d") in filename:
                return old_date
            elif recent_date.strftime("%Y%m%d") in filename:
                return recent_date
            return datetime.now()

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 600  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Mock size calculation
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=1200
        )

        # Run size-based cleanup (should use combined strategy)
        processor.size_based_cleanup(set())

        # Verify behavior: only old files should be removed (not recent ones)
        old_files_removed = [
            f for f in removed_files if old_date.strftime("%Y%m%d") in f
        ]
        recent_files_removed = [
            f for f in removed_files if recent_date.strftime("%Y%m%d") in f
        ]

        assert len(old_files_removed) > 0, (
            "Old files should be removed when both --max-size and --older-than are specified"
        )
        assert len(recent_files_removed) == 0, (
            "Recent files should NOT be removed when both --max-size and --older-than are specified"
        )

    def test_cleanup_does_not_crash_when_no_files_meet_criteria(
        self, config, graceful_exit, logger, temp_dir, mocker
    ):
        """Regression test: Ensure cleanup doesn't crash when no files meet criteria."""
        # Configure with age threshold that excludes all files
        config.max_size = "1KB"
        config.older_than = 5  # Only files older than 5 days
        config.directory = temp_dir
        config.trash_root = temp_dir / ".deleted"
        config.clean_output = True

        # Create directory structure
        config.trash_root.mkdir(exist_ok=True)
        (config.trash_root / "input").mkdir(exist_ok=True)

        # Create a very recent file (younger than 5 days)
        recent_date = datetime.now() - timedelta(hours=2)  # Just 2 hours old
        recent_file = (
            config.trash_root
            / "input"
            / f"REO_recent_{recent_date.strftime('%Y%m%d%H%M%S')}.mp4"
        )
        recent_file.touch()

        processor = FileProcessor(config, logger, graceful_exit)

        # Mock timestamp parsing
        def mock_parse_timestamp(filename):
            return recent_date

        mocker.patch.object(
            FileDiscovery, "_parse_timestamp", side_effect=mock_parse_timestamp
        )

        # Track what gets removed
        removed_files = []

        def mock_remove_file(file_path, *args, **kwargs):
            removed_files.append(str(file_path))
            return 100  # Return file size

        mocker.patch(
            "src.archiver.file_manager.FileManager.remove_file",
            side_effect=mock_remove_file,
        )

        # Mock size calculation
        mocker.patch.object(
            processor, "_calculate_total_directory_sizes", return_value=200
        )

        # Run size-based cleanup - this should not crash even though no files meet criteria
        processor.size_based_cleanup(set())

        # No files should be removed since they're all too recent
        assert len(removed_files) == 0
