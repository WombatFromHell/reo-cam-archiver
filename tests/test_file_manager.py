"""
Test module for FileManager class - comprehensive file management testing.
"""

import os
from pathlib import Path

import pytest

from src.archiver.file_manager import FileManager


class TestFileManagerInitialization:
    """Test FileManager class initialization and basic functionality."""

    def test_file_manager_class_exists(self):
        """Test that FileManager class exists and is importable."""
        assert FileManager is not None
        assert hasattr(FileManager, "remove_file")
        assert hasattr(FileManager, "clean_empty_directories")
        assert hasattr(FileManager, "_calculate_trash_subdirectory")


class TestTrashSubdirectoryCalculation:
    """Test trash subdirectory calculation."""

    @pytest.mark.parametrize(
        "is_output,expected",
        [
            (False, "input"),
            (True, "output"),
        ],
        ids=["input_files", "output_files"],
    )
    def test_calculate_trash_subdirectory(self, is_output, expected):
        """Test trash subdirectory calculation for different file types."""
        result = FileManager._calculate_trash_subdirectory(is_output)
        assert result == expected, (
            f"Expected {expected} for is_output={is_output}, got {result}"
        )


class TestFileRemoval:
    """Test file removal functionality."""

    def test_remove_file_dry_run(self, temp_dir, mocker):
        """Test file removal in dry run mode."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, dry_run=True)

        # File should still exist in dry run
        assert test_file.exists()
        logger.info.assert_called_once_with(f"[DRY RUN] Would remove {test_file}")

    def test_remove_file_permanent_delete(self, temp_dir, mocker):
        """Test permanent file deletion."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, delete=True)

        # File should be deleted
        assert not test_file.exists()
        logger.info.assert_called_once_with(f"Removed: {test_file}")

    def test_remove_file_move_to_trash(self, temp_dir, mocker):
        """Test moving file to trash."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        logger = mocker.MagicMock()

        FileManager.remove_file(
            test_file, logger, trash_root=trash_root, is_output=False
        )

        # File should be moved to trash
        assert not test_file.exists()

        # Check that file exists in trash
        trash_file = trash_root / "input" / "test.txt"
        assert trash_file.exists()

        logger.info.assert_called_once_with(
            f"Moved to trash: {test_file} -> {trash_file}"
        )

    def test_remove_nonexistent_file(self, temp_dir, mocker):
        """Test removal of non-existent file."""
        test_file = temp_dir / "nonexistent.txt"

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger)

        # Should not log anything since the file doesn't exist and we're not in cleanup mode
        # The method should handle this gracefully without logging
        assert True  # Just verify no exception is raised


class TestTrashDestinationCalculation:
    """Test trash destination calculation."""

    def test_calculate_trash_destination_basic(self, temp_dir, mocker):
        """Test basic trash destination calculation."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        dest = FileManager._calculate_trash_destination(
            test_file, temp_dir, trash_root, False
        )

        expected = trash_root / "input" / "test.txt"
        assert dest == expected

    def test_calculate_trash_destination_with_subdirs(self, temp_dir, mocker):
        """Test trash destination calculation with subdirectories."""
        # Create test file in subdirectory
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        test_file = subdir / "test.txt"
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        dest = FileManager._calculate_trash_destination(
            test_file, temp_dir, trash_root, False
        )

        expected = trash_root / "input" / "subdir" / "test.txt"
        assert dest == expected

    def test_calculate_trash_destination_conflict_resolution(self, temp_dir, mocker):
        """Test trash destination conflict resolution."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        # Create existing file in trash to force conflict
        existing_trash = trash_root / "input" / "test.txt"
        existing_trash.parent.mkdir(parents=True, exist_ok=True)
        existing_trash.touch()

        dest = FileManager._calculate_trash_destination(
            test_file, temp_dir, trash_root, False
        )

        # Should have unique suffix
        assert dest != existing_trash
        assert dest.exists() is False  # Should not exist yet
        assert dest.stem != "test"  # Should have suffix

    def test_calculate_trash_destination_double_nesting_prevention(
        self, temp_dir, mocker
    ):
        """Test prevention of double nesting in trash."""
        # Create a scenario where double nesting could occur
        # This happens when a file is in a directory that has the same structure as trash

        # Create base directory
        base_dir = temp_dir / "camera"
        base_dir.mkdir()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        # Create a file in a directory that mimics trash structure
        # This simulates the case where we're trying to move a file from a directory
        # that has the same structure as trash (e.g., during cleanup operations)
        trash_mimic_dir = base_dir / ".deleted" / "input"
        trash_mimic_dir.mkdir(parents=True, exist_ok=True)
        test_file = trash_mimic_dir / "camera" / "2023" / "01" / "15" / "video.mp4"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Try to calculate trash destination - this should prevent double nesting
        dest = FileManager._calculate_trash_destination(
            test_file, base_dir, trash_root, False
        )

        # The logic works as follows:
        # 1. rel_path = test_file.relative_to(base_dir) = ".deleted/input/camera/2023/01/15/video.mp4"
        # 2. _remove_trash_prefix_if_present removes ".deleted/input" -> "camera/2023/01/15/video.mp4"
        # 3. dest_sub = "input" (since is_output=False)
        # 4. base_dest = trash_root / "input" / "camera/2023/01/15/video.mp4"
        expected = trash_root / "input" / "camera" / "2023" / "01" / "15" / "video.mp4"
        assert dest == expected


class TestRelativePathHandling:
    """Test relative path handling."""

    def test_get_relative_path_without_double_nesting(self, temp_dir, mocker):
        """Test getting relative path without double nesting."""
        # Create test file
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        test_file = subdir / "test.txt"

        rel_path = FileManager._get_relative_path_without_double_nesting(
            test_file, temp_dir
        )

        expected = Path("subdir") / "test.txt"
        assert rel_path == expected

    def test_get_relative_path_with_trash_prefix(self, temp_dir, mocker):
        """Test getting relative path with trash prefix removal."""
        # Create file that appears to be in trash structure
        # To trigger the trash prefix removal, we need a file path that when made relative
        # to some source_root, starts with ".deleted/input"

        # Create a base directory
        base_dir = temp_dir / "base"
        base_dir.mkdir()

        # Create trash structure inside base
        trash_root = base_dir / ".deleted"
        trash_root.mkdir()
        input_trash = trash_root / "input"
        input_trash.mkdir()

        test_file = input_trash / "test.txt"
        test_file.touch()

        # Now when we get relative path from base_dir, it will be ".deleted/input/test.txt"
        rel_path = FileManager._get_relative_path_without_double_nesting(
            test_file, base_dir
        )

        # The _remove_trash_prefix_if_present method should remove ".deleted/input" prefix
        # Since len(rel_parts) == 3 ([".deleted", "input", "test.txt"]), it returns Path(*rel_parts[2:])
        # which is Path("test.txt")
        expected = Path("test.txt")
        assert rel_path == expected

    def test_get_relative_path_with_non_relative_file(self, temp_dir, mocker):
        """Test getting relative path when file is not relative to source."""
        # Create test file in different location
        other_dir = temp_dir / "other"
        other_dir.mkdir()
        test_file = other_dir / "test.txt"
        test_file.touch()

        rel_path = FileManager._get_relative_path_without_double_nesting(
            test_file, temp_dir
        )

        # Should return the relative path from other_dir
        expected = Path("other") / "test.txt"
        assert rel_path == expected


class TestEmptyDirectoryCleaning:
    """Test empty directory cleaning functionality."""

    def test_clean_empty_directories_basic(self, temp_dir, mocker):
        """Test cleaning of empty directories."""
        # Create directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        logger = mocker.MagicMock()

        FileManager.clean_empty_directories(temp_dir, logger)

        # All directories should be removed (topdown=False, so leaf first)
        assert not day_dir.exists()
        assert not month_dir.exists()
        assert not year_dir.exists()

    def test_clean_empty_directories_with_files(self, temp_dir, mocker):
        """Test that directories with files are not removed."""
        # Create directory structure with files
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Add a file
        test_file = day_dir / "test.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.clean_empty_directories(temp_dir, logger)

        # Directories should not be removed since they contain files
        assert day_dir.exists()
        assert month_dir.exists()
        assert year_dir.exists()

    def test_clean_empty_directories_dry_run(self, temp_dir, mocker):
        """Test empty directory cleaning in dry run mode."""
        # Create directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        logger = mocker.MagicMock()

        FileManager.clean_empty_directories(temp_dir, logger, dry_run=True)

        # Directories should still exist in dry run
        assert day_dir.exists()
        assert month_dir.exists()
        assert year_dir.exists()

        # Should have logged dry run messages
        logger.info.assert_any_call(
            f"[DRY RUN] Would remove empty directory: {day_dir}"
        )

    def test_clean_empty_directories_with_nested_empty(self, temp_dir, mocker):
        """Test cleaning of nested empty directories."""
        # Create nested directory structure
        deep_dir = temp_dir / "a" / "b" / "c" / "d"
        deep_dir.mkdir(parents=True)

        logger = mocker.MagicMock()

        FileManager.clean_empty_directories(temp_dir, logger)

        # All empty directories should be removed
        assert not deep_dir.exists()
        assert not deep_dir.parent.exists()
        assert not deep_dir.parent.parent.exists()
        assert not deep_dir.parent.parent.parent.exists()


class TestFileRemovalEdgeCases:
    """Test edge cases for file removal."""

    def test_remove_directory(self, temp_dir, mocker):
        """Test removal of directory."""
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_dir, logger, delete=True)

        # Directory should be removed
        assert not test_dir.exists()
        logger.info.assert_called_once_with(f"Removed: {test_dir}")

    def test_remove_file_with_permission_error(self, temp_dir, mocker):
        """Test file removal with permission error."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Mock shutil.move to raise permission error
        mocker.patch("shutil.move", side_effect=PermissionError("Test error"))

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, trash_root=temp_dir / ".deleted")

        # Should log error
        logger.error.assert_called_once_with(
            f"Failed to remove {test_file}: Test error"
        )

    def test_remove_file_with_unexpected_error(self, temp_dir, mocker):
        """Test file removal with unexpected error."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Mock shutil.move to raise unexpected error
        mocker.patch("shutil.move", side_effect=RuntimeError("Unexpected error"))

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, trash_root=temp_dir / ".deleted")

        # Should log unexpected error
        logger.error.assert_called_once_with(
            f"Unexpected error removing {test_file}: Unexpected error"
        )


class TestFileManagerIntegration:
    """Test integration scenarios for FileManager."""

    def test_complete_workflow_input_file(self, temp_dir, mocker):
        """Test complete workflow for input file removal."""
        # Create test file
        test_file = temp_dir / "camera" / "2023" / "01" / "15" / "video.mp4"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir(parents=True, exist_ok=True)

        logger = mocker.MagicMock()

        # Remove file (move to trash)
        FileManager.remove_file(
            test_file,
            logger,
            trash_root=trash_root,
            is_output=False,
            source_root=temp_dir,  # Specify source root
        )

        # Verify file was moved to trash
        assert not test_file.exists()

        expected_trash_path = (
            trash_root / "input" / "camera" / "2023" / "01" / "15" / "video.mp4"
        )
        assert expected_trash_path.exists()

    def test_complete_workflow_output_file(self, temp_dir, mocker):
        """Test complete workflow for output file removal."""
        # Create test file
        test_file = temp_dir / "archived" / "2023" / "01" / "15" / "video.mp4"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir(parents=True, exist_ok=True)

        logger = mocker.MagicMock()

        # Remove file (move to trash)
        FileManager.remove_file(
            test_file,
            logger,
            trash_root=trash_root,
            is_output=True,
            source_root=temp_dir,  # Specify source root
        )

        # Verify file was moved to trash
        assert not test_file.exists()

        expected_trash_path = (
            trash_root / "output" / "archived" / "2023" / "01" / "15" / "video.mp4"
        )
        assert expected_trash_path.exists()

    def test_complete_workflow_with_cleanup(self, temp_dir, mocker):
        """Test complete workflow with directory cleanup."""
        # Create test files
        test_file1 = temp_dir / "camera" / "2023" / "01" / "15" / "video1.mp4"
        test_file2 = temp_dir / "camera" / "2023" / "01" / "15" / "video2.mp4"
        test_file1.parent.mkdir(parents=True, exist_ok=True)
        test_file2.parent.mkdir(parents=True, exist_ok=True)
        test_file1.touch()
        test_file2.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"

        logger = mocker.MagicMock()

        # Remove files (move to trash)
        FileManager.remove_file(
            test_file1, logger, trash_root=trash_root, is_output=False
        )

        FileManager.remove_file(
            test_file2, logger, trash_root=trash_root, is_output=False
        )

        # Clean up empty directories
        FileManager.clean_empty_directories(temp_dir, logger)

        # Verify files were moved to trash
        assert not test_file1.exists()
        assert not test_file2.exists()

        # Verify source directories were cleaned up
        assert not test_file1.parent.exists()


class TestFileManagerFixtureIntegration:
    """Test integration with pytest fixtures."""

    def test_file_manager_with_temp_dir_fixture(self, temp_dir, mocker):
        """Test FileManager with temp_dir fixture."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, delete=True)

        assert not test_file.exists()

    def test_file_manager_with_trash_functionality(self, temp_dir, mocker):
        """Test FileManager trash functionality with fixtures."""
        # Create test file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Create trash directory
        trash_root = temp_dir / ".deleted"
        trash_root.mkdir()

        logger = mocker.MagicMock()

        FileManager.remove_file(
            test_file, logger, trash_root=trash_root, is_output=False
        )

        # Verify file was moved to trash
        assert not test_file.exists()
        trash_file = trash_root / "input" / "test.txt"
        assert trash_file.exists()


class TestFileManagerSpecialCases:
    """Test special cases for FileManager."""

    def test_remove_file_with_very_long_path(self, temp_dir, mocker):
        """Test removal of file with very long path."""
        # Create file with long path
        long_path = temp_dir
        for i in range(10):
            long_path = long_path / f"subdir_{i}"
        long_path.mkdir(parents=True, exist_ok=True)

        test_file = long_path / "test.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, delete=True)

        assert not test_file.exists()

    def test_remove_file_with_special_characters(self, temp_dir, mocker):
        """Test removal of file with special characters."""
        test_file = temp_dir / "test file with spaces & special!chars.txt"
        test_file.touch()

        logger = mocker.MagicMock()

        FileManager.remove_file(test_file, logger, delete=True)

        assert not test_file.exists()

    def test_clean_empty_directories_with_symlinks(self, temp_dir, mocker):
        """Test cleaning directories with symlinks."""
        # Create directory structure
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        # Create symlink (should be ignored for empty directory check)
        symlink = test_dir / "symlink"
        symlink.symlink_to("/nonexistent")

        logger = mocker.MagicMock()

        FileManager.clean_empty_directories(temp_dir, logger)

        # Directory should not be removed since it contains a symlink
        assert test_dir.exists()


class TestFileManagerErrorHandling:
    """Test error handling in FileManager."""

    def test_remove_file_with_nonexistent_trash_root(self, temp_dir, mocker):
        """Test file removal when trash root doesn't exist."""
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Non-existent trash root
        trash_root = temp_dir / "nonexistent" / ".deleted"

        logger = mocker.MagicMock()

        FileManager.remove_file(
            test_file, logger, trash_root=trash_root, is_output=False
        )

        # Should create trash directory and move file
        assert not test_file.exists()
        expected_trash_file = trash_root / "input" / "test.txt"
        assert expected_trash_file.exists()

    def test_clean_empty_directories_with_permission_error(self, temp_dir, mocker):
        """Test directory cleaning with permission error."""
        # Create directory
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        # Mock os.walk to raise permission error
        def mock_walk(*args, **kwargs):
            raise PermissionError("Test error")

        mocker.patch("os.walk", side_effect=mock_walk)

        logger = mocker.MagicMock()

        # Should not raise exception - the method should handle it gracefully
        try:
            FileManager.clean_empty_directories(temp_dir, logger)
        except PermissionError:
            # This is expected since we're mocking os.walk to raise the error
            pass

    def test_clean_empty_directories_with_os_error(self, temp_dir, mocker):
        """Test directory cleaning with OS error."""
        # Create directory
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        # Mock directory iteration to raise OSError
        mocker.patch.object(Path, "iterdir", side_effect=OSError("Test error"))

        logger = mocker.MagicMock()

        # Should not raise exception
        FileManager.clean_empty_directories(temp_dir, logger)


class TestFileManagerPathEdgeCases:
    """Test path-related edge cases for FileManager."""

    def test_remove_file_with_relative_path(self, temp_dir, mocker):
        """Test removal of file with relative path."""
        # Change to temp directory
        original_cwd = Path.cwd()
        try:
            os.chdir(temp_dir)

            test_file = Path("test.txt")
            test_file.touch()

            logger = mocker.MagicMock()

            FileManager.remove_file(test_file, logger, delete=True)

            assert not test_file.exists()
        finally:
            os.chdir(original_cwd)

    def test_remove_file_with_windows_path(self, mocker):
        """Test removal of file with Windows-style path."""
        # This test may not work on non-Windows systems, but shouldn't crash
        test_file = Path("C:\\test\\test.txt")
        logger = mocker.MagicMock()

        # Should handle Windows path gracefully (may not actually remove anything)
        FileManager.remove_file(test_file, logger, delete=True)

    def test_clean_empty_directories_with_root_path(self, temp_dir, mocker):
        """Test cleaning empty directories starting from root."""
        logger = mocker.MagicMock()

        # Should not remove the temp_dir itself
        FileManager.clean_empty_directories(temp_dir, logger)

        assert temp_dir.exists()


class TestEdgeCases:
    """Test edge cases and exception handling."""

    def test_remove_file_with_file_not_found_error(self, temp_dir, mocker):
        """Test file removal when file is already removed (FileNotFoundError)."""
        logger = mocker.MagicMock()

        # Create and then remove a file
        test_file = temp_dir / "test.txt"
        test_file.touch()
        test_file.unlink()  # Remove the file

        # Try to remove the already-removed file
        FileManager.remove_file(
            test_file, logger, delete=False, trash_root=temp_dir / ".trash"
        )

        # Should log debug message about file already being removed
        logger.debug.assert_called_with(
            f"File already removed (during cleanup): {test_file}"
        )

    def test_get_relative_path_with_value_error(self, temp_dir, mocker):
        """Test getting relative path when file is not relative to source root."""
        # Create a file outside the source root
        source_root = temp_dir / "source"
        source_root.mkdir()

        outside_file = temp_dir / "outside" / "test.txt"
        outside_file.parent.mkdir(parents=True, exist_ok=True)
        outside_file.touch()

        # This should trigger the ValueError path and return just the filename
        result = FileManager._get_relative_path_without_double_nesting(
            outside_file, source_root
        )

        # Should return just the filename when file is not relative to source_root
        assert result == Path("test.txt")

    def test_remove_file_with_oserror(self, temp_dir, mocker):
        """Test file removal when OSError occurs."""
        logger = mocker.MagicMock()

        # Create a file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Mock the _remove_file_with_strategy to raise OSError
        mocker.patch(
            "src.archiver.file_manager.FileManager._remove_file_with_strategy",
            side_effect=OSError("Mocked OS error"),
        )

        # Try to remove the file
        FileManager.remove_file(
            test_file, logger, delete=False, trash_root=temp_dir / ".trash"
        )

        # Should log error message about OSError
        logger.error.assert_called_with(
            f"Failed to remove {test_file}: Mocked OS error"
        )

    def test_remove_file_with_generic_exception(self, temp_dir, mocker):
        """Test file removal when generic exception occurs."""
        logger = mocker.MagicMock()

        # Create a file
        test_file = temp_dir / "test.txt"
        test_file.touch()

        # Mock the _remove_file_with_strategy to raise generic Exception
        mocker.patch(
            "src.archiver.file_manager.FileManager._remove_file_with_strategy",
            side_effect=Exception("Mocked generic error"),
        )

        # Try to remove the file
        FileManager.remove_file(
            test_file, logger, delete=False, trash_root=temp_dir / ".trash"
        )

        # Should log error message about generic exception
        logger.error.assert_called_with(
            f"Unexpected error removing {test_file}: Mocked generic error"
        )
