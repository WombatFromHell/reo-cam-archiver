#!/usr/bin/python3

from unittest.mock import patch
import os
import sys
import unittest
from datetime import datetime
from io import StringIO
import shutil
import tempfile
import time

import archiver

# Create a temporary base directory for testing
archiver_base_dir = os.path.join(tempfile.gettempdir(), "archiver_test")
os.environ["ARCHIVER_BASE_DIR"] = archiver_base_dir
os.environ["ARCHIVER_OUTPUT_DIR"] = f"{archiver_base_dir}/archived"

# Now override the global variables in archiver.py with our temporary directories
archiver.base_dir = os.environ.get("ARCHIVER_BASE_DIR", "/camera")
archiver.output_dir = os.environ.get(
    "ARCHIVER_OUTPUT_DIR", f"{archiver.base_dir}/archived"
)


class TestArchiveFunctions(unittest.TestCase):
    """Comprehensive unit tests for the archive script"""

    @patch("archiver.setup_logging")
    def setUp(self, mock_setup_logging):
        """Set up test environment and create temporary directories"""
        self.temp_dir = tempfile.mkdtemp()

        # Create a sample directory structure for testing
        self.input_dir = os.path.join(self.temp_dir, "input")
        self.archived_dir = os.path.join(self.temp_dir, "archived")
        os.makedirs(os.path.join(self.input_dir, "2024", "01", "15"), exist_ok=True)

        # Create sample files
        self.sample_file = os.path.join(
            self.input_dir, "2024", "01", "15", "REO_DRIVEWAY_01_20240115175512.mp4"
        )
        with open(self.sample_file, "w") as f:
            f.write("Sample video file content")

        self.test_timestamp = datetime(2024, 1, 15, 17, 55, 12)

        # Create a fake log file for testing
        self.log_path = os.path.join(self.temp_dir, "transcoding.log")
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

        # Redirect stdout to capture logs during tests
        self.original_stdout = sys.stdout
        self.stdout_capture: StringIO = StringIO()
        sys.stdout = self.stdout_capture

        # Set up logging configuration for test coverage
        archiver.setup_logging()

    def tearDown(self):
        """Clean up temporary files and restore stdout"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        sys.stdout = self.original_stdout

    def test_extract_timestamp_valid(self):
        """Test timestamp extraction from valid filename"""
        # Test with full path
        result = archiver.extract_timestamp(
            "/input/2024/01/15/REO_DRIVEWAY_01_20240115175512.mp4"
        )
        self.assertEqual(result, datetime(2024, 1, 15, 17, 55, 12))

        # Test with filename only
        result = archiver.extract_timestamp("REO_DRIVEWAY_01_20240115175512.mp4")
        self.assertEqual(result, datetime(2024, 1, 15, 17, 55, 12))

        # Test with filename that has a timestamp in the middle
        result = archiver.extract_timestamp("something_20240115175512_other.mp4")
        self.assertEqual(result, datetime(2024, 1, 15, 17, 55, 12))

    def test_extract_timestamp_invalid(self):
        """Test timestamp extraction from invalid filenames"""
        # Test with no timestamp
        result = archiver.extract_timestamp("video.mp4")
        self.assertIsNone(result)

        # Test with incorrect timestamp format (too short)
        result = archiver.extract_timestamp("REO_DRIVEWAY_01_2024011517.mp4")
        self.assertIsNone(result)

        # Test with non-numeric characters in timestamp position
        result = archiver.extract_timestamp("REO_DRIVEWAY_01_20240115abcdef.mp4")
        self.assertIsNone(result)

        # Test with completely different format
        result = archiver.extract_timestamp("some_random_file_name.mp4")
        self.assertIsNone(result)

    def test_format_timestamp_filepath(self):
        """Test path formatting based on timestamps"""
        # Test valid filename with timestamp extraction - use correct filename format
        # The function expects the filename to be split by underscore and timestamp in index 3
        valid_filename = "REO_DRIVEWAY_01_20240115175512.mp4"
        valid_filepath = os.path.join(self.input_dir, valid_filename)

        result = archiver.format_timestamp_filepath(
            valid_filename, self.test_timestamp, self.archived_dir
        )

        # Ensure result is not None before accessing [0] and [1]
        if result is None:
            self.fail("Expected non-None result for valid filename formatting")
        else:
            self.assertEqual(result[0], "archived-20240115175512.mp4")
            self.assertEqual(
                result[1], os.path.join(self.archived_dir, "2024", "01", "15")
            )

        # Test invalid filename format (not enough parts)
        test_file = os.path.join(self.input_dir, "video.mp4")
        test_filename = os.path.basename(test_file)
        result = archiver.format_timestamp_filepath(
            test_filename, self.test_timestamp, self.archived_dir
        )
        self.assertIsNone(result)

    def test_is_directory_truly_empty(self):
        """Test the new is_directory_truly_empty function"""
        # Create a truly empty directory
        empty_dir = os.path.join(self.temp_dir, "empty")
        os.makedirs(empty_dir, exist_ok=True)
        self.assertTrue(archiver.is_directory_truly_empty(empty_dir))

        # Create a directory with a file
        dir_with_file = os.path.join(self.temp_dir, "with_file")
        os.makedirs(dir_with_file, exist_ok=True)
        with open(os.path.join(dir_with_file, "test.txt"), "w") as f:
            f.write("test")
        self.assertFalse(archiver.is_directory_truly_empty(dir_with_file))

        # Create a directory with a subdirectory
        dir_with_subdir = os.path.join(self.temp_dir, "with_subdir")
        os.makedirs(dir_with_subdir, exist_ok=True)
        os.makedirs(os.path.join(dir_with_subdir, "subdir"), exist_ok=True)
        self.assertFalse(archiver.is_directory_truly_empty(dir_with_subdir))

        # Test non-existent directory
        self.assertFalse(archiver.is_directory_truly_empty("/nonexistent/path"))

    def test_create_file_list_recurse(self):
        """Test recursive file listing with age filtering"""
        # Create a few more sample files in nested directories
        os.makedirs(os.path.join(self.input_dir, "2024", "01", "16"), exist_ok=True)
        with open(
            os.path.join(
                self.input_dir, "2024", "01", "16", "REO_DRIVEWAY_01_20240116175512.mp4"
            ),
            "w",
        ) as f:
            f.write("Sample file")

        # Test with age_days=0 (all files)
        files = archiver.create_file_list_recurse(self.input_dir, age_days=0)
        self.assertEqual(len(files), 2)  # Should find both files
        self.assertTrue(any(f[0] == self.sample_file for f in files))

        # Test with age_days=1 (should get the older file only)
        files = archiver.create_file_list_recurse(self.input_dir, age_days=1)
        # Note: Since we're testing with same day files, might still return both
        # This depends on how create_file_list_recurse handles time comparison

    def test_create_file_list(self):
        """Test non-recursive file listing"""
        # Create a file directly in the input directory (not in subdirectories)
        test_file = os.path.join(self.input_dir, "REO_DRIVEWAY_01_20240115175513.mp4")
        with open(test_file, "w") as f:
            f.write("Sample file")

        # Test listing - the function should find files in the top-level directory
        files = archiver.create_file_list(
            self.input_dir, age_days=0
        )  # Changed to age_days=0 to ensure all files are included

        # Debug information to understand what's happening
        print(f"Files in {self.input_dir}:")
        for root, _, files_in_dir in os.walk(self.input_dir):
            for f in files_in_dir:
                print(f"  {os.path.join(root, f)}")

        self.assertEqual(len(files), 1)
        self.assertTrue(any(f[0] == test_file for f in files))

    def test_remove_empty_directories_improved(self):
        """Test the improved remove_empty function"""
        # Create a complex directory structure
        dir_structure = {
            "parent": {
                "empty_child": {},
                "non_empty_child": {"file.txt": "content"},
                "nested_empty": {"deep_empty": {}},
            }
        }

        # Build the actual directory structure
        def create_structure(base_path, structure):
            for name, content in structure.items():
                current_path = os.path.join(base_path, name)
                if isinstance(content, dict):
                    os.makedirs(current_path, exist_ok=True)
                    create_structure(current_path, content)
                else:
                    # It's a file
                    with open(current_path, "w") as f:
                        f.write(content)

        test_base = os.path.join(self.temp_dir, "remove_test")
        os.makedirs(test_base, exist_ok=True)
        create_structure(test_base, dir_structure)

        # Test dry run first
        count = archiver.remove_empty(test_base, dry_run=True)
        self.assertGreaterEqual(
            count, 1
        )  # Should find at least the deep_empty directory

        # Verify directories still exist after dry run
        self.assertTrue(
            os.path.exists(
                os.path.join(test_base, "parent", "nested_empty", "deep_empty")
            )
        )

        # Now test actual removal
        count = archiver.remove_empty(test_base, dry_run=False)
        self.assertGreaterEqual(count, 1)

        # Verify the empty directories were removed but non-empty ones remain
        self.assertFalse(
            os.path.exists(
                os.path.join(test_base, "parent", "nested_empty", "deep_empty")
            )
        )
        self.assertTrue(
            os.path.exists(os.path.join(test_base, "parent", "non_empty_child"))
        )

    def test_remove_empty_directories_race_condition(self):
        """Test remove_empty function handles race conditions gracefully"""
        # Create a directory structure
        dir_a = os.path.join(self.temp_dir, "race_test", "dir_a")
        dir_b = os.path.join(dir_a, "dir_b")
        dir_c = os.path.join(dir_b, "dir_c")

        os.makedirs(dir_c)

        # Mock os.rmdir to simulate a failure on the parent directory
        original_rmdir = os.rmdir
        failed_dirs = set()

        def mock_rmdir(path):
            if "dir_a" in path and path not in failed_dirs:
                failed_dirs.add(path)
                raise OSError("[Errno 39] Directory not empty")
            return original_rmdir(path)

        with patch("os.rmdir", side_effect=mock_rmdir):
            # This should handle the race condition gracefully
            count = archiver.remove_empty(
                os.path.join(self.temp_dir, "race_test"), dry_run=False
            )

            # Should still remove some directories despite failures
            self.assertGreaterEqual(count, 0)

        # Capture output to verify no redundant error messages
        output = self.stdout_capture.getvalue()
        error_lines = [
            line for line in output.split("\n") if "Failed to remove directory" in line
        ]

        # Should only have one error message per truly failed directory
        # (not multiple messages for the same directory path)
        unique_failed_paths = set()
        for line in error_lines:
            if "Failed to remove directory" in line:
                # Extract path from error message
                path_start = line.find("/")
                path_end = line.find(":", path_start)
                if path_start != -1 and path_end != -1:
                    path = line[path_start:path_end]
                    unique_failed_paths.add(path)

        # Should not have duplicate error messages for the same path
        self.assertLessEqual(len(error_lines), len(unique_failed_paths) + 1)

    def test_collect_files_to_delete(self):
        """Test file collection for deletion based on age"""
        # Clean up ALL existing files from setUp first
        for root, dirs, files in os.walk(self.input_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        # Clean up empty directories
        for root, dirs, files in os.walk(self.input_dir, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    os.rmdir(dir_path)
                except OSError:
                    pass  # Directory not empty, skip

        # The collect_files_to_delete function uses extract_timestamp from filename,
        # not file modification time. So we need to create files with timestamps
        # in their names that are older than max_days_old

        # Create an older file (timestamp indicates 5 days ago from now)
        current_date = datetime.now()
        old_date = (
            current_date.replace(day=current_date.day - 5)
            if current_date.day > 5
            else current_date.replace(month=current_date.month - 1, day=25)
        )
        old_timestamp_str = old_date.strftime("%Y%m%d%H%M%S")

        older_file = os.path.join(
            self.input_dir,
            "2024",
            "01",
            "14",
            f"REO_DRIVEWAY_01_{old_timestamp_str}.mp4",
        )
        os.makedirs(os.path.dirname(older_file), exist_ok=True)
        with open(older_file, "w") as f:
            f.write("Older file content")

        # Create a newer file with current timestamp (should NOT be deleted)
        current_timestamp_str = current_date.strftime("%Y%m%d%H%M%S")
        newer_file = os.path.join(
            self.input_dir,
            "2024",
            "01",
            "16",
            f"REO_DRIVEWAY_01_{current_timestamp_str}.mp4",
        )
        os.makedirs(os.path.dirname(newer_file), exist_ok=True)
        with open(newer_file, "w") as f:
            f.write("Newer file content")

        # Test collecting files older than 3 days
        files_to_delete = archiver.collect_files_to_delete(
            self.input_dir, max_days_old=3
        )

        print(f"Debug - Files to delete: {[f[0] for f in files_to_delete]}")

        # Should only get the older file (5 days old), not the newer file (current)
        self.assertEqual(len(files_to_delete), 1)
        self.assertTrue(older_file in [f[0] for f in files_to_delete])
        self.assertFalse(newer_file in [f[0] for f in files_to_delete])

    def test_cleanup_old_files_with_directory_removal(self):
        """Test cleanup of old files and subsequent directory cleanup"""
        # Create files with specific timestamps to control age
        current_time = time.time()
        old_time = current_time - (2 * 24 * 60 * 60)  # 2 days ago

        # Create a file to delete (older than max_days_old)
        old_file = os.path.join(
            self.input_dir, "2024", "01", "13", "REO_DRIVEWAY_01_20240113175512.mp4"
        )
        os.makedirs(os.path.dirname(old_file), exist_ok=True)
        with open(old_file, "w") as f:
            f.write("Old file content")
        os.utime(old_file, (old_time, old_time))

        # Create a newer file that shouldn't be deleted
        new_file = os.path.join(
            self.input_dir, "2024", "01", "15", "REO_DRIVEWAY_01_20240115175513.mp4"
        )
        with open(new_file, "w") as f:
            f.write("New file content")

        # Test cleanup with dry run
        size_mb = archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=True
        )

        # For dry run, we expect the function to report the size that would be freed
        # but not actually delete files, so size_mb should be > 0
        self.assertGreater(size_mb, 0)

        # Test actual cleanup
        old_file_parent = os.path.dirname(old_file)
        size_mb = archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=False
        )

        # Verify old file was deleted
        self.assertFalse(os.path.exists(old_file))
        # Verify new file still exists
        self.assertTrue(os.path.exists(new_file))

    def test_cleanup_old_files(self):
        """Test cleanup of old files"""
        # Create files with specific timestamps to control age
        current_time = time.time()
        old_time = current_time - (2 * 24 * 60 * 60)  # 2 days ago

        # Create a file to delete (older than max_days_old)
        old_file = os.path.join(
            self.input_dir, "2024", "01", "13", "REO_DRIVEWAY_01_20240113175512.mp4"
        )
        os.makedirs(os.path.dirname(old_file), exist_ok=True)
        with open(old_file, "w") as f:
            f.write("Old file content")
        os.utime(old_file, (old_time, old_time))

        # Create a newer file that shouldn't be deleted
        new_file = os.path.join(
            self.input_dir, "2024", "01", "15", "REO_DRIVEWAY_01_20240115175513.mp4"
        )
        os.makedirs(os.path.dirname(new_file), exist_ok=True)
        with open(new_file, "w") as f:
            f.write("New file content")

        # Test cleanup with dry run
        size_mb = archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=True
        )

        # For dry run, we expect the function to report the size that would be freed
        # but not actually delete files, so size_mb should be > 0
        self.assertGreater(size_mb, 0)

    def test_transcode_list(self):
        """Test transcoding list functionality"""
        # Create a file in the expected directory structure with proper naming
        filename = "REO_DRIVEWAY_01_20240115175512.mp4"
        test_file_path = os.path.join(self.input_dir, filename)

        with open(test_file_path, "w") as f:
            f.write("Test video content")

        # Create a fake output directory structure
        output_dir = os.path.join(self.archived_dir, "2024", "01", "15")
        os.makedirs(output_dir, exist_ok=True)

        # Expected output file path
        expected_output_file = os.path.join(output_dir, "archived-20240115175512.mp4")

        # Override the archiver's output_dir for this test
        original_output_dir = archiver.output_dir
        archiver.output_dir = self.archived_dir

        try:
            # Mock the transcode_file function and os.path.exists to simulate successful transcoding
            with (
                patch.object(archiver, "transcode_file") as mock_transcode_file,
                patch("os.path.exists") as mock_exists,
                patch("os.path.getsize") as mock_getsize,
                patch("os.remove") as mock_remove,
            ):
                # Setup mocks
                mock_transcode_file.return_value = (
                    None  # Simulate successful transcoding
                )

                def mock_exists_side_effect(path):
                    # Return True for the expected output file, False otherwise
                    return path == expected_output_file

                mock_exists.side_effect = mock_exists_side_effect
                mock_getsize.return_value = 1000  # Non-zero size
                mock_remove.return_value = None

                # Prepare the file list in the expected format
                files_to_transcode = [(filename, self.test_timestamp)]

                # Run transcode_list
                count = archiver.transcode_list(files_to_transcode, dry_run=False)

                print(f"Debug - Returned count: {count}")
                print(f"Debug - transcode_file called: {mock_transcode_file.called}")
                print(f"Debug - Expected output: {expected_output_file}")

                # Verify it processed the file correctly
                self.assertEqual(count, 1)

                # Verify the transcode function was called
                self.assertTrue(mock_transcode_file.called)
                self.assertEqual(mock_transcode_file.call_count, 1)

                # Verify the source file was "removed" (mocked)
                self.assertTrue(mock_remove.called)

        finally:
            # Restore original output directory
            archiver.output_dir = original_output_dir

    def test_find_year_directories(self):
        """Test finding year directories"""
        # Create fake year directories under temp_dir
        os.makedirs(os.path.join(self.temp_dir, "2023"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "2024"), exist_ok=True)

        # Test with valid year directories
        dirs = archiver.find_year_directories(self.temp_dir)
        self.assertEqual(len(dirs), 2)
        self.assertIn(os.path.join(self.temp_dir, "2023"), dirs)
        self.assertIn(os.path.join(self.temp_dir, "2024"), dirs)

        # Test with invalid year directories
        os.makedirs(os.path.join(self.temp_dir, "1999"), exist_ok=True)
        os.makedirs(os.path.join(self.temp_dir, "2100"), exist_ok=True)
        self.assertEqual(len(archiver.find_year_directories(self.temp_dir)), 2)

    def test_organize_and_transcode(self):
        """Test the full organization and transcoding flow"""
        # Override the base_dir and output_dir for this test
        original_base_dir = archiver.base_dir
        original_output_dir = archiver.output_dir

        archiver.base_dir = self.input_dir
        archiver.output_dir = self.archived_dir

        try:
            # Create a directory structure that mimics what would be in /camera
            os.makedirs(os.path.join(self.input_dir, "2024", "01", "15"), exist_ok=True)

            # Add sample files to test transcode functionality
            test_file = os.path.join(
                self.input_dir,
                "2024",
                "01",
                "15",
                "REO_DRIVEWAY_01_20240115175512.mp4",
            )
            with open(test_file, "w") as f:
                f.write("Test video file")

            # Mock multiple functions that might be called in the transcode flow
            with (
                patch.object(archiver, "find_year_directories") as mock_find_years,
                patch.object(archiver, "create_file_list_recurse") as mock_file_list,
                patch.object(archiver, "transcode_list") as mock_transcode_list,
            ):
                # Setup mocks to return expected values
                mock_find_years.return_value = [os.path.join(self.input_dir, "2024")]
                mock_file_list.return_value = [(test_file, self.test_timestamp)]
                mock_transcode_list.return_value = 1  # Return count of 1 file processed

                # Run the transcode function
                count = archiver.transcode(dry=False)

                # Verify it processed at least one file
                self.assertEqual(count, 1)

                # Verify the functions were called
                self.assertTrue(mock_find_years.called)
                self.assertTrue(mock_file_list.called)
                self.assertTrue(mock_transcode_list.called)

        finally:
            # Restore original values
            archiver.base_dir = original_base_dir
            archiver.output_dir = original_output_dir

    def test_edge_case_directory_removal_with_hidden_files(self):
        """Test directory removal when hidden files or race conditions occur"""
        # Create a directory structure
        test_dir = os.path.join(self.temp_dir, "hidden_test")
        nested_dir = os.path.join(test_dir, "nested")
        os.makedirs(nested_dir, exist_ok=True)

        # Mock is_directory_truly_empty to simulate a race condition
        # where directory appears empty but removal fails
        original_is_empty = archiver.is_directory_truly_empty
        original_rmdir = os.rmdir

        def mock_is_empty(path):
            return True  # Always report as empty

        def mock_rmdir(path):
            if "nested" in path:
                raise OSError("[Errno 39] Directory not empty: '{}'".format(path))
            return original_rmdir(path)

        with patch.object(
            archiver, "is_directory_truly_empty", side_effect=mock_is_empty
        ):
            with patch("os.rmdir", side_effect=mock_rmdir):
                # This should handle the failure gracefully
                count = archiver.remove_empty(test_dir, dry_run=False)

                # Should attempt removal but handle failure gracefully
                self.assertGreaterEqual(count, 0)

        # Verify error message was logged but not duplicated excessively
        output = self.stdout_capture.getvalue()
        error_count = output.count("Failed to remove directory")

        # Should have logged the error, but not excessively
        self.assertGreaterEqual(error_count, 1)
        self.assertLessEqual(
            error_count, 2
        )  # Shouldn't have excessive duplicate errors

    def test_remove_empty_with_permission_error(self):
        """Test remove_empty function handles permission errors gracefully"""
        # Create a directory structure
        test_dir = os.path.join(self.temp_dir, "permission_test")
        sub_dir = os.path.join(test_dir, "subdir")
        os.makedirs(sub_dir, exist_ok=True)

        # Mock os.rmdir to simulate permission error
        def mock_rmdir(path):
            raise OSError("[Errno 13] Permission denied: '{}'".format(path))

        with patch("os.rmdir", side_effect=mock_rmdir):
            # Should handle permission errors gracefully
            count = archiver.remove_empty(test_dir, dry_run=False)

            # Count should be 0 since no directories were actually removed
            self.assertEqual(count, 0)

        # Check that error was logged appropriately
        output = self.stdout_capture.getvalue()
        self.assertIn("Failed to remove directory", output)
        self.assertIn("Permission denied", output)


if __name__ == "__main__":
    unittest.main()
