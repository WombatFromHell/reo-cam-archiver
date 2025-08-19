#!/usr/bin/python3

import os
import sys
import tempfile
import unittest
from io import StringIO
from datetime import datetime, timedelta
from unittest.mock import patch

import archiver


class TestArchiveFunctions(unittest.TestCase):
    """Comprehensive unit tests for the archive script"""

    temp_dir: str
    input_dir: str
    archived_dir: str
    test_timestamp: datetime

    @patch("archiver.setup_logging")
    def setUp(self, mock_setup_logging):
        """Set up test environment and create temporary directories"""
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.temp_dir, "input")
        os.makedirs(os.path.join(self.input_dir, "2024", "01", "15"), exist_ok=True)
        self.archived_dir = os.path.join(self.temp_dir, "archived")

        archiver.base_dir = self.input_dir  # <-- source directory
        archiver.output_dir = self.archived_dir  # <-- destination

        self.sample_file = os.path.join(
            self.input_dir,
            "2024",
            "01",
            "15",
            "REO_DRIVEWAY_01_20240115175512.mp4",
        )
        with open(self.sample_file, "w") as f:
            f.write("Sample video file content")

        self.log_path = os.path.join(self.temp_dir, "transcoding.log")
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

        self.original_stdout = sys.stdout
        self.stdout_capture: StringIO = StringIO()
        sys.stdout = self.stdout_capture

        self.test_timestamp = datetime.now()

        archiver.setup_logging()

    def test_cleanup_old_files(self):
        """Test cleanup of old files (MP4 only)."""
        # Create an MP4 older than max_days_old
        old_file = self._make_file(
            os.path.join(self.input_dir, "2024", "01", "13"), days_old=5
        )
        new_file = self._make_file(
            os.path.join(self.input_dir, "2024", "01", "15"), days_old=0
        )

        # Dry‑run – should report size > 0
        size_mb = archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=True
        )
        self.assertGreater(size_mb, 0)

        # Actual cleanup
        archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=False
        )

        # Old MP4 removed; new one stays
        self.assertFalse(os.path.exists(old_file))
        self.assertTrue(os.path.exists(new_file))

    def test_transcode_list(self):
        """Test transcoding list functionality"""
        # Create a file in the expected directory structure with proper naming
        filename = "REO_DRIVEWAY_01_20240115175512.mp4"
        test_file_path = os.path.join(self.input_dir, "2024", "01", "15", filename)

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
            with (
                patch.object(archiver, "transcode_file") as mock_transcode_file,
                patch("os.path.exists", return_value=False) as mock_exists,
                patch("os.path.getsize") as mock_getsize,
                patch("os.remove") as mock_remove,
            ):
                mock_transcode_file.return_value = True
                files_to_transcode = [(test_file_path, self.test_timestamp)]
                count = archiver.transcode_list(files_to_transcode, dry_run=False)
                # Verify it processed the file correctly
                self.assertEqual(count, 1)  # Should now pass

        finally:
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
        test_dir = os.path.join(self.temp_dir, "hidden_test")
        nested_dir = os.path.join(test_dir, "nested")
        os.makedirs(nested_dir, exist_ok=True)

        _ = archiver.is_directory_truly_empty
        original_rmdir = os.rmdir

        def mock_is_empty(_):
            return True

        def mock_rmdir(path):
            if "nested" in path:
                raise OSError("[Errno 39] Directory not empty: '{}'".format(path))
            return original_rmdir(path)

        with patch.object(
            archiver, "is_directory_truly_empty", side_effect=mock_is_empty
        ):
            with patch("os.rmdir", side_effect=mock_rmdir):
                count = archiver.remove_empty(test_dir, dry_run=False)
                self.assertGreaterEqual(count, 0)

        output = self.stdout_capture.getvalue()
        error_count = output.count("Failed to remove directory")
        self.assertGreaterEqual(error_count, 1)
        self.assertLessEqual(error_count, 2)

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

    def _make_file(self, path: str, days_old: int = 0, ext: str = "mp4") -> str:
        now = datetime.now()
        past = now - timedelta(days=days_old)
        ts = past.strftime("%Y%m%d%H%M%S")

        dirname, basename = os.path.split(path)

        # Create a proper filename with timestamp regardless of path type
        filename = f"REO_DRIVEWAY_01_{ts}.{ext}"

        if not basename or os.path.isdir(path):
            full_path = os.path.join(path, filename)
            # Ensure directory exists before creating file
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
        else:
            full_path = os.path.join(dirname, filename)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, "w") as f:
            f.write("dummy")

        if ext == "mp4" or ext == "jpg":
            try:
                import time

                # Parse the timestamp from filename
                ts_str = filename.split("_")[-1].split(".")[0]
                file_time = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
                # Set the modification time to match the filename's timestamp
                os.utime(full_path, (time.time(), time.mktime(file_time.timetuple())))
            except Exception as e:
                print(f"Warning: Could not parse timestamp from {filename}: {e}")

        return full_path

    def test_cleanup_old_files_with_paired_jpg(self):
        """Verify that a paired .jpg is removed together with its old .mp4."""
        # Create an MP4 that is older than the cutoff (5 days ago)
        mp4_path = self._make_file(
            os.path.join(self.input_dir, "2024", "01", "13"), days_old=5
        )

        # Create a matching JPG in the same directory
        jpg_path = mp4_path.rsplit(".", 1)[0] + ".jpg"
        with open(jpg_path, "w") as f:
            f.write("dummy photo")

        # Ensure both files exist before cleanup
        self.assertTrue(os.path.exists(mp4_path))
        self.assertTrue(os.path.exists(jpg_path))

        # Run the cleanup – dry‑run is False to actually delete
        archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=False
        )

        # Both files should be gone
        self.assertFalse(os.path.exists(mp4_path))
        self.assertFalse(os.path.exists(jpg_path))

    def test_cleanup_old_files_with_orphaned_jpg(self):
        """Verify that an orphaned .jpg (no matching .mp4) is removed if old enough."""
        # Create a JPG older than the cutoff, but no MP4 exists
        jpg_path = self._make_file(
            os.path.join(self.input_dir, "2024", "01", "12"), days_old=5, ext="jpg"
        )

        # Verify file exists before cleanup
        self.assertTrue(os.path.exists(jpg_path))

        # Run the cleanup – dry‑run is False to actually delete
        archiver.cleanup_old_files(
            self.input_dir, max_days_old=1, max_size_gb=0.1, dry_run=False
        )

        # The orphaned JPG should be gone
        self.assertFalse(os.path.exists(jpg_path))


if __name__ == "__main__":
    unittest.main()
