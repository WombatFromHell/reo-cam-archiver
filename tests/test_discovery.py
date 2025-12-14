"""
Test module for FileDiscovery class - comprehensive file discovery testing.
"""

from datetime import datetime
from pathlib import Path

import pytest

from src.archiver.discovery import FileDiscovery


class TestFileDiscoveryInitialization:
    """Test FileDiscovery class initialization and basic functionality."""

    def test_file_discovery_class_exists(self):
        """Test that FileDiscovery class exists and is importable."""
        assert FileDiscovery is not None
        assert hasattr(FileDiscovery, "discover_files")
        assert hasattr(FileDiscovery, "_parse_timestamp")
        assert hasattr(FileDiscovery, "_parse_timestamp_from_archived_filename")


class TestTimestampParsing:
    """Test timestamp parsing functionality."""

    @pytest.mark.parametrize(
        "filename,expected_timestamp,method",
        [
            # Valid camera files
            (
                "REO_camera_20230115120000.mp4",
                datetime(2023, 1, 15, 12, 0, 0),
                "_parse_timestamp",
            ),
            (
                "REO_camera_20230115120000.jpg",
                datetime(2023, 1, 15, 12, 0, 0),
                "_parse_timestamp",
            ),
            # Invalid format
            ("invalid_filename.mp4", None, "_parse_timestamp"),
            # Out of range years
            ("REO_camera_19990115120000.mp4", None, "_parse_timestamp"),
            ("REO_camera_21000115120000.mp4", None, "_parse_timestamp"),
            # Valid archived files
            (
                "archived-20230115120000.mp4",
                datetime(2023, 1, 15, 12, 0, 0),
                "_parse_timestamp_from_archived_filename",
            ),
            # Invalid archived files
            ("invalid_archived.mp4", None, "_parse_timestamp_from_archived_filename"),
            (
                "archived-18990115120000.mp4",
                None,
                "_parse_timestamp_from_archived_filename",
            ),
            (
                "archived-21000115120000.mp4",
                None,
                "_parse_timestamp_from_archived_filename",
            ),
        ],
        ids=[
            "valid_mp4",
            "valid_jpg",
            "invalid_format",
            "year_1999",
            "year_2100",
            "valid_archived",
            "invalid_archived",
            "archived_year_1899",
            "archived_year_2100",
        ],
    )
    def test_timestamp_parsing(self, filename, expected_timestamp, method):
        """Test timestamp parsing with various filename formats."""
        parse_method = getattr(FileDiscovery, method)
        timestamp = parse_method(filename)

        if expected_timestamp is None:
            assert timestamp is None, f"Expected None for {filename}, got {timestamp}"
        else:
            assert timestamp is not None, f"Expected timestamp for {filename}, got None"
            assert timestamp.year == expected_timestamp.year
            assert timestamp.month == expected_timestamp.month
            assert timestamp.day == expected_timestamp.day
            assert timestamp.hour == expected_timestamp.hour
            assert timestamp.minute == expected_timestamp.minute
            assert timestamp.second == expected_timestamp.second


class TestDirectoryStructureValidation:
    """Test directory structure validation."""

    @pytest.mark.parametrize(
        "rel_parts,expected",
        [
            # Valid structure
            ((".", "2023", "01", "15", "REO_camera_20230115120000.mp4"), True),
            # Invalid length (missing day)
            ((".", "2023", "01", "REO_camera_20230115120000.mp4"), False),
            # Invalid year
            ((".", "999", "01", "15", "REO_camera_20230115120000.mp4"), False),
            # Invalid month
            ((".", "2023", "13", "15", "REO_camera_20230115120000.mp4"), False),
            # Invalid day
            ((".", "2023", "01", "32", "REO_camera_20230115120000.mp4"), False),
        ],
        ids=[
            "valid_structure",
            "invalid_length",
            "invalid_year",
            "invalid_month",
            "invalid_day",
        ],
    )
    def test_validate_directory_structure(self, rel_parts, expected):
        """Test directory structure validation with various configurations."""
        result = FileDiscovery._validate_directory_structure(rel_parts)
        assert result == expected, f"Expected {expected} for {rel_parts}, got {result}"


class TestFileValidation:
    """Test file validation functionality."""

    def test_validate_file_type_with_directory(self, temp_dir):
        """Test file type validation with a directory."""
        test_dir = temp_dir / "test_dir"
        test_dir.mkdir()

        result = FileDiscovery._validate_file_type(test_dir, False)
        assert result is False

    def test_validate_file_type_with_file(self, temp_dir):
        """Test file type validation with a regular file."""
        test_file = temp_dir / "test.mp4"
        test_file.touch()

        result = FileDiscovery._validate_file_type(test_file, False)
        assert result is True

    def test_validate_file_structure_valid(self, temp_dir):
        """Test file structure validation with valid structure."""
        # Create valid directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        test_file = day_dir / "REO_camera_20230115120000.mp4"
        test_file.touch()

        result = FileDiscovery._validate_file_structure(test_file, temp_dir, False)
        assert result is True

    def test_validate_file_structure_invalid(self, temp_dir):
        """Test file structure validation with invalid structure."""
        # Create invalid directory structure (missing year/month/day)
        test_file = temp_dir / "invalid.mp4"
        test_file.touch()

        result = FileDiscovery._validate_file_structure(test_file, temp_dir, False)
        assert result is False

    def test_validate_file_structure_with_relative_to_error(self, temp_dir):
        """Test file structure validation when relative_to raises ValueError."""
        # Create a file outside the base directory to trigger ValueError
        outside_dir = temp_dir.parent / "outside"
        outside_dir.mkdir(exist_ok=True)

        test_file = outside_dir / "test.mp4"
        test_file.touch()

        result = FileDiscovery._validate_file_structure(test_file, temp_dir, False)
        assert result is False


class TestFileProcessing:
    """Test file processing functionality."""

    def test_process_file_valid(self, temp_dir):
        """Test processing of a valid file."""
        # Create valid directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        test_file = day_dir / "REO_camera_20230115120000.mp4"
        test_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._process_file(
            test_file, temp_dir, mp4s, mapping, None, False, False
        )

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115120000" in mapping

    def test_process_file_invalid_timestamp(self, temp_dir):
        """Test processing of a file with invalid timestamp."""
        # Create valid directory structure but invalid filename
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        test_file = day_dir / "invalid_filename.mp4"
        test_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._process_file(
            test_file, temp_dir, mp4s, mapping, None, False, False
        )

        assert len(mp4s) == 0
        assert len(mapping) == 0

    def test_process_file_invalid_structure(self, temp_dir):
        """Test processing of a file with invalid directory structure."""
        # Create file in wrong location
        test_file = temp_dir / "invalid.mp4"
        test_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._process_file(
            test_file, temp_dir, mp4s, mapping, None, False, False
        )

        assert len(mp4s) == 0
        assert len(mapping) == 0


class TestDirectoryScanning:
    """Test directory scanning functionality."""

    def test_scan_directory_empty(self, temp_dir):
        """Test scanning an empty directory."""
        mp4s = []
        mapping = {}

        FileDiscovery._scan_directory(temp_dir, mp4s, mapping, None, False, False, None)

        assert len(mp4s) == 0
        assert len(mapping) == 0

    def test_scan_directory_with_valid_files(self, temp_dir):
        """Test scanning a directory with valid files."""
        # Create valid directory structure with files
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        mp4_file = day_dir / "REO_camera_20230115120000.mp4"
        jpg_file = day_dir / "REO_camera_20230115120000.jpg"
        mp4_file.touch()
        jpg_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._scan_directory(temp_dir, mp4s, mapping, None, False, False, None)

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115120000" in mapping
        assert ".mp4" in mapping["20230115120000"]
        assert ".jpg" in mapping["20230115120000"]

    def test_scan_directory_with_mixed_files(self, temp_dir):
        """Test scanning a directory with mixed valid and invalid files."""
        # Create valid directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Valid files
        valid_mp4 = day_dir / "REO_camera_20230115120000.mp4"
        valid_jpg = day_dir / "REO_camera_20230115120000.jpg"
        valid_mp4.touch()
        valid_jpg.touch()

        # Invalid files (wrong structure)
        invalid_file = temp_dir / "invalid.mp4"
        invalid_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._scan_directory(temp_dir, mp4s, mapping, None, False, False, None)

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115120000" in mapping


class TestTrashHandling:
    """Test trash directory handling."""

    def test_scan_trash_directory(self, temp_dir):
        """Test scanning trash directory."""
        # Create trash directory structure
        trash_dir = temp_dir / ".deleted" / "input"
        trash_dir.mkdir(parents=True)

        # Add files to trash
        trash_file = trash_dir / "REO_camera_20230115120000.mp4"
        trash_file.touch()

        mp4s = []
        mapping = {}
        trash_files = set()

        FileDiscovery._scan_directory(
            trash_dir, mp4s, mapping, trash_files, True, False, None
        )

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert len(trash_files) == 1
        assert trash_file in trash_files

    def test_trash_file_exclusion(self, temp_dir):
        """Test that files in trash are excluded from normal scanning."""
        # Create main directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Create trash directory
        trash_dir = temp_dir / ".deleted" / "input"
        trash_dir.mkdir(parents=True)

        # Add file to trash
        trash_file = trash_dir / "REO_camera_20230115120000.mp4"
        trash_file.touch()

        # Add valid file to main directory
        valid_file = day_dir / "REO_camera_20230115130000.mp4"
        valid_file.touch()

        mp4s = []
        mapping = {}

        # Scan main directory (should exclude trash files)
        FileDiscovery._scan_directory(
            temp_dir, mp4s, mapping, None, False, False, temp_dir / ".deleted"
        )

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115130000" in mapping  # Only the valid file
        assert "20230115120000" not in mapping  # Trash file excluded


class TestOutputDirectoryScanning:
    """Test output directory scanning."""

    def test_scan_output_directory_with_archived_files(self, temp_dir):
        """Test scanning output directory with archived files."""
        # Create output directory structure
        output_dir = temp_dir / "archived"
        year_dir = output_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Add archived file
        archived_file = day_dir / "archived-20230115120000.mp4"
        archived_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._scan_directory(
            output_dir, mp4s, mapping, None, False, True, None
        )

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115120000" in mapping

    def test_scan_output_directory_with_regular_files(self, temp_dir):
        """Test scanning output directory with regular camera files."""
        # Create output directory structure
        output_dir = temp_dir / "archived"
        year_dir = output_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Add regular camera file (should be found since it has valid timestamp)
        regular_file = day_dir / "REO_camera_20230115120000.mp4"
        regular_file.touch()

        mp4s = []
        mapping = {}

        FileDiscovery._scan_directory(
            output_dir, mp4s, mapping, None, False, True, None
        )

        # The file should be found because it has a valid timestamp
        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert "20230115120000" in mapping


class TestDiscoverFilesIntegration:
    """Test the main discover_files method."""

    def test_discover_files_basic(self, temp_dir):
        """Test basic file discovery."""
        # Create valid directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        mp4_file = day_dir / "REO_camera_20230115120000.mp4"
        jpg_file = day_dir / "REO_camera_20230115120000.jpg"
        mp4_file.touch()
        jpg_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert len(trash_files) == 0
        assert "20230115120000" in mapping

    def test_discover_files_with_trash(self, temp_dir):
        """Test file discovery with trash directory."""
        # Create main directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Create trash directory
        trash_dir = temp_dir / ".deleted"
        input_trash = trash_dir / "input"
        output_trash = trash_dir / "output"
        input_trash.mkdir(parents=True)
        output_trash.mkdir()

        # Add files
        main_file = day_dir / "REO_camera_20230115120000.mp4"
        main_file.touch()

        trash_file = input_trash / "REO_camera_20230115130000.mp4"
        trash_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            temp_dir, trash_root=trash_dir
        )

        assert len(mp4s) == 2  # Main file + trash file
        assert len(mapping) == 2
        assert len(trash_files) == 1
        assert trash_file in trash_files

    def test_discover_files_with_output_directory(self, temp_dir):
        """Test file discovery with output directory."""
        # Create main directory structure
        main_year_dir = temp_dir / "2023"
        main_month_dir = main_year_dir / "01"
        main_day_dir = main_month_dir / "15"
        main_day_dir.mkdir(parents=True)

        # Create output directory structure
        output_dir = temp_dir / "archived"
        output_year_dir = output_dir / "2023"
        output_month_dir = output_year_dir / "01"
        output_day_dir = output_month_dir / "15"
        output_day_dir.mkdir(parents=True)

        # Add files
        main_file = main_day_dir / "REO_camera_20230115120000.mp4"
        main_file.touch()

        archived_file = output_day_dir / "archived-20230115130000.mp4"
        archived_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(
            temp_dir, output_directory=output_dir, clean_output=True
        )

        assert len(mp4s) == 2  # Main file + archived file
        assert len(mapping) == 2
        assert "20230115120000" in mapping
        assert "20230115130000" in mapping


class TestFileDiscoveryEdgeCases:
    """Test edge cases for file discovery."""

    def test_discover_files_nonexistent_directory(self):
        """Test file discovery with non-existent directory."""
        nonexistent_dir = Path("/nonexistent/directory")

        mp4s, mapping, trash_files = FileDiscovery.discover_files(nonexistent_dir)

        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_empty_directory(self, temp_dir):
        """Test file discovery with empty directory."""
        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0

    def test_discover_files_with_invalid_timestamp_files(self, temp_dir):
        """Test file discovery with files having invalid timestamps."""
        # Create directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Add file with invalid timestamp
        invalid_file = day_dir / "invalid_filename.mp4"
        invalid_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 0
        assert len(mapping) == 0
        assert len(trash_files) == 0


class TestFileDiscoveryFixtureIntegration:
    """Test integration with pytest fixtures."""

    def test_discover_files_with_camera_dir_fixture(self, camera_dir):
        """Test file discovery with camera_dir fixture."""
        # Create test files using the fixture
        date = datetime(2023, 1, 15, 12, 0, 0)
        year_dir = camera_dir / str(date.year)
        month_dir = year_dir / f"{date.month:02d}"
        day_dir = month_dir / f"{date.day:02d}"
        day_dir.mkdir(parents=True)

        mp4_file = day_dir / f"REO_camera_{date.strftime('%Y%m%d%H%M%S')}.mp4"
        jpg_file = day_dir / f"REO_camera_{date.strftime('%Y%m%d%H%M%S')}.jpg"
        mp4_file.touch()
        jpg_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(camera_dir)

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert len(trash_files) == 0
        assert date.strftime("%Y%m%d%H%M%S") in mapping

    def test_discover_files_with_make_camera_file_fixture(self, make_camera_file):
        """Test file discovery with make_camera_file fixture."""
        # Create test file using fixture
        timestamp = datetime(2023, 1, 15, 12, 0, 0)
        mp4_file = make_camera_file(timestamp, "mp4")
        _jpg_file = make_camera_file(timestamp, "jpg")

        # Get the base directory (parent of the year directory)
        base_dir = mp4_file.parent.parent.parent.parent

        mp4s, mapping, trash_files = FileDiscovery.discover_files(base_dir)

        assert len(mp4s) == 1
        assert len(mapping) == 1
        assert len(trash_files) == 0
        assert timestamp.strftime("%Y%m%d%H%M%S") in mapping


class TestFileDiscoverySpecialCases:
    """Test special cases for file discovery."""

    def test_discover_files_with_multiple_years(self, temp_dir):
        """Test file discovery across multiple years."""
        # Create files in different years
        for year in [2022, 2023, 2024]:
            year_dir = temp_dir / str(year)
            month_dir = year_dir / "01"
            day_dir = month_dir / "15"
            day_dir.mkdir(parents=True)

            mp4_file = day_dir / f"REO_camera_{year}0115120000.mp4"
            mp4_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 3
        assert len(mapping) == 3
        assert "20220115120000" in mapping
        assert "20230115120000" in mapping
        assert "20240115120000" in mapping

    def test_discover_files_with_mixed_file_types(self, temp_dir):
        """Test file discovery with mixed file types."""
        # Create directory structure
        year_dir = temp_dir / "2023"
        month_dir = year_dir / "01"
        day_dir = month_dir / "15"
        day_dir.mkdir(parents=True)

        # Add various file types
        mp4_file = day_dir / "REO_camera_20230115120000.mp4"
        jpg_file = day_dir / "REO_camera_20230115120000.jpg"
        txt_file = day_dir / "REO_camera_20230115120000.txt"  # Should be ignored
        invalid_file = day_dir / "invalid.mp4"  # Should be ignored

        mp4_file.touch()
        jpg_file.touch()
        txt_file.touch()
        invalid_file.touch()

        mp4s, mapping, trash_files = FileDiscovery.discover_files(temp_dir)

        assert len(mp4s) == 1  # Only MP4 files count for mp4s list
        assert len(mapping) == 1
        assert ".mp4" in mapping["20230115120000"]
        assert ".jpg" in mapping["20230115120000"]
        assert ".txt" not in mapping["20230115120000"]
