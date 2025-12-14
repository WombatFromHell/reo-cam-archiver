"""Test that simulates the archive-task.sh script execution."""

import subprocess
import sys
from pathlib import Path

import pytest


class TestArchiveTaskScript:
    """Test archive-task.sh script functionality."""

    @pytest.mark.integration
    def test_archive_task_parameters(self):
        """Test that the archive-task.sh script parameters work correctly."""

        # Change to the project directory
        project_dir = Path("/var/home/josh/Projects/camera-archiver")
        zipapp_path = project_dir / "dist" / "archiver.pyz"

        # Ensure the zipapp bundle exists (this test requires the bundle to be built)
        if not zipapp_path.exists():
            pytest.skip("Zipapp bundle not found. Run 'make build' first.")

        # Test the exact parameters from archive-task.sh
        result = subprocess.run(
            [
                sys.executable,
                str(zipapp_path),
                "--older-than",
                "5",
                "--max-size",
                "500GB",
                "--no-skip",
                "-y",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        # We expect this to fail gracefully with a directory-related error,
        # not with an import error
        assert "No module named" not in result.stderr, (
            f"Import error detected: {result.stderr}"
        )

        # Check for the specific error we were seeing before the fix
        assert "Error: No module named 'src'" not in result.stderr, (
            f"Original 'No module named src' error still present: {result.stderr}"
        )

        # The expected error should be about the input directory not existing
        assert "Input directory does not exist" in result.stderr, (
            f"Expected directory error not found. STDERR: {result.stderr}"
        )

    @pytest.mark.integration
    def test_archive_task_help(self):
        """Test that the archive-task.sh script can show help."""

        # Change to the project directory
        project_dir = Path("/var/home/josh/Projects/camera-archiver")
        zipapp_path = project_dir / "dist" / "archiver.pyz"

        # Ensure the zipapp bundle exists
        if not zipapp_path.exists():
            pytest.skip("Zipapp bundle not found. Run 'make build' first.")

        # Test help command
        result = subprocess.run(
            [sys.executable, str(zipapp_path), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        # Help should work without errors
        assert result.returncode == 0, (
            f"Help command failed with return code {result.returncode}"
        )
        assert "Camera Archiver" in result.stdout, (
            f"Expected 'Camera Archiver' in help output: {result.stdout}"
        )
        assert "No module named" not in result.stderr, (
            f"Import error in help: {result.stderr}"
        )
