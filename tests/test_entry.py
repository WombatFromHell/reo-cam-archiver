"""
Tests for the entry module.
"""

from src.entry import main


class TestEntryModule:
    """Test entry module functionality."""

    def test_main_function_exists(self):
        """Test that main function exists and is callable."""
        # Verify that main function exists and is callable
        assert callable(main)

    def test_main_function_import(self):
        """Test that main function can be imported."""
        # Verify that we can import the main function
        from src.entry import main as imported_main

        assert imported_main is main

    def test_main_function_basic_structure(self):
        """Test that main function has basic expected structure."""
        # Verify that the main function is defined in the entry module
        import src.entry

        assert hasattr(src.entry, "main")
        assert src.entry.main is main


class TestEntryPointScript:
    """Test entry point script behavior."""

    def test_script_execution(self):
        """Test script execution via __main__."""
        # This test verifies that the script can be executed
        # We'll test the basic structure rather than actual execution

        # Import the module to verify it's structured correctly
        import src.entry

        # Verify that main function exists
        assert hasattr(src.entry, "main")
        assert callable(src.entry.main)

        # Verify that __name__ handling is present
        docstring = src.entry.__doc__ or ""
        assert "__main__" in docstring or "entry point" in docstring.lower()
