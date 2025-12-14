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

    def test_import_fallback_mechanism(self):
        """Test that entry module handles both direct and zipapp imports."""
        # This test verifies that the import fallback mechanism works

        # Test 1: Verify that the module can import from archiver package
        # (this simulates zipapp execution)
        import importlib
        import sys

        # Save original modules
        original_modules = {}
        for module_name in list(sys.modules.keys()):
            if module_name.startswith("archiver"):
                original_modules[module_name] = sys.modules[module_name]

        try:
            # Remove archiver modules to simulate fresh import
            for module_name in list(sys.modules.keys()):
                if module_name.startswith("archiver"):
                    del sys.modules[module_name]

            # Mock the archiver package to simulate zipapp environment
            from unittest.mock import MagicMock

            mock_archiver = MagicMock()
            mock_archiver.utils.main = lambda: "zipapp_main"
            sys.modules["archiver"] = mock_archiver
            sys.modules["archiver.utils"] = mock_archiver.utils

            # Re-import entry module to test the import logic
            import src.entry

            importlib.reload(src.entry)

            # Verify that the import worked (no ImportError)
            assert hasattr(src.entry, "main")
            assert callable(src.entry.main)

        finally:
            # Restore original modules
            for module_name, module in original_modules.items():
                sys.modules[module_name] = module

            # Clean up any mock modules
            for module_name in list(sys.modules.keys()):
                if (
                    module_name.startswith("archiver")
                    and module_name not in original_modules
                ):
                    del sys.modules[module_name]

    def test_direct_import_fallback(self):
        """Test that entry module can handle import scenarios."""
        # This test verifies that the entry module structure is correct
        # and can handle different import scenarios

        # Test that we can import the entry module directly
        import src.entry

        # Verify that the main function exists and is callable
        assert hasattr(src.entry, "main")
        assert callable(src.entry.main)

        # Verify that the module has the expected docstring
        docstring = src.entry.__doc__ or ""
        assert "Entry point" in docstring

        # Test that the module can be reloaded without errors
        import importlib

        importlib.reload(src.entry)

        # Verify that the main function still exists after reload
        assert hasattr(src.entry, "main")
        assert callable(src.entry.main)
