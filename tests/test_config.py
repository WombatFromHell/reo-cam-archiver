"""
Test module for Config class - comprehensive configuration testing.
"""

import tempfile
from pathlib import Path

import pytest

from src.archiver.config import Config, parse_args


class TestConfigInitialization:
    """Test Config class initialization and basic functionality."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "defaults",
                "args": {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "older_than": 30,
                    "log_file": None,
                },
                "expected": {
                    "directory": Path("/camera"),
                    "output": Path("/camera/archived"),
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": Path("/camera/.deleted"),
                    "cleanup": False,
                    "clean_output": False,
                    "older_than": 30,
                    "log_file": Path("/camera/archiver.log"),
                },
            },
            {
                "name": "custom_values",
                "args": {
                    "directory": "/custom/camera",
                    "output": "/custom/output",
                    "dry_run": True,
                    "no_confirm": True,
                    "no_skip": True,
                    "delete": True,
                    "trash_root": "/custom/trash",
                    "cleanup": True,
                    "clean_output": True,
                    "age": 60,
                    "older_than": 60,
                    "log_file": "/custom/log.txt",
                },
                "expected": {
                    "directory": Path("/custom/camera"),
                    "output": Path("/custom/output"),
                    "dry_run": True,
                    "no_confirm": True,
                    "no_skip": True,
                    "delete": True,
                    "trash_root": None,  # Should be None when delete=True
                    "cleanup": True,
                    "clean_output": True,
                    "older_than": 60,
                    "log_file": Path("/custom/log.txt"),
                },
            },
            {
                "name": "with_max_size",
                "args": {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "older_than": 30,
                    "log_file": None,
                    "max_size": "500GB",
                },
                "expected": {
                    "directory": Path("/camera"),
                    "output": Path("/camera/archived"),
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": Path("/camera/.deleted"),
                    "cleanup": False,
                    "clean_output": False,
                    "older_than": 30,
                    "log_file": Path("/camera/archiver.log"),
                    "max_size": "500GB",
                },
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "defaults"},
                {"name": "custom_values"},
                {"name": "with_max_size"},
            ]
        ],
    )
    def test_config_initialization(self, mocker, test_case):
        """Test Config initialization with various configurations."""
        args = mocker.Mock()
        for key, value in test_case["args"].items():
            setattr(args, key, value)

        config = Config(args)

        # Check all expected values
        for key, expected_value in test_case["expected"].items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )


class TestPathResolution:
    """Test path resolution methods."""

    @pytest.mark.parametrize(
        "method,args_config,expected",
        [
            # _resolve_output_path tests
            (
                "_resolve_output_path",
                {"output": "/custom/output", "directory": "/camera"},
                Path("/custom/output"),
            ),
            (
                "_resolve_output_path",
                {"output": None, "directory": "/camera"},
                Path("/camera/archived"),
            ),
            # _resolve_trash_root tests
            (
                "_resolve_trash_root",
                {"delete": True, "trash_root": "/custom/trash", "directory": "/camera"},
                None,
            ),
            (
                "_resolve_trash_root",
                {
                    "delete": False,
                    "trash_root": "/custom/trash",
                    "directory": "/camera",
                },
                Path("/custom/trash"),
            ),
            (
                "_resolve_trash_root",
                {"delete": False, "trash_root": None, "directory": "/camera"},
                Path("/camera/.deleted"),
            ),
        ],
        ids=[
            "output_path_custom",
            "output_path_default",
            "trash_root_with_delete",
            "trash_root_custom",
            "trash_root_default",
        ],
    )
    def test_path_resolution_methods(self, mocker, method, args_config, expected):
        """Test path resolution methods with various configurations."""
        args = mocker.Mock()
        for key, value in args_config.items():
            setattr(args, key, value)

        result = getattr(Config, method)(args)
        assert result == expected, f"{method} failed: expected {expected}, got {result}"


class TestParseArgs:
    """Test parse_args function."""

    @pytest.mark.parametrize(
        "args_list,expected",
        [
            (
                [],
                {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "older_than": 30,
                    "max_size": None,
                    "log_file": None,
                },
            ),
            (
                [
                    "/custom/camera",
                    "-o",
                    "/custom/output",
                    "--dry-run",
                    "-y",
                    "--no-skip",
                    "--delete",
                    "--trash-root",
                    "/custom/trash",
                    "--cleanup",
                    "--clean-output",
                    "--older-than",
                    "60",
                    "--max-size",
                    "500GB",
                    "--log-file",
                    "/custom/log.txt",
                ],
                {
                    "directory": "/custom/camera",
                    "output": "/custom/output",
                    "dry_run": True,
                    "no_confirm": True,
                    "no_skip": True,
                    "delete": True,
                    "trash_root": "/custom/trash",
                    "cleanup": True,
                    "clean_output": True,
                    "older_than": 60,
                    "max_size": "500GB",
                    "log_file": "/custom/log.txt",
                },
            ),
            (
                ["/custom/camera"],
                {
                    "directory": "/custom/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "older_than": 30,  # Should still have default
                    "max_size": None,
                    "log_file": None,
                },
            ),
        ],
        ids=["defaults", "custom_values", "only_directory"],
    )
    def test_parse_args(self, mocker, args_list, expected):
        """Test parse_args with various argument configurations."""
        args = parse_args(args_list)

        # Check all expected values
        for key, expected_value in expected.items():
            actual_value = getattr(args, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )


class TestConfigEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "empty_directory",
                "args": {
                    "directory": "",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "directory": Path(""),
                    "output": Path("") / "archived",
                },
            },
            {
                "name": "relative_paths",
                "args": {
                    "directory": "./camera",
                    "output": "../output",
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "directory": Path("./camera"),
                    "output": Path("../output"),
                },
            },
            {
                "name": "windows_paths",
                "args": {
                    "directory": "C:\\camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "directory": Path("C:\\camera"),
                    "output": Path("C:\\camera/archived"),
                },
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "empty_directory"},
                {"name": "relative_paths"},
                {"name": "windows_paths"},
            ]
        ],
    )
    def test_config_edge_cases(self, mocker, test_case):
        """Test Config with various edge case configurations."""
        args = mocker.Mock()
        for key, value in test_case["args"].items():
            setattr(args, key, value)

        config = Config(args)

        # Check all expected values
        for key, expected_value in test_case["expected"].items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )


class TestConfigIntegration:
    """Test integration between parse_args and Config."""

    @pytest.mark.parametrize(
        "args_list,expected",
        [
            (
                [],
                {
                    "directory": Path("/camera"),
                    "output": Path("/camera/archived"),
                    "trash_root": Path("/camera/.deleted"),
                    "log_file": Path("/camera/archiver.log"),
                },
            ),
            (
                [
                    "/test/camera",
                    "--output",
                    "/test/output",
                    "--trash-root",
                    "/test/trash",
                    "--older-than",
                    "90",
                    "--max-size",
                    "1TB",
                ],
                {
                    "directory": Path("/test/camera"),
                    "output": Path("/test/output"),
                    "trash_root": Path("/test/trash"),
                    "older_than": 90,
                    "max_size": "1TB",
                },
            ),
            (
                [
                    "/test/camera",
                    "--delete",
                    "--trash-root",
                    "/test/trash",  # This should be ignored
                ],
                {
                    "delete": True,
                    "trash_root": None,  # Should be None when delete=True
                },
            ),
        ],
        ids=["defaults", "custom_values", "delete_overrides_trash"],
    )
    def test_full_integration(self, mocker, args_list, expected):
        """Test full integration from args parsing to Config creation."""
        args = parse_args(args_list)
        config = Config(args)

        # Check all expected values
        for key, expected_value in expected.items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )


class TestConfigTypeSafety:
    """Test type safety and validation."""

    @pytest.mark.parametrize(
        "field,expected_type",
        [
            ("directory", Path),
            ("output", Path),
            ("dry_run", bool),
            ("no_confirm", bool),
            ("no_skip", bool),
            ("delete", bool),
            ("cleanup", bool),
            ("clean_output", bool),
            ("older_than", int),
        ],
        ids=[
            f"{field}_type"
            for field in [
                "directory",
                "output",
                "dry_run",
                "no_confirm",
                "no_skip",
                "delete",
                "cleanup",
                "clean_output",
                "older_than",
            ]
        ],
    )
    def test_config_field_types(self, mocker, field, expected_type):
        """Test that Config fields have correct types."""
        args = mocker.Mock()
        args.directory = "/camera"
        args.output = None
        args.dry_run = False
        args.no_confirm = False
        args.no_skip = False
        args.delete = False
        args.trash_root = None
        args.cleanup = False
        args.clean_output = False
        args.age = 30
        args.older_than = 30
        args.log_file = None

        config = Config(args)

        actual_value = getattr(config, field)
        if field in ["trash_root", "log_file"]:
            # These can be None or Path
            assert actual_value is None or isinstance(actual_value, expected_type)
        else:
            assert isinstance(actual_value, expected_type), (
                f"Field {field} should be {expected_type}, got {type(actual_value)}"
            )

    @pytest.mark.parametrize(
        "invalid_age,expected_value",
        [
            ("invalid", "invalid"),
            (None, None),
            (3.14, 3.14),  # Float should be accepted
            ([], []),  # List should be accepted
        ],
        ids=["string_age", "none_age", "float_age", "list_age"],
    )
    def test_config_with_invalid_age_type(self, mocker, invalid_age, expected_value):
        """Test Config behavior with various invalid age types."""
        args = mocker.Mock()
        args.directory = "/camera"
        args.output = None
        args.dry_run = False
        args.no_confirm = False
        args.no_skip = False
        args.delete = False
        args.trash_root = None
        args.cleanup = False
        args.clean_output = False
        args.age = invalid_age
        args.older_than = invalid_age
        args.log_file = None

        config = Config(args)
        # Should still work, age will be the provided value
        assert config.older_than == expected_value


class TestConfigPathEdgeCases:
    """Test path-related edge cases."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "temp_directory",
                "setup": lambda: {
                    "directory": tempfile.mkdtemp(),
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": lambda args_config: {
                    "directory": Path(args_config["directory"]),
                    "output": Path(args_config["directory"]) / "archived",
                    "trash_root": Path(args_config["directory"]) / ".deleted",
                },
            },
            {
                "name": "nonexistent_paths",
                "setup": lambda: {
                    "directory": "/nonexistent/camera",
                    "output": "/nonexistent/output",
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": "/nonexistent/trash",
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": "/nonexistent/log.txt",
                },
                "expected": lambda args_config: {
                    "directory": Path("/nonexistent/camera"),
                    "output": Path("/nonexistent/output"),
                    "trash_root": Path("/nonexistent/trash"),
                },
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "temp_directory"},
                {"name": "nonexistent_paths"},
            ]
        ],
    )
    def test_config_path_edge_cases(self, mocker, test_case):
        """Test Config with various path edge case configurations."""
        args_config = test_case["setup"]()
        expected_config = test_case["expected"](args_config)

        args = mocker.Mock()
        for key, value in args_config.items():
            setattr(args, key, value)

        config = Config(args)

        # For temp directory test, we need to clean up
        if test_case["name"] == "temp_directory":
            try:
                # Check all expected values
                for key, expected_value in expected_config.items():
                    actual_value = getattr(config, key)
                    assert actual_value == expected_value, (
                        f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
                    )
            finally:
                # Clean up temp directory
                import shutil

                shutil.rmtree(args_config["directory"])
        else:
            # Check all expected values
            for key, expected_value in expected_config.items():
                actual_value = getattr(config, key)
                assert actual_value == expected_value, (
                    f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
                )


class TestConfigArgumentValidation:
    """Test argument validation scenarios."""

    @pytest.mark.parametrize(
        "args_list,expected_exception,expected_value",
        [
            (["--older-than", "invalid"], SystemExit, None),
            (["--older-than", "-1"], None, -1),
            ([], None, "/camera"),
        ],
        ids=["invalid_older_than", "negative_older_than", "missing_directory"],
    )
    def test_parse_args_validation(
        self, mocker, args_list, expected_exception, expected_value
    ):
        """Test parse_args with various validation scenarios."""
        if expected_exception:
            with pytest.raises(expected_exception):
                parse_args(args_list)
        else:
            args = parse_args(args_list)
            if expected_value == "/camera":
                assert args.directory == expected_value
            else:
                assert args.older_than == expected_value


class TestConfigFixtureIntegration:
    """Test integration with pytest fixtures."""

    @pytest.mark.parametrize(
        "fixture_name,expected_values",
        [
            (
                "mock_args",
                {
                    "directory": Path("/camera"),
                    "output": Path("/camera/archived"),
                    "dry_run": False,
                    "no_confirm": False,
                    "delete": False,
                    "trash_root": Path("/camera/.deleted"),
                },
            ),
            (
                "config",
                {
                    "directory": Path("/camera"),
                    "output": Path("/camera/archived"),
                    "trash_root": Path("/camera/.deleted"),
                },
            ),
        ],
        ids=["mock_args_fixture", "config_fixture"],
    )
    def test_config_with_fixtures(self, request, fixture_name, expected_values):
        """Test Config with various pytest fixtures."""
        # Get the fixture dynamically
        fixture = request.getfixturevalue(fixture_name)

        if fixture_name == "mock_args":
            config = Config(fixture)
        else:
            config = fixture

        # Check all expected values
        for key, expected_value in expected_values.items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )


class TestConfigSpecialCases:
    """Test special configuration cases."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "name": "delete_overrides_trash",
                "args": {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": True,
                    "trash_root": "/custom/trash",  # Should be ignored
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "delete": True,
                    "trash_root": None,  # Should be None regardless of trash_root value
                },
            },
            {
                "name": "cleanup_without_delete",
                "args": {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": False,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": True,
                    "clean_output": True,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "cleanup": True,
                    "clean_output": True,
                    "trash_root": Path("/camera/.deleted"),  # Should use default trash
                },
            },
            {
                "name": "no_skip_flag",
                "args": {
                    "directory": "/camera",
                    "output": None,
                    "dry_run": False,
                    "no_confirm": False,
                    "no_skip": True,
                    "delete": False,
                    "trash_root": None,
                    "cleanup": False,
                    "clean_output": False,
                    "age": 30,
                    "log_file": None,
                },
                "expected": {
                    "no_skip": True,
                },
            },
        ],
        ids=[
            case["name"]
            for case in [
                {"name": "delete_overrides_trash"},
                {"name": "cleanup_without_delete"},
                {"name": "no_skip_flag"},
            ]
        ],
    )
    def test_config_special_cases(self, mocker, test_case):
        """Test Config with various special case configurations."""
        args = mocker.Mock()
        for key, value in test_case["args"].items():
            setattr(args, key, value)

        config = Config(args)

        # Check all expected values
        for key, expected_value in test_case["expected"].items():
            actual_value = getattr(config, key)
            assert actual_value == expected_value, (
                f"Mismatch for {key}: expected {expected_value}, got {actual_value}"
            )
