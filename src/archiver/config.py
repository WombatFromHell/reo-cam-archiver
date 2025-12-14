"""
Configuration management for the Camera Archiver application.
"""

import argparse
from pathlib import Path
from typing import Optional


class Config:
    """Configuration holder with strict typing"""

    def __init__(self, args: argparse.Namespace):
        self.directory: Path = Path(args.directory)
        self.output: Path = self._resolve_output_path(args)
        self.dry_run: bool = args.dry_run
        self.no_confirm: bool = args.no_confirm
        self.no_skip: bool = args.no_skip
        self.delete: bool = args.delete
        self.trash_root: Optional[Path] = self._resolve_trash_root(args)
        self.cleanup: bool = args.cleanup
        self.clean_output: bool = args.clean_output
        # Use --older-than argument (default is 30, set by argparse)
        self.older_than: int = args.older_than
        self.max_size: Optional[str] = getattr(args, "max_size", None)
        self.log_file: Optional[Path] = (
            Path(args.log_file) if args.log_file else self.directory / "archiver.log"
        )

    @staticmethod
    def _resolve_trash_root(args) -> Optional[Path]:
        """Resolve trash root based on delete flag and args."""
        if args.delete:  # If delete flag is set, don't use trash regardless
            return None
        else:
            return (
                Path(args.trash_root)
                if args.trash_root
                else Path(args.directory) / ".deleted"
            )

    @staticmethod
    def _resolve_output_path(args) -> Path:
        """Resolve output directory path."""
        return Path(args.output) if args.output else Path(args.directory) / "archived"


def parse_args(args: Optional[list] = None) -> argparse.Namespace:
    """Parse command line arguments

    Args:
        args: Optional list of arguments to parse. If None, uses sys.argv[1:]
    """
    parser = argparse.ArgumentParser(description="Camera Archiver")
    parser.add_argument(
        "directory",
        nargs="?",
        default="/camera",
        help="Input directory containing camera footage (defaults to /camera)",
    )
    parser.add_argument("-o", "--output", help="Output directory for archived footage")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    parser.add_argument(
        "-y", "--no-confirm", action="store_true", help="Skip confirmation prompts"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip files that already have archives",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Permanently delete files instead of moving to trash",
    )
    parser.add_argument(
        "--trash-root",
        help="Root directory for trash (defaults to /camera/.deleted)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old files based on age and size",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Also clean output directory during cleanup",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        default=30,
        help="Only remove files older than specified days (default: 30)",
    )
    parser.add_argument(
        "--max-size",
        type=str,
        help="Maximum size for cleanup (e.g., 500GB, 1TB) - deletes oldest files first when exceeded",
    )
    parser.add_argument("--log-file", help="Log file path")
    return parser.parse_args(args)
