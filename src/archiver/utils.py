"""
Utility functions and constants for the Camera Archiver application.
"""

import logging
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
)

if TYPE_CHECKING:
    from .graceful_exit import GracefulExit
    from .processor import FileProcessor
    from .progress import ProgressReporter

# Constants
MIN_ARCHIVE_SIZE_BYTES = 1_048_576  # 1MB
DEFAULT_PROGRESS_WIDTH = 30
PROGRESS_UPDATE_INTERVAL = 5  # seconds for non-TTY output
LOG_ROTATION_SIZE = 4_194_304  # 4MB (4096KB) in bytes

# Global lock for coordinating logging and progress updates
OUTPUT_LOCK = threading.Lock()

# Global reference to the active progress reporter to allow clearing
ACTIVE_PROGRESS_REPORTER: Optional["ProgressReporter"] = None


# Type Definitions
FilePath = Path
Timestamp = datetime
FileSize = int
ProgressCallback = Callable[[float], None]


# Type aliases for complex return types
# Return type for FileDiscovery.discover_files method
DiscoveredFiles = Tuple[
    List[Tuple[FilePath, Timestamp]],  # List of (file_path, timestamp) tuples
    Dict[
        str, Dict[str, Optional[FilePath]]
    ],  # Mapping of timestamp keys to file extensions and paths (allowing None)
    Set[FilePath],  # Set of trash file paths
]

# Type alias for timestamp-to-file mapping (common pattern in the codebase)
TimestampFileMapping = Dict[str, Dict[str, Optional[FilePath]]]

# Type definitions for action plans
GenericAction = Dict[str, Any]


class TranscodeAction(TypedDict):
    type: Literal["transcode"]
    input: FilePath
    output: FilePath
    jpg_to_remove: Optional[FilePath]


class RemovalAction(TypedDict):
    type: str
    file: FilePath
    reason: str


# Union type for action items to allow both TypedDict and generic dict
ActionItem = Union[TranscodeAction, RemovalAction, GenericAction]


class ActionPlan(TypedDict):
    transcoding: List[TranscodeAction]
    removals: List[RemovalAction]


# Type alias for action plan that's compatible with both TypedDict and dict
ActionPlanType = Union[ActionPlan, Dict[str, List[Dict[str, Any]]]]


def parse_size(size_str: str) -> int:
    """Parse size string like '500GB', '1TB', etc. into bytes.

    Args:
        size_str: Size string with unit (e.g., '500GB', '1TB', '100MB')

    Returns:
        Size in bytes
    """
    size_str = size_str.strip().upper()

    # Define multipliers
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }

    # Find the numeric part and unit
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([A-Z]+)$", size_str)

    if not match:
        raise ValueError(
            f"Invalid size format: {size_str}. Expected format like '500GB', '1TB', etc."
        )

    number = float(match.group(1))
    unit = match.group(2)

    if unit not in multipliers:
        raise ValueError(
            f"Unknown size unit: {unit}. Supported units: B, KB, MB, GB, TB"
        )

    return int(number * multipliers[unit])


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string with appropriate unit.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string (e.g., '1.23 GiB', '512.00 MiB')
    """
    if size_bytes < 0:
        return "0 B"

    # Define units and their byte values (using 1024-based units)
    units = [
        ("B", 1),
        ("KiB", 1024),
        ("MiB", 1024**2),
        ("GiB", 1024**3),
        ("TiB", 1024**4),
    ]

    # Find the appropriate unit
    unit = units[0][0]
    divisor = units[0][1]

    for i in range(len(units) - 1, -1, -1):
        unit_name, unit_value = units[i]
        if size_bytes >= unit_value:
            unit = unit_name
            divisor = unit_value
            break

    # Format the value with 2 decimal places
    value = size_bytes / divisor
    return f"{value:.2f} {unit}"


def display_plan(plan: ActionPlanType, logger, config) -> None:
    """Display the action plan to the user"""
    _display_plan_header(logger)
    _display_transcoding_actions(plan, logger)
    _display_removal_actions(plan, logger)
    _display_cleanup_information(config, logger)
    _display_plan_footer(logger)


def _display_plan_header(logger) -> None:
    """Display the plan header."""
    logger.info("=== ACTION PLAN ===")


def _display_transcoding_actions(plan: ActionPlanType, logger) -> None:
    """Display transcoding actions."""
    logger.info(f"Transcoding {len(plan['transcoding'])} files:")
    for i, action in enumerate(plan["transcoding"], 1):
        logger.info(f"  {i}. {action['input']} -> {action['output']}")
        _display_paired_jpg_removal(action, logger)


def _display_paired_jpg_removal(
    action: Union[TranscodeAction, Dict[str, Any]], logger
) -> None:
    """Display information about paired JPG removal."""
    if action.get("jpg_to_remove"):
        logger.info(f"      + Removing paired JPG: {action['jpg_to_remove']}")


def _display_removal_actions(plan: ActionPlanType, logger) -> None:
    """Display removal actions."""
    logger.info(f"Removing {len(plan['removals'])} files:")
    for i, action in enumerate(plan["removals"], 1):
        logger.info(f"  {i}. {action['file']} - {action['reason']}")


def _display_cleanup_information(config, logger) -> None:
    """Display cleanup information if cleanup is enabled."""
    if config.cleanup:
        age_cutoff = _calculate_age_cutoff(config)
        _display_age_cutoff_info(config, age_cutoff, logger)
        _display_cleanup_scope(config, logger)
        _display_size_limit_info(config, logger)


def _calculate_age_cutoff(config) -> datetime:
    """Calculate age cutoff for cleanup."""
    from datetime import timedelta

    return datetime.now() - timedelta(days=config.older_than)


def _display_age_cutoff_info(config, age_cutoff: datetime, logger) -> None:
    """Display age cutoff information."""
    logger.info(
        f"Cleanup enabled: Files older than {age_cutoff.strftime('%Y-%m-%d %H:%M:%S')} "
        f"will be removed based on age threshold of {config.older_than} days"
    )


def _display_cleanup_scope(config, logger) -> None:
    """Display cleanup scope information."""
    if config.clean_output:
        logger.info("Cleanup scope: Source files, archive files, and trash files")
    else:
        logger.info(
            "Cleanup scope: Source files and trash files (archive files excluded)"
        )


def _display_size_limit_info(config, logger) -> None:
    """Display size limit information if specified."""
    if config.max_size and isinstance(config.max_size, str):
        _display_size_limit_with_error_handling(config, logger)


def _display_size_limit_with_error_handling(config, logger) -> None:
    """Display size limit with error handling."""
    try:
        max_bytes = parse_size(config.max_size)
        logger.info(
            f"Size limit: {config.max_size} ({max_bytes} bytes) - will remove oldest files if exceeded"
        )
        logger.info(
            "Size-based cleanup priority: 1) trash files, 2) archived files, 3) source files"
        )
    except ValueError:
        logger.warning(f"Invalid max-size value: {config.max_size}")


def _display_plan_footer(logger) -> None:
    """Display the plan footer."""
    logger.info("=== END PLAN ===")


def confirm_plan(plan: ActionPlanType, config, logger) -> bool:
    """Ask for user confirmation"""
    if config.no_confirm:
        return True

    suffix = " [Y/n]" if False else " [y/N]"
    try:
        response = (
            input(f"Proceed with transcoding and file removals?{suffix}: ")
            .strip()
            .lower()
        )
        if not response:
            return False
        return response in ("y", "yes")
    except KeyboardInterrupt:
        return False


def _setup_environment(config, logger) -> int:
    """Setup the environment by checking directories and creating output directory if needed."""
    # Check storage
    if not config.directory.exists():
        logger.error(f"Input directory does not exist: {config.directory}")
        return 1

    # Create output directory if needed
    if config.output and not config.output.exists():
        config.output.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created output directory: {config.output}")

    return 0


def _perform_discovery(config, logger) -> Tuple[List, Dict, Set]:
    """Perform file discovery and return discovered files."""
    from .discovery import FileDiscovery

    logger.info("Discovering files")

    mp4s, mapping, trash_files = FileDiscovery.discover_files(
        config.directory, config.trash_root, config.output, config.clean_output
    )

    logger.info(f"Discovered {len(mp4s)} MP4 files")
    return mp4s, mapping, trash_files


def _handle_dry_run_mode(
    config, logger, graceful_exit, processor, plan, mapping
) -> int:
    """Handle the dry run mode execution."""
    from .progress import ProgressReporter

    logger.info("Processing files (dry run - no actual filesystem changes)")

    progress_reporter = ProgressReporter(
        total_files=len(plan["transcoding"]),
        graceful_exit=graceful_exit,
        silent=True,  # Don't show progress bar since no real work is done
    )

    with progress_reporter:
        processor.execute_plan(plan, progress_reporter)

    # Stage 4: Cleanup (in dry run mode)
    if config.cleanup:
        logger.info("Cleaning up files (dry run - no actual filesystem changes)")
        processor.cleanup_orphaned_files(mapping)

        # Perform size-based cleanup if max-size is specified
        if config.max_size and isinstance(config.max_size, str):
            processor.size_based_cleanup(set())  # empty set for trash_files in dry run

    logger.info("Dry run completed - no transcoding or removals performed")
    return 0


def _handle_real_execution(
    config, logger, graceful_exit, processor, plan, mapping, trash_files
) -> int:
    """Handle the real execution mode."""
    from .progress import ProgressReporter

    logger.info("Processing files")

    progress_reporter = ProgressReporter(
        total_files=len(plan["transcoding"]),
        graceful_exit=graceful_exit,
        silent=False,
    )

    with progress_reporter:
        processor.execute_plan(plan, progress_reporter)

    # Stage 4: Cleanup
    if config.cleanup:
        logger.info("Cleaning up files")
        processor.cleanup_orphaned_files(mapping)

        # Perform size-based cleanup if max-size is specified
        if config.max_size and isinstance(config.max_size, str):
            processor.size_based_cleanup(trash_files)

    logger.info("Archiving completed successfully")
    return 0


def run_archiver(config) -> int:
    """Main pipeline function that orchestrates the archiving process.

    Args:
        config: Configuration object containing all settings for the archiving process

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    logger = _setup_logging(config)
    graceful_exit = _setup_graceful_exit()

    try:
        return _execute_archiver_pipeline(config, logger, graceful_exit)
    except Exception as e:
        return _handle_archiver_error(e, logger)


def _setup_logging(config) -> "logging.Logger":
    """Setup logging for the archiver."""
    from .logger import Logger

    return Logger.setup(config)


def _setup_graceful_exit() -> "GracefulExit":
    """Setup graceful exit handling."""
    from .graceful_exit import GracefulExit, setup_signal_handlers

    graceful_exit = GracefulExit()
    setup_signal_handlers(graceful_exit)
    return graceful_exit


def _execute_archiver_pipeline(
    config, logger: "logging.Logger", graceful_exit: "GracefulExit"
) -> int:
    """Execute the complete archiver pipeline."""
    if _perform_environment_setup(config, logger) != 0:
        return 1

    mp4s, mapping, trash_files = _perform_discovery(config, logger)

    if _should_skip_processing(mp4s, logger):
        return 0

    return _execute_processing_pipeline(
        config, logger, graceful_exit, mp4s, mapping, trash_files
    )


def _perform_environment_setup(config, logger: "logging.Logger") -> int:
    """Perform environment setup."""
    return _setup_environment(config, logger)


def _should_skip_processing(mp4s: List, logger: "logging.Logger") -> bool:
    """Check if processing should be skipped."""
    if not mp4s:
        logger.info("No files to process")
        return True
    return False


def _execute_processing_pipeline(
    config,
    logger: "logging.Logger",
    graceful_exit: "GracefulExit",
    mp4s: List,
    mapping: Dict,
    trash_files: Set,
) -> int:
    """Execute the main processing pipeline."""
    from .processor import FileProcessor

    logger.info("Planning operations")

    processor = FileProcessor(config, logger, graceful_exit)
    plan = processor.generate_action_plan(mp4s, mapping)

    _display_and_handle_plan(
        plan, logger, config, graceful_exit, processor, mapping, trash_files
    )

    return 0


def _display_and_handle_plan(
    plan: ActionPlanType,
    logger: "logging.Logger",
    config,
    graceful_exit: "GracefulExit",
    processor: "FileProcessor",
    mapping: Dict,
    trash_files: Set,
) -> None:
    """Display plan and handle execution based on configuration."""
    display_plan(plan, logger, config)

    if config.dry_run:
        _handle_dry_run_mode(config, logger, graceful_exit, processor, plan, mapping)
    else:
        _handle_real_execution_if_confirmed(
            plan, config, logger, graceful_exit, processor, mapping, trash_files
        )


def _handle_real_execution_if_confirmed(
    plan: ActionPlanType,
    config,
    logger: "logging.Logger",
    graceful_exit: "GracefulExit",
    processor: "FileProcessor",
    mapping: Dict,
    trash_files: Set,
) -> None:
    """Handle real execution if user confirms."""
    if _is_user_confirmation_required(config, plan, logger):
        _handle_real_execution(
            config, logger, graceful_exit, processor, plan, mapping, trash_files
        )


def _is_user_confirmation_required(
    config, plan: ActionPlanType, logger: "logging.Logger"
) -> bool:
    """Check if user confirmation is required."""
    if not confirm_plan(plan, config, logger):
        logger.info("Operation cancelled by user")
        return False
    return True


def _handle_archiver_error(e: Exception, logger: "logging.Logger") -> int:
    """Handle archiver errors with logging."""
    logger.error(f"Error: {e}")
    return 1


def main() -> int:
    """Main entry point for the Camera Archiver application.

    Parses command line arguments, creates configuration, and runs the archiver.

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    from .config import Config, parse_args

    args = parse_args()
    config = Config(args)
    return run_archiver(config)


if __name__ == "__main__":
    import sys

    sys.exit(main())
