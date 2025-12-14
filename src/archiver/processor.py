"""
File processing operations for the Camera Archiver application.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from .config import Config
from .discovery import FileDiscovery
from .file_manager import FileManager
from .graceful_exit import GracefulExit
from .transcoder import Transcoder
from .utils import (
    ActionPlanType,
    FilePath,
    Timestamp,
    TimestampFileMapping,
)


class FileProcessor:
    """Handles file processing operations with strict typing including plan generation and execution."""

    def __init__(
        self, config: Config, logger: logging.Logger, graceful_exit: GracefulExit
    ):
        """Initialize the FileProcessor with configuration and dependencies.

        Args:
            config: Configuration object with archiving settings
            logger: Logger instance for logging operations
            graceful_exit: GracefulExit instance for handling shutdown signals
        """
        self.config: Config = config
        self.logger: logging.Logger = logger
        self.graceful_exit: GracefulExit = graceful_exit

    def _calculate_age_cutoff(self) -> Optional[datetime]:
        """Calculate age cutoff for file processing."""
        if self.config.older_than > 0:
            return datetime.now() - timedelta(days=self.config.older_than)
        return None

    def _should_skip_file_due_to_age(
        self, fp: FilePath, ts: Timestamp, age_cutoff: Optional[datetime]
    ) -> bool:
        """Determine if file should be skipped due to age cutoff."""
        if age_cutoff and ts >= age_cutoff:
            self.logger.debug(
                f"Skipping {fp}: timestamp {ts} is newer than age cutoff {age_cutoff}"
            )
            return True
        return False

    def _should_skip_transcoding(
        self, outp: FilePath, cleanup_mode: bool = False
    ) -> bool:
        """Determine if transcoding should be skipped."""
        if cleanup_mode:
            return True

        if not self.config.no_skip and outp.exists():
            try:
                file_stat = outp.stat()
                return file_stat.st_size > 1_048_576  # MIN_ARCHIVE_SIZE_BYTES
            except (OSError, TypeError):
                return False

        return False

    def _create_skip_removal_actions(
        self,
        fp: FilePath,
        jpg: Optional[FilePath],
        outp: FilePath,
        cleanup_mode: bool,
        removal_actions: List[Dict[str, Any]],
    ) -> None:
        """Create removal actions for files that should skip transcoding."""
        reason = (
            f"Skipping transcoding: archive exists at {outp}"
            if not cleanup_mode
            else "Skipping transcoding: cleanup mode enabled"
        )

        removal_actions.append(
            {
                "type": "source_removal_after_skip",
                "file": fp,
                "reason": reason,
            }
        )

        if jpg:
            jpg_reason = (
                "Skipping transcoding: archive exists for paired MP4"
                if not cleanup_mode
                else "Skipping transcoding: cleanup mode enabled"
            )
            removal_actions.append(
                {
                    "type": "jpg_removal_after_skip",
                    "file": jpg,
                    "reason": jpg_reason,
                }
            )

    def _create_transcoding_actions(
        self,
        fp: FilePath,
        outp: FilePath,
        jpg: Optional[FilePath],
        transcoding_actions: List[Dict[str, Any]],
        removal_actions: List[Dict[str, Any]],
    ) -> None:
        """Create transcoding and related removal actions."""
        transcoding_actions.append(
            {
                "type": "transcode",
                "input": fp,
                "output": outp,
                "jpg_to_remove": jpg,
            }
        )

        removal_actions.append(
            {
                "type": "source_removal_after_transcode",
                "file": fp,
                "reason": f"Source file for transcoded archive at {outp}",
            }
        )

        if jpg:
            removal_actions.append(
                {
                    "type": "jpg_removal_after_transcode",
                    "file": jpg,
                    "reason": "Paired with transcoded MP4",
                }
            )

    def generate_action_plan(
        self,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: TimestampFileMapping,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a plan of all transcoding and removal actions to be performed.

        Args:
            mp4s: List of tuples containing file paths and timestamps
            mapping: Dictionary mapping timestamps to file extensions and paths

        Returns:
            Dictionary containing lists of transcoding and removal actions
        """
        transcoding_actions: List[Dict[str, Any]] = []
        removal_actions: List[Dict[str, Any]] = []

        # Calculate age cutoff
        age_cutoff = self._calculate_age_cutoff()
        cleanup_mode = self.config.cleanup

        for fp, ts in mp4s:
            # Skip trash files (placeholder for actual implementation)
            if fp in set():  # Would be trash_files in actual implementation
                continue

            outp = self._output_path(fp, ts)
            jpg = mapping.get(ts.strftime("%Y%m%d%H%M%S"), {}).get(".jpg")

            # Skip files newer than age cutoff
            if self._should_skip_file_due_to_age(fp, ts, age_cutoff):
                continue

            # Determine if transcoding should be skipped
            should_skip = self._should_skip_transcoding(outp, cleanup_mode)

            if should_skip:
                self._create_skip_removal_actions(
                    fp, jpg, outp, cleanup_mode, removal_actions
                )
            else:
                self._create_transcoding_actions(
                    fp, outp, jpg, transcoding_actions, removal_actions
                )

        # Cast to maintain compatibility with expected return type
        result: Dict[str, List[Dict[str, Any]]] = {
            "transcoding": transcoding_actions,  # type: ignore
            "removals": removal_actions,  # type: ignore
        }
        return result

    def _determine_source_root(self, file_path: FilePath) -> Tuple[FilePath, bool]:
        """Determine source root and output flag for a file."""
        # Determine if this file is from the output directory
        # by checking if it's within the output directory path
        is_output_file = False
        if self.config.output and self.config.clean_output:
            try:
                file_path.relative_to(self.config.output)
                is_output_file = True
            except ValueError:
                # File is not within output directory
                is_output_file = False

        # Determine the appropriate source root based on whether it's input or output
        source_root = self.config.output if is_output_file else self.config.directory
        return source_root, is_output_file

    def _execute_transcoding_action(
        self, action: Dict[str, Any], progress_reporter
    ) -> bool:
        """Execute a single transcoding action."""
        input_path = action["input"]
        output_path = action["output"]

        self.logger.info(f"Processing {input_path}")
        progress_reporter.start_file()

        # Create progress callback
        def progress_callback(pct: float) -> None:
            if not self.graceful_exit.should_exit():
                progress_reporter.update_progress(pct)

        # Transcode file
        success = Transcoder.transcode_file(
            input_path,
            output_path,
            self.logger,
            progress_callback,
            self.graceful_exit,
            dry_run=self.config.dry_run,
        )

        if success:
            progress_reporter.finish_file()
            self.logger.info(f"Successfully transcoded {input_path} -> {output_path}")
        else:
            self.logger.error(f"Failed to transcode {input_path}")

        return success

    def _remove_paired_jpg(self, jpg_path: Union[str, FilePath, None]) -> None:
        """Remove paired JPG file if it exists."""
        if jpg_path:
            # Convert string to Path if needed (for test compatibility)
            file_path = Path(jpg_path) if isinstance(jpg_path, str) else jpg_path
            FileManager.remove_file(
                file_path,
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=False,
                source_root=self.config.directory,
            )

    def _remove_source_file(
        self, file_path: Union[str, FilePath], removal_actions: list
    ) -> None:
        """Remove source file and update removal actions list."""
        # Convert string to Path if needed (for test compatibility)
        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

        # Find and remove the source removal action
        source_removal_action = None
        for removal_action in removal_actions:
            if removal_action.get(
                "type"
            ) == "source_removal_after_transcode" and removal_action["file"] == str(
                file_path_obj
            ):
                source_removal_action = removal_action
                break

        if source_removal_action:
            FileManager.remove_file(
                source_removal_action["file"],
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=False,
                source_root=self.config.directory,
            )
            removal_actions.remove(source_removal_action)  # type: ignore

    def _filter_removal_actions(
        self, removal_actions: list, failed_transcodes: set, failed_jpgs_to_remove: set
    ) -> list:
        """Filter removal actions to skip files related to failed transcodes."""
        remaining_removal_actions = []
        for action in removal_actions:
            if self._should_skip_removal_action(
                action, failed_transcodes, failed_jpgs_to_remove
            ):
                continue
            remaining_removal_actions.append(action)
        return remaining_removal_actions

    def _should_skip_removal_action(
        self, action: dict, failed_transcodes: set, failed_jpgs_to_remove: set
    ) -> bool:
        """Determine if a removal action should be skipped."""
        if self._is_source_removal_for_failed_transcode(action, failed_transcodes):
            self._log_skipped_removal(action, "transcoding failure")
            return True

        if self._is_jpg_removal_for_failed_transcode(action, failed_jpgs_to_remove):
            self._log_skipped_removal(action, "transcoding failure")
            return True

        return False

    def _is_source_removal_for_failed_transcode(
        self, action: dict, failed_transcodes: set
    ) -> bool:
        """Check if action is for removing source file of failed transcode."""
        return (
            action.get("type") == "source_removal_after_transcode"
            and action["file"] in failed_transcodes
        )

    def _is_jpg_removal_for_failed_transcode(
        self, action: dict, failed_jpgs_to_remove: set
    ) -> bool:
        """Check if action is for removing JPG of failed transcode."""
        return (
            action.get("type") == "jpg_removal_after_transcode"
            and action["file"] in failed_jpgs_to_remove
        )

    def _log_skipped_removal(self, action: dict, reason: str) -> None:
        """Log information about skipped removal."""
        self.logger.info(f"Skipping removal of {action['file']} due to {reason}")

    def _execute_removal_action(self, action: dict, exceptions: list) -> None:
        """Execute a single removal action with exception handling."""
        file_path = action["file"]
        source_root, is_output_file = self._determine_source_root(file_path)

        try:
            FileManager.remove_file(
                file_path,
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=is_output_file,
                source_root=source_root,
            )
        except Exception as e:
            exceptions.append(e)

    def _handle_removal_exceptions(self, exceptions: list) -> None:
        """Handle and log removal exceptions."""
        if exceptions:
            try:
                raise ExceptionGroup("Removal failures", exceptions)
            except ExceptionGroup:
                # Log the exception group but continue processing
                self.logger.error(
                    f"Multiple removal failures occurred: {len(exceptions)} total"
                )
                for exc in exceptions:
                    self.logger.error(f"  - {str(exc)}")

    def execute_plan(self, plan: ActionPlanType, progress_reporter) -> bool:
        """Execute the action plan generated by generate_action_plan.

        Args:
            plan: Dictionary containing transcoding and removal actions
            progress_reporter: ProgressReporter instance for tracking progress

        Returns:
            bool: True if execution completed successfully, False otherwise
        """
        # Cast to specific types to maintain internal type safety
        transcoding_actions: List[Dict[str, Any]] = plan["transcoding"]  # type: ignore
        removal_actions: List[Dict[str, Any]] = plan["removals"]  # type: ignore

        # Track failed transcodes to avoid removing their source files and paired JPGs
        failed_transcodes = set()
        failed_jpgs_to_remove = set()

        # Execute transcoding actions
        for i, action in enumerate(transcoding_actions, 1):
            if self.graceful_exit.should_exit():
                break

            success = self._execute_transcoding_action(action, progress_reporter)

            if success:
                # Remove paired JPG if exists
                jpg = action.get("jpg_to_remove")
                self._remove_paired_jpg(jpg)

                # Remove source file after successful transcoding
                self._remove_source_file(action["input"], removal_actions)
            else:
                failed_transcodes.add(action["input"])
                # Add the paired JPG to the failed set too if it exists
                jpg = action.get("jpg_to_remove")
                if jpg:
                    failed_jpgs_to_remove.add(jpg)

        # Filter and execute remaining removal actions
        remaining_removal_actions = self._filter_removal_actions(
            removal_actions, failed_transcodes, failed_jpgs_to_remove
        )

        # Execute the filtered removal actions with ExceptionGroup for batch operations
        exceptions = []
        for action in remaining_removal_actions:
            if self.graceful_exit.should_exit():
                break

            self._execute_removal_action(action, exceptions)

        self._handle_removal_exceptions(exceptions)

        return True

    def _handle_action_type(self, action_type: str) -> str:
        """Handle action types using pattern matching."""
        return self._get_action_description(action_type)

    def _get_action_description(self, action_type: str) -> str:
        """Get description for action type using pattern matching."""
        # Use match statement for action types (Python 3.10+ feature)
        match action_type:
            case "transcode":
                return "Processing transcoding action"
            case "source_removal_after_transcode":
                return "Processing source removal after transcode"
            case "jpg_removal_after_transcode":
                return "Processing JPG removal after transcode"
            case "source_removal_after_skip":
                return "Processing source removal after skip"
            case "jpg_removal_after_skip":
                return "Processing JPG removal after skip"
            case _:
                return self._handle_unknown_action_type(action_type)

    def _handle_unknown_action_type(self, action_type: str) -> str:
        """Handle unknown action types with logging."""
        self.logger.warning(f"Unknown action type: {action_type}")
        return f"Processing unknown action type: {action_type}"

    def cleanup_orphaned_files(self, mapping: TimestampFileMapping) -> None:
        """Remove orphaned JPG files and clean empty directories.

        Args:
            mapping: Dictionary mapping timestamps to file extensions and paths
        """
        count = self._remove_orphaned_jpg_files(mapping)
        self._log_orphaned_files_removal(count)
        self._clean_empty_directories()

    def _remove_orphaned_jpg_files(self, mapping: TimestampFileMapping) -> int:
        """Remove orphaned JPG files that don't have corresponding MP4 files."""
        count = 0
        for key, files in mapping.items():
            if self.graceful_exit.should_exit():
                break

            jpg = files.get(".jpg")
            mp4 = files.get(".mp4")
            if self._is_orphaned_jpg(jpg, mp4):
                self._remove_orphaned_jpg_file(jpg)
                count += 1

        return count

    def _is_orphaned_jpg(
        self, jpg: Optional[FilePath], mp4: Optional[FilePath]
    ) -> bool:
        """Check if a JPG file is orphaned (has no corresponding MP4)."""
        return jpg is not None and mp4 is None

    def _remove_orphaned_jpg_file(self, jpg: Optional[FilePath]) -> None:
        """Remove a single orphaned JPG file."""
        if jpg is None:
            return

        self.logger.info(f"Found orphaned JPG (no MP4 pair): {jpg}")
        is_output_file, source_root = self._determine_jpg_source_info(jpg)

        FileManager.remove_file(
            jpg,
            self.logger,
            dry_run=self.config.dry_run,
            delete=self.config.delete,
            trash_root=self.config.trash_root,
            is_output=is_output_file,
            source_root=source_root,
        )

    def _determine_jpg_source_info(self, jpg: FilePath) -> Tuple[bool, FilePath]:
        """Determine if JPG is from output directory and get appropriate source root."""
        is_output_file = self._is_jpg_from_output_directory(jpg)
        source_root = self._get_source_root_for_jpg(jpg, is_output_file)
        return is_output_file, source_root

    def _is_jpg_from_output_directory(self, jpg: FilePath) -> bool:
        """Check if JPG file is from the output directory."""
        if not self.config.output:
            return False

        try:
            jpg.relative_to(self.config.output)
            return True
        except ValueError:
            return False

    def _get_source_root_for_jpg(self, jpg: FilePath, is_output_file: bool) -> FilePath:
        """Get the appropriate source root for a JPG file."""
        return self.config.output if is_output_file else self.config.directory

    def _log_orphaned_files_removal(self, count: int) -> None:
        """Log the results of orphaned files removal."""
        if not self.graceful_exit.should_exit():
            self.logger.info(f"Removed {count} orphaned JPG files")

    def _clean_empty_directories(self) -> None:
        """Clean empty directories."""
        FileManager.clean_empty_directories(
            self.config.directory, self.logger, dry_run=self.config.dry_run
        )

    def _get_directory_size(self, directory: FilePath) -> int:
        """Calculate the total size of all files in a directory"""
        if not directory.exists():
            return 0
        total = 0
        for path in directory.rglob("*"):
            if path.is_file():
                try:
                    total += path.stat().st_size
                except OSError:
                    continue  # Skip files that can't be accessed
        return total

    def _collect_files_for_cleanup(
        self, directory: FilePath, priority: int, is_output: bool = False
    ) -> List[Tuple[Timestamp, int, FilePath]]:
        """Collect files from a directory with their timestamps and priority"""
        files_with_info = []

        if not directory.exists():
            return files_with_info

        for file_path in directory.rglob("*.*"):
            if not file_path.is_file():
                continue

            try:
                # Parse timestamp
                ts = FileDiscovery._parse_timestamp(file_path.name)
                if not ts and is_output:
                    # Try parsing from archived files for output directory
                    ts = FileDiscovery._parse_timestamp_from_archived_filename(
                        file_path.name
                    )
                    if not ts:
                        continue
                elif not ts:
                    continue

                files_with_info.append((ts, priority, file_path))
            except Exception:
                continue  # Skip files we can't process

        return files_with_info

    def _should_skip_file_for_cleanup(self, file_path: FilePath) -> bool:
        """Determine if a file should be skipped during cleanup collection."""
        return self._is_file_in_trash_directory(
            file_path
        ) or self._is_file_in_output_directory_when_not_cleaning_output(file_path)

    def _is_file_in_trash_directory(self, file_path: FilePath) -> bool:
        """Check if file is in trash directory."""
        return (
            self.config.trash_root is not None
            and self.config.trash_root in file_path.parents
        )

    def _is_file_in_output_directory_when_not_cleaning_output(
        self, file_path: FilePath
    ) -> bool:
        """Check if file is in output directory but we're not cleaning output."""
        return (
            self.config.output is not None
            and self.config.output in file_path.parents
            and not self.config.clean_output
        )

    def _should_skip_file_due_to_age_for_cleanup(
        self, ts: Timestamp, age_cutoff: Optional[datetime]
    ) -> bool:
        """Determine if a file should be skipped due to age during cleanup."""
        if age_cutoff and ts >= age_cutoff:
            return True
        return False

    def _collect_source_files_for_cleanup(
        self,
    ) -> List[Tuple[Timestamp, int, FilePath]]:
        """Collect source files that meet age requirements for cleanup"""
        files_with_info = []

        # Calculate age cutoff if specified
        age_cutoff = None
        if self.config.older_than > 0:
            age_cutoff = datetime.now() - timedelta(days=self.config.older_than)

        for file_path in self.config.directory.rglob("*.*"):
            if not file_path.is_file():
                continue

            try:
                # Skip files that should be excluded from cleanup
                if self._should_skip_file_for_cleanup(file_path):
                    continue

                ts = FileDiscovery._parse_timestamp(file_path.name)
                if not ts:
                    continue  # Skip files we can't parse timestamps for

                # Skip files that are too new (respect age requirement)
                if self._should_skip_file_due_to_age_for_cleanup(ts, age_cutoff):
                    continue

                files_with_info.append(
                    (ts, 3, file_path)
                )  # priority 3 for source files
            except Exception:
                continue  # Skip files we can't process

        return files_with_info

    def _remove_file_for_cleanup(
        self, file_path: FilePath, total_size: int, max_bytes: int, removed_size: int
    ) -> int:
        """Remove a single file during cleanup and return updated removed size"""
        try:
            # Get file size before removal
            file_size = file_path.stat().st_size

            # Determine source root and output flag
            source_root, is_output_file = self._determine_source_root(file_path)

            # Remove the file
            FileManager.remove_file(
                file_path,
                self.logger,
                dry_run=self.config.dry_run,
                delete=self.config.delete,
                trash_root=self.config.trash_root,
                is_output=is_output_file,
                source_root=source_root,
            )

            removed_size += file_size
            self.logger.info(
                f"Removed {file_path} ({file_size} bytes) due to size-based cleanup"
            )

        except Exception as e:
            self.logger.error(f"Failed to remove {file_path} during size cleanup: {e}")

        return removed_size

    def size_based_cleanup(self, trash_files: Set[FilePath]) -> None:
        """Perform size-based cleanup by removing oldest files first.

        Files are removed in this priority order:
        1. ./.deleted/... (trash files first)
        2. ./archived/... (archived files second)
        3. ./<YYYY>/<MM>/<DD>/... (source files last)

        Args:
            trash_files: Set of trash files discovered during file discovery
        """
        if not self.config.max_size:
            return

        max_bytes = self._parse_max_size_with_error_handling()
        if max_bytes is None:
            return

        total_size = self._calculate_total_directory_sizes()

        if self._is_cleanup_needed(total_size, max_bytes):
            self._perform_cleanup_operations(total_size, max_bytes)

    def _parse_max_size_with_error_handling(self) -> Optional[int]:
        """Parse max_size configuration with error handling."""
        try:
            from .utils import parse_size

            if self.config.max_size:
                return parse_size(self.config.max_size)
            return None
        except ValueError as e:
            self.logger.error(f"Invalid max-size value: {e}")
            return None

    def _calculate_total_directory_sizes(self) -> int:
        """Calculate total size of all directories under our control."""
        trash_size = self._get_trash_directory_size()
        archived_size = self._get_archived_directory_size()
        source_size = self._get_directory_size(self.config.directory)
        return trash_size + archived_size + source_size

    def _get_trash_directory_size(self) -> int:
        """Get size of trash directory if configured."""
        if self.config.trash_root:
            trash_dir = self.config.directory / ".deleted"
            return self._get_directory_size(trash_dir)
        return 0

    def _get_archived_directory_size(self) -> int:
        """Get size of archived directory if configured."""
        if self.config.output:
            return self._get_directory_size(self.config.output)
        return 0

    def _is_cleanup_needed(self, total_size: int, max_bytes: int) -> bool:
        """Check if cleanup is needed based on size comparison."""
        if total_size <= max_bytes:
            self.logger.info(
                f"Current size ({total_size} bytes) is within limit ({max_bytes} bytes), no size-based cleanup needed"
            )
            return False

        self.logger.info(
            f"Current size ({total_size} bytes) exceeds limit ({max_bytes} bytes), starting size-based cleanup..."
        )
        return True

    def _perform_cleanup_operations(self, total_size: int, max_bytes: int) -> None:
        """Perform the actual cleanup operations."""
        all_files_with_info = self._collect_all_files_for_cleanup()
        sorted_files = self._sort_files_by_priority_and_age(all_files_with_info)
        removed_size = self._remove_files_until_under_limit(
            sorted_files, total_size, max_bytes
        )
        self._log_cleanup_results(total_size, max_bytes, removed_size)

    def _collect_all_files_for_cleanup(self) -> List[Tuple[Timestamp, int, FilePath]]:
        """Collect all files that are candidates for cleanup."""
        all_files_with_info = []

        # Add trash files (priority 1 - highest priority for removal)
        if self.config.trash_root:
            all_files_with_info.extend(self._collect_trash_files_for_cleanup())

        # Add archived files (priority 2 - second priority for removal)
        if self.config.output:
            all_files_with_info.extend(self._collect_archived_files_for_cleanup())

        # Add source files from the input directory (priority 3 - lowest priority for removal)
        all_files_with_info.extend(self._collect_source_files_for_cleanup())

        return all_files_with_info

    def _collect_trash_files_for_cleanup(self) -> List[Tuple[Timestamp, int, FilePath]]:
        """Collect trash files for cleanup."""
        trash_files = []
        if self.config.trash_root:
            for trash_type in ["input", "output"]:
                trash_dir = self.config.trash_root / trash_type
                trash_files.extend(self._collect_files_for_cleanup(trash_dir, 1))
        return trash_files

    def _collect_archived_files_for_cleanup(
        self,
    ) -> List[Tuple[Timestamp, int, FilePath]]:
        """Collect archived files for cleanup."""
        return self._collect_files_for_cleanup(self.config.output, 2, is_output=True)

    def _sort_files_by_priority_and_age(
        self, files_with_info: List[Tuple[Timestamp, int, FilePath]]
    ) -> List[Tuple[Timestamp, int, FilePath]]:
        """Sort files by priority (ascending) and then by timestamp (ascending, oldest first)."""
        return sorted(files_with_info, key=lambda x: (x[1], x[0]))

    def _remove_files_until_under_limit(
        self,
        sorted_files: List[Tuple[Timestamp, int, FilePath]],
        total_size: int,
        max_bytes: int,
    ) -> int:
        """Remove files until we're under the size limit."""
        removed_size = 0

        for ts, priority, file_path in sorted_files:
            if total_size - removed_size <= max_bytes:
                break  # We're now under the limit

            if self.graceful_exit.should_exit():
                break

            removed_size = self._remove_file_for_cleanup(
                file_path, total_size, max_bytes, removed_size
            )

        return removed_size

    def _log_cleanup_results(
        self, total_size: int, max_bytes: int, removed_size: int
    ) -> None:
        """Log the results of the cleanup operation."""
        self.logger.info(
            f"Size-based cleanup completed. Removed {removed_size} bytes. Current size: {total_size - removed_size} bytes"
        )

    def _output_path(self, input_file: FilePath, timestamp: Timestamp) -> FilePath:
        """Generate output path for archived file"""
        return (
            self.config.output
            / str(timestamp.year)
            / f"{timestamp.month:02d}"
            / f"{timestamp.day:02d}"
            / f"archived-{timestamp.strftime('%Y%m%d%H%M%S')}.mp4"
        )
