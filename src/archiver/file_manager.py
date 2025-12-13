"""
File management operations for the Camera Archiver application.
"""

import os
import shutil
import time
from pathlib import Path
from typing import Optional

from .utils import FilePath


class FileManager:
    """Handles file operations with strict typing"""

    @staticmethod
    def _calculate_trash_subdirectory(is_output: bool) -> str:
        """Calculate the trash subdirectory based on whether it's an output file.

        Args:
            is_output: True if the file is from the output directory, False otherwise

        Returns:
            Subdirectory name ('output' or 'input')
        """
        return "output" if is_output else "input"

    @staticmethod
    def remove_file(
        file_path: FilePath,
        logger,
        dry_run: bool = False,
        delete: bool = False,
        trash_root: Optional[FilePath] = None,
        is_output: bool = False,
        source_root: Optional[FilePath] = None,
    ) -> None:
        """Remove a file, optionally moving to trash"""
        if dry_run:
            logger.info(f"[DRY RUN] Would remove {file_path}")
            return

        try:
            FileManager._remove_file_with_strategy(
                file_path, logger, delete, trash_root, is_output, source_root
            )
        except FileNotFoundError:
            logger.debug(f"File already removed (during cleanup): {file_path}")
        except OSError as e:
            logger.error(f"Failed to remove {file_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error removing {file_path}: {e}")

    @staticmethod
    def _remove_file_with_strategy(
        file_path: FilePath,
        logger,
        delete: bool,
        trash_root: Optional[FilePath],
        is_output: bool,
        source_root: Optional[FilePath],
    ) -> None:
        """Remove file using appropriate strategy (trash or delete)"""
        if source_root is None:
            source_root = file_path.parent

        if not delete and trash_root:
            FileManager._move_to_trash(file_path, source_root, trash_root, is_output, logger)
        else:
            FileManager._delete_file(file_path, logger)

    @staticmethod
    def _move_to_trash(
        file_path: FilePath,
        source_root: FilePath,
        trash_root: FilePath,
        is_output: bool,
        logger,
    ) -> None:
        """Move file to trash directory"""
        new_dest = FileManager._calculate_trash_destination(
            file_path, source_root, trash_root, is_output
        )
        new_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file_path), str(new_dest))
        logger.info(f"Moved to trash: {file_path} -> {new_dest}")

    @staticmethod
    def _delete_file(file_path: FilePath, logger) -> None:
        """Delete file permanently"""
        if file_path.is_file():
            file_path.unlink()
        elif file_path.is_dir():
            file_path.rmdir()
        else:
            logger.warning(f"Unsupported file type for removal: {file_path}")
        logger.info(f"Removed: {file_path}")

    @staticmethod
    def _calculate_trash_destination(
        file_path: FilePath,
        source_root: FilePath,
        trash_root: FilePath,
        is_output: bool = False,
    ) -> FilePath:
        """Calculate the destination path in trash for a given file"""
        dest_sub = FileManager._calculate_trash_subdirectory(is_output)
        rel_path = FileManager._get_relative_path_without_double_nesting(file_path, source_root)
        base_dest = trash_root / dest_sub / rel_path
        return FileManager._resolve_unique_destination(base_dest)

    @staticmethod
    def _get_relative_path_without_double_nesting(
        file_path: FilePath, source_root: FilePath
    ) -> FilePath:
        """Get relative path while preventing double nesting in trash directories"""
        try:
            rel_path = file_path.relative_to(source_root)
        except ValueError:
            # If file_path is not relative to source_root, use just the filename
            return Path(file_path.name)
        
        return FileManager._remove_trash_prefix_if_present(rel_path)

    @staticmethod
    def _remove_trash_prefix_if_present(rel_path: FilePath) -> FilePath:
        """Remove trash directory prefix if present to avoid double nesting"""
        rel_parts = rel_path.parts
        if (
            len(rel_parts) >= 2
            and rel_parts[0] == ".deleted"
            and rel_parts[1] in ("input", "output")
        ):
            # The file is already in trash structure. Remove the ".deleted/input" or
            # ".deleted/output" prefix to avoid double nesting
            return Path(*rel_parts[2:]) if len(rel_parts) > 2 else Path(rel_path.name)
        return rel_path

    @staticmethod
    def _resolve_unique_destination(base_dest: FilePath) -> FilePath:
        """Resolve a unique destination path by adding timestamp suffix if needed"""
        counter = 0
        new_dest = base_dest

        while new_dest.exists():
            counter += 1
            suffix = f"_{int(time.time())}_{counter}"
            stem = new_dest.stem + suffix
            new_dest = new_dest.parent / (stem + new_dest.suffix)

        return new_dest

    @staticmethod
    def clean_empty_directories(
        directory: FilePath, logger, dry_run: bool = False
    ) -> None:
        """Remove empty date-structured directories"""
        for dirpath, dirs, files in os.walk(directory, topdown=False):
            p = Path(dirpath)
            if p == directory:
                continue

            FileManager._clean_empty_directory_if_applicable(p, logger, dry_run)

    @staticmethod
    def _clean_empty_directory_if_applicable(
        directory_path: FilePath, logger, dry_run: bool
    ) -> None:
        """Clean empty directory if it meets criteria"""
        try:
            if FileManager._is_directory_empty(directory_path):
                FileManager._remove_empty_directory(directory_path, logger, dry_run)
        except OSError:
            pass

    @staticmethod
    def _is_directory_empty(directory_path: FilePath) -> bool:
        """Check if directory is empty"""
        return not any(directory_path.iterdir())

    @staticmethod
    def _remove_empty_directory(
        directory_path: FilePath, logger, dry_run: bool
    ) -> None:
        """Remove empty directory with appropriate logging"""
        if dry_run:
            logger.info(f"[DRY RUN] Would remove empty directory: {directory_path}")
        else:
            directory_path.rmdir()
            logger.info(f"Removed empty directory: {directory_path}")
