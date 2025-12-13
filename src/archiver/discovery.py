"""
File discovery operations for the Camera Archiver application.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .utils import DiscoveredFiles, FilePath, Timestamp, TimestampFileMapping


class FileDiscovery:
    """Handles file discovery operations with strict typing"""

    @staticmethod
    def _validate_directory_structure(rel_parts: tuple) -> bool:
        """Validate that directory structure follows YYYY/MM/DD pattern"""
        try:
            if len(rel_parts) < 4:
                return False

            y, m, d = rel_parts[-4], rel_parts[-3], rel_parts[-2]
            y_int, m_int, d_int = int(y), int(m), int(d)

            return (
                1000 <= y_int <= 9999 and 1 <= m_int <= 12 and 1 <= d_int <= 31
            )
        except (ValueError, AttributeError):
            return False

    @staticmethod
    def _validate_file_type(file_path: FilePath, is_trash: bool) -> bool:
        """Validate that the file is a regular file and handle trash logic."""
        # Skip if not a file
        if not file_path.is_file():
            return False
        
        # File is valid for processing
        return True

    @staticmethod
    def _validate_file_structure(
        file_path: FilePath, base_directory: FilePath, is_trash: bool
    ) -> bool:
        """Validate directory structure for non-trash files."""
        if not is_trash:
            try:
                rel_parts = file_path.relative_to(base_directory).parts
                return FileDiscovery._validate_directory_structure(rel_parts)
            except (ValueError, AttributeError):
                return False
        return True

    @staticmethod
    def _parse_file_timestamp(filename: str, is_output: bool) -> Optional[Timestamp]:
        """Parse timestamp from filename with fallback for archived files."""
        ts = FileDiscovery._parse_timestamp(filename)
        if not ts and is_output:
            # Try parsing from archived files for output directory
            ts = FileDiscovery._parse_timestamp_from_archived_filename(filename)
        return ts

    @staticmethod
    def _update_discovery_results(
        file_path: FilePath,
        ts: Timestamp,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: TimestampFileMapping,
        is_trash: bool,
        trash_files: Optional[Set[FilePath]] = None,
    ) -> None:
        """Update discovery results with file information."""
        # Update results
        key = ts.strftime("%Y%m%d%H%M%S")
        ext = file_path.suffix.lower()
        mapping.setdefault(key, {})[ext] = file_path
        if ext == ".mp4":
            mp4s.append((file_path, ts))
        
        # Add to trash files if applicable
        if is_trash and trash_files is not None:
            trash_files.add(file_path)

    @staticmethod
    def _process_file(
        file_path: FilePath,
        base_directory: FilePath,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: TimestampFileMapping,
        trash_files: Optional[Set[FilePath]] = None,
        is_trash: bool = False,
        is_output: bool = False,
    ) -> None:
        """Process a single file and update discovery results"""
        # Validate file type
        if not FileDiscovery._validate_file_type(file_path, is_trash):
            return

        # Validate directory structure
        if not FileDiscovery._validate_file_structure(file_path, base_directory, is_trash):
            return

        # Parse timestamp
        ts = FileDiscovery._parse_file_timestamp(file_path.name, is_output)
        if not ts:
            return

        # Update discovery results
        FileDiscovery._update_discovery_results(
            file_path, ts, mp4s, mapping, is_trash, trash_files
        )

    @staticmethod
    def _scan_directory(
        directory: FilePath,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: TimestampFileMapping,
        trash_files: Optional[Set[FilePath]] = None,
        is_trash: bool = False,
        is_output: bool = False,
        trash_root: Optional[FilePath] = None,
    ) -> None:
        """Scan a directory and process all valid files"""
        if not directory.exists():
            return

        for file_path in directory.rglob("*.*"):
            FileDiscovery._process_file_if_valid(
                file_path, directory, mp4s, mapping, trash_files, is_trash, is_output, trash_root
            )

    @staticmethod
    def _process_file_if_valid(
        file_path: FilePath,
        directory: FilePath,
        mp4s: List[Tuple[FilePath, Timestamp]],
        mapping: TimestampFileMapping,
        trash_files: Optional[Set[FilePath]],
        is_trash: bool,
        is_output: bool,
        trash_root: Optional[FilePath],
    ) -> None:
        """Process file if it's valid and not in trash directory"""
        # Skip files in trash directory unless we're specifically scanning trash
        if trash_root and not is_trash and trash_root in file_path.parents:
            return

        FileDiscovery._process_file(
            file_path, directory, mp4s, mapping, trash_files, is_trash, is_output
        )

    @staticmethod
    def discover_files(
        directory: FilePath,
        trash_root: Optional[FilePath] = None,
        output_directory: Optional[FilePath] = None,
        clean_output: bool = False,
    ) -> DiscoveredFiles:
        """Discover camera files with valid timestamps"""
        mp4s: List[Tuple[FilePath, Timestamp]] = []
        mapping: TimestampFileMapping = {}
        trash_files: Set[FilePath] = set()

        # Scan base directory
        FileDiscovery._scan_directory(directory, mp4s, mapping, trash_files=None, trash_root=trash_root)

        # Scan output directory if clean_output is specified
        if clean_output and output_directory:
            FileDiscovery._scan_directory(
                output_directory, mp4s, mapping, 
                trash_files=None, is_output=True, trash_root=trash_root
            )

        # Scan trash directory if enabled
        if trash_root:
            for trash_type in ["input", "output"]:
                trash_dir = trash_root / trash_type
                FileDiscovery._scan_directory(
                    trash_dir, mp4s, mapping, 
                    trash_files=trash_files, is_trash=True, trash_root=None
                )

        return mp4s, mapping, trash_files

    @staticmethod
    def _parse_timestamp(filename: str) -> Optional[Timestamp]:
        """Extract timestamp from filename"""
        TIMESTAMP_RE = re.compile(r"REO_.*_(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = TIMESTAMP_RE.search(filename)
        if not m:
            return None

        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None

    @staticmethod
    def _parse_timestamp_from_archived_filename(filename: str) -> Optional[Timestamp]:
        """Extract timestamp from archived filename (e.g., archived-20230115120000.mp4)"""
        # Pattern to match archived files: archived-YYYYMMDDHHMMSS.ext
        ARCHIVED_RE = re.compile(r"archived-(\d{14})\.(mp4|jpg)$", re.IGNORECASE)
        m = ARCHIVED_RE.search(filename)
        if not m:
            return None

        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
            return ts if 2000 <= ts.year <= 2099 else None
        except ValueError:
            return None
