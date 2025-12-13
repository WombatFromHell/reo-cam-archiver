# Camera Archiver Module
# Main module initialization for the camera archiver functionality

from .config import Config, parse_args
from .discovery import FileDiscovery
from .file_manager import FileManager
from .graceful_exit import GracefulExit, setup_signal_handlers
from .logger import Logger, ThreadSafeStreamHandler
from .processor import FileProcessor
from .progress import ProgressReporter
from .transcoder import Transcoder
from .utils import (
    ACTIVE_PROGRESS_REPORTER,
    DEFAULT_PROGRESS_WIDTH,
    LOG_ROTATION_SIZE,
    MIN_ARCHIVE_SIZE_BYTES,
    OUTPUT_LOCK,
    PROGRESS_UPDATE_INTERVAL,
    ActionItem,
    ActionPlan,
    ActionPlanType,
    DiscoveredFiles,
    FilePath,
    FileSize,
    GenericAction,
    ProgressCallback,
    RemovalAction,
    Timestamp,
    TimestampFileMapping,
    TranscodeAction,
    confirm_plan,
    display_plan,
    main,
    parse_size,
    run_archiver,
)

__all__ = [
    "Config",
    "parse_args",
    "FileDiscovery",
    "Transcoder",
    "FileManager",
    "FileProcessor",
    "ProgressReporter",
    "Logger",
    "ThreadSafeStreamHandler",
    "GracefulExit",
    "setup_signal_handlers",
    "parse_size",
    "display_plan",
    "confirm_plan",
    "run_archiver",
    "main",
    "MIN_ARCHIVE_SIZE_BYTES",
    "DEFAULT_PROGRESS_WIDTH",
    "PROGRESS_UPDATE_INTERVAL",
    "LOG_ROTATION_SIZE",
    "OUTPUT_LOCK",
    "ACTIVE_PROGRESS_REPORTER",
    "FilePath",
    "Timestamp",
    "FileSize",
    "ProgressCallback",
    "DiscoveredFiles",
    "TimestampFileMapping",
    "TranscodeAction",
    "RemovalAction",
    "ActionItem",
    "ActionPlan",
    "ActionPlanType",
    "GenericAction",
]
