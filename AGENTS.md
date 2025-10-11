# AGENTS.md

## Overview

The Camera Archiver is a Python application designed to automatically transcode and archive camera footage based on timestamp parsing. It intelligently manages storage by transcoding videos to a smaller format and cleaning up old files based on size and age thresholds. The application supports dry-run mode, graceful shutdown, and configurable trash management.

## Commands

- To run the full test suite with code coverage: `uv run slipcover -m pytest -v ./test_archiver.py`
- To run just the test suite: `uv run pytest -v ./test_archiver.py`
- Use `ruff check ./archiver.py` and `pyright ./archiver.py` for syntax, type checking, and linting
- Use `ruff format ./archiver.py` to ensure standardized formatting

## Code Style

- Avoid the use of decorators on functions/classes where possible (test suite decorators are okay)

## High-Level Architecture

The system is organized into distinct, single-responsibility classes that work together in a pipeline:

```
[User Input] -> [Config] -> [FileDiscovery] -> [FileProcessor] -> [Transcoder/FileManager] -> [Cleanup]
```

### Core Components

#### 1. Config

- **Purpose**: Manages command-line arguments and application configuration
- **Responsibilities**: Parse and store user input, validate paths, manage flags (dry-run, delete, etc.)
- **Dependencies**: argparse, Path objects

#### 2. FileDiscovery

- **Purpose**: Discovers camera files matching timestamp-based naming patterns
- **Responsibilities**:
  - Recursively scan directories for video files (MP4) and metadata (JPG)
  - Parse timestamps from filenames (e.g., `REO_..._YYYYMMDDHHMMSS.mp4`)
  - Map files by timestamp for pairing MP4/JPG files
  - Include trash files in discovery when applicable
- **Dependencies**: Path, re, datetime

#### 3. FileProcessor

- **Purpose**: Orchestrates the archiving pipeline and manages execution
- **Responsibilities**:
  - Generate action plans (transcoding, removals)
  - Execute plans with progress reporting
  - Handle cleanup operations for orphaned files
- **Dependencies**: Config, Logger, Transcoder, FileManager, GracefulExit

#### 4. Transcoder

- **Purpose**: Handles video transcoding operations using ffmpeg
- **Responsibilities**:
  - Execute ffmpeg processes with QSV hardware acceleration
  - Monitor transcoding progress and report to UI
  - Extract video duration for progress calculations
  - Handle process lifecycle and graceful termination
- **Dependencies**: subprocess, shutil, re, Path

#### 5. FileManager

- **Purpose**: Manages file system operations including deletion and trash
- **Responsibilities**:
  - Remove files with optional trash management
  - Calculate trash destinations to prevent conflicts
  - Clean empty date-structured directories
- **Dependencies**: Path, shutil, os, time

#### 6. ProgressReporter

- **Purpose**: Provides real-time progress updates to the user
- **Responsibilities**:
  - Track progress of transcoding operations
  - Render progress bars to stderr
  - Coordinate with logging to avoid output conflicts
  - Support graceful exit during operations
- **Dependencies**: threading, time, sys

#### 7. Logger

- **Purpose**: Sets up application logging with rotation
- **Responsibilities**:
  - Configure file and console logging
  - Handle log rotation based on file size
  - Provide thread-safe logging output
- **Dependencies**: logging, Path, shutil

#### 8. GracefulExit

- **Purpose**: Manages application shutdown signals
- **Responsibilities**:
  - Handle SIGINT, SIGTERM, SIGHUP signals
  - Coordinate graceful shutdown across components
  - Signal cancellation to long-running operations
- **Dependencies**: signal, threading

## Component Interactions

### Discovery Phase

1. `FileDiscovery` scans input directory and optional trash/output directories
2. Discovers MP4 files with valid timestamps in expected date-based directory structure
3. Maps files by timestamp to enable MP4/JPG pairing
4. Returns list of MP4 files, timestamp mapping, and trash files

### Planning Phase

1. `FileProcessor` receives discovery results
2. Generates action plan with two categories:
   - Transcoding actions: input MP4 → transcoded output
   - Removal actions: source files, paired JPGs, or cleanup-only entries
3. Considers age thresholds, existing archives, and skip settings

### Execution Phase

1. `FileProcessor` executes transcoding actions via `Transcoder`
2. Each successful transcoding triggers source file removal via `FileManager`
3. Progress is reported through `ProgressReporter`
4. Remaining removal actions are executed after transcoding
5. Cleanup operations remove orphaned files and empty directories

## Standard Workflows

### 1. Basic Archiving Workflow

```
Input: Directory path
1. Parse command-line arguments into Config
2. Discover files using FileDiscovery
3. Generate action plan using FileProcessor
4. Display plan and request confirmation (unless --no-confirm)
5. Execute transcoding and removal actions
6. Report progress in real-time
7. Complete with cleanup if requested
```

### 2. Cleanup-Only Workflow

```
Input: --cleanup flag with age threshold
1. Execute discovery phase
2. Generate removal-only action plan
3. Execute removal actions without transcoding
4. Clean orphaned files and empty directories
```

### 3. Dry Run Workflow

```
Input: --dry-run flag
1. Execute discovery phase
2. Generate full action plan
3. Display plan without requesting confirmation
4. Execute plan with dry-run mode (no actual file changes)
5. Log what would have been performed
```

### 4. Trash Management Workflow

```
Default behavior:
1. Files marked for deletion are moved to trash directory
2. Trash destination calculated to prevent conflicts
3. Original file path structure preserved in trash
4. Trash can be disabled with --delete flag for permanent removal
```

## Key Features

### Hardware Acceleration

- Uses QSV (Intel Quick Sync Video) hardware acceleration via ffmpeg
- Maintains good performance while reducing CPU load

### Thread Safety

- Global OUTPUT_LOCK coordinates progress updates and logging
- ThreadSafeStreamHandler prevents output conflicts
- GracefulExit supports cancellation during operations

### Signal Handling

- Responds to SIGINT (Ctrl+C), SIGTERM, SIGHUP for graceful shutdown
- Cancels long-running operations when shutdown is requested
- Cleans up resources before termination

### File Organization

- Maintains date-based directory structure (YYYY/MM/DD)
- Predictable archived filenames (archived-YYYYMMDDHHMMSS.mp4)
- Automatic cleanup of empty date directories

### Trash Management with Directory Preservation

- Files are moved to trash with their original directory structure preserved
- Source root directory is properly passed to removal functions to maintain path integrity
- Uses subdirectories (`input` and `output`) within trash to separate file types

### Error Recovery

- Robust error handling in subprocess execution
- Process monitoring with timeout and force-kill capabilities
- Logging of error details for debugging

## Known Issues and Fixes

### Directory Structure Preservation in Trash

- **Issue**: Previously, when files were moved to trash, they weren't preserving the full input directory structure (e.g., `/camera/<YYYY>/<MM>/<DD>/...`).
- **Root Cause**: The `source_root` parameter was incorrectly being set to `file_path.parent` instead of the main directory root when calling `FileManager.remove_file`.
- **Fix**: Updated all calls to `FileManager.remove_file` to pass the correct `source_root` (either `config.directory` for input files or `config.output` for output files) to ensure the full directory structure is preserved in trash.

## Testing Methodologies

### Testing Design Philosophy

- **Primary Focus**: Integration and end-to-end tests that verify the complete workflow
- **Secondary Focus**: Unit tests for specific functions/classes that are difficult to cover through integration tests
- Tertiary Focus: Written tests should not overlap with existing tests or testing concerns, if a newly written test can be extended from an existing test with a similar concern (by using 'parameterization', 'mocker', or 'monkeypatch' features of pytest/pytest-mock) this should always be preferred over a separate test

### Testing Tools and Features

- **pytest Parameterization**: Extensive use of `@pytest.mark.parametrize` to test multiple scenarios with different input combinations
- **pytest-mock Integration**: Leverages the `mocker` fixture for creating mocks and `monkeypatch` for modifying behavior during tests
- **Reusable Test Fixtures**: Uses pytest fixtures like `tmp_path` for creating temporary directories for test isolation
- **Comprehensive Coverage**: Tests cover various scenarios including edge cases, error conditions, and different configuration combinations
- Code Coverage Focus: Code coverage reports should be run before and after 'archiver.py' is significantly changed to ensure the code coverage is not negatively impacted

### Test Organization

- Tests are organized by class corresponding to each main component in archiver.py
- Each test method follows the pattern of setting up mocks, executing the code under test, and asserting expected outcomes
- Integration tests validate the interaction between multiple components
- Special attention given to testing thread safety and signal handling scenarios
