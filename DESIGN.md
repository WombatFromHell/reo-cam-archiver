# Camera Archiver Design Document

## Overview

The Camera Archiver is a Python application designed to transcode and archive camera footage based on timestamp parsing, with intelligent cleanup based on size and age thresholds. The system processes video files from a structured directory hierarchy, transcodes them to a standardized format, and manages the lifecycle of both source and archived files.

## Architecture

The application follows a modular architecture with clear separation of concerns, making it maintainable and testable. The main components work together in a pipeline to discover, process, and manage camera footage files.

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Config        │    │   Logger        │    │ GracefulExit    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ FileDiscovery   │    │ ProgressReporter│    │ ThreadSafe      │
│                 │    │                 │    │ StreamHandler   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FileManager   │    │   Transcoder    │    │  FileProcessor  │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Main Pipeline │
                    └─────────────────┘
```

## Core Components

### Config

The `Config` class is responsible for parsing and storing configuration from command-line arguments. It provides a centralized configuration object with strict typing for all parameters.

**Key Responsibilities:**

- Parse command-line arguments
- Provide default values for optional parameters
- Validate configuration values
- Store configuration in a structured format

**Key Configuration Options:**

- Input directory (default: `/camera`)
- Output directory (default: `{input}/archived`)
- Trash directory (default: `{input}/.deleted`)
- Age threshold for cleanup (default: 30 days)
- Operation modes (dry-run, cleanup, delete, etc.)

### GracefulExit

The `GracefulExit` class provides thread-safe handling of exit signals, allowing the application to terminate cleanly when interrupted.

**Key Responsibilities:**

- Track exit requests in a thread-safe manner
- Provide a centralized mechanism for checking exit status
- Ensure clean termination of all operations

### ProgressReporter

The `ProgressReporter` class provides visual feedback on the progress of file operations, with support for both TTY and non-TTY environments.

**Key Responsibilities:**

- Display progress bars for file operations
- Calculate and display elapsed time
- Handle graceful exit during progress reporting
- Provide context manager support for clean initialization and cleanup

### Logger

The `Logger` class sets up thread-safe logging with rotation, ensuring that log files don't grow indefinitely.

**Key Responsibilities:**

- Configure logging handlers (file and console)
- Implement log file rotation based on size
- Provide thread-safe logging output
- Handle exceptions in logging operations

### FileDiscovery

The `FileDiscovery` class is responsible for finding camera files with valid timestamps in the directory structure.

**Key Responsibilities:**

- Scan directory hierarchy for camera files
- Parse timestamps from filenames
- Build mappings between related files (MP4 and JPG pairs)
- Handle different directory structures (input, output, trash)

**Directory Structure:**

```
/camera/
├── YYYY/
│   ├── MM/
│   │   ├── DD/
│   │   │   ├── REO_camera_YYYYMMDDHHMMSS.mp4
│   │   │   └── REO_camera_YYYYMMDDHHMMSS.jpg
```

### FileManager

The `FileManager` class handles file operations including removal, trash management, and directory cleanup.

**Key Responsibilities:**

- Remove files either permanently or by moving to trash
- Calculate appropriate trash destinations
- Clean empty directories
- Handle file operation errors gracefully

### Transcoder

The `Transcoder` class handles video transcoding using ffmpeg with QSV hardware acceleration.

**Key Responsibilities:**

- Execute ffmpeg transcoding processes
- Monitor transcoding progress
- Handle transcoding errors
- Support graceful exit during transcoding

**Transcoding Pipeline:**

1. Get video duration using ffprobe
2. Execute ffmpeg with QSV hardware acceleration
3. Monitor progress and update callbacks
4. Handle completion or errors

### FileProcessor

The `FileProcessor` class coordinates the overall file processing workflow, from planning to execution.

**Key Responsibilities:**

- Generate action plans for file operations
- Execute transcoding and removal operations
- Handle cleanup of orphaned files
- Manage the overall processing pipeline

## Data Flow

The application follows a structured pipeline for processing camera footage:

1. **Discovery Phase**
   - Scan input directory for camera files
   - Parse timestamps from filenames
   - Build mappings between related files
   - Optionally scan output and trash directories

2. **Planning Phase**
   - Generate action plans based on configuration
   - Apply age and size filters
   - Determine which files need transcoding
   - Plan removal of source files

3. **Execution Phase**
   - Execute transcoding operations
   - Monitor progress
   - Remove source files after successful transcoding
   - Handle errors gracefully

4. **Cleanup Phase**
   - Remove orphaned files
   - Clean empty directories
   - Perform additional cleanup if requested

## Configuration

The application is configured through command-line arguments, with sensible defaults for most options:

### Basic Configuration

- `directory`: Input directory containing camera footage (default: `/camera`)
- `output`: Output directory for archived footage (default: `{input}/archived`)
- `log-file`: Path to log file (default: `{input}/archiver.log`)

### Operation Modes

- `dry-run`: Show what would be done without executing
- `cleanup`: Clean up old files based on age and size
- `clean-output`: Also clean output directory during cleanup
- `delete`: Permanently delete files instead of moving to trash

### Behavior Options

- `no-confirm`: Skip confirmation prompts
- `no-skip`: Don't skip files that already have archives
- `age`: Age in days for cleanup (default: 30)
- `trash-root`: Root directory for trash (default: `{input}/.deleted`)

## Error Handling

The application implements comprehensive error handling at multiple levels:

1. **File Operation Errors**
   - Graceful handling of missing files
   - Proper cleanup of partial operations
   - Detailed error logging

2. **Transcoding Errors**
   - Detection of ffmpeg failures
   - Cleanup of partial output files
   - Detailed error reporting

3. **System Errors**
   - Handling of permission issues
   - Graceful handling of disk space issues
   - Recovery from transient errors

4. **Signal Handling**
   - Clean termination on SIGINT, SIGTERM, and SIGHUP
   - Proper cleanup of resources
   - Preservation of data integrity

## Testing Strategy

The test suite follows a comprehensive testing strategy with multiple test categories:

1. **Unit Tests**
   - Testing individual components in isolation
   - Mocking external dependencies
   - Verifying correct behavior with various inputs

2. **Integration Tests**
   - Testing component interactions
   - Verifying end-to-end workflows
   - Testing with realistic file structures

3. **Edge Case Tests**
   - Testing error conditions
   - Verifying behavior with unusual inputs
   - Testing resource exhaustion scenarios

4. **Performance Tests**
   - Testing with large file sets
   - Verifying memory usage
   - Testing concurrent operations

### Testing Considerations

Our test design utilizes the 'pytest' and 'pytest-mock' modules to reduce boilerplate through the user of test parametrization, 'mocker' and 'monkeypatch' utility functions, and a deliberate decision to avoid the use of the 'unittest' module when designing and building out tests. Tests should be categorized and split into separate files based on their position on the testing pyramid: 'test_e2e.py' for end-to-end tests, 'test_integrations.py' for integration tests, 'test_units.py' for unit tests, and 'conftest.py' for fixtures and utility functions used throughout the test suite.

## Thread Safety

The application is designed to be thread-safe in several key areas:

1. **Logging**
   - Thread-safe logging handlers
   - Coordinated output with progress reporting
   - Proper synchronization of log writes

2. **Exit Handling**
   - Thread-safe exit flag
   - Proper synchronization across threads
   - Clean termination of all operations

3. **Progress Reporting**
   - Thread-safe progress updates
   - Coordinated output with logging
   - Proper handling of concurrent updates

## Future Considerations

1. **Performance Enhancements**
   - Parallel processing of multiple files
   - Optimized transcoding pipelines
   - Improved memory usage for large file sets

2. **Feature Enhancements**
   - Support for additional video formats
   - More sophisticated file organization
   - Advanced filtering options

3. **Monitoring and Observability**
   - Metrics collection and reporting
   - Integration with monitoring systems
   - Enhanced logging and debugging capabilities

4. **Scalability**
   - Distributed processing capabilities
   - Cloud storage integration
   - Handling of larger file sets

## Security Considerations

1. **File System Access**
   - Proper validation of file paths
   - Prevention of directory traversal attacks
   - Secure handling of temporary files

2. **Process Execution**
   - Validation of ffmpeg parameters
   - Prevention of command injection
   - Secure handling of subprocess execution

3. **Data Protection**
   - Secure handling of sensitive footage
   - Proper cleanup of temporary data
   - Access control for archived footage

## Conclusion

The Camera Archiver is designed with a focus on reliability, maintainability, and performance. Its modular architecture allows for easy extension and modification, while comprehensive error handling ensures data integrity throughout the processing pipeline. The test suite provides confidence in the system's behavior across a wide range of scenarios and edge cases.
