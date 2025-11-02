# Camera Archiver Design Document

## Overview

Our camera archiver is a Python application that discovers, transcodes, and archives Reolink camera footage, that has been uploaded to an FTP server directory, based on timestamp parsing, with intelligent cleanup based on size and age thresholds. It uses ffmpeg with QSV (Intel) hardware encoding acceleration (for use with NAS devices that use common Intel chips) for video transcoding and provides a robust file management system with trash support.

## Architecture

```mermaid
graph TB
    subgraph "Main Application"
        main[main function] --> parse_args[parse_args]
        main --> run_archiver[run_archiver]
        run_archiver --> Config[Config]
        run_archiver --> Logger[Logger]
        run_archiver --> GracefulExit[GracefulExit]
        run_archiver --> FileDiscovery[FileDiscovery]
        run_archiver --> FileProcessor[FileProcessor]
        run_archiver --> ProgressReporter[ProgressReporter]
    end

    subgraph "Core Components"
        FileProcessor --> Transcoder[Transcoder]
        FileProcessor --> FileManager[FileManager]
        Transcoder --> ffmpeg[ffmpeg subprocess]
        FileManager --> trash[Trash Management]
    end

    subgraph "Utilities"
        Logger --> ThreadSafeStreamHandler[ThreadSafeStreamHandler]
        GracefulExit --> signal_handlers[Signal Handlers]
    end
```

## System Components

### Config

Configuration holder that processes command-line arguments and provides a centralized configuration object.

### FileDiscovery

Discovers camera files with valid timestamps in the expected directory structure (`/camera/YYYY/MM/DD/*.*`). It also scans trash and output directories when needed.

### Transcoder

Handles video transcoding using ffmpeg with QSV hardware acceleration. It provides progress callbacks and supports graceful interruption.

### FileManager

Manages file operations including moving to trash, permanent deletion, and cleaning empty directories.

### FileProcessor

Orchestrates the file processing workflow, generating action plans and executing them.

### ProgressReporter

Provides thread-safe progress reporting with time estimates and visual progress bars.

### Logger

Sets up logging with rotation support and thread-safe console output.

### GracefulExit

Handles graceful shutdown when signals are received.

## Workflow

```mermaid
flowchart TD
    Start([Start]) --> ParseArgs[Parse Arguments]
    ParseArgs --> SetupConfig[Setup Configuration]
    SetupConfig --> SetupLogger[Setup Logger]
    SetupLogger --> SetupGracefulExit[Setup Graceful Exit]
    SetupGracefulExit --> DiscoverFiles[Discover Files]
    DiscoverFiles --> NoFiles{No Files Found?}
    NoFiles -->|Yes| End([End])
    NoFiles -->|No| GeneratePlan[Generate Action Plan]
    GeneratePlan --> DisplayPlan[Display Plan]
    DisplayPlan --> DryRun{Dry Run Mode?}
    DryRun -->|Yes| ExecuteDryRun[Execute Plan in Dry Run]
    ExecuteDryRun --> End
    DryRun -->|No| ConfirmPlan{User Confirms?}
    ConfirmPlan -->|No| End
    ConfirmPlan -->|Yes| ExecutePlan[Execute Plan]
    ExecutePlan --> Cleanup{Cleanup Enabled?}
    Cleanup -->|Yes| CleanupFiles[Cleanup Files]
    Cleanup -->|No| End
    CleanupFiles --> End
```

## File Processing Workflow

```mermaid
flowchart TD
    Start([Start Processing]) --> ForEachFile[For Each File]
    ForEachFile --> Transcode{Transcode Needed?}
    Transcode -->|No| SkipToRemoval[Skip to Removal]
    Transcode -->|Yes| StartTranscode[Start Transcoding]
    StartTranscode --> UpdateProgress[Update Progress]
    UpdateProgress --> TranscodeSuccess{Transcode Success?}
    TranscodeSuccess -->|No| LogError[Log Error]
    LogError --> NextFile[Next File]
    TranscodeSuccess -->|Yes| RemoveJPG[Remove Paired JPG]
    RemoveJPG --> RemoveSource[Remove Source File]
    RemoveSource --> NextFile
    SkipToRemoval --> RemoveFile[Remove File]
    RemoveFile --> NextFile
    NextFile --> MoreFiles{More Files?}
    MoreFiles -->|Yes| ForEachFile
    MoreFiles -->|No| CleanupOrphans[Cleanup Orphaned Files]
    CleanupOrphans --> CleanEmptyDirs[Clean Empty Directories]
    CleanEmptyDirs --> End([End Processing])
```

## Test Design

### Test Structure

```mermaid
graph TB
    subgraph "Test Suite"
        UnitTests[Unit Tests<br/>test_units.py]
        IntegrationTests[Integration Tests<br/>test_integrations.py]
        E2ETests[End-to-End Tests<br/>test_e2e.py]
    end

    subgraph "Test Fixtures"
        conftest[conftest.py<br/>Fixtures and Utilities]
        conftest --> temp_dir[temp_dir]
        conftest --> camera_dir[camera_dir]
        conftest --> archived_dir[archived_dir]
        conftest --> trash_dir[trash_dir]
        conftest --> sample_files[sample_files]
        conftest --> mock_args[mock_args]
        conftest --> mock_transcode_success[mock_transcode_success]
        conftest --> mock_transcode_fail[mock_transcode_fail]
        conftest --> mock_transcode_interrupt[mock_transcode_interrupt]
    end

    UnitTests --> conftest
    IntegrationTests --> conftest
    E2ETests --> conftest
```

### Testing Framework

The Camera Archiver uses pytest and pytest-mock for comprehensive testing:

```mermaid
graph TB
    subgraph "Testing Framework"
        pytest[pytest]
        pytest_mock[pytest-mock]
        parametrize[pytest.mark.parametrize]
        fixtures[pytest fixtures]
        mocker[mocker fixture]
    end

    subgraph "Test Types"
        unit[Unit Tests]
        integration[Integration Tests]
        e2e[End-to-End Tests]
    end

    pytest --> unit
    pytest --> integration
    pytest --> e2e
    pytest_mock --> mocker
    pytest --> fixtures
    pytest --> parametrize
```

### Test Fixtures

The test suite relies heavily on fixtures defined in `conftest.py`:

```mermaid
graph TB
    subgraph "Fixture Hierarchy"
        temp_dir[temp_dir<br/>Temporary directory]
        camera_dir[camera_dir<br/>Based on temp_dir]
        archived_dir[archived_dir<br/>Based on temp_dir]
        trash_dir[trash_dir<br/>Based on temp_dir]
        sample_files[sample_files<br/>Based on camera_dir]
        logger[logger<br/>Based on temp_dir]
        mock_args[mock_args<br/>Standalone]
        config[config<br/>Based on mock_args]
        graceful_exit[graceful_exit<br/>Standalone]
        mock_transcode_success[mock_transcode_success<br/>Session scope]
        mock_transcode_fail[mock_transcode_fail<br/>Session scope]
        mock_transcode_interrupt[mock_transcode_interrupt<br/>Session scope]
    end

    temp_dir --> camera_dir
    temp_dir --> archived_dir
    temp_dir --> trash_dir
    camera_dir --> sample_files
    temp_dir --> logger
    mock_args --> config
```

### Parametrization Strategy

The test suite uses parametrization to test multiple scenarios efficiently:

```mermaid
graph TB
    subgraph "Parametrization Examples"
        ConfigParams[Config parameter combinations]
        TimestampParams[Timestamp parsing scenarios]
        TranscodeParams[Transcoding outcomes]
        FileRemovalParams[File removal modes]
    end

    subgraph "Parametrization Benefits"
        ReducedDuplication[Reduced test duplication]
        ComprehensiveCoverage[Comprehensive scenario coverage]
        ClearIntent[Clear test intent]
        Maintainability[Easier maintenance]
    end

    ConfigParams --> ReducedDuplication
    TimestampParams --> ComprehensiveCoverage
    TranscodeParams --> ClearIntent
    FileRemovalParams --> Maintainability
```

### Mocking Strategy

The test suite uses pytest-mock for mocking external dependencies:

```mermaid
graph TB
    subgraph "Mocking Targets"
        Subprocess[subprocess.Popen]
        FileOperations[shutil.move, Path.unlink]
        SystemCalls[os.kill, signal.signal]
        ExternalTools[ffmpeg, ffprobe]
    end

    subgraph "Mocking Techniques"
        SideEffects[Side effects for error conditions]
        ReturnValues[Controlled return values]
        Spies[Spies on method calls]
        AutoMocking[Auto-mocking of dependencies]
    end

    Subprocess --> SideEffects
    FileOperations --> ReturnValues
    SystemCalls --> Spies
    ExternalTools --> AutoMocking
```

### Test Organization

The test suite is organized into three main categories:

1. **Unit Tests** (`test_units.py`):
   - Test individual components in isolation
   - Heavy use of mocking to isolate components
   - Extensive parametrization for edge cases
   - Focus on business logic and error handling

2. **Integration Tests** (`test_integrations.py`):
   - Test component interactions
   - Limited mocking to preserve real interactions
   - Focus on data flow between components
   - Test error propagation across components

3. **End-to-End Tests** (`test_e2e.py`):
   - Test complete workflows
   - Minimal mocking to preserve real behavior
   - Focus on user-facing functionality
   - Test system-level error handling

### Test Implementation Guidelines

1. **Fixture Usage**:
   - Use fixtures from `conftest.py` whenever possible
   - Create composable fixtures that build on each other
   - Use appropriate fixture scopes (function, session)
   - Leverage auto-use fixtures for common setup

2. **Parametrization**:
   - Use `pytest.mark.parametrize` for testing multiple scenarios
   - Group related parameters together
   - Use descriptive parameter IDs
   - Consider using `pytest.mark.parametrize` for error cases

3. **Mocking**:
   - Use the `mocker` fixture from pytest-mock
   - Mock at the appropriate level (method vs. module)
   - Use side effects for error conditions
   - Verify mock calls when testing interactions

4. **Assertion Strategy**:
   - Use specific assertions with clear messages
   - Test both positive and negative cases
   - Verify state changes and side effects
   - Use pytest's built-in assertion introspection

5. **Test Organization**:
   - Group related tests in classes
   - Use descriptive test method names
   - Document complex test scenarios
   - Keep tests focused and independent

### Test Coverage Strategy

```mermaid
graph TB
    subgraph "Coverage Areas"
        HappyPath[Happy path scenarios]
        ErrorHandling[Error handling paths]
        EdgeCases[Edge cases and boundary conditions]
        IntegrationPoints[Component integration points]
    end

    subgraph "Coverage Techniques"
        StatementCoverage[Statement coverage]
        BranchCoverage[Branch coverage]
        PathCoverage[Path coverage]
        MutationTesting[Mutation testing]
    end

    HappyPath --> StatementCoverage
    ErrorHandling --> BranchCoverage
    EdgeCases --> PathCoverage
    IntegrationPoints --> MutationTesting
```

## Error Handling

The system implements comprehensive error handling at multiple levels:

1. **File Operations**: Handles missing files, permission errors, and disk space issues
2. **Transcoding**: Handles ffmpeg errors, missing dependencies, and hardware acceleration failures
3. **Signal Handling**: Gracefully handles SIGINT, SIGTERM, and SIGHUP
4. **Logging**: Handles log rotation errors and file permission issues

## Configuration

The system accepts the following command-line arguments:

- `directory`: Input directory containing camera footage (default: /camera)
- `-o, --output`: Output directory for archived footage
- `--dry-run`: Show what would be done without executing
- `-y, --no-confirm`: Skip confirmation prompts
- `--no-skip`: Don't skip files that already have archives
- `--delete`: Permanently delete files instead of moving to trash
- `--trash-root`: Root directory for trash
- `--cleanup`: Clean up old files based on age and size
- `--clean-output`: Also clean output directory during cleanup
- `--age`: Age in days for cleanup (default: 30)
- `--log-file`: Log file path

## Dependencies

- Python 3.7+
- pytest and pytest-mock for testing
- ffmpeg with QSV hardware acceleration support
- Standard library modules: argparse, logging, os, re, shutil, signal, subprocess, sys, threading, time, datetime, pathlib, typing
