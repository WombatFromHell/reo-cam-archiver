# AGENTS.md

## Overview

Actionable tool usage guidelines for agentic tools when working with the `camera-archiver` codebase.

## Development Commands

- `make test` - Run test suite
- `make radon` - Check code complexity
- `make quality` - Run code quality checks
- `make build` - Build zipapp bundle
- `uv run pytest -xvs` - Manually run test suite
- `dist/archiver.pyz --help` - Test zipapp bundle

## New Module Structure

The project has been restructured into modular components under `src/archiver/`:

```bash
src/
├── archiver/
│   ├── __init__.py          # Main module exports
│   ├── config.py            # Configuration management
│   ├── discovery.py         # File discovery operations
│   ├── transcoder.py        # Video transcoding operations
│   ├── file_manager.py      # File management operations
│   ├── processor.py         # File processing operations
│   ├── progress.py          # Progress reporting
│   ├── logger.py            # Logging setup
│   ├── graceful_exit.py     # Graceful exit handling
│   └── utils.py             # Utility functions and constants
├── entry.py                # Entry point for zipapp
└── __init__.py             # Package initialization
```

## Testing with New Structure

All tests have been updated to use the new module structure:

```python
# Import pattern for tests
from src.archiver import Config, FileDiscovery, Transcoder
```

## zipapp Usage

The project now uses zipapp for bundling:

```bash
# Build the bundle
make build

# Run the bundle
dist/archiver.pyz /path/to/camera/files

# Get help
dist/archiver.pyz --help
```
