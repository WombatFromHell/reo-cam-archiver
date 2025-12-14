"""Entry point for Camera Archiver application."""

import sys

# Handle both direct execution and zipapp execution
try:
    # Try importing from the archiver package (zipapp bundle)
    from archiver.utils import main
except ImportError:
    # Fallback to importing from src.archiver (development environment)
    from src.archiver.utils import main

if __name__ == "__main__":
    sys.exit(main())
