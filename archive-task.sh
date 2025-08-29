#!/bin/sh
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.py --age 3 --no-skip
