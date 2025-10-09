#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.py --cleanup --clean-output --age 14 -y
