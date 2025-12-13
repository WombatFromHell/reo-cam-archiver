#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.pyz --cleanup --clean-output --age 14 --max-size 500GB -y
