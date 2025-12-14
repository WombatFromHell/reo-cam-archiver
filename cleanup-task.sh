#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.pyz --cleanup --clean-output --older-than 14 --max-size 500GB -y
