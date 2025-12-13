#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.pyz --age 5 --no-skip -y
