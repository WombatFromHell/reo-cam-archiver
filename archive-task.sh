#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /camera/archiver.pyz --older-than 5 --max-size 500GB --no-skip -y
