#!/usr/bin/env bash
PYTHON=$(command -v python3)
exec "$PYTHON" /app/archiver.pyz --older-than 7 --max-size 1TB --no-skip -y "$@"
