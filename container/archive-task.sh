#!/usr/bin/env bash
exec /app/reo-archiver.sh --archive --age 7 --no-skip --execute "$@"
