#!/usr/bin/env bash
/app/reo-archiver.sh --archive --age "${ARCHIVE_MAX_AGE:-5}" --no-skip --execute "$@"
