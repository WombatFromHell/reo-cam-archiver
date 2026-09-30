#!/usr/bin/env bash
/app/reo-archiver.sh --archive --age "${ARCHIVE_MAX_AGE:-5}" --archive-age "${ARCHIVE_AGE:-15}" --no-skip --execute "$@"
