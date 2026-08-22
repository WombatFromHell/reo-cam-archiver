#!/usr/bin/env bash
/app/reo-archiver.sh --age "${CLEANUP_MAX_AGE:-7}" --execute "$@"
