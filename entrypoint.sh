#!/usr/bin/env bash
# ponytail: container runs as PUID:PGID (set via compose `user:`), so supercronic
# and every job (reo-archiver.sh, ffmpeg) execute unprivileged. /dev/dri access
# comes from compose `group_add: RENDER_GROUP`. supercronic passes the container
# env to jobs, so no env-file gymnastics are needed. Log to /data/cron.log, which
# is writable by PUID:PGID through the mounted volume; fall back to /tmp if not.
LOGFILE=/data/cron.log
: >>"$LOGFILE" 2>/dev/null || LOGFILE=/tmp/cron.log
exec /usr/local/bin/supercronic /app/99archival >>"$LOGFILE" 2>&1
