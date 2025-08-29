#!/usr/bin/env bash
/usr/local/bin/supercronic /root/99archival >/var/log/cron.log 2>&1 &
tail -f /dev/null # stay alive
