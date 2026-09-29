#!/usr/bin/env bats
# Hermetic smoke tests for reo-archiver.sh (the archive step).
#
# No real ffmpeg: a stub on PATH records each invocation and writes a
# 1 MiB output file (the script's MIN_OUTPUT_SIZE_BYTES threshold). It
# fails on a missing input, like real ffmpeg. Every test runs in its own
# tempdir; nothing outside it is touched.

SCRIPT="./reo-archiver.sh"

setup() {
  TMP="$(mktemp -d)"
  DATA="$TMP/data"
  ARCHIVE="$DATA/archived"
  TRASH="$DATA/.deleted"
  mkdir -p "$DATA" "$TMP/bin"

  export FAKE_FFMPEG_LOG="$TMP/ffmpeg.log"
  export FAKE_FFMPEG_FAIL=0

  cat >"$TMP/bin/ffmpeg" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$FAKE_FFMPEG_LOG"
[[ "${FAKE_FFMPEG_FAIL:-0}" == 1 ]] && exit 1
prev="" input=""
for a in "$@"; do
  [[ "$prev" == "-i" ]] && input="$a"
  prev="$a"
done
[[ -f "$input" ]] || exit 1
if [[ "${FAKE_FFMPEG_PROGRESS:-0}" == 1 ]]; then
  echo "frame= 1 time=00:00:05.000000 speed=1.0x" >&2
  echo "frame= 2 time=00:00:10.000000 speed=1.0x" >&2
fi
out="${@: -1}"
mkdir -p "$(dirname "$out")"
head -c "${FAKE_FFMPEG_SIZE:-1048576}" /dev/zero >"$out"
EOF
  cat >"$TMP/bin/ffprobe" <<'EOF'
#!/usr/bin/env bash
echo 10
EOF
  chmod +x "$TMP/bin/ffmpeg" "$TMP/bin/ffprobe"
  export PATH="$TMP/bin:$PATH"
}

teardown() { rm -rf "$TMP"; }

run_archiver() { bash "$SCRIPT" --dir "$DATA" --no-log "$@"; }

old_ts() { date -d "-10 days" +%Y%m%d%H%M%S; }
new_ts() { date +%Y%m%d%H%M%S; }

archive_out() {
  local ts="$1"
  echo "$ARCHIVE/${ts:0:4}/${ts:4:2}/${ts:6:2}/archived-$ts.mp4"
}

# --- Smoke ---

@test "smoke: --help exits 0 and prints usage" {
  run bash "$SCRIPT" --help
  [[ $status -eq 0 ]]
  [[ "$output" == *"Usage:"* ]]
}

@test "smoke: missing target dir is an error" {
  run bash "$SCRIPT" --dir "$TMP/nope" --no-log
  [[ $status -ne 0 ]]
  [[ "$output" == *"Directory not found"* ]]
}

# --- Collection ---

@test "collection: only timestamped files older than --age are selected" {
  local ts now
  ts="$(old_ts)"
  now="$(new_ts)"
  touch "$DATA/$ts.mp4" "$DATA/$now.mp4" "$DATA/no-timestamp.mp4"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ -f "$(archive_out "$ts")" ]]
  [[ ! -f "$DATA/$ts.mp4" ]]
  [[ -f "$DATA/$now.mp4" ]]
  [[ -f "$DATA/no-timestamp.mp4" ]]
}

# --- PHASE 3: archive core ---

@test "archive: old mp4 is transcoded and original trashed" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ -f "$(archive_out "$ts")" ]]
  [[ -f "$TRASH/input/$ts.mp4" ]]
  [[ ! -f "$DATA/$ts.mp4" ]]
  grep -q -- "-i $DATA/$ts.mp4" "$FAKE_FFMPEG_LOG"
}

@test "archive: existing output >= 1MiB is skipped, original trashed" {
  local ts out
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  out="$(archive_out "$ts")"
  mkdir -p "$(dirname "$out")"
  head -c 1048576 /dev/zero >"$out"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  # skip means "already archived": no ffmpeg, source disposed
  [[ ! -f "$FAKE_FFMPEG_LOG" ]]
  [[ -f "$TRASH/input/$ts.mp4" ]]
  [[ ! -f "$DATA/$ts.mp4" ]]
  [[ "$output" == *"skipping"* ]]
}

@test "archive: --no-skip retranscodes over existing output" {
  local ts out
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  out="$(archive_out "$ts")"
  mkdir -p "$(dirname "$out")"
  head -c 1048576 /dev/zero >"$out"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --no-skip --execute
  [[ $status -eq 0 ]]
  [[ -f "$FAKE_FFMPEG_LOG" ]]
  [[ -f "$TRASH/input/$ts.mp4" ]]
}

@test "archive: old jpg is trashed, not transcoded" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.jpg"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ ! -f "$FAKE_FFMPEG_LOG" ]]
  [[ -f "$TRASH/input/$ts.jpg" ]]
}

@test "delete mode: --no-trash permanently removes old file" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.jpg"
  run run_archiver --trash "$TRASH" --age 5 --no-trash --execute
  [[ $status -eq 0 ]]
  [[ ! -f "$DATA/$ts.jpg" ]]
  [[ ! -d "$TRASH" ]]
}

@test "archive: ffmpeg failure keeps original, leaves no partial output" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  FAKE_FFMPEG_FAIL=1 run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ -f "$DATA/$ts.mp4" ]]
  [[ ! -f "$(archive_out "$ts")" ]]
  [[ "$output" == *"Transcoding failed"* ]]
}

# --- PHASE 1: size limit ---

@test "size limit: archive pass runs before size enforcement (both videos transcoded)" {
  local old1 old2
  old1="$(date -d '-20 days' +%Y%m%d%H%M%S)"
  old2="$(date -d '-10 days' +%Y%m%d%H%M%S)"
  # NOTE: enforcement only counts files inside YYYY/ year dirs
  mkdir -p "$DATA/2024"
  head -c 1000000 /dev/zero >"$DATA/2024/$old1.mp4"
  head -c 1000000 /dev/zero >"$DATA/2024/$old2.mp4"
  MAX_SIZE=1MB run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  # Both old videos are transcoded by the archive pass (PHASE 1) before
  # size enforcement (PHASE 3). The old order trashed them first, so
  # nothing was ever transcoded.
  grep -q -- "-i $DATA/2024/$old1.mp4" "$FAKE_FFMPEG_LOG"
  grep -q -- "-i $DATA/2024/$old2.mp4" "$FAKE_FFMPEG_LOG"
  # Size enforcement still ran and removed files.
  [[ "$output" == *"Size-based cleanup: removed"* ]]
}

@test "regression: over limit, old video is transcoded, not trashed first" {
  local old1
  old1="$(date -d '-20 days' +%Y%m%d%H%M%S)"
  mkdir -p "$DATA/2024"
  # 2MB file at a 1MB limit is OVER the limit, so the size pass triggers.
  # (A 1MB file at a 1MB limit is "within limit" and would never exercise
  # the bug — the size pass only acts when total > MAX_SIZE.)
  head -c 2000000 /dev/zero >"$DATA/2024/$old1.mp4"
  MAX_SIZE=1MB run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  # The video was transcoded by the archive pass, not trashed first by
  # size enforcement (the old buggy order). Fails on the pre-fix code.
  grep -q -- "-i $DATA/2024/$old1.mp4" "$FAKE_FFMPEG_LOG"
}

# --- PHASE 2: trash cleanup ---

@test "trash cleanup: trashed files older than TRASH_AGE are purged" {
  local ts
  ts="$(date -d '-30 days' +%Y%m%d%H%M%S)"
  mkdir -p "$TRASH/input"
  touch "$TRASH/input/$ts.mp4"
  TRASH_AGE=21 run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ ! -f "$TRASH/input/$ts.mp4" ]]
}

# --- Hygiene ---

@test "hygiene: empty directories are removed" {
  mkdir -p "$DATA/2024"
  run run_archiver --age 5 --no-trash --execute
  [[ $status -eq 0 ]]
  [[ ! -d "$DATA/2024" ]]
}

@test "dry-run: nothing is created, moved, or deleted" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --dry-run
  [[ $status -eq 0 ]]
  [[ -f "$DATA/$ts.mp4" ]]
  [[ ! -d "$ARCHIVE" ]]
  [[ ! -d "$TRASH" ]]
  [[ "$output" == *"[DRY-RUN] Would archive"* ]]
}

# --- Bug pins (comprehensive review) ---

@test "archive: mixed-case .Mp4 is transcoded, not trashed" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.Mp4"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ -f "$(archive_out "$ts")" ]]
  [[ ! -f "$DATA/$ts.Mp4" ]]
  [[ -f "$TRASH/input/$ts.Mp4" ]]
}

@test "cli: unknown option exits non-zero" {
  run bash "$SCRIPT" --bogus --no-log
  [[ $status -ne 0 ]]
}

@test "cli: --dir with no value is a clean error" {
  run bash "$SCRIPT" --no-log --dir
  [[ $status -ne 0 ]]
  [[ "$output" == *"Missing value"* ]]
  [[ "$output" != *"unbound variable"* ]]
}

@test "cli: --max-size invalid format is a clean error" {
  run bash "$SCRIPT" --max-size 10PB --no-log
  [[ $status -ne 0 ]]
  [[ "$output" == *"Invalid size format"* ]]
}

@test "archive: output under 1MiB is treated as failure, original kept" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  FAKE_FFMPEG_SIZE=1024 run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ -f "$DATA/$ts.mp4" ]]
  [[ ! -f "$(archive_out "$ts")" ]]
  [[ "$output" == *"Transcoding failed"* ]]
}

@test "dry-run: trash cleanup does not purge old trashed files" {
  local ts
  ts="$(date -d '-30 days' +%Y%m%d%H%M%S)"
  mkdir -p "$TRASH/input"
  touch "$TRASH/input/$ts.mp4"
  run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --dry-run
  [[ $status -eq 0 ]]
  [[ -f "$TRASH/input/$ts.mp4" ]]
}

@test "logging: --log file captures warnings" {
  run bash "$SCRIPT" --dir "$DATA" --age 5 --log archiver.log --dry-run
  [[ $status -eq 0 ]]
  [[ -f "$DATA/archiver.log" ]]
  grep -q "WARN" "$DATA/archiver.log"
}

# --- Progress bar ---

@test "progress: interactive run draws a progress bar" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  IS_INTERACTIVE=true FAKE_FFMPEG_PROGRESS=1 run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ "$output" == *"Progress [1/1]"* ]]
  [[ "$output" == *"100%"* ]]
}

@test "progress: non-interactive run draws no progress bar" {
  local ts
  ts="$(old_ts)"
  touch "$DATA/$ts.mp4"
  FAKE_FFMPEG_PROGRESS=1 run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  [[ "$output" != *"Progress ["* ]]
}
