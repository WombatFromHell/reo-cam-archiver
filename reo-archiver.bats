#!/usr/bin/env bats
# Hermetic smoke tests for reo-archiver.sh (the archive step).
#
# No real ffmpeg: a stub on PATH records each invocation and writes a
# 1 MiB output file (the script's MIN_OUTPUT_SIZE_BYTES threshold). It
# fails on a missing input, like real ffmpeg. Every test runs in its own
# tempdir; nothing outside it is touched.

REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../.." && pwd)"
SCRIPT="$REPO_ROOT/reo-archiver.sh"

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
out="${@: -1}"
mkdir -p "$(dirname "$out")"
head -c 1048576 /dev/zero >"$out"
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

archive_out() { local ts="$1"; echo "$ARCHIVE/${ts:0:4}/${ts:4:2}/${ts:6:2}/archived-$ts.mp4"; }

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

@test "size limit: over limit trashes oldest first, then archive pass runs" {
  local old1 old2
  old1="$(date -d '-20 days' +%Y%m%d%H%M%S)"
  old2="$(date -d '-10 days' +%Y%m%d%H%M%S)"
  # NOTE: enforcement only counts files inside YYYY/ year dirs
  mkdir -p "$DATA/2024"
  head -c 1000000 /dev/zero >"$DATA/2024/$old1.mp4"
  head -c 1000000 /dev/zero >"$DATA/2024/$old2.mp4"
  MAX_SIZE=1MB run run_archiver --archive "$ARCHIVE" --trash "$TRASH" --age 5 --execute
  [[ $status -eq 0 ]]
  # oldest trashed by PHASE 1 (size limit), newest by PHASE 3 (archive);
  # the trashed file is skipped in PHASE 3 instead of aborting the run
  [[ -f "$TRASH/input/2024/$old1.mp4" ]]
  [[ -f "$TRASH/input/2024/$old2.mp4" ]]
  [[ -f "$(archive_out "$old2")" ]]
  [[ "$output" == *"Size-based cleanup: removed 1 files"* ]]
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
