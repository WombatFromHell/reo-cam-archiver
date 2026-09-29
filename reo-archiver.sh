#!/usr/bin/env bash

# ==============================================================================
# Script Name: reo-archiver.sh
# Description: Removes or archives camera files older than N days.
# ==============================================================================

set -euo pipefail

# --- Configuration & Defaults ---
SCRIPT_NAME="$(basename "$0")"
DEFAULT_TARGET_DIR="/data"
DEFAULT_ARCHIVE_DIR="/data/archived"
DEFAULT_TRASH_DIR="/data/.deleted"
DEFAULT_AGE_DAYS="${MAX_AGE:-14}"
DEFAULT_TRASH_AGE_DAYS="${TRASH_AGE:-21}"
DEFAULT_LOG_FILENAME="archiver.log"
DEFAULT_DRY_RUN=true
DEFAULT_MAX_SIZE="${MAX_SIZE:-256GB}"
MAX_LOG_ROTATIONS="${MAX_LOGS:-3}"
MIN_OUTPUT_SIZE_BYTES=1048576

# --- Global State ---
TARGET_DIR=""
AGE_DAYS=""
DRY_RUN=true
ENABLE_LOGGING=true
ARCHIVE_MODE=false
ARCHIVE_DIR=""
SKIP_EXISTING=true
USE_TRASH=true
TRASH_DIR=""
MAX_SIZE_BYTES=0

# --- File Collection Cache ---
declare -a SIZE_LIMIT_FILES=()
declare -a TRASH_CLEANUP_FILES=()
declare -a MAIN_PROCESSING_FILES=()
TOTAL_FILE_COUNT=0

# --- Progress State ---
# Progress bar + colors only when stderr is a TTY (so cron runs, whose
# stderr goes to a log file, stay quiet). IS_INTERACTIVE=true forces on —
# used by the bats suite.
IS_INTERACTIVE="${IS_INTERACTIVE:-$([[ -t 2 ]] && echo true || echo false)}"
PROGRESS_TOTAL_FILES=0
PROGRESS_CURRENT_FILE=0
PROGRESS_FILE_START=0
PROGRESS_RUN_START=0

# --- Summary Statistics (Associative Array) ---
declare -A STATS=(
  [archived_count]=0 [archived_size]=0
  [deleted_count]=0 [deleted_size]=0
  [trashed_count]=0 [trashed_size]=0
  [size_limit_count]=0 [size_limit_size]=0
  [trash_cleanup_count]=0 [trash_cleanup_size]=0
)

# --- Logging & Output ---
log() { echo -e "$*"; }

_log() {
  local level="$1" color="$2" fd="$3"
  shift 3
  local msg="[$level] $*"

  if [[ $IS_INTERACTIVE == true && -n $color ]]; then
    msg="[$color$level\033[0m] $*"
  fi

  echo -e "$msg" >&"$fd"
}

log_info() { _log "INFO" "" 1 "$*"; }
log_success() { _log "OK" "\033[0;32m" 1 "$*"; }
log_warn() { _log "WARN" "\033[1;33m" 2 "$*"; }
log_error() { _log "ERROR" "\033[0;31m" 2 "$*"; }

# --- Progress Bar Functions ---
format_duration() { printf "%02d:%02d:%02d" $(($1 / 3600)) $(($1 % 3600 / 60)) $(($1 % 60)); }
clear_progress_line() {
  [[ $IS_INTERACTIVE == true ]] && printf "\r\033[K" >&2
  return 0
}

draw_progress_bar() {
  [[ $IS_INTERACTIVE != true ]] && return

  local count=$1 total=$2 pct=$3
  local width=10

  local filled=$((pct * width / 100))
  local empty=$((width - filled))

  local bar=""
  for ((i = 0; i < filled; i++)); do bar+="#"; done
  for ((i = 0; i < empty; i++)); do bar+="-"; done

  printf "\rProgress [%d/%d] %3d%% [%s] %s (Total: %s) " \
    "$count" "$total" "$pct" "$bar" "$(format_duration $(($(date +%s) - PROGRESS_FILE_START)))" "$(format_duration $(($(date +%s) - PROGRESS_RUN_START)))" >&2
}

update_progress_from_ffmpeg() {
  local duration=$1 line=$2
  if [[ $line =~ time=([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
    local s=$((10#${BASH_REMATCH[1]} * 3600 + 10#${BASH_REMATCH[2]} * 60 + 10#${BASH_REMATCH[3]}))
    if [[ $duration -gt 0 ]]; then
      local pct=$((s * 100 / duration))
      [[ $pct -gt 100 ]] && pct=100
      draw_progress_bar "$PROGRESS_CURRENT_FILE" "$PROGRESS_TOTAL_FILES" "$pct"
    fi
  fi
}

# --- Utility Functions ---
get_file_size() { stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null; }
get_video_duration() { ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$1" 2>/dev/null | cut -d. -f1 || echo "0"; }
get_cutoff_timestamp() { date -d "-$1 days" +%Y%m%d%H%M%S; }

extract_timestamp() {
  local base="${1%.*}"
  local ts="${base: -14}"
  [[ ${#ts} -eq 14 ]] && [[ $ts =~ ^[0-9]+$ ]] && echo "$ts" || echo ""
}

build_archive_path() { echo "${ARCHIVE_DIR}/${1:0:4}/${1:4:2}/${1:6:2}/archived-${1}.mp4"; }

# ponytail: the old "input" branch was a dead-code duplicate of the default
# (category="input", source_root=TARGET_DIR) — removed rather than kept as
# unreachable clarity. Only the archive-vs-input distinction changes behavior.
build_trash_path() {
  local file="${1%/}"

  # Fall back to the default archive dir so files are categorized correctly
  # even if ARCHIVE_MODE is currently off but archived files exist already.
  local check_archive_dir="${ARCHIVE_DIR:-$DEFAULT_ARCHIVE_DIR}"
  check_archive_dir="${check_archive_dir%/}"

  local source_root="$TARGET_DIR" category="input"
  if [[ -n $check_archive_dir && $file == "$check_archive_dir"* ]]; then
    source_root="$check_archive_dir"
    category="output"
  fi

  printf "%s/%s/%s\n" "$TRASH_DIR" "$category" "${file#"$source_root"/}"
}

rotate_logs() {
  local log="$1" max="$2"
  [[ ! -f $log ]] && return
  for ((i = max - 1; i >= 0; i--)); do mv "${log}.${i}" "${log}.$((i + 1))" 2>/dev/null || true; done
  mv "$log" "${log}.0"
}

# --- Centralized File Collection ---
collect_all_files() {
  local cutoff_ts
  cutoff_ts=$(get_cutoff_timestamp "$AGE_DAYS")

  local trash_cutoff_ts
  trash_cutoff_ts=$(get_cutoff_timestamp "$DEFAULT_TRASH_AGE_DAYS")

  log_info "Collecting files from all managed directories..."

  SIZE_LIMIT_FILES=()
  TRASH_CLEANUP_FILES=()
  MAIN_PROCESSING_FILES=()
  TOTAL_FILE_COUNT=0

  local file size filename base ts is_video location
  local find_args=()

  local sources=()
  [[ -d $TRASH_DIR ]] && sources+=("$TRASH_DIR|trash")
  sources+=("$TARGET_DIR|input")
  [[ $ARCHIVE_MODE == true && -d $ARCHIVE_DIR ]] && sources+=("$ARCHIVE_DIR|archive")

  for src in "${sources[@]}"; do
    IFS='|' read -r root location <<<"$src"
    [[ ! -d $root ]] && continue

    find_args=("$root" -type f \( -iname "*.mp4" -o -iname "*.jpg" \))
    if [[ $location == "input" ]]; then
      [[ -d $TRASH_DIR ]] && find_args+=(! -path "${TRASH_DIR}/*")
      [[ $ARCHIVE_MODE == true && -d $ARCHIVE_DIR ]] && find_args+=(! -path "${ARCHIVE_DIR}/*")
    fi
    find_args+=(-printf '%p\0%s\0')

    while IFS= read -r -d '' file && IFS= read -r -d '' size; do
      filename="${file##*/}"
      base="${filename%.*}"
      ts="${base: -14}"

      if [[ ${#ts} -eq 14 && $ts == [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9] ]]; then
        is_video="false"
        [[ ${filename,,} == *.mp4 ]] && is_video="true"

        TOTAL_FILE_COUNT=$((TOTAL_FILE_COUNT + 1))

        if [[ $ts < $cutoff_ts ]]; then
          SIZE_LIMIT_FILES+=("$file|$ts|$size|$location")
          [[ $location == "input" ]] && MAIN_PROCESSING_FILES+=("$file|$ts|$size|$is_video")
        fi

        [[ $location == "trash" && $ts < $trash_cutoff_ts ]] && TRASH_CLEANUP_FILES+=("$file|$ts|$size")
      fi
    done < <(find "${find_args[@]}" 2>/dev/null)
  done

  log_info "Collected $TOTAL_FILE_COUNT total files across all directories."
  return 0
}

# --- Size Utilities ---
parse_size() {
  local input="$1"
  local size_value size_unit

  if [[ $input =~ ^([0-9]+\.?[0-9]*)([KMGTkmgt]i?[Bb]?)$ ]]; then
    size_value="${BASH_REMATCH[1]}"
    size_unit="${BASH_REMATCH[2]}"
  else
    echo "0"
    return 1
  fi

  size_unit="${size_unit^^}"

  local multiplier=1
  case "$size_unit" in
  KB | K) multiplier=1000 ;;
  KIB) multiplier=1024 ;;
  MB | M) multiplier=1000000 ;;
  MIB) multiplier=1048576 ;;
  GB | G) multiplier=1000000000 ;;
  GIB) multiplier=1073741824 ;;
  TB | T) multiplier=1000000000000 ;;
  TIB) multiplier=1099511627776 ;;
  B | "") multiplier=1 ;;
  *)
    echo "0"
    return 1
    ;;
  esac

  awk -v val="$size_value" -v mult="$multiplier" 'BEGIN { printf "%.0f", val * mult }'
}

format_size() {
  local bytes=$1
  if [[ $bytes -lt 1024 ]]; then
    echo "${bytes}B"
  elif [[ $bytes -lt 1048576 ]]; then
    awk -v b="$bytes" 'BEGIN { printf "%.2fKiB", b/1024 }'
  elif [[ $bytes -lt 1073741824 ]]; then
    awk -v b="$bytes" 'BEGIN { printf "%.2fMiB", b/1048576 }'
  elif [[ $bytes -lt 1099511627776 ]]; then
    awk -v b="$bytes" 'BEGIN { printf "%.2fGiB", b/1073741824 }'
  else
    awk -v b="$bytes" 'BEGIN { printf "%.2fTiB", b/1099511627776 }'
  fi
}

get_directory_size() {
  local dir="$1"
  [[ ! -d $dir ]] && echo "0" && return
  du -sb "$dir" 2>/dev/null | cut -f1 || echo "0"
}

# --- Core Logic: Transcode ---
transcode_file() {
  local input="$1" output="$2"

  if [[ $SKIP_EXISTING == true ]] && [[ -f $output ]] && [[ $(get_file_size "$output") -ge $MIN_OUTPUT_SIZE_BYTES ]]; then
    log_warn "Output exists (>= 1MB), skipping: $(basename "$output")"
    return 0
  fi

  mkdir -p "$(dirname "$output")"
  PROGRESS_FILE_START=$(date +%s)
  local duration=0
  [[ $IS_INTERACTIVE == true ]] && duration=$(get_video_duration "$input")

  log_info "Transcoding: $(basename "$input")"

  local cmd=(ffmpeg -hide_banner -hwaccel qsv -hwaccel_output_format qsv -y -i "$input"
    -vf scale_qsv=w=1024:h=768:mode=hq -global_quality 26 -c:v h264_qsv -an)

  local status=0

  set +e
  if [[ $IS_INTERACTIVE == true ]]; then
    cmd+=(-progress pipe:1 "$output")
    "${cmd[@]}" 2>&1 | while IFS= read -r line; do update_progress_from_ffmpeg "$duration" "$line"; done
    status=${PIPESTATUS[0]}
  else
    cmd+=("$output")
    "${cmd[@]}" >/dev/null 2>&1
    status=$?
  fi
  set -e

  clear_progress_line

  if [[ $status -ne 0 ]] || [[ ! -f $output ]] || [[ $(get_file_size "$output") -lt $MIN_OUTPUT_SIZE_BYTES ]]; then
    log_error "Transcoding failed or output too small (Code: $status): $(basename "$input")"
    rm -f "$output"
    return 1
  fi

  log_success "Transcoding success: $(basename "$output")"
  return 0
}

# --- Core Logic: Disposal ---
# ponytail: single source of truth for "remove a file, respecting trash/dry-run".
# stat_prefix lets callers tag the same physical mv/rm/log operation into
# different summary buckets without duplicating that logic. force_delete lets
# a caller bypass the trash-move even when USE_TRASH is on — required for a
# file that is already sitting inside TRASH_DIR, where "trashing" it again
# would just re-nest it under itself instead of freeing any space (root cause
# of files never actually being removed during size-limit enforcement).
dispose_file() {
  local file="$1" reason="$2" stat_prefix="${3:-}" force_delete="${4:-false}"
  local file_size
  file_size=$(get_file_size "$file" || echo 0)

  local will_trash=false
  [[ $USE_TRASH == true && $force_delete != true ]] && will_trash=true

  local verb="DELETED"
  [[ $will_trash == true ]] && verb="TRASHED"
  [[ -z $stat_prefix ]] && stat_prefix=$([[ $will_trash == true ]] && echo "trashed" || echo "deleted")

  if [[ $DRY_RUN == true ]]; then
    log "[DRY-RUN] Would ${verb,,}: $file ($reason)"
  elif [[ $will_trash == true ]]; then
    local dest
    dest=$(build_trash_path "$file")
    mkdir -p "$(dirname "$dest")"
    mv "$file" "$dest"
    log "[$verb] $file ($reason)"
  else
    rm -f "$file"
    log "[$verb] $file ($reason)"
  fi

  STATS[${stat_prefix}_count]=$((STATS[${stat_prefix}_count] + 1))
  STATS[${stat_prefix}_size]=$((STATS[${stat_prefix}_size] + file_size))
}

# --- Core Logic: Strategy Handlers ---

handle_archive_strategy() {
  local src="$1"
  local filename="$2"
  local ts
  ts=$(extract_timestamp "$filename")

  local dest
  dest=$(build_archive_path "$ts")

  PROGRESS_CURRENT_FILE=$((PROGRESS_CURRENT_FILE + 1))

  local src_size
  src_size=$(get_file_size "$src")

  if [[ $DRY_RUN == true ]]; then
    log "[DRY-RUN] Would archive: $filename -> $dest"
    STATS[archived_count]=$((STATS[archived_count] + 1))
    STATS[archived_size]=$((STATS[archived_size] + src_size))
    return 0
  fi

  if transcode_file "$src" "$dest"; then
    STATS[archived_count]=$((STATS[archived_count] + 1))
    STATS[archived_size]=$((STATS[archived_size] + src_size))
    dispose_file "$src" "Archived source"
  else
    log_error "Archive failed, keeping original: $filename"
  fi
}

# --- Main Processing Loop Logic ---
process_file() {
  local file="$1"
  local is_video="$2"
  # ponytail: PHASE 1 (size limit) may have trashed this file after
  # collection; skip it instead of aborting the run on the missing file.
  [[ -f $file ]] || { log_warn "Skipping missing file: $file"; return 0; }
  local filename
  filename=$(basename "$file")

  if [[ $ARCHIVE_MODE == true ]] && [[ $is_video == true ]]; then
    handle_archive_strategy "$file" "$filename"
  else
    dispose_file "$file" "Old file"
  fi
}

# --- Size-Based Cleanup ---
enforce_size_limit() {
  [[ $MAX_SIZE_BYTES -le 0 ]] && return

  local trash_size=0 input_size=0 archive_size=0

  log_info "Calculating directory sizes..."

  if [[ -d $TRASH_DIR ]]; then
    trash_size=$(get_directory_size "$TRASH_DIR")
    log_info "Trash size: $(format_size "$trash_size")"
  fi

  if [[ -d $TARGET_DIR ]]; then
    for year_dir in "$TARGET_DIR"/[0-9][0-9][0-9][0-9]; do
      [[ -d $year_dir ]] || continue
      local year_size
      year_size=$(get_directory_size "$year_dir")
      input_size=$((input_size + year_size))
    done
    log_info "Input size: $(format_size "$input_size")"
  fi

  if [[ $ARCHIVE_MODE == true ]] && [[ -d $ARCHIVE_DIR ]]; then
    archive_size=$(get_directory_size "$ARCHIVE_DIR")
    log_info "Archive size: $(format_size "$archive_size")"
  fi

  local total_size=$((trash_size + input_size + archive_size))
  log_info "Total managed size: $(format_size "$total_size") / $(format_size "$MAX_SIZE_BYTES")"

  if [[ $total_size -le $MAX_SIZE_BYTES ]]; then
    log_success "Total size within limit. No size-based cleanup needed."
    echo ""
    return
  fi

  local excess=$((total_size - MAX_SIZE_BYTES))
  log_warn "Exceeding size limit by $(format_size "$excess"). Finding files to remove..."

  local -a sorted_candidates=()
  if [[ ${#SIZE_LIMIT_FILES[@]} -gt 0 ]]; then
    while IFS= read -r line; do
      sorted_candidates+=("$line")
    done < <(printf "%s\n" "${SIZE_LIMIT_FILES[@]}" | sort -t'|' -k2)
  fi

  log_info "Found ${#sorted_candidates[@]} eligible files older than $AGE_DAYS days."

  # Remove files oldest-first until under the limit. Reuses dispose_file
  # (same trash/dry-run/logging path as main processing) tagged into the
  # size_limit_* stat bucket, instead of a second copy of that logic.
  #
  # Files whose location is already "trash" must be force-deleted rather than
  # trashed again: TARGET_DIR is a path prefix of TRASH_DIR, so re-trashing an
  # already-trashed file just re-nests it deeper inside itself and never
  # actually frees any space.
  local removed_size=0
  local removed_count=0

  for entry in "${sorted_candidates[@]}"; do
    [[ $removed_size -ge $excess ]] && break

    IFS='|' read -r file_path _ file_size location <<<"$entry"

    if [[ $location == "trash" ]]; then
      dispose_file "$file_path" "Size limit exceeded (already in trash)" "size_limit" true
    else
      dispose_file "$file_path" "Size limit exceeded" "size_limit"
    fi

    removed_size=$((removed_size + file_size))
    removed_count=$((removed_count + 1))
  done

  log_success "Size-based cleanup: removed $removed_count files ($(format_size "$removed_size"))"
  echo ""
}

# --- Cleanup & Setup ---
cleanup_trash_folder() {
  [[ ! -d $TRASH_DIR ]] && return

  log_info "Cleaning trash folder (files older than $DEFAULT_TRASH_AGE_DAYS days)..."

  local cleaned_count=0

  if [[ ${#TRASH_CLEANUP_FILES[@]} -gt 0 ]]; then
    for entry in "${TRASH_CLEANUP_FILES[@]}"; do
      IFS='|' read -r file_path _ file_size <<<"$entry"

      if [[ $DRY_RUN == true ]]; then
        log "[DRY-RUN] Would permanently delete from trash: $(basename "$file_path")"
      else
        rm -f "$file_path" && log "[PERMANENTLY DELETED] $(basename "$file_path")"
      fi

      cleaned_count=$((cleaned_count + 1))

      STATS[trash_cleanup_count]=$((STATS[trash_cleanup_count] + 1))
      STATS[trash_cleanup_size]=$((STATS[trash_cleanup_size] + file_size))
    done
  fi

  [[ $cleaned_count -gt 0 ]] && log_info "Cleaned $cleaned_count files from trash."
}

remove_empty_directories() {
  log_info "Scanning for empty directories..."

  local dirs_to_scan=()
  dirs_to_scan+=("$TARGET_DIR")
  [[ $ARCHIVE_MODE == true ]] && [[ -d $ARCHIVE_DIR ]] && dirs_to_scan+=("$ARCHIVE_DIR")
  [[ $USE_TRASH == true ]] && [[ -d $TRASH_DIR ]] && dirs_to_scan+=("$TRASH_DIR")

  for scan_dir in "${dirs_to_scan[@]}"; do
    log_info "Checking for empty directories in: $scan_dir"

    while IFS= read -r -d '' dir; do
      if [[ -z "$(ls -A "$dir" 2>/dev/null)" ]]; then
        if [[ $DRY_RUN == true ]]; then
          log "[DRY-RUN] Would remove empty directory: $dir"
        else
          if rmdir "$dir" 2>/dev/null; then
            log "[REMOVED] Empty directory: $dir"
          fi
        fi
      fi
    done < <(find "$scan_dir" -mindepth 1 -type d -depth -print0 2>/dev/null || true)
  done
}

# --- Summary Display ---
display_summary() {
  local mode_label="DRY-RUN"
  [[ $DRY_RUN == false ]] && mode_label="EXECUTED"

  log_info "Run Mode: $mode_label"
  echo ""

  echo "Main Processing (files older than $AGE_DAYS days):"
  if [[ $ARCHIVE_MODE == true ]]; then
    printf "  Archived:        %d files (%s)\n" "${STATS[archived_count]}" "$(format_size "${STATS[archived_size]}")"
  fi
  if [[ $USE_TRASH == true ]]; then
    printf "  Trashed:         %d files (%s)\n" "${STATS[trashed_count]}" "$(format_size "${STATS[trashed_size]}")"
  else
    printf "  Deleted:         %d files (%s)\n" "${STATS[deleted_count]}" "$(format_size "${STATS[deleted_size]}")"
  fi
  echo ""

  if [[ $MAX_SIZE_BYTES -gt 0 && ${STATS[size_limit_count]} -gt 0 ]]; then
    echo "Size Limit Enforcement:"
    printf "  Removed:         %d files (%s)\n" "${STATS[size_limit_count]}" "$(format_size "${STATS[size_limit_size]}")"
    echo ""
  fi

  if [[ ${STATS[trash_cleanup_count]} -gt 0 ]]; then
    echo "Trash Cleanup (files older than $DEFAULT_TRASH_AGE_DAYS days):"
    printf "  Purged:          %d files (%s)\n" "${STATS[trash_cleanup_count]}" "$(format_size "${STATS[trash_cleanup_size]}")"
    echo ""
  fi

  local total_count=$((STATS[archived_count] + STATS[deleted_count] + STATS[trashed_count] + STATS[size_limit_count] + STATS[trash_cleanup_count]))
  local total_size=$((STATS[archived_size] + STATS[deleted_size] + STATS[trashed_size] + STATS[size_limit_size] + STATS[trash_cleanup_size]))

  echo "============================================================"
  printf "Total Files Processed: %d files (%s)\n" "$total_count" "$(format_size "$total_size")"
  echo "============================================================"
}

cleanup_on_signal() {
  clear_progress_line
  log_error "Script interrupted."
  exit 130
}
trap cleanup_on_signal SIGINT

# --- Argument Parsing & Usage ---
usage() {
  cat <<EOF
Usage: $SCRIPT_NAME [OPTIONS]
Options:
  --dir PATH         Directory to search (Default: $DEFAULT_TARGET_DIR)
  --age DAYS         Remove files older than this many days (Default: $DEFAULT_AGE_DAYS)
  --archive [PATH]   Transcode files to archive directory (Default: $DEFAULT_ARCHIVE_DIR)
  --trash [PATH]     Move deleted files to trash (Default: $DEFAULT_TRASH_DIR)
  --no-trash         Permanently delete files (disabled by default)
  --no-skip          Force transcoding even if output exists
  --max-size SIZE    Maximum total size (Default: $DEFAULT_MAX_SIZE, use 0 to disable)
  --log FILENAME     Log filename (Default: $DEFAULT_LOG_FILENAME)
  --no-log           Disable logging
  --dry-run          Simulate actions (Default)
  --execute          Execute actions
  --help             Show help
EOF
  exit "${1:-0}"
}

parse_args() {
  TARGET_DIR="$DEFAULT_TARGET_DIR"
  AGE_DAYS="$DEFAULT_AGE_DAYS"
  DRY_RUN="$DEFAULT_DRY_RUN"
  LOG_FILENAME="$DEFAULT_LOG_FILENAME"
  ENABLE_LOGGING=true
  ARCHIVE_MODE=false
  ARCHIVE_DIR=""
  SKIP_EXISTING=true
  USE_TRASH=true
  TRASH_DIR="$DEFAULT_TRASH_DIR"

  if ! MAX_SIZE_BYTES=$(parse_size "$DEFAULT_MAX_SIZE"); then
    log_error "Invalid default MAX_SIZE value: '$DEFAULT_MAX_SIZE'"
    exit 1
  fi

  local DRY_RUN_REQUESTED=false

  while [[ $# -gt 0 ]]; do
    case "$1" in
    --dir)
      [[ $# -ge 2 ]] || { log_error "Missing value for --dir"; exit 1; }
      TARGET_DIR="$2"
      shift 2
      ;;
    --age)
      [[ $# -ge 2 ]] || { log_error "Missing value for --age"; exit 1; }
      [[ ! $2 =~ ^[0-9]+$ || $2 -lt 2 ]] && {
        log_error "Age must be integer >= 2"
        exit 1
      }
      AGE_DAYS="$2"
      shift 2
      ;;
    --archive)
      ARCHIVE_MODE=true
      if [[ $# -gt 1 ]] && [[ $2 != --* ]]; then
        ARCHIVE_DIR="$2"
        shift 2
      else
        ARCHIVE_DIR="$DEFAULT_ARCHIVE_DIR"
        shift
      fi
      ;;
    --trash)
      USE_TRASH=true
      if [[ $# -gt 1 ]] && [[ $2 != --* ]]; then
        TRASH_DIR="$2"
        shift 2
      else
        TRASH_DIR="$DEFAULT_TRASH_DIR"
        shift
      fi
      ;;
    --no-trash)
      USE_TRASH=false
      shift
      ;;
    --no-skip)
      SKIP_EXISTING=false
      shift
      ;;
    --max-size)
      [[ $# -ge 2 ]] || { log_error "Missing value for --max-size"; exit 1; }
      if [[ $2 == "0" ]]; then
        MAX_SIZE_BYTES=0
      else
        MAX_SIZE_BYTES=$(parse_size "$2" || true)
        if [[ $MAX_SIZE_BYTES -eq 0 ]]; then
          log_error "Invalid size format: $2 (use format like 1TiB, 500GiB, 100GB, or 0 to disable)"
          exit 1
        fi
      fi
      shift 2
      ;;
    --log)
      [[ $# -ge 2 ]] || { log_error "Missing value for --log"; exit 1; }
      LOG_FILENAME="$2"
      shift 2
      ;;
    --no-log)
      ENABLE_LOGGING=false
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      DRY_RUN_REQUESTED=true
      shift
      ;;
    --execute)
      if [[ $DRY_RUN_REQUESTED == false ]]; then
        DRY_RUN=false
      fi
      shift
      ;;
    --help | -h) usage ;;
    *)
      log_error "Unknown option: $1"
      usage 1
      ;;
    esac
  done
}

validate_environment() {
  [[ ! -d $TARGET_DIR ]] && {
    log_error "Directory not found: $TARGET_DIR"
    exit 1
  }
  if [[ $ARCHIVE_MODE == true ]]; then
    command -v ffmpeg &>/dev/null || {
      log_error "ffmpeg not found."
      exit 1
    }
    command -v ffprobe &>/dev/null || {
      log_error "ffprobe not found."
      exit 1
    }
  fi
}

setup_logging() {
  [[ $ENABLE_LOGGING != true ]] && return
  local log_path="${TARGET_DIR}/${LOG_FILENAME}"
  rotate_logs "$log_path" "$MAX_LOG_ROTATIONS"
  exec > >(tee -a "$log_path") 2>&1
}

display_config() {
  local cutoff
  cutoff=$(get_cutoff_timestamp "$AGE_DAYS")

  echo "Camera Cleanup Script - $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"
  log_info "Target Dir : $TARGET_DIR"
  log_info "Age        : $AGE_DAYS days (before $cutoff)"
  log_info "Mode       : $([[ $ARCHIVE_MODE == true ]] && echo "ARCHIVE -> $ARCHIVE_DIR" || echo "DELETE")"
  log_info "Trash      : $([[ $USE_TRASH == true ]] && echo "ENABLED -> $TRASH_DIR" || echo "DISABLED")"

  if [[ $MAX_SIZE_BYTES -gt 0 ]]; then
    log_info "Size Limit : $(format_size "$MAX_SIZE_BYTES")"
  else
    log_info "Size Limit : DISABLED"
  fi

  if [[ $DRY_RUN == true ]]; then
    log_warn "Run Mode   : DRY-RUN (No changes)"
  else
    log_warn "Run Mode   : EXECUTE"
  fi
  echo "------------------------------------------------------------"
}

# --- Main Execution ---
main() {
  parse_args "$@"
  validate_environment
  setup_logging

  display_config

  collect_all_files

  echo "============================================================"
  echo "PHASE 1: Size Limit Enforcement"
  echo "============================================================"
  enforce_size_limit

  if [[ $USE_TRASH == true ]] && [[ ${#TRASH_CLEANUP_FILES[@]} -gt 0 ]]; then
    echo "============================================================"
    echo "PHASE 2: Trash Cleanup"
    echo "============================================================"
    cleanup_trash_folder
    echo ""
  fi

  echo "============================================================"
  echo "PHASE 3: Main File Processing"
  echo "============================================================"

  PROGRESS_RUN_START=$(date +%s)

  PROGRESS_TOTAL_FILES=0
  if [[ $ARCHIVE_MODE == true ]] && [[ ${#MAIN_PROCESSING_FILES[@]} -gt 0 ]]; then
    for entry in "${MAIN_PROCESSING_FILES[@]}"; do
      IFS='|' read -r _ _ _ is_video <<<"$entry"
      [[ $is_video == "true" ]] && PROGRESS_TOTAL_FILES=$((PROGRESS_TOTAL_FILES + 1))
    done
  else
    PROGRESS_TOTAL_FILES=${#MAIN_PROCESSING_FILES[@]}
  fi

  log_info "Found ${#MAIN_PROCESSING_FILES[@]} total files ($PROGRESS_TOTAL_FILES video files to process)."

  if [[ ${#MAIN_PROCESSING_FILES[@]} -gt 0 ]]; then
    for entry in "${MAIN_PROCESSING_FILES[@]}"; do
      IFS='|' read -r file_path _ _ is_video <<<"$entry"
      process_file "$file_path" "$is_video"
    done
  fi

  clear_progress_line
  remove_empty_directories

  echo ""
  echo "============================================================"
  echo "PHASE 4: Summary"
  echo "============================================================"
  display_summary

  echo "Script completed at $(date '+%Y-%m-%d %H:%M:%S')"
}

main "$@"
