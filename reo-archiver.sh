#!/usr/bin/env bash

# ==============================================================================
# Script Name: reo-archiver.sh
# Description: Removes or archives camera files older than N days.
#              Optimized for speed with detailed progress reporting.
#
# Usage:       ./reo-archiver.sh --dir /camera --age 14 [--archive] [--execute]
# ==============================================================================

# Strict Mode
set -euo pipefail

# --- Configuration & Defaults ---
SCRIPT_NAME="$(basename "$0")"
DEFAULT_TARGET_DIR="/camera"
DEFAULT_ARCHIVE_DIR="/camera/archived"
DEFAULT_AGE_DAYS=14
DEFAULT_DRY_RUN=true
DEFAULT_LOG_FILENAME="archiver.log"
MAX_LOG_ROTATIONS=3
MIN_OUTPUT_SIZE_BYTES=1048576
FFMPEG_PID=""

# --- Progress Bar Configuration ---
IS_INTERACTIVE=false
PROGRESS_BAR_WIDTH=10
PROGRESS_CURRENT_FILE=0 # Current file number
PROGRESS_TOTAL_FILES=0  # Total files to process
PROGRESS_FILE_PCT=0     # Current file completion percentage
PROGRESS_FILE_START=0
PROGRESS_RUN_START=0

# --- Logging Functions ---
log_info() { echo -e "[INFO] $*"; }
log_success() { echo -e "[\033[0;32mOK\033[0m] $*"; }
log_warn() { echo -e "[\033[1;33mWARN\033[0m] $*" >&2; }
log_error() { echo -e "[\033[0;31mERROR\033[0m] $*" >&2; }

# --- Terminal Detection ---
detect_interactive_terminal() {
  if [[ -t 1 ]] && [[ "${TERM:-}" != "dumb" ]]; then
    IS_INTERACTIVE=true
  else
    IS_INTERACTIVE=false
  fi
}

# --- Progress Bar Functions ---
format_duration() {
  local seconds=$1
  local hours=$((seconds / 3600))
  local minutes=$(((seconds % 3600) / 60))
  local secs=$((seconds % 60))
  if [[ $hours -gt 0 ]]; then
    printf "%02d:%02d:%02d" "$hours" "$minutes" "$secs"
  else
    printf "%02d:%02d" "$minutes" "$secs"
  fi
}

clear_progress_line() {
  [[ "$IS_INTERACTIVE" != true ]] && return
  printf "\r\033[K" >&2
}

draw_progress_bar() {
  [[ "$IS_INTERACTIVE" != true ]] && return

  local count=$1
  local total=$2
  local file_pct=$3
  local file_elapsed=$4
  local total_elapsed=$5

  local filled_width=$((file_pct * PROGRESS_BAR_WIDTH / 100))
  local empty_width=$((PROGRESS_BAR_WIDTH - filled_width))
  local bar=""

  for ((i = 0; i < filled_width; i++)); do bar="${bar}#"; done
  for ((i = 0; i < empty_width; i++)); do bar="${bar}-"; done

  local file_time
  file_time=$(format_duration "$file_elapsed")
  local total_time
  total_time=$(format_duration "$total_elapsed")

  # Detailed format: Progress [Cur/Total] FilePct% [Bar] Time (TotalTime)
  printf "\rProgress [%d/%d] %3d%% [%s] %s (Total: %s) " \
    "$count" "$total" "$file_pct" "$bar" "$file_time" "$total_time" >&2
}

update_progress_from_ffmpeg() {
  local duration=$1
  local line=$2

  if [[ "$line" =~ time=([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
    local hours=${BASH_REMATCH[1]}
    local minutes=${BASH_REMATCH[2]}
    local seconds=${BASH_REMATCH[3]}

    # Base 10 forced for arithmetic
    local current_seconds=$((10#$hours * 3600 + 10#$minutes * 60 + 10#$seconds))

    if [[ $duration -gt 0 ]]; then
      PROGRESS_FILE_PCT=$((current_seconds * 100 / duration))
      [[ $PROGRESS_FILE_PCT -gt 100 ]] && PROGRESS_FILE_PCT=100

      local now
      now=$(date +%s)
      local file_elapsed=$((now - PROGRESS_FILE_START))
      local total_elapsed=$((now - PROGRESS_RUN_START))

      draw_progress_bar "$PROGRESS_CURRENT_FILE" "$PROGRESS_TOTAL_FILES" \
        "$PROGRESS_FILE_PCT" "$file_elapsed" "$total_elapsed"
    fi
  fi
}

get_video_duration() {
  local input_file=$1
  local duration
  duration=$(ffprobe -v error -show_entries format=duration \
    -of default=noprint_wrappers=1:nokey=1 "$input_file" 2>/dev/null || echo "0")
  echo "${duration%.*}"
}

# --- Signal Handling ---
cleanup_on_signal() {
  echo ""
  log_warn "Received interrupt signal (SIGINT/CTRL+C)"
  clear_progress_line
  if [[ -n "$FFMPEG_PID" ]] && kill -0 "$FFMPEG_PID" 2>/dev/null; then
    log_warn "Terminating ffmpeg process (PID: $FFMPEG_PID)..."
    kill -TERM "$FFMPEG_PID" 2>/dev/null
    wait "$FFMPEG_PID" 2>/dev/null
  fi
  log_error "Script interrupted by user. Exiting."
  exit 130
}

trap cleanup_on_signal SIGINT

# --- Usage & Help ---
usage() {
  cat <<EOF
Usage: $SCRIPT_NAME [OPTIONS]
Options:
  --dir PATH         Directory to search (Default: $DEFAULT_TARGET_DIR)
  --age DAYS         Remove files older than this many days (Default: $DEFAULT_AGE_DAYS)
  --archive [PATH]   Transcode files to archive directory (Default: $DEFAULT_ARCHIVE_DIR)
  --no-skip          Force transcoding even if output exists and is > 1MB
  --log FILENAME     Log filename in target directory (Default: $DEFAULT_LOG_FILENAME)
  --no-log           Disable logging to file
  --dry-run          Simulate deletion without actually removing files (Default: enabled)
  --execute          Actually delete/archive files (disables dry-run)
  --help             Show this help message
EOF
  exit 0
}

# --- Argument Parsing ---
parse_args() {
  TARGET_DIR="$DEFAULT_TARGET_DIR"
  AGE_DAYS="$DEFAULT_AGE_DAYS"
  DRY_RUN="$DEFAULT_DRY_RUN"
  LOG_FILENAME="$DEFAULT_LOG_FILENAME"
  ENABLE_LOGGING=true
  ARCHIVE_MODE=false
  ARCHIVE_DIR=""
  SKIP_EXISTING=true

  while [[ $# -gt 0 ]]; do
    case "$1" in
    --dir)
      TARGET_DIR="$2"
      shift 2
      ;;
    --age)
      if ! [[ "$2" =~ ^[0-9]+$ ]]; then
        log_error "Age must be a positive integer."
        exit 1
      fi
      if [[ "$2" -lt 2 ]]; then
        log_error "Age must be greater than 1 day."
        exit 1
      fi
      AGE_DAYS="$2"
      shift 2
      ;;
    --archive)
      ARCHIVE_MODE=true
      if [[ $# -gt 1 ]] && [[ "$2" != --* ]]; then
        ARCHIVE_DIR="$2"
        shift 2
      else
        ARCHIVE_DIR="$DEFAULT_ARCHIVE_DIR"
        shift
      fi
      ;;
    --no-skip)
      SKIP_EXISTING=false
      shift
      ;;
    --log)
      LOG_FILENAME="$2"
      shift 2
      ;;
    --no-log)
      ENABLE_LOGGING=false
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --execute)
      DRY_RUN=false
      shift
      ;;
    --help | -h) usage ;;
    *)
      log_error "Unknown option: $1"
      usage
      ;;
    esac
  done
  export TARGET_DIR AGE_DAYS DRY_RUN LOG_FILENAME ENABLE_LOGGING ARCHIVE_MODE ARCHIVE_DIR SKIP_EXISTING
}

# --- Core Logic ---
get_cutoff_timestamp() {
  date -d "-$1 days" +%Y%m%d%H%M%S
}

get_recent_threshold() {
  date -d "-24 hours" +%Y%m%d%H%M%S
}

extract_timestamp() {
  local filename="$1"
  local base="${filename%.*}"
  local ts="${base: -14}"
  if [[ ${#ts} -eq 14 ]] && [[ "$ts" =~ ^[0-9]+$ ]]; then
    echo "$ts"
  else
    echo ""
  fi
}

rotate_logs() {
  local log_path="$1"
  local max_rotations="$2"
  [[ ! -f "$log_path" ]] && return
  for ((i = max_rotations - 1; i >= 0; i--)); do
    local old_log="${log_path}.${i}"
    local new_log="${log_path}.$((i + 1))"
    [[ -f "$old_log" ]] && mv "$old_log" "$new_log"
  done
  mv "$log_path" "${log_path}.0"
}

get_file_size() {
  local file="$1"
  stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null
}

transcode_file() {
  local input_file="$1"
  local output_file="$2"
  local skip_existing="$3"

  if [[ -f "$output_file" ]] && [[ "$skip_existing" == true ]]; then
    local output_size
    output_size=$(get_file_size "$output_file")
    if [[ "$output_size" -ge "$MIN_OUTPUT_SIZE_BYTES" ]]; then
      log_warn "Output exists (>= 1MB), skipping: $(basename "$output_file")"
      return 0
    fi
  fi

  mkdir -p "$(dirname "$output_file")"
  PROGRESS_FILE_START=$(date +%s)
  PROGRESS_FILE_PCT=0

  local duration=0
  [[ "$IS_INTERACTIVE" == true ]] && duration=$(get_video_duration "$input_file")

  if [[ "$IS_INTERACTIVE" == true ]]; then
    log_info "Transcoding: $(basename "$input_file")"

    ffmpeg -hide_banner -hwaccel qsv -hwaccel_output_format qsv -y \
      -i "$input_file" \
      -vf scale_qsv=w=1024:h=768:mode=hq \
      -global_quality 26 \
      -c:v h264_qsv \
      -an \
      -progress pipe:1 \
      "$output_file" 2>&1 | while IFS= read -r line; do
      update_progress_from_ffmpeg "$duration" "$line"
    done &
    FFMPEG_PID=$!
  else
    log_info "Transcoding: $(basename "$input_file")"
    ffmpeg -hide_banner -hwaccel qsv -hwaccel_output_format qsv -y \
      -i "$input_file" \
      -vf scale_qsv=w=1024:h=768:mode=hq \
      -global_quality 26 \
      -c:v h264_qsv \
      -an \
      "$output_file" >/dev/null 2>&1 &
    FFMPEG_PID=$!
  fi

  if ! wait "$FFMPEG_PID"; then
    FFMPEG_PID=""
    clear_progress_line
    log_error "Transcoding failed: ffmpeg exited with error"
    rm -f "$output_file" # remove incomplete output
    return 1
  fi

  FFMPEG_PID=""
  clear_progress_line

  if [[ ! -f "$output_file" ]]; then
    log_error "Transcoding failed: output file missing"
    return 1
  fi

  local output_size
  output_size=$(get_file_size "$output_file")

  if [[ "$output_size" -lt "$MIN_OUTPUT_SIZE_BYTES" ]]; then
    log_error "Transcoding produced file < 1MB ($output_size bytes)"
    rm -f "$output_file"
    return 1
  fi

  local now
  now=$(date +%s)
  local file_elapsed=$((now - PROGRESS_FILE_START))
  log_success "Transcoding success (Size: $output_size bytes) - $(format_duration "$file_elapsed")"
  return 0
}

build_archive_path() {
  local archive_dir="$1"
  local timestamp="$2"
  local year="${timestamp:0:4}"
  local month="${timestamp:4:2}"
  local day="${timestamp:6:2}"
  echo "${archive_dir}/${year}/${month}/${day}/archived-${timestamp}.mp4"
}

remove_empty_directories() {
  local target_dir="$1"
  local dry_run="$2"
  local count_removed=0
  log_info "Scanning for empty directories..."

  while IFS= read -r -d '' dir; do
    if [[ -z "$(ls -A "$dir" 2>/dev/null)" ]]; then
      if [[ "$dry_run" == true ]]; then
        echo "[DRY-RUN] Would remove empty directory: $dir"
      else
        rmdir "$dir"
        echo "[REMOVED] Empty directory: $dir"
      fi
      count_removed=$((count_removed + 1))
    fi
  done < <(find "$target_dir" -mindepth 1 -type d -print0 | sort -zr)

  if [[ $count_removed -gt 0 ]]; then
    local msg="Removed"
    [[ "$dry_run" == true ]] && msg="Would remove"
    log_success "$msg $count_removed empty directories."
  fi
}

validate_environment() {
  [[ ! -d "$TARGET_DIR" ]] && {
    log_error "Directory not found: $TARGET_DIR"
    exit 1
  }
  if [[ "$ARCHIVE_MODE" == true ]]; then
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
  [[ "$ENABLE_LOGGING" != true ]] && return 0
  LOG_PATH="${TARGET_DIR}/${LOG_FILENAME}"
  rotate_logs "$LOG_PATH" "$MAX_LOG_ROTATIONS"
  exec > >(tee -a "$LOG_PATH")
}

display_config() {
  local cutoff_ts="$1"

  echo "============================================================"
  echo "Camera Cleanup Script - $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"

  log_info "Target Directory : $TARGET_DIR"
  log_info "Age Threshold    : $AGE_DAYS days (before $cutoff_ts)"

  if [[ "$ARCHIVE_MODE" == true ]]; then
    log_info "Mode             : ARCHIVE -> $ARCHIVE_DIR"
  else
    log_info "Mode             : DELETE"
  fi

  if [[ "$DRY_RUN" == true ]]; then
    log_warn "DRY-RUN MODE (No changes will be made)"
  else
    log_warn "EXECUTE MODE"
  fi

  echo "------------------------------------------------------------"
}

display_summary() {
  local count_total="$1"
  local count_deleted="$2"
  local count_archived="$3"
  local count_failed="$4"
  echo "------------------------------------------------------------"
  log_success "Processed $count_total files."
  if [[ "$ARCHIVE_MODE" == true ]]; then
    log_success "Archived: $count_archived | Failed: $count_failed"
  else
    log_success "Deleted: $count_deleted"
  fi
}

# Fast file count using GNU grep (handles null bytes safely and runs in C-speed)
# This replaces the slow Bash loop for counting.
get_total_files_fast() {
  find "$TARGET_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) -print0 | grep -cz '' || echo "0"
}

# --- File Filtering Function ---
# Populates the files_to_process array with files matching age criteria
filter_files_by_age() {
  local cutoff_ts="$1"
  local -n files_array="$2"

  while IFS= read -r -d '' file; do
    local filename
    filename=$(basename "$file")
    local file_ts
    file_ts=$(extract_timestamp "$filename")

    # Guards
    [[ -z "$file_ts" ]] && continue
    [[ "$file_ts" < "$cutoff_ts" ]] || continue

    files_array+=("$file")
  done < <("${find_cmd[@]}")
}

# --- Main Execution ---
main() {
  detect_interactive_terminal
  parse_args "$@"
  validate_environment
  setup_logging

  count_total=0
  count_deleted=0
  count_archived=0
  count_failed=0

  local cutoff_ts
  cutoff_ts=$(get_cutoff_timestamp "$AGE_DAYS")

  display_config "$cutoff_ts"

  PROGRESS_RUN_START=$(date +%s)
  PROGRESS_CURRENT_FILE=0

  # Build find command based on mode
  local find_cmd
  if [[ "$ARCHIVE_MODE" == true ]]; then
    find_cmd=(find "$TARGET_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) ! -path "${ARCHIVE_DIR}/*" -print0)
  else
    find_cmd=(find "$TARGET_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) -print0)
  fi

  # 1. Scan and filter files
  log_info "Scanning and filtering files..."
  local -a files_to_process=()
  filter_files_by_age "$cutoff_ts" files_to_process

  PROGRESS_TOTAL_FILES=${#files_to_process[@]}

  if [[ "$ARCHIVE_MODE" == true ]]; then
    # Count only mp4 files for progress in archive mode
    local mp4_count=0
    for file in "${files_to_process[@]}"; do
      [[ "$(basename "$file")" =~ \.(mp4|MP4)$ ]] && mp4_count=$((mp4_count + 1))
    done
    PROGRESS_TOTAL_FILES=$mp4_count # Set to mp4 count for progress bar
    log_info "Found $mp4_count .mp4 files to transcode (${#files_to_process[@]} total files). Starting processing..."
  else
    PROGRESS_TOTAL_FILES=${#files_to_process[@]}
    log_info "Found $PROGRESS_TOTAL_FILES files to process. Starting processing..."
  fi

  # 2. Process filtered files
  for file in "${files_to_process[@]}"; do
    local filename
    filename=$(basename "$file")

    count_total=$((count_total + 1))

    if [[ "$DRY_RUN" == true ]]; then
      if [[ "$ARCHIVE_MODE" == true ]]; then
        # Only archive video files, delete images
        if [[ "$filename" =~ \.(mp4|MP4)$ ]]; then
          PROGRESS_CURRENT_FILE=$((PROGRESS_CURRENT_FILE + 1))
          local archive_path
          archive_path=$(build_archive_path "$ARCHIVE_DIR" "$(extract_timestamp "$filename")")
          echo "[DRY-RUN] Would archive: $filename"
          count_archived=$((count_archived + 1))
        else
          echo "[DRY-RUN] Would remove: $filename"
          count_deleted=$((count_deleted + 1))
        fi
      else
        echo "[DRY-RUN] Would delete: $filename"
        count_deleted=$((count_deleted + 1))
      fi
    else
      if [[ "$ARCHIVE_MODE" == true ]]; then
        # Only archive video files, delete images
        if [[ "$filename" =~ \.(mp4|MP4)$ ]]; then
          PROGRESS_CURRENT_FILE=$((PROGRESS_CURRENT_FILE + 1))
          local archive_path
          archive_path=$(build_archive_path "$ARCHIVE_DIR" "$(extract_timestamp "$filename")")
          if transcode_file "$file" "$archive_path" "$SKIP_EXISTING"; then
            rm -f "$file"
            echo "[ARCHIVED] $filename → $(basename "$archive_path")"
            count_archived=$((count_archived + 1))
          else
            log_error "Keeping original file due to transcode failure: $filename"
            count_failed=$((count_failed + 1))
          fi
        else
          rm -f "$file"
          echo "[DELETED] $filename"
          count_deleted=$((count_deleted + 1))
        fi
      else
        rm -f "$file"
        echo "[DELETED] $filename"
        count_deleted=$((count_deleted + 1))
      fi
    fi
  done

  clear_progress_line
  display_summary "$count_total" "$count_deleted" "$count_archived" "$count_failed"

  echo "------------------------------------------------------------"
  remove_empty_directories "$TARGET_DIR" "$DRY_RUN"

  echo "============================================================"
  echo "Script completed at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"
  echo ""
}

main "$@"
