#!/usr/bin/env bash

# ==============================================================================
# Script Name: reo-archiver.sh
# Description: Removes or archives camera files older than N days.
# ==============================================================================

set -euo pipefail

# --- Configuration & Defaults ---
SCRIPT_NAME="$(basename "$0")"
DEFAULT_TARGET_DIR="/camera"
DEFAULT_ARCHIVE_DIR="/camera/archived"
DEFAULT_TRASH_DIR="/camera/.deleted"
DEFAULT_AGE_DAYS=14
DEFAULT_TRASH_AGE_DAYS=21
DEFAULT_LOG_FILENAME="archiver.log"
DEFAULT_DRY_RUN=true
MAX_LOG_ROTATIONS=3
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

# --- Progress State ---
IS_INTERACTIVE=false
PROGRESS_TOTAL_FILES=0
PROGRESS_CURRENT_FILE=0
PROGRESS_FILE_START=0
PROGRESS_RUN_START=0

# --- Logging & Output ---
log() { echo -e "$*"; }

_log() {
  local level="$1" color="$2" fd="$3"
  shift 3
  local msg="[$level] $*"

  if [[ "$IS_INTERACTIVE" == true && -n "$color" ]]; then
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
clear_progress_line() { [[ "$IS_INTERACTIVE" == true ]] && printf "\r\033[K" >&2; }

draw_progress_bar() {
  [[ "$IS_INTERACTIVE" != true ]] && return

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
  if [[ "$line" =~ time=([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
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
  [[ ${#ts} -eq 14 ]] && [[ "$ts" =~ ^[0-9]+$ ]] && echo "$ts" || echo ""
}

build_archive_path() { echo "${ARCHIVE_DIR}/${1:0:4}/${1:4:2}/${1:6:2}/archived-${1}.mp4"; }
build_trash_path() {
  local file="$1" prefix="$TARGET_DIR"
  [[ "$file" == "$ARCHIVE_DIR"* ]] && prefix="$ARCHIVE_DIR"
  echo "${TRASH_DIR}/${file#"$prefix"/}"
}

rotate_logs() {
  local log="$1" max="$2"
  [[ ! -f "$log" ]] && return
  for ((i = max - 1; i >= 0; i--)); do mv "${log}.${i}" "${log}.$((i + 1))" 2>/dev/null || true; done
  mv "$log" "${log}.0"
}

# --- Core Logic: Transcode ---
transcode_file() {
  local input="$1" output="$2"

  if [[ "$SKIP_EXISTING" == true ]] && [[ -f "$output" ]] && [[ $(get_file_size "$output") -ge $MIN_OUTPUT_SIZE_BYTES ]]; then
    log_warn "Output exists (>= 1MB), skipping: $(basename "$output")"
    return 0
  fi

  mkdir -p "$(dirname "$output")"
  PROGRESS_FILE_START=$(date +%s)
  local duration=0
  [[ "$IS_INTERACTIVE" == true ]] && duration=$(get_video_duration "$input")

  log_info "Transcoding: $(basename "$input")"

  local cmd=(ffmpeg -hide_banner -hwaccel qsv -hwaccel_output_format qsv -y -i "$input"
    -vf scale_qsv=w=1024:h=768:mode=hq -global_quality 26 -c:v h264_qsv -an)

  local status=0

  # disable 'set -e' temporarily so we can catch failures manually
  set +e
  if [[ "$IS_INTERACTIVE" == true ]]; then
    cmd+=(-progress pipe:1 "$output")
    "${cmd[@]}" 2>&1 | while IFS= read -r line; do update_progress_from_ffmpeg "$duration" "$line"; done
    status=${PIPESTATUS[0]} # Capture exit code of ffmpeg (first command in pipe)
  else
    cmd+=("$output")
    "${cmd[@]}" >/dev/null 2>&1
    status=$?
  fi
  set -e

  clear_progress_line

  if [[ $status -ne 0 ]] || [[ ! -f "$output" ]] || [[ $(get_file_size "$output") -lt $MIN_OUTPUT_SIZE_BYTES ]]; then
    log_error "Transcoding failed or output too small (Code: $status): $(basename "$input")"
    rm -f "$output"
    return 1
  fi

  log_success "Transcoding success: $(basename "$output")"
  return 0
}

# --- Core Logic: Disposal ---
dispose_file() {
  local file="$1"
  local reason="$2"

  if [[ "$DRY_RUN" == true ]]; then
    if [[ "$USE_TRASH" == true ]]; then
      log "[DRY-RUN] Would trash: $file ($reason)"
    else
      log "[DRY-RUN] Would delete: $file ($reason)"
    fi
    return 0
  fi

  if [[ "$USE_TRASH" == true ]]; then
    local dest
    dest=$(build_trash_path "$file")
    mkdir -p "$(dirname "$dest")"
    mv "$file" "$dest"
    log "[TRASHED] $file ($reason)"
  else
    rm -f "$file"
    log "[DELETED] $file ($reason)"
  fi
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

  if [[ "$DRY_RUN" == true ]]; then
    log "[DRY-RUN] Would archive: $filename -> $dest"
    # Simulate disposal for dry run consistency
    return 0
  fi

  if transcode_file "$src" "$dest"; then
    dispose_file "$src" "Archived source"
  else
    log_error "Archive failed, keeping original: $filename"
  fi
}

# --- Main Processing Loop Logic ---
process_file() {
  local file="$1"
  local filename
  filename=$(basename "$file")

  local is_video=false
  [[ "$filename" =~ \.(mp4|MP4)$ ]] && is_video=true

  if [[ "$ARCHIVE_MODE" == true ]] && [[ "$is_video" == true ]]; then
    handle_archive_strategy "$file" "$filename"
  else
    # Images or non-archive mode
    dispose_file "$file" "Old file"
  fi
}

# --- Cleanup & Setup ---
cleanup_trash_folder() {
  [[ ! -d "$TRASH_DIR" ]] && return

  local trash_age="${DEFAULT_TRASH_AGE_DAYS:-30}"
  local cutoff
  cutoff=$(get_cutoff_timestamp "$trash_age")

  log_info "Cleaning trash folder (files older than $trash_age days)..."

  while IFS= read -r -d '' file; do
    local ts
    ts=$(extract_timestamp "$(basename "$file")")
    [[ -n "$ts" && "$ts" < "$cutoff" ]] || continue

    if [[ "$DRY_RUN" == true ]]; then
      log "[DRY-RUN] Would permanently delete from trash: $(basename "$file")"
    else
      rm -f "$file" && log "[PERMANENTLY DELETED] $(basename "$file")"
    fi
  done < <(find "$TRASH_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) -print0)
}

remove_empty_directories() {
  log_info "Scanning for empty directories..."
  while IFS= read -r -d '' dir; do
    [[ -z "$(ls -A "$dir" 2>/dev/null)" ]] || continue
    if [[ "$DRY_RUN" == true ]]; then
      log "[DRY-RUN] Would remove empty directory: $dir"
    else
      rmdir "$dir" && log "[REMOVED] Empty directory: $dir"
    fi
  done < <(find "$TARGET_DIR" -mindepth 1 -type d -print0 | sort -zr)
}

cleanup_on_signal() {
  clear_progress_line
  # Note: Since we removed backgrounding, ffmpeg receives the SIGINT directly
  # and will exit on its own. We just need to exit the script cleanly.
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
  --log FILENAME     Log filename (Default: $DEFAULT_LOG_FILENAME)
  --no-log           Disable logging
  --dry-run          Simulate actions (Default)
  --execute          Execute actions
  --help             Show help
EOF
  exit 0
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

  local DRY_RUN_REQUESTED=false

  while [[ $# -gt 0 ]]; do
    case "$1" in
    --dir)
      TARGET_DIR="$2"
      shift 2
      ;;
    --age)
      [[ ! "$2" =~ ^[0-9]+$ || "$2" -lt 2 ]] && {
        log_error "Age must be integer >= 2"
        exit 1
      }
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
    --trash)
      USE_TRASH=true
      if [[ $# -gt 1 ]] && [[ "$2" != --* ]]; then
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
      DRY_RUN_REQUESTED=true
      shift
      ;;
    --execute)
      if [[ "$DRY_RUN_REQUESTED" == false ]]; then
        DRY_RUN=false
      fi
      shift
      ;;
    --help | -h) usage ;;
    *)
      log_error "Unknown option: $1"
      usage
      ;;
    esac
  done
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
  [[ "$ENABLE_LOGGING" != true ]] && return
  local log_path="${TARGET_DIR}/${LOG_FILENAME}"
  rotate_logs "$log_path" "$MAX_LOG_ROTATIONS"
  exec > >(tee -a "$log_path")
}

display_config() {
  local cutoff
  cutoff=$(get_cutoff_timestamp "$AGE_DAYS")
  echo "============================================================"
  echo "Camera Cleanup Script - $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"
  log_info "Target Dir : $TARGET_DIR"
  log_info "Age        : $AGE_DAYS days (before $cutoff)"
  log_info "Mode       : $([[ "$ARCHIVE_MODE" == true ]] && echo "ARCHIVE -> $ARCHIVE_DIR" || echo "DELETE")"
  log_info "Trash      : $([[ "$USE_TRASH" == true ]] && echo "ENABLED -> $TRASH_DIR" || echo "DISABLED")"

  if [[ "$DRY_RUN" == true ]]; then
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

  local cutoff_ts
  cutoff_ts=$(get_cutoff_timestamp "$AGE_DAYS")

  display_config

  # Phase 1: Trash Cleanup
  if [[ "$USE_TRASH" == true ]]; then
    echo "============================================================"
    echo "PHASE 1: Trash Cleanup"
    echo "============================================================"
    cleanup_trash_folder
    echo ""
  fi

  # Phase 2: File Processing
  echo "============================================================"
  echo "PHASE 2: Main File Processing"
  echo "============================================================"

  PROGRESS_RUN_START=$(date +%s)

  local exclude_args=()
  [[ "$ARCHIVE_MODE" == true ]] && exclude_args+=(! -path "${ARCHIVE_DIR}/*")
  [[ "$USE_TRASH" == true ]] && exclude_args+=(! -path "${TRASH_DIR}/*")

  local files=()
  while IFS= read -r -d '' file; do
    local ts
    ts=$(extract_timestamp "$(basename "$file")")

    if [[ -n "$ts" && "$ts" < "$cutoff_ts" ]]; then
      files+=("$file")

      if [[ "$ARCHIVE_MODE" == true ]]; then
        if [[ "$file" =~ \.(mp4|MP4)$ ]]; then
          PROGRESS_TOTAL_FILES=$((PROGRESS_TOTAL_FILES + 1))
        fi
      else
        PROGRESS_TOTAL_FILES=$((PROGRESS_TOTAL_FILES + 1))
      fi
    fi
  done < <(find "$TARGET_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) "${exclude_args[@]}" -print0)

  log_info "Found ${#files[@]} total files ($PROGRESS_TOTAL_FILES video files to process)."

  for file in "${files[@]}"; do
    process_file "$file"
  done

  clear_progress_line
  remove_empty_directories

  echo "============================================================"
  echo "Script completed at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"
}

main "$@"
