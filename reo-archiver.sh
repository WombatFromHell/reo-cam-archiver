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
DEFAULT_MAX_SIZE="1TB"
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
MAX_SIZE_BYTES=0

# --- File Collection Cache ---
# Arrays to hold pre-collected file data (populated once, used by all phases)
declare -a ALL_FILES=()             # All files with metadata: "path|timestamp|size|is_video"
declare -a SIZE_LIMIT_FILES=()      # Files eligible for size-based cleanup
declare -a TRASH_CLEANUP_FILES=()   # Files in trash older than trash age
declare -a MAIN_PROCESSING_FILES=() # Files for main processing (archive/delete)

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
  local file="$1"

  # Default to 'input' category (files originating from TARGET_DIR)
  local source_root="$TARGET_DIR"
  local category="input"

  # Override: switch to 'output' category for archived files
  if [[ -n "${ARCHIVE_DIR:-}" ]] && [[ "$file" == "$ARCHIVE_DIR"* ]]; then
    source_root="$ARCHIVE_DIR"
    category="output"
  fi

  # Assemble path: TRASH_DIR/<category>/<relative_path>
  # where <relative_path> = '<YYYY>/<MM>/<DD>/...'
  printf "%s/%s/%s\n" "$TRASH_DIR" "$category" "${file#"$source_root"/}"
}

rotate_logs() {
  local log="$1" max="$2"
  [[ ! -f "$log" ]] && return
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

  # Clear arrays
  ALL_FILES=()
  SIZE_LIMIT_FILES=()
  TRASH_CLEANUP_FILES=()
  MAIN_PROCESSING_FILES=()

  # Collect from trash directory
  if [[ -d "$TRASH_DIR" ]]; then
    while IFS= read -r -d '' file; do
      local ts size is_video
      ts=$(extract_timestamp "$(basename "$file")")
      [[ -z "$ts" ]] && continue

      size=$(get_file_size "$file")
      is_video="false"
      [[ "$file" =~ \.(mp4|MP4)$ ]] && is_video="true"

      ALL_FILES+=("$file|$ts|$size|$is_video|trash")

      # For size limit enforcement (age threshold)
      [[ "$ts" < "$cutoff_ts" ]] && SIZE_LIMIT_FILES+=("$file|$ts|$size")

      # For trash cleanup (trash age threshold)
      [[ "$ts" < "$trash_cutoff_ts" ]] && TRASH_CLEANUP_FILES+=("$file|$ts|$size")
    done < <(find "$TRASH_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) -print0 2>/dev/null)
  fi

  # Collect from input directory (excluding trash and archive)
  local exclude_args=()
  [[ -d "$TRASH_DIR" ]] && exclude_args+=(! -path "${TRASH_DIR}/*")
  [[ "$ARCHIVE_MODE" == true ]] && [[ -d "$ARCHIVE_DIR" ]] && exclude_args+=(! -path "${ARCHIVE_DIR}/*")

  while IFS= read -r -d '' file; do
    local ts size is_video
    ts=$(extract_timestamp "$(basename "$file")")
    [[ -z "$ts" ]] && continue

    size=$(get_file_size "$file")
    is_video="false"
    [[ "$file" =~ \.(mp4|MP4)$ ]] && is_video="true"

    ALL_FILES+=("$file|$ts|$size|$is_video|input")

    # For size limit enforcement (age threshold)
    [[ "$ts" < "$cutoff_ts" ]] && SIZE_LIMIT_FILES+=("$file|$ts|$size")

    # For main processing (age threshold)
    [[ "$ts" < "$cutoff_ts" ]] && MAIN_PROCESSING_FILES+=("$file|$ts|$size|$is_video")
  done < <(find "$TARGET_DIR" -type f \( -iname "*.mp4" -o -iname "*.jpg" \) "${exclude_args[@]}" -print0 2>/dev/null)

  # Collect from archive directory (if archive mode enabled)
  if [[ "$ARCHIVE_MODE" == true ]] && [[ -d "$ARCHIVE_DIR" ]]; then
    while IFS= read -r -d '' file; do
      local ts size is_video
      ts=$(extract_timestamp "$(basename "$file")")
      [[ -z "$ts" ]] && continue

      size=$(get_file_size "$file")
      is_video="true" # Archive only has videos

      ALL_FILES+=("$file|$ts|$size|$is_video|archive")

      # For size limit enforcement (age threshold)
      [[ "$ts" < "$cutoff_ts" ]] && SIZE_LIMIT_FILES+=("$file|$ts|$size")
    done < <(find "$ARCHIVE_DIR" -type f -iname "*.mp4" -print0 2>/dev/null)
  fi

  log_info "Collected ${#ALL_FILES[@]} total files across all directories."
}

# --- Size Utilities ---
parse_size() {
  local input="$1"
  local size_value size_unit

  # Extract numeric value and unit
  if [[ "$input" =~ ^([0-9]+\.?[0-9]*)([KMGTkmgt]i?[Bb]?)$ ]]; then
    size_value="${BASH_REMATCH[1]}"
    size_unit="${BASH_REMATCH[2]}"
  else
    echo "0"
    return 1
  fi

  # Convert to uppercase for consistency
  size_unit="${size_unit^^}"

  # Calculate bytes based on unit
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

  # Use awk for floating point multiplication
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
  [[ ! -d "$dir" ]] && echo "0" && return
  du -sb "$dir" 2>/dev/null | cut -f1 || echo "0"
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

# --- Size-Based Cleanup ---
enforce_size_limit() {
  [[ $MAX_SIZE_BYTES -le 0 ]] && return

  echo "============================================================"
  echo "PHASE 0: Size Limit Enforcement"
  echo "============================================================"

  # Calculate sizes in priority order: trash, input, archive
  local trash_size=0 input_size=0 archive_size=0

  log_info "Calculating directory sizes..."

  if [[ -d "$TRASH_DIR" ]]; then
    trash_size=$(get_directory_size "$TRASH_DIR")
    log_info "Trash size: $(format_size "$trash_size")"
  fi

  # Calculate input size (year directories in TARGET_DIR)
  if [[ -d "$TARGET_DIR" ]]; then
    for year_dir in "$TARGET_DIR"/[0-9][0-9][0-9][0-9]; do
      [[ -d "$year_dir" ]] || continue
      local year_size
      year_size=$(get_directory_size "$year_dir")
      input_size=$((input_size + year_size))
    done
    log_info "Input size: $(format_size "$input_size")"
  fi

  if [[ "$ARCHIVE_MODE" == true ]] && [[ -d "$ARCHIVE_DIR" ]]; then
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

  # Use pre-collected files and sort by timestamp (oldest first)
  local -a sorted_candidates=()
  while IFS= read -r line; do
    sorted_candidates+=("$line")
  done < <(printf "%s\n" "${SIZE_LIMIT_FILES[@]}" | sort -t'|' -k2)

  log_info "Found ${#sorted_candidates[@]} eligible files older than $AGE_DAYS days."

  # Remove files oldest-first until we're under the limit
  local removed_size=0
  local removed_count=0

  for entry in "${sorted_candidates[@]}"; do
    [[ $removed_size -ge $excess ]] && break

    IFS='|' read -r file_path _ file_size <<<"$entry"

    if [[ "$DRY_RUN" == true ]]; then
      log "[DRY-RUN] Would permanently delete: $(basename "$file_path") ($(format_size "$file_size"))"
    else
      rm -f "$file_path" && log "[SIZE-LIMIT] Deleted: $(basename "$file_path") ($(format_size "$file_size"))"
    fi

    removed_size=$((removed_size + file_size))
    removed_count=$((removed_count + 1))
  done

  log_success "Size-based cleanup: removed $removed_count files ($(format_size "$removed_size"))"
  log_info "New total size: $(format_size $((total_size - removed_size)))"
  echo ""
}

# --- Cleanup & Setup ---
cleanup_trash_folder() {
  [[ ! -d "$TRASH_DIR" ]] && return

  log_info "Cleaning trash folder (files older than $DEFAULT_TRASH_AGE_DAYS days)..."

  local cleaned_count=0
  for entry in "${TRASH_CLEANUP_FILES[@]}"; do
    IFS='|' read -r file_path _ _ <<<"$entry"

    if [[ "$DRY_RUN" == true ]]; then
      log "[DRY-RUN] Would permanently delete from trash: $(basename "$file_path")"
    else
      rm -f "$file_path" && log "[PERMANENTLY DELETED] $(basename "$file_path")"
    fi
    cleaned_count=$((cleaned_count + 1))
  done

  [[ $cleaned_count -gt 0 ]] && log_info "Cleaned $cleaned_count files from trash."
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
  --max-size SIZE    Maximum total size (Default: $DEFAULT_MAX_SIZE, use 0 to disable)
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

  # Parse default max size
  MAX_SIZE_BYTES=$(parse_size "$DEFAULT_MAX_SIZE")

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
    --max-size)
      if [[ "$2" == "0" ]]; then
        MAX_SIZE_BYTES=0
      else
        MAX_SIZE_BYTES=$(parse_size "$2")
        if [[ $MAX_SIZE_BYTES -eq 0 ]]; then
          log_error "Invalid size format: $2 (use format like 1TiB, 500GiB, 100GB, or 0 to disable)"
          exit 1
        fi
      fi
      shift 2
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

  if [[ $MAX_SIZE_BYTES -gt 0 ]]; then
    log_info "Size Limit : $(format_size "$MAX_SIZE_BYTES")"
  else
    log_info "Size Limit : DISABLED"
  fi

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

  display_config

  # COLLECT ALL FILES ONCE - used by all phases
  collect_all_files

  # Phase 0: Size Limit Enforcement (if enabled)
  enforce_size_limit

  # Phase 1: Trash Cleanup
  if [[ "$USE_TRASH" == true ]] && [[ ${#TRASH_CLEANUP_FILES[@]} -gt 0 ]]; then
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

  # Count video files for progress tracking
  PROGRESS_TOTAL_FILES=0
  if [[ "$ARCHIVE_MODE" == true ]]; then
    for entry in "${MAIN_PROCESSING_FILES[@]}"; do
      IFS='|' read -r _ _ _ is_video <<<"$entry"
      [[ "$is_video" == "true" ]] && PROGRESS_TOTAL_FILES=$((PROGRESS_TOTAL_FILES + 1))
    done
  else
    PROGRESS_TOTAL_FILES=${#MAIN_PROCESSING_FILES[@]}
  fi

  log_info "Found ${#MAIN_PROCESSING_FILES[@]} total files ($PROGRESS_TOTAL_FILES video files to process)."

  # Process each file
  for entry in "${MAIN_PROCESSING_FILES[@]}"; do
    IFS='|' read -r file_path _ _ _ <<<"$entry"
    process_file "$file_path"
  done

  clear_progress_line
  remove_empty_directories

  echo "============================================================"
  echo "Script completed at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "============================================================"
}

main "$@"
