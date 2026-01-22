#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:-}"
JOBS="${JOBS:-11}"              # or pass -j N
BASE_NAME="honza"
BYTES=10000
SLEEP_BETWEEN=0.25

ts() { date +'%Y-%m-%dT%H:%M:%S.%3N'; }

usage() {
  echo "USAGE: $0 <file> [-j N]" >&2
  echo "FILE FORMAT: each line must be: url,\"description\"" >&2
  echo "EXAMPLE LINE: https://example.com/data.csv,\"Aktivní osoby v ROS\"" >&2
  exit 2
}

err() {
  echo "[$(ts)]: ERROR: $*" >&2
  exit 1
}

# Parse args: first positional is file, optional -j
parse_args() {
  local file="${1:-}"
  shift || true
  INPUT_FILE="$file"

  while [ $# -gt 0 ]; do
    case "$1" in
      -j|--jobs)
        shift
        JOBS="${1:-}"
        [ -n "${JOBS}" ] || err "missing value for -j/--jobs"
        ;;
      *)
        err "unknown argument: $1"
        ;;
    esac
    shift || true
  done
}

# Parse `url,"description"` line
parse_line() {
  local line="$1"
  local url="" desc=""
  while IFS=, read -r col1 col2; do
    [[ -z "${col1// /}" ]] && continue
    col2="${col2%\"}"
    col2="${col2#\"}"
    url="$col1"
    desc="$col2"
  done <<< "$line"

  [[ -n "${url// /}" ]] || err "parsed URL is empty; expected: url,\"description\""
  printf '%s\n' "$url|$desc"
}

run_one() {
  local job_id="$1"
  local line="$2"

  local parsed url desc
  parsed="$(parse_line "$line")"
  url="${parsed%%|*}"
  desc="${parsed#*|}"

  local workdir config name runid counter
  runid="$$"
  counter=0

  log() {
    counter=$((counter + 1))
    local wd
#    wd="$(basename "$workdir" 2>/dev/null || true)"
    #echo "[$(ts)]: INFO: (pid=${runid} job=${job_id} wd=${wd} desc=${desc} step ${counter}) $*" >&2
    echo "[$(ts)]: INFO: ${desc}, $*" >&2
  }

  cleanup() {
    # best-effort cleanup
    if [ -n "${workdir:-}" ] && [ -d "$workdir" ]; then
      rm -rf "$workdir"
    fi
  }

  trap cleanup EXIT INT TERM

  workdir="$(mktemp -d -t etl_probe_run."${job_id}".XXXXXXXX)"
  cd "$workdir" || err "failed to cd into temp workdir: $workdir"

  # Unique per-job name/config to avoid collisions (sqlite files, outputs, etc.)
  name="${BASE_NAME}-${job_id}"
  config="${name}.json"

  log "setup workdir"
  log "generating ETL config into ${workdir}/${config}"
  probe \
    -backend sqlite \
    -save \
    -bytes "$BYTES" \
    -name "$name" \
    -url "$url" \
    -breakout ico \
    > "$config"

  log "running ETL with config ${workdir}/${config}"
  etl -metrics-backend none -config "$config"

  log "done!"

  # throttle a bit per job to avoid hammering endpoints
  sleep "$SLEEP_BETWEEN"
}

main() {
  parse_args "$@"

  [ -n "${INPUT_FILE:-}" ] || usage
  [ -f "$INPUT_FILE" ] || err "file not found: $INPUT_FILE"
  [[ "$JOBS" =~ ^[0-9]+$ ]] || err "JOBS must be an integer, got: $JOBS"
  [ "$JOBS" -ge 1 ] || err "JOBS must be >= 1"

  # Read all non-empty, non-comment lines into an array.
  mapfile -t LINES < <(grep -vE '^[[:space:]]*(#|$)' "$INPUT_FILE" || true)
  [ "${#LINES[@]}" -gt 0 ] || err "no runnable lines found in: $INPUT_FILE"

  # Simple job queue with bash builtins (no xargs/parallel dependency).
  local i=0
  local active=0
  local job_id=0

  while [ $i -lt "${#LINES[@]}" ]; do
    # If at concurrency limit, wait for any job to finish.
    if [ "$active" -ge "$JOBS" ]; then
      wait -n
      active=$((active - 1))
      continue
    fi

    job_id=$((job_id + 1))
    run_one "$job_id" "${LINES[$i]}" &
    active=$((active + 1))
    i=$((i + 1))
  done

  # Wait for remaining jobs
  while [ "$active" -gt 0 ]; do
    wait -n
    active=$((active - 1))
  done
}

main "$@"


