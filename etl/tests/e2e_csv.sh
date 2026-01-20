#!/bin/bash
set -euo pipefail

INPUT_FILE="${1:-}"
NAME="honza"
CONFIG="${NAME}.json"
BYTES=10000
SLEEP_BETWEEN=0.25

# Global run-scoped state (set per input line).
URL=""
DESCRIPTION=""
counter=0
WORKDIR=""
RUNID="$$"
LOCKDIR="/tmp/e2e_csv.lock"

ts() { date +'%Y-%m-%dT%H:%M:%S.%3N'; }

log(){
  entry="$*"
  counter=$((counter + 1))
  local wd=""
  if [ -n "${WORKDIR:-}" ]; then
    wd="$(basename "$WORKDIR")"
  fi
  echo "[$(ts)]: INFO: (pid=${RUNID} wd=${wd} desc=${DESCRIPTION} step ${counter}) ${entry}" >&2
}

err() {
  echo "[$(ts)]: ERROR: (pid=${RUNID} desc=${DESCRIPTION}) $*" >&2
  exit 1
}

usage() {
  echo "USAGE: $0 <file>" >&2
  echo "FILE FORMAT: each line must be: url,\"description\"" >&2
  echo "EXAMPLE LINE: https://example.com/data.csv,\"Aktivní osoby v ROS\"" >&2
  err "missing input file argument"
}

cleanup() {
  log "cleanup"

  # Remove workdir if created.
  if [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ]; then
    rm -rf "$WORKDIR"
  fi

  # Release lock (best-effort).
  if [ -d "$LOCKDIR" ]; then
    rmdir "$LOCKDIR" 2>/dev/null || true
  fi
}

lock_acquire() {
  if ! mkdir "$LOCKDIR" 2>/dev/null; then
    err "lock held ($LOCKDIR). Another run is active, or a stale lock exists."
  fi
}

setup_workdir() {
  log "setup workdir"
  WORKDIR="$(mktemp -d -t etl_probe_run.XXXXXXXX)"
  cd "$WORKDIR" || err "failed to cd into temp workdir: $WORKDIR"
}

parse_line() {
  local line="$1"

  URL=""
  DESCRIPTION=""

  while IFS=, read -r col1 col2; do
    [[ -z "${col1// /}" ]] && continue

    col2="${col2%\"}"
    col2="${col2#\"}"

    URL="$col1"
    DESCRIPTION="$col2"
  done <<< "$line"

  if [[ -z "${URL// /}" ]]; then
    err "parsed URL is empty; expected format: url,\"description\""
  fi
}

generate_etl_config() {
  log "generating ETL config into ${WORKDIR}/${CONFIG}"
  if ! probe \
    -backend sqlite \
    -save \
    -bytes "$BYTES" \
    -name "$NAME" \
    -url "$URL" \
    -breakout ico \
    > "$CONFIG"; then
    err "failed to run generate_etl_config"
  fi
}

run_etl() {
  log "running ETL with config ${WORKDIR}/${CONFIG}"
  if ! etl -config "$CONFIG"; then
    err "failed to run etl"
  fi
}

run_one() {
  local line="$1"

  # Reset per-run counters/state.
  counter=0
  WORKDIR=""

  parse_line "$line"
  lock_acquire
  trap cleanup EXIT INT TERM

  setup_workdir
  generate_etl_config
  run_etl

  log "done (temp dir will be removed): $WORKDIR"

  # Run cleanup now (and disable trap) so the next run can acquire lock.
  trap - EXIT INT TERM
  cleanup
}

main() {
  if [ -z "${INPUT_FILE:-}" ]; then
    usage
  fi
  if [ ! -f "$INPUT_FILE" ]; then
    err "file not found: $INPUT_FILE"
  fi

  # Read file line-by-line without using a pipe (avoids subshell surprises).
  while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comment lines
    [[ -z "${line// /}" ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    run_one "$line"

    # Small delay between runs to avoid hammering endpoints and logs.
    sleep "$SLEEP_BETWEEN"
  done < "$INPUT_FILE"
}

main

