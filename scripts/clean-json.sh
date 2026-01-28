#!/usr/bin/env bash
set -euo pipefail

# Clean-code ideas used:
# - "set -euo pipefail" to fail fast on errors / unset vars
# - avoid `ls | while read` (breaks on spaces/newlines); loop over globs instead
# - small function = clearer intent + reusable
# - use mktemp + mv for safe overwrite (reduces risk of partial writes)

format_json_in_place() {
  local file="$1"
  local tmp
  tmp="$(mktemp "${file}.XXXXXX.tmp")"

  # Write jq's canonical output (sorted keys) to a temp file, then replace original.
  jq -S . "$file" > "$tmp"
  mv "$tmp" "$file"
}

# Loop over JSON files in the current directory.
# If none exist, do nothing (nullglob).
shopt -s nullglob
#for file in *.json; do
file="$1"
  # 1) Validate JSON quickly; jq exits non-zero on invalid JSON.
  if ! jq -e . "$file" >/dev/null; then
    echo "invalid json in file: $file" >&2
    exit 1
  fi

  # 2) If file already matches jq's formatting, skip.
  if diff -q "$file" <(jq -S . "$file") >/dev/null; then
    continue
  fi

  echo "cleaning: $file"
  # 3) Otherwise, rewrite it in jq's canonical format.
  format_json_in_place "$file"
#done

