#!/bin/bash
NAME="azet.sk"
counter=0
log(){
  entry="$*"
  counter=$((counter + 1))
  echo "[$(date +'%Y-%m-%dT%H:%M:%S')]: INFO: (${NAME} step ${counter}) ${entry}" >&2
}

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S')]: ERROR: $*" >&2
  exit 1
}

#### config / knobs ####
mappings_dir="/app/configs/mappings/azet.sk"
URL="https://www.azet.sk/katalog/"

JGET_CONCURRENCY=1
TIMEOUT="120s"
MAX_SUBCATEGORY_URLS=3
MAX_DETAIL_URLS=2

export E2E=1

# Create a single-use temp working directory and run everything inside it.
TMPDIR="$(mktemp -d -t azet_e2e.XXXXXXXX)" || err "failed to create temp dir"
cleanup_tmpdir() {
  rm -rf "$TMPDIR"
}
trap cleanup_tmpdir EXIT INT TERM

cd "$TMPDIR" || err "failed to cd into temp dir: $TMPDIR"

get_category_urls() {
  local tmpdir="$1"
  local url="$2"
  local mappings="$3"

  log "extract category urls"
  #### get URLs of top-level categories ####
  wget -q -O "${tmpdir}/index.html" "$url"
  extract_html -mappings "${mappings}/category.json" -dir "$tmpdir" | jq -r '.[].href' > "${tmpdir}/category-urls"
}

generate_subcategory_urls() {
  local tmpdir="$1"
  local mappings="$2"
  local limit="$3"

  log "generate subcategory urls"
  head -1 "${tmpdir}/category-urls" | while read -r line; do
    extract_html -mappings "${mappings}/subcategory-url-builder.json" -timeout "$TIMEOUT" -url "$line" | expand_hrefs
  done | sort | uniq | head -"$limit" > "${tmpdir}/subcategory-urls"
}

fetch_subcategory_pages() {
  local tmpdir="$1"

  log "get subcategory pages"
  jget -o "${tmpdir}/subcategory" -i "${tmpdir}/subcategory-urls" -n "$JGET_CONCURRENCY" -name azet.sk -t "$TIMEOUT" > /dev/null
}

transform_subcategory_pages() {
  local tmpdir="$1"
  local mappings="$2"

  log "make subcategory.json from subcategory files"
  extract_html -mappings "${mappings}/subcategory.json" -dir "${tmpdir}/subcategory" | jq > "${tmpdir}/subcategory.json"
}

generate_detail_urls() {
  local tmpdir="$1"
  local limit="$2"

  log "output detail urls"
  jq -r '.[].href' "${tmpdir}/subcategory.json" | sort | uniq | head -"$limit" > "${tmpdir}/detail-urls"
}

fetch_detail_pages() {
  local tmpdir="$1"

  log "get detail pages"
  jget -n "$JGET_CONCURRENCY" -i "${tmpdir}/detail-urls" -o "${tmpdir}/detail" -t "$TIMEOUT" > /dev/null
}

transform_detail_pages() {
  local tmpdir="$1"
  local mappings="$2"

  log "transform detail files' html to JSON"
  extract_html -mappings "${mappings}/detail.json" -dir "${tmpdir}/detail" | jq > "${tmpdir}/detail.json"
}

prepare_subcategory_pages() {
  local tmpdir="$1"
  local mappings="$2"

  generate_subcategory_urls "$tmpdir" "$mappings" "$MAX_SUBCATEGORY_URLS"
  fetch_subcategory_pages "$tmpdir"
  transform_subcategory_pages "$tmpdir" "$mappings"
}

prepare_detail_pages() {
  local tmpdir="$1"
  local mappings="$2"

  generate_detail_urls "$tmpdir" "$MAX_DETAIL_URLS"
  fetch_detail_pages "$tmpdir"
  transform_detail_pages "$tmpdir" "$mappings"
}

validate_subcategory() {
  local tmpdir="$1"
  local mappings="$2"

  log "validate mappings of subcategory pages are valid"
  export E2E_TARGET_URLS
  E2E_TARGET_URLS="$(paste -sd, "${tmpdir}/subcategory-urls")"
  export E2E_MAPPINGS_PATH="${mappings}/subcategory.json"
  (cd /app && go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./...) | grep -E -v 'no test'
}

validate_detail() {
  local tmpdir="$1"
  local mappings="$2"

  log "validate mappings of detail pages are valid"
  export E2E_TARGET_URLS
  E2E_TARGET_URLS="$(paste -sd, "${tmpdir}/detail-urls")"
  E2E_TARGET_URLS="${E2E_TARGET_URLS},https://www.azet.sk/firma/2074/mr-real-s-r-o_1/,https://www.azet.sk/firma/1193737/brands-alliance-service-s-r-o/,https://www.azet.sk/firma/1247928/ivmo-real-s-r-o/,https://www.azet.sk/firma/1229449/stavega-s-r-o/"
  export E2E_MAPPINGS_PATH="${mappings}/detail.json"
  (cd /app && go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./...) | grep -v 'no test'
}

validate() {
  local tmpdir="$1"
  local mappings="$2"

  validate_subcategory "$tmpdir" "$mappings"
  validate_detail "$tmpdir" "$mappings"
}

generate_config() {
  local tmpdir="$1"

  log "generate etl config file"
  probe -backend sqlite -bytes 1000000 -job detail -name detail -url "file://${tmpdir}/detail.json" -pretty -multitable > "${tmpdir}/config.json"
}

run_etl() {
  local tmpdir="$1"

  log "run ETL"
  if ! etl -metrics-backend none -config "${tmpdir}/config.json"; then
    err "etl failed"
  fi
}

main() {
  get_category_urls "$TMPDIR" "$URL" "$mappings_dir"
  prepare_subcategory_pages "$TMPDIR" "$mappings_dir"
  prepare_detail_pages "$TMPDIR" "$mappings_dir"

  generate_config "$TMPDIR"
  validate "$TMPDIR" "$mappings_dir"

  run_etl "$TMPDIR"
}
main
