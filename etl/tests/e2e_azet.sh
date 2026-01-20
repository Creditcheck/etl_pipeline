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

mappings_dir="/app/configs/mappings/azet.sk"
URL="https://www.azet.sk/katalog/"

export E2E=1
START_DIR="$(pwd)"

# Create a single-use temp working directory and run everything inside it.
TMPDIR="$(mktemp -d -t azet_e2e.XXXXXXXX)" || err "failed to create temp dir"
cleanup_tmpdir() {
  rm -rf "$TMPDIR"
}
trap cleanup_tmpdir EXIT INT TERM

cd "$TMPDIR" || err "failed to cd into temp dir: $TMPDIR"

get_category_urls() {
  log "extract category urls"
  #### get URLs of top-level categories ####
  wget -q -O index.html "$URL"
  extract_html -mappings "${mappings_dir}/category.json" -dir . | jq -r '.[].href' > category-urls
}

generate_subcategory_urls() {
  log "generate subcategory urls"
  head -1 category-urls | while read -r line; do
    extract_html -mappings "${mappings_dir}/subcategory-url-builder.json" -timeout 120s -url "$line" | expand_hrefs
  done | sort | uniq | head -3 > subcategory-urls
}

get_subcategory_pages() {
  log "get subcategory pages"
  jget -o subcategory -i subcategory-urls -n 1 -name azet.sk -t 120s > /dev/null
}

do_subcategory_mappings() {
  log "make subcategory.json from subcategory files"
  extract_html -mappings "${mappings_dir}/subcategory.json" -dir subcategory | jq > subcategory.json
}

prepare_detail_pages() {
  generate_detail_urls
  get_detail_pages
  do_detail_mappings
}

prepare_subcategory_pages() {
  generate_subcategory_urls
  get_subcategory_pages
  do_subcategory_mappings
}

validate() {
  #cd /app || return 1
  validate_subcategory
  validate_detail
}

generate_detail_urls() {
  log "output detail urls"
  jq -r '.[].href' subcategory.json | sort | uniq | head -2 > detail-urls
}

get_detail_pages() {
  log "get detail pages"
  jget -n 1 -i detail-urls -o detail -t 120s > /dev/null
}

do_detail_mappings() {
  log "transform detail files' html to JSON"
  extract_html -mappings "${mappings_dir}/detail.json" -dir detail | jq > "${TMPDIR}/detail.json"
}

validate_subcategory() {
  log "validate mappings of subcategory pages are valid"
  export E2E_TARGET_URLS
  E2E_TARGET_URLS="$(paste -sd, "${TMPDIR}/subcategory-urls")"
  export E2E_MAPPINGS_PATH="${mappings_dir}/subcategory.json"
  go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./... | grep -E -v 'no test'
}

validate_detail() {
  log "validate mappings of detail pages are valid"
  export E2E_TARGET_URLS
  E2E_TARGET_URLS="$(paste -sd, "${TMPDIR}/detail-urls")"
  E2E_TARGET_URLS="${E2E_TARGET_URLS},https://www.azet.sk/firma/2074/mr-real-s-r-o_1/,https://www.azet.sk/firma/1193737/brands-alliance-service-s-r-o/,https://www.azet.sk/firma/1247928/ivmo-real-s-r-o/,https://www.azet.sk/firma/1229449/stavega-s-r-o/"
  export E2E_MAPPINGS_PATH="${mappings_dir}/detail.json"
  go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./... | grep -v 'no test'
}

generate_config() {
  log "generate etl config file"
  #cd "$START_DIR" || return 1
  probe -backend sqlite -bytes 1000000 -job detail -name detail -url "file://${TMPDIR}/detail.json" -pretty -multitable > "${TMPDIR}/config.json"
}

run_etl() {
  log "run ETL"
  # this is a hack until paths and env are settled
  if ! etl -config "${TMPDIR}/config.json"; then
    err "etl failed"
  fi
}

main() {
  get_category_urls
  prepare_subcategory_pages
  prepare_detail_pages

  # Keep these alongside other temp outputs for validation.
  cp detail-urls "${TMPDIR}/"
  cp subcategory-urls "${TMPDIR}/"

  generate_config
  validate

  run_etl
}
main

