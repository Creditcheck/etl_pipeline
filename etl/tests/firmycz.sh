#!/bin/bash
#### DESCRIPTION: runs end to end tests for firmy.cz
NAME="firmy.cz" 
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

require_command() {
    local cmd="$1"
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: Required command '$cmd' not found in PATH." >&2
        exit 1
    fi
}

for program in get-subcategory-urls etl extract_html jget probe; do
  require_command "$program"
done

# Create a single-use temp working directory
TMPDIR="$(mktemp -d -t "${NAME}_e2e.XXXXXXXX")" || err "failed to create temp dir"

#### config / knobs ####
mappings_dir="/app/configs/mappings/firmy.cz"
URL="https://www.firmy.cz/?hp=1"
JGET_CONCURRENCY=3
TIMEOUT="120s"
MAX_SUBCATEGORY_URLS=4
MAX_DETAIL_URLS=2
E2E=1 # enables end to end go tests
export E2E

CATEGORY_URLS="${TMPDIR}/category-urls"
ETL_CONFIG="${TMPDIR}/config.json"
SUBCATEGORY_URLS="${TMPDIR}/subcategory-urls"
SUBCATEGORY_DIR="${TMPDIR}/subcategory"
SUBCATEGORY_JSON="${TMPDIR}/subcategory.json"
SUBCATEGORY_MAPPINGS="${mappings_dir}/subcategory-page.json"
SUBCATEGORY_BUILDER_MAPPINGS="${mappings_dir}/subcategory-builder.json"
DETAIL_URLS="${TMPDIR}/detail-urls"
DETAIL_DIR="${TMPDIR}/detail"
DETAIL_JSON="${TMPDIR}/detail.json"
DETAIL_MAPPINGS="${mappings_dir}/detail-page.json"

cleanup_tmpdir() {
  rm -rf "$TMPDIR"
}
# ensure cleanup when process recieves common 'stop running now' signals
trap cleanup_tmpdir EXIT INT TERM

cd "$TMPDIR" || err "failed to cd into temp dir: $TMPDIR"

get_category_urls() {
  #### gets URLs of top-level categories ####
  local url="$1"
  local category_urls="$2"
  local mappings="$3"
  local mapping_file="${mappings}/category.json"
  local max=1

  log "create list of category urls"
  extract_html -mappings "$mapping_file" -url "$url" | jq -r '.[].href[]' | head -"$max" > "$category_urls"
}

remove_lines() {
  # removes lines in fileA from fileB
  local fileA="$1"
  local fileB="$2"
  local tmp

  log "remove_lines"
  tmp="$(mktemp)" || return 1

  # ensure cleanup even on error
  trap 'rm -f "$tmp"' RETURN

  awk 'NR==FNR { seen[$0]; next } !($0 in seen)' "$fileA" "$fileB"
}

paginate_subcategory() {
	# input: url (string), count (int)
	# output: list of strings in format
	# url
	# url?page=2
	# url?page=3

	log "generate paginated subcategory urls"
	url="$1"
	mapping_file="$2"
	extract_html -mappings "$mapping_file" -url "$url" | jq -r '.href as $u | (.count|tonumber) as $n | $u, (range(2; $n+1) | "\($u)?page=\(.)")'
}

generate_subcategory_urls() {
  local category_urls="$1"
  local subcategory_urls="$2"
  local css_class_name='category'
  local depth=1
  local workers="$JGET_CONCURRENCY"
  local max="$MAX_SUBCATEGORY_URLS"
  local builder_mappings="$SUBCATEGORY_BUILDER_MAPPINGS"

  log "generate subcategory urls"

  get-subcategory-urls -depth "$depth" -i "$category_urls" -n "$workers" -class "$css_class_name" > "${subcategory_urls}.tmp"
  
  sort "${subcategory_urls}.tmp" | uniq > "$subcategory_urls"
  cp "$subcategory_urls" "${subcategory_urls}.tmp"

  remove_lines "$category_urls" "${subcategory_urls}.tmp" | head -"$max" > "$subcategory_urls"

  cp "$subcategory_urls" "${subcategory_urls}.tmp"

  # subcategory urls has urls in the form:
  # https://www.firmy.cz/foo/bar
  # 
  # its results are paginated. for each url, we output its paginated urls:
  cat "${subcategory_urls}.tmp" | while read -r url; do 
    paginate_subcategory "$url" "$builder_mappings"
  done | head -"$max" > "$subcategory_urls"

}

fetch_subcategory_pages() {
  local subcategory_urls="$1"
  local subcategory_dir="$2"
  local name="$NAME"
  local max="$MAX_SUBCATEGORY_URLS"

  log "get subcategory pages"
  # if 0, use all urls, otherwise use the first X urls
  if [ "$max" -eq 0 ]; then
    cp "$subcategory_urls" urls
  else
    head -n "$max" "$subcategory_urls" > urls
  fi

  jget -o "$subcategory_dir" -i urls -n "$JGET_CONCURRENCY" -name "$name" -t "$TIMEOUT"
}

transform_subcategory_pages() {
  # input: 
  #   mapping (string, html -> json mappings), 
  #   subcategory_dir (string, directory with subcategory html files)
  # 
  # output: 
  # json file 
  local subcategory_mappings="$1"
  local subcategory_dir="$2"
  local subcategory_json="$3"

  log "make subcategory.json from subcategory files"
  extract_html -mappings "$subcategory_mappings" -dir "$subcategory_dir" > "$subcategory_json"
}

prepare_subcategory_pages() {
  local category_urls="$1"
  local subcategory_urls="$SUBCATEGORY_URLS"
  local subcategory_dir="$SUBCATEGORY_DIR"
  local subcategory_json="$SUBCATEGORY_JSON"
  local subcategory_mappings="$SUBCATEGORY_MAPPINGS"

  generate_subcategory_urls "$category_urls" "$subcategory_urls"
  fetch_subcategory_pages "$subcategory_urls" "$subcategory_dir"
  transform_subcategory_pages "$subcategory_mappings" "$subcategory_dir" "$subcategory_json"
}

generate_detail_urls() {
  local subcategory_json="$1"
  local detail_urls="$2"
  local max="$3"

  log "output detail urls"
  jq -r '.[].href' "$subcategory_json" | sort | uniq | head -"$max" > "$detail_urls"
}

fetch_detail_pages() {
  local detail_urls="$1"
  local detail_dir="$2"

  log "get detail pages"
  jget -n "$JGET_CONCURRENCY" -i "$detail_urls" -o "$detail_dir"
}

transform_detail_pages() {
  # converts detail page html files to json 
  # output: json data file 

  local mapping_file="$1"
  local detail_dir="$2"
  local detail_json="$3"

  log "transform detail files: html to json"
  extract_html -mappings "$mapping_file" -dir "$detail_dir" > "$detail_json"
}

prepare_detail_pages() {
  local detail_urls="$DETAIL_URLS"
  local detail_mappings="$DETAIL_MAPPINGS"
  local detail_dir="$DETAIL_DIR"
  local detail_json="$DETAIL_JSON"

  generate_detail_urls "$SUBCATEGORY_JSON" "$detail_urls" "$MAX_DETAIL_URLS"
  fetch_detail_pages "$detail_urls" "$detail_dir"
  transform_detail_pages "$detail_mappings" "$detail_dir" "$detail_json"
}

validate() {
  local urls="$1"
  local mappings="$2"
  local other_urls="${3:-}"


  log "validate mappings ${mappings}"
  E2E_TARGET_URLS="$(paste -sd, "$urls")$other_urls"
  E2E_MAPPINGS_PATH="$mappings"
  export E2E_TARGET_URLS
  export E2E_MAPPINGS_PATH
  cur=$(pwd)
  cd /app || exit 1
  if ! go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./...;then
    err "validate mappings $mappings go tests failed"
  fi
  cd "$cur" || exit 1
}

generate_etl_config() {
  local detail_json="$1"
  local etl_config="$2"
  local name="$NAME"
  local bytes=1000000

  log "generate etl config file"
  probe -backend sqlite -bytes "$bytes" -name "detail" -url "file://${detail_json}" > "$etl_config"
}

run_etl() {
  local etl_config="$1"

  log "run ETL"
  if ! etl -metrics-backend none -config "$etl_config"; then
    err "etl failed"
  fi
}

main() {
  get_category_urls "$URL" "$CATEGORY_URLS" "$mappings_dir"
  prepare_subcategory_pages "$CATEGORY_URLS"
  prepare_detail_pages

  generate_etl_config "$DETAIL_JSON" "$ETL_CONFIG"
  validate "$DETAIL_URLS" "$DETAIL_MAPPINGS" ",https://www.firmy.cz/detail/13103653-ck-blue-style-pardubice-zelene-predmesti.html,https://www.firmy.cz/detail/155496-emos-spol-s-r-o-prerov-i-mesto.html"
  validate "$SUBCATEGORY_URLS" "$SUBCATEGORY_MAPPINGS"

  run_etl "$ETL_CONFIG"
}
main
