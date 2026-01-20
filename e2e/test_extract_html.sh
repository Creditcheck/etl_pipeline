#!/bin/bash
#### should be NAME="$1"
NAME="azet.sk"
counter=0
export E2E=1 
log(){
  entry="$*"
  counter=$((counter + 1))
  echo "[$(date +'%Y-%m-%dT%H:%M:%S')]: INFO: (${NAME} step ${counter}) ${entry}" >&2
}

err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S')]: ERROR: $*" >&2
  exit 1
}

cd ../etl
test_subcategory_pages() {
  log "subcategory pages"
  export E2E_MAPPINGS_PATH="../../configs/mappings/azet.sk/mappings-subcategory-page.json"
  export E2E_TARGET_URLS='https://www.azet.sk/katalog/asistencne-sluzby_3/,https://www.azet.sk/katalog/auto-moto-internetove-obchody/,https://www.azet.sk/katalog/autoskoly/' 
  go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./...
}

test_detail_pages() {
  log "detail pages"
  export E2E_MAPPINGS_PATH="../../configs/mappings/azet.sk/mappings-detail-experimental.json"
  export E2E_TARGET_URLS='https://www.azet.sk/firma/2074/mr-real-s-r-o_1/,https://www.azet.sk/firma/1193737/brands-alliance-service-s-r-o/,https://www.azet.sk/firma/1247928/ivmo-real-s-r-o/,https://www.azet.sk/firma/1229449/stavega-s-r-o/'
  go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./...
}

main() {
	test_subcategory_pages
	test_detail_pages
}
main
