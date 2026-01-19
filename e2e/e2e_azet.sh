#!/bin/bash
mappings_dir="/home/zippy/projects/etl_pipeline/etl/configs/mappings/azet.sk"
#mkdir e2e_test
#cd e2e_test
#### get URLs of top-level categories ####
wget -q -O index.html https://www.azet.sk/katalog/

extract_html -mappings "${mappings_dir}/mappings-top-level.json" -dir . | jq -r '.[].href' > top-level-urls

#### generate subcategory urls ####
head -1 top-level-urls | while read line; do 
	extract_html -mappings "${mappings_dir}/mappings-build-all-category-pages.json" -timeout 120s -url "$line" | go run generate_subcategory_urls.go
done | sort | uniq | head -3 > subcategory-urls

#### get subcategory pages ####
jget -o subcategory -i subcategory-urls -n 1 -name azet.sk -t 120s > /dev/null

#### make subcategory.json from subcategory files ####
extract_html -mappings "${mappings_dir}/mappings-subcategory-page.json" -dir subcategory | jq > subcategory.json

#### output list of detail urls ####
cat subcategory.json | jq -r '.[].href' | sort | uniq | head -1 > detail-urls.all

#### get detail pages ####
jget -n 1 -i detail-urls.all -o detail -t 120s > /dev/null

cp detail-urls.all /tmp/
cp subcategory-urls /tmp/

#### transform detail.html to JSON ####
extract_html -mappings "${mappings_dir}/mappings-detail.json" -dir detail | jq > detail.json

cd /home/zippy/projects/etl_pipeline/etl

echo "================== azet subcategory pages ==========================="
export E2E_TARGET_URLS=$(paste -sd, /tmp/subcategory-urls)
export E2E=1 
export E2E_MAPPINGS_PATH="${mappings_dir}/mappings-subcategory-page.json"
go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./... | grep -E -v 'no test'

echo "================== azet detail pages ==========================="
export E2E_TARGET_URLS=$(paste -sd, /tmp/detail-urls.all)
E2E_TARGET_URLS="${E2E_TARGET_URLS},https://www.azet.sk/firma/2074/mr-real-s-r-o_1/,https://www.azet.sk/firma/1193737/brands-alliance-service-s-r-o/,https://www.azet.sk/firma/1247928/ivmo-real-s-r-o/,https://www.azet.sk/firma/1229449/stavega-s-r-o/"

export E2E=1 
export E2E_MAPPINGS_PATH="${mappings_dir}/mappings-detail.json"
go test -run '^TestE2E_Strict_MappingsPopulateAcrossMultiplePages$' -tags=e2e -count=1 ./... | grep -v 'no test'

rm /tmp/subcategory-urls
rm /tmp/detail-urls.all
