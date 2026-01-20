#!/usr/bin/env sh
#set -euxo pipefail

#### DESCRIPTION: verifies 
# etl and probe compile
# probe generates a usable config
# etl can run the config and output valid metrics
# it is looking for this metric to have a value of 2 in the pushgateway/metrics API:
# etl_records_total{instance="",job="sample",kind="inserted"} 2

# Run from e2e/ directory
cd "$(dirname "$0")"

# 1. Build etl & probe via docker compose build
DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 docker compose build etl
docker compose run --rm etl -c '
NAME="sample"
# create sample.csv
#
echo -n "id,shape,volume
1,candycane,301
2,swallow,940" > sample.csv
#
# Generate JSON config from local CSV.
  probe \
    -url="file://sample.csv" \
    -name="$NAME" \
    -bytes=8192 \
    -multitable \
    -backend=sqlite \
    -pretty > /configs/pipeline.json

  cat /configs/pipeline.json

# 5. Run ETL with generated config; SQLite DB will be created in /data/db/etl.db
output=$(etl -config /configs/pipeline.json | grep summary | grep -o "inserted=.*" | sed "s/ .*//g")


# 6. Verify name in config
GOT=$(grep -c "$NAME" /configs/pipeline.json)
echo "GOT ${GOT}"
echo "output ${output}"
if [ "$GOT" -lt 4 ]; then
		exit 1
fi

if [[ "$output" == "inserted=2" ]]
then
	# echo "E2E test passed."
	exit 0
else
	echo "FAILED: E2E test failed"
	exit 1
fi

' 
result="$?"

docker compose down

echo ""
if [ $result -eq 0 ]
then
	echo "E2E test passed."
else
	echo "E2E test failed."
fi
echo ""
exit "$result"
