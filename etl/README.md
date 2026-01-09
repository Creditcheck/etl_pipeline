# Generic ETL

# memory 
GOMEMLIMIT=50000000

# TODO
add documentation
    - probe
    - etl
        - config structure
        - example configs

# Tests
docker compose run go-tests

## Integration tests
### mssql
        source .env  
        docker compose --profile mssql up --force-recreate -d  
        export MSSQL_TEST_DSN='sqlserver://sa:P@ssw0rd123!@0.0.0.0:1433?database=testdb'  
        go test ./... -tags=integration  

### postgres
    docker compose --profile postgres up --force-recreate -d  
    export TEST_PG_DSN='postgresql://user:P@ssw0rd123!@0.0.0.0:5432/testdb?sslmode=disable'  
    go test ./... -tags=integration  

### coverage
go test ./... -count=1 -coverprofile=/tmp/coverage.out; go tool cover -html=/tmp/coverage.out
