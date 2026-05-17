set dotenv-load := true

user_id := "42"
name := "Ada"
initial_plan := "free"
upgraded_plan := "pro"
docker_config := ".docker"
compose := "DOCKER_CONFIG=" + docker_config + " docker compose"
cdc := "cargo run --quiet --bin cdc --"
indexer := "cargo run --quiet --bin indexer --"

default:
    @just --list

docker-config:
    mkdir -p {{docker_config}}
    test -f {{docker_config}}/config.json || printf '{"auths":{}}\n' > {{docker_config}}/config.json

# Start Postgres, Redpanda, Kafka Connect, and OpenSearch.
up: docker-config
    {{compose}} up -d --wait

# Stop and remove local demo services.
down:
    {{compose}} down

# Stop and remove local demo services and container state.
clean:
    {{compose}} down -v --remove-orphans

# Show local demo service status.
status:
    {{compose}} ps

# Tail logs for one service, e.g. `just logs redpanda`.
logs service="opensearch":
    {{compose}} logs -f {{service}}

# Create the source table and register the Debezium connector.
bootstrap:
    {{cdc}} bootstrap

# Show the Debezium connector status.
connect-status:
    {{cdc}} connector-status

# Seed Postgres and the OpenSearch read model with the initial user document.
seed:
    {{cdc}} seed --user-id {{user_id}} --name {{name}} --plan {{initial_plan}}

# Query the current OpenSearch read model.
query:
    {{cdc}} query --user-id {{user_id}}

# Query the current Postgres source row.
source-query:
    {{cdc}} source-query --user-id {{user_id}}

# Update the source row; Debezium publishes the CDC event: free -> pro.
produce-upgrade:
    {{cdc}} produce --user-id {{user_id}} --name {{name}} --from {{initial_plan}} --to {{upgraded_plan}}

# Consume one CDC event and apply it to OpenSearch.
index-once:
    {{indexer}} once

# Run the indexer continuously.
indexer-run:
    {{indexer}} run

# Delete source rows and the OpenSearch read model index.
reset:
    {{cdc}} reset

# Run the full teaching loop.
demo: up
    just bootstrap
    just reset
    just seed
    just query
    just produce-upgrade
    just query
    just index-once
    just query

# Format Rust code.
fmt:
    cargo fmt --check

# Run Clippy with warnings denied.
clippy:
    cargo clippy --all-targets --all-features -- -D warnings

# Run unit tests.
test:
    cargo test

# Run the Docker-backed stale-read integration test.
integration-test: up
    cargo test --test stale_read_flow -- --ignored --nocapture

# Run all local checks.
check: fmt clippy test
