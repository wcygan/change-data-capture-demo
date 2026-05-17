# Rust Change Data Capture Demo

## Purpose

This repository is a small, runnable Rust demo that shows how Change Data
Capture (CDC) keeps a derived read model in sync with source data.

The intended audience is engineers who have heard terms like CDC, Debezium,
Kafka, OpenSearch, stale reads, or read models, but have not seen the pieces
work together in a concrete local example.

The demo should make one idea obvious:

> A write can commit in the source database before every downstream system has
> observed it. CDC turns that write into a durable event so downstream systems
> can catch up without adding extra work to the request path.

## Teaching Story

The demo follows one user row:

```sql
UPDATE public.users
SET plan = 'pro'
WHERE id = 42;
```

Before downstream indexing completes, OpenSearch may still return:

```json
{
  "id": 42,
  "name": "Ada",
  "plan": "free"
}
```

After the Debezium event is consumed by the Rust indexer, OpenSearch should
return:

```json
{
  "id": 42,
  "name": "Ada",
  "plan": "pro"
}
```

The user should be able to see the stale read, update the source row, run the
indexer, and see the derived view catch up.

## Pipeline

The local demo uses the real CDC components:

- Postgres stores the source `public.users` row.
- Debezium runs as a Kafka Connect source connector.
- Redpanda provides the Kafka-compatible broker.
- OpenSearch stores the derived read model.
- Rust binaries provide the operator CLI and indexer.

The simulator path can still be useful for narrow event tests, but the main demo
path should be the real Postgres -> Debezium -> Kafka -> Rust -> OpenSearch
pipeline.

## Architecture

```text
Rust CLI
  bootstrap/seed/query/produce
        |
        | seed/update source row
        v
Postgres public.users
        |
        | logical decoding through pgoutput
        v
Debezium Postgres connector in Kafka Connect
        |
        | publish update event
        v
Redpanda topic: app.public.users
        |
        | consume event
        v
Rust indexer
        |
        | update document users/_doc/42
        v
OpenSearch
        |
        | query observed user document
        v
Rust CLI
```

## Local Developer Experience

The demo has one obvious path:

```bash
just up
just bootstrap
just reset
just seed
just query
just produce-upgrade
just query
just index-once
just query
```

Expected behavior:

1. `just up` starts Postgres, Redpanda, Kafka Connect, and OpenSearch.
2. `just bootstrap` creates `public.users` and registers the Debezium connector.
3. `just reset` clears source rows and the OpenSearch index.
4. `just seed` writes `public.users` and `users/_doc/42` with `plan: free`.
5. The first `just query` shows `plan: free`.
6. `just produce-upgrade` updates Postgres from `free` to `pro`.
7. The next `just query` can still show `plan: free`, demonstrating the stale
   derived read.
8. `just index-once` consumes the Debezium event and updates OpenSearch.
9. The final `just query` shows `plan: pro`.

The demo also supports a long-running indexer:

```bash
cargo run --bin indexer -- run
```

That mode is useful after the reader understands the stale-read window.

## Rust Binaries

### `cdc`

The `cdc` binary is the operator-facing CLI.

Commands:

```text
cdc bootstrap
cdc seed --user-id 42 --name Ada --plan free
cdc source-query --user-id 42
cdc query --user-id 42
cdc produce --user-id 42 --name Ada --from free --to pro
cdc connector-status
cdc delete-connector
cdc reset
```

Responsibilities:

- Create the source table and register the Debezium connector.
- Seed Postgres and the OpenSearch read model with a known user document.
- Query Postgres and OpenSearch so the reader can compare source and derived
  state.
- Update the source row so Debezium emits the CDC event.
- Reset local demo state when useful.
- Print concise, readable output for people following the demo manually.

### `indexer`

The `indexer` binary consumes CDC events and updates OpenSearch.

Modes:

```text
indexer run
indexer once
```

Responsibilities:

- Consume records from `app.public.users`.
- Decode Kafka key/value JSON produced by the Debezium Postgres connector.
- Validate that the event targets `public.users` and is an update.
- Apply `value.after` to OpenSearch document `users/_doc/{id}`.
- Commit the Kafka offset only after the OpenSearch update succeeds.
- Log each step clearly enough that readers can follow the data movement.

## Repo Shape

```text
Cargo.toml
SPEC.md
README.md
docker-compose.yml
justfile

src/
  bin/
    cdc.rs
    indexer.rs
  cli.rs
  config.rs
  connect.rs
  event.rs
  kafka.rs
  search.rs
  source.rs

tests/
  stale_read_flow.rs
```

Module responsibilities:

- `config.rs`: environment variables, default URLs, topic names, and index names.
- `connect.rs`: Kafka Connect REST calls for Debezium connector registration and
  readiness.
- `event.rs`: Debezium-style event structs, Kafka key/value decoding, and event
  validation.
- `kafka.rs`: Redpanda/Kafka consumer setup and offset commits.
- `search.rs`: OpenSearch document setup, writes, queries, and reset helpers.
- `source.rs`: Postgres source table setup, writes, queries, and reset helpers.

Keep the implementation linear and explicit. This project is a teaching tool,
so readable control flow matters more than abstraction density.

## Event Contract

Debezium publishes the Kafka key and value separately. With the JSON converter
and schemas disabled, the relevant update event looks like:

```json
// Kafka key
{
  "id": 42
}
```

```json
// Kafka value
{
  "before": {
    "id": 42,
    "name": "Ada",
    "plan": "free"
  },
  "after": {
    "id": 42,
    "name": "Ada",
    "plan": "pro"
  },
  "source": {
    "schema": "public",
    "table": "users",
    "lsn": 24023128
  },
  "op": "u"
}
```

The exact JSON contains additional Debezium metadata, but the indexer should
preserve these important teaching fields:

- Kafka key `id`: the primary key.
- `before`: the previous source row when available.
- `after`: the committed source row.
- `source.schema`: the source schema.
- `source.table`: the source table.
- `source.lsn`: a source ordering marker.
- `op`: the operation type, with `u` meaning update.

The source table uses `REPLICA IDENTITY FULL` so update events include enough
`before` data for the stale-read story. The indexer still treats `before` as
optional at the Rust boundary because real Debezium deployments can emit partial
`before` rows.

## OpenSearch Contract

The demo uses one index:

```text
users
```

The document ID matches the user ID:

```text
users/_doc/42
```

The stored document is intentionally small:

```json
{
  "id": 42,
  "name": "Ada",
  "plan": "pro"
}
```
