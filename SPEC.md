# Rust Change Data Capture Demo

## Purpose

This repository will become a small, runnable Rust demo that shows how Change
Data Capture (CDC) keeps a derived read model in sync with source data.

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
UPDATE users
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

After the CDC event is consumed by the Rust indexer, OpenSearch should return:

```json
{
  "id": 42,
  "name": "Ada",
  "plan": "pro"
}
```

The user should be able to see the stale read, produce the change event, run the
indexer, and see the derived view catch up.

## First Version

The first version should be a CDC simulator, not a full Postgres + Debezium
deployment.

That means:

- A Rust CLI produces Debezium-style change records to a Kafka-compatible topic.
- Redpanda provides the local Kafka-compatible broker.
- A Rust indexer consumes the topic and updates OpenSearch.
- The same Rust CLI queries OpenSearch so the reader can observe stale and
  current values.

This keeps the project easy to run and easy to understand. Once the core demo is
clear, a later phase can replace the simulated producer with real Postgres
logical decoding through Debezium.

## Architecture

```text
Rust CLI
  seed/query/produce
        |
        | produce Debezium-style event
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

The final demo should have one obvious path:

```bash
just up
just seed
just query
just produce-upgrade
just query
just index-once
just query
```

Expected behavior:

1. `just up` starts Redpanda and OpenSearch.
2. `just seed` writes `users/_doc/42` to OpenSearch with `plan: free`.
3. The first `just query` shows `plan: free`.
4. `just produce-upgrade` writes a CDC event showing `free -> pro` to
   `app.public.users`.
5. The next `just query` can still show `plan: free`, demonstrating the stale
   derived read.
6. `just index-once` consumes the event and updates OpenSearch.
7. The final `just query` shows `plan: pro`.

The demo may also support a long-running indexer:

```bash
cargo run --bin indexer -- run
```

That mode is useful after the reader understands the stale-read window.

## Rust Binaries

### `cdc`

The `cdc` binary is the operator-facing CLI.

Planned commands:

```text
cdc seed --user-id 42 --name Ada --plan free
cdc query --user-id 42
cdc produce --user-id 42 --name Ada --from free --to pro
cdc reset
```

Responsibilities:

- Seed the OpenSearch read model with a known user document.
- Query OpenSearch and print the current observed document.
- Produce Debezium-style update events to Redpanda.
- Reset local demo state when useful.
- Print concise, readable output for people following the demo manually.

### `indexer`

The `indexer` binary consumes CDC events and updates OpenSearch.

Planned modes:

```text
indexer run
indexer once
```

Responsibilities:

- Consume records from `app.public.users`.
- Deserialize the event envelope.
- Validate that the event targets the expected table.
- Apply `value.after` to OpenSearch document `users/_doc/{id}`.
- Commit the Kafka offset only after the OpenSearch update succeeds.
- Log each step clearly enough that readers can follow the data movement.

## Suggested Repo Shape

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
  config.rs
  event.rs
  kafka.rs
  search.rs
```

Module responsibilities:

- `config.rs`: environment variables, default URLs, topic names, and index names.
- `event.rs`: Debezium-style event structs and helper constructors.
- `kafka.rs`: producer and consumer setup.
- `search.rs`: OpenSearch document setup, writes, queries, and reset helpers.

Keep the implementation linear and explicit. This project is a teaching tool,
so readable control flow matters more than abstraction density.

## Event Contract

The produced message should intentionally resemble a Debezium update event:

```json
{
  "topic": "app.public.users",
  "key": {
    "id": 42
  },
  "value": {
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
}
```

The exact JSON can be simpler than real Debezium output, but it should preserve
the important teaching fields:

- `key.id`: the primary key.
- `value.before`: the previous source row.
- `value.after`: the committed source row.
- `value.source.schema`: the source schema.
- `value.source.table`: the source table.
- `value.source.lsn`: a source ordering marker.
- `value.op`: the operation type, with `u` meaning update.

## OpenSearch Contract

The demo should use one index:

```text
users
```

The document ID should match the user ID:

```text
users/_doc/42
```

The stored document should be intentionally small:

```json
{
  "id": 42,
  "name": "Ada",
  "plan": "pro"
}
```

This keeps the demo focused on CDC mechanics rather than search schema design.

## Dependencies

Likely Rust dependencies:

- `tokio` for async runtime.
- `clap` for CLI parsing.
- `serde` and `serde_json` for event/document serialization.
- `rdkafka` for Kafka-compatible producer and consumer access.
- `opensearch` for OpenSearch access.
- `anyhow` for application-level error context.
- `tracing` and `tracing-subscriber` for readable logs.

The final dependency set should be selected during implementation, but the
project should stay small and conventional.

## Non-Goals

The first version will not:

- Run real Postgres logical decoding.
- Run Debezium Connect.
- Model every Debezium envelope field.
- Teach Kafka partitioning, compaction, schema registries, or exactly-once
  processing.
- Build a production-ready indexing service.

Those topics are valuable, but they would make the first teaching loop harder
to run and harder to read.

## Later Phases

After the simulator works, possible next steps are:

1. Add a real Postgres container.
2. Add Debezium and Kafka Connect.
3. Replace `cdc produce` with an actual SQL update against Postgres.
4. Keep the same Rust indexer and OpenSearch query path.
5. Add a small walkthrough page or terminal transcript that maps each command to
   the CDC concept it demonstrates.

The important constraint is that each phase should preserve the same core story:
source write, stale derived read, durable change event, indexer catch-up,
correct derived read.

## Verification Goals

The project is done when a new reader can run a short command sequence and
observe:

1. OpenSearch starts with `plan: free`.
2. A CDC update event is produced with `before.plan: free` and `after.plan: pro`.
3. OpenSearch still reads `plan: free` before the indexer processes the event.
4. The Rust indexer consumes the event and updates the OpenSearch document.
5. OpenSearch reads `plan: pro` after indexing.

Implementation should include focused tests for:

- Event JSON serialization and deserialization.
- Mapping `value.after` into an OpenSearch document.
- Ignoring or rejecting events for unexpected tables.
- CLI argument parsing for the main demo commands.

End-to-end validation should be available through `just` once Docker services
are part of the repo.
