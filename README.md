# Rust Change Data Capture Demo

This is a small Rust demo for the stale-read window that CDC solves.

The first version uses a simulator instead of Postgres and Debezium:

- `cdc` seeds and queries an OpenSearch read model.
- `cdc produce` writes a Debezium-style update event to Redpanda.
- `indexer once` consumes one event and applies `value.after` to OpenSearch.

## Run The Demo

Run the whole teaching loop:

```bash
just demo
```

Or step through it manually:

```bash
just up
just seed
just query
just produce-upgrade
just query
just index-once
just query
```

Expected story:

1. `just seed` writes `users/_doc/42` with `plan: free`.
2. The first `just query` shows `plan: free`.
3. `just produce-upgrade` writes a CDC event with `before.plan: free` and `after.plan: pro`.
4. The next `just query` still shows `plan: free`, because the read model has not caught up.
5. `just index-once` consumes the event and updates OpenSearch.
6. The final `just query` shows `plan: pro`.

For continuous indexing after the manual stale-read window is clear:

```bash
just indexer-run
```

## Commands

```bash
just demo
just up
just down
just status
just logs redpanda
just logs opensearch
just seed
just query
just produce-upgrade
just index-once
just indexer-run
just reset
```

## Configuration

The defaults are for the included Docker Compose services.

| Variable | Default |
| --- | --- |
| `CDC_KAFKA_BROKERS` | `localhost:19092` |
| `CDC_TOPIC` | `app.public.users` |
| `CDC_CONSUMER_GROUP` | `cdc-demo-indexer` |
| `CDC_OPENSEARCH_URL` | `http://localhost:9200` |
| `CDC_OPENSEARCH_INDEX` | `users` |
| `CDC_CONSUME_TIMEOUT_MS` | `10000` |

## Verify

```bash
just check
```

`just check` runs formatting, Clippy, and the focused unit tests for event JSON,
OpenSearch document mapping, unexpected-table rejection, and CLI parsing.
