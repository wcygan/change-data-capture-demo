# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust teaching demo for the stale-read window solved by change data capture. Core library modules live in `src/`: `event.rs` defines Debezium-style user update payloads, `source.rs` wraps Postgres source access, `connect.rs` wraps Kafka Connect/Debezium registration, `kafka.rs` wraps Redpanda/Kafka consumer behavior, `search.rs` wraps OpenSearch read-model access, `config.rs` reads `CDC_*` environment defaults, and `cli.rs` defines Clap commands. Binary entry points are `src/bin/cdc.rs` and `src/bin/indexer.rs`. Integration tests live in `tests/`; the Docker-backed flow is `tests/stale_read_flow.rs`. Local services are described in `docker-compose.yml`, and common workflows are exposed through `justfile`.

## Build, Test, and Development Commands

Use `just --list` to see the command surface. Key commands:

- `just up`: start Postgres, Redpanda, Kafka Connect, and OpenSearch with Docker Compose.
- `just bootstrap`: create the source table and register the Debezium connector.
- `just demo`: run the full stale-read teaching loop.
- `just seed`, `just source-query`, `just query`, `just produce-upgrade`, `just index-once`: step through the demo manually.
- `just indexer-run`: run the indexer continuously.
- `just reset`: delete source rows and the OpenSearch read-model index.
- `just check`: run `cargo fmt --check`, Clippy with warnings denied, and unit tests.
- `just integration-test`: start services and run the ignored Docker-backed real Debezium stale-read flow test.

## Coding Style & Naming Conventions

Use Rust 2024 idioms and keep code readable: clear names, simple branches, and small helper functions for multi-step behavior. Run `cargo fmt` before submitting changes; CI-equivalent formatting uses `cargo fmt --check`. Treat `cargo clippy --all-targets --all-features -- -D warnings` as the linting bar. Keep public data structures explicit and serializable where they model CDC payloads. Prefer `snake_case` for functions, modules, tests, and local variables; use `PascalCase` for types and enum variants.

## Testing Guidelines

Place fast unit tests next to the module under `#[cfg(test)]`. Use descriptive test names such as `rejects_events_for_unexpected_tables`. Run `just test` for normal local tests and `just check` before a handoff. Tests that need Docker services should live under `tests/`, be marked ignored, and run through `just integration-test`.

## Commit & Pull Request Guidelines

The current history is minimal (`init`), so use short imperative commit subjects like `add stale-read integration test` or `harden CDC event validation`. Keep PRs focused, describe the demo behavior changed, list verification commands run, and note any Docker, OpenSearch, Redpanda, or `CDC_*` configuration impact.

## Security & Configuration Tips

Do not commit `.env`, `.env.*`, `.docker/`, logs, or machine-specific Docker overrides. Keep defaults local-development friendly, and document any new environment variable in `README.md` plus `config.rs`. The checked-in Postgres/Debezium credentials are local demo defaults only.
