use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;

#[test]
#[ignore = "requires Docker services; run `just integration-test`"]
fn real_debezium_flow_catches_up_after_index_once() -> Result<()> {
    let config = TestConfig::isolated();

    let result = run_real_debezium_flow(&config);
    let cleanup_result = cleanup(&config);

    result?;
    cleanup_result
}

fn run_real_debezium_flow(config: &TestConfig) -> Result<()> {
    bootstrap(config)?;
    reset_read_model(config)?;

    let seeded_user = seed_user(config)?;
    assert_plan(&seeded_user, "free")?;

    let source_before = query_source_user(config)?;
    assert_plan(&source_before, "free")?;

    let first_query = query_user(config)?;
    assert_plan(&first_query, "free")?;

    let updated_source = produce_upgrade_event(config)?;
    assert_plan(&updated_source, "pro")?;

    let stale_query = query_user(config)?;
    assert_plan(&stale_query, "free")?;

    index_once(config)?;

    let caught_up_query = query_user(config)?;
    assert_plan(&caught_up_query, "pro")?;

    Ok(())
}

struct TestConfig {
    topic_prefix: String,
    topic: String,
    consumer_group: String,
    index: String,
    connector_name: String,
    connector_slot_name: String,
    connector_publication_name: String,
    user_id: i64,
}

impl TestConfig {
    fn isolated() -> Self {
        let suffix = unique_suffix();

        let topic_prefix = format!("cdc_test_{suffix}");

        Self {
            topic: format!("{topic_prefix}.public.users"),
            topic_prefix,
            consumer_group: format!("cdc-test-indexer-{suffix}"),
            index: format!("cdc-test-users-{suffix}"),
            connector_name: format!("cdc-test-connector-{suffix}"),
            connector_slot_name: format!("cdc_test_slot_{suffix}"),
            connector_publication_name: format!("cdc_test_publication_{suffix}"),
            user_id: 42,
        }
    }

    fn apply_to(&self, command: &mut Command) {
        command
            .env("CDC_TOPIC_PREFIX", &self.topic_prefix)
            .env("CDC_TOPIC", &self.topic)
            .env("CDC_CONSUMER_GROUP", &self.consumer_group)
            .env("CDC_OPENSEARCH_INDEX", &self.index)
            .env("CDC_CONNECTOR_NAME", &self.connector_name)
            .env("CDC_CONNECTOR_SLOT_NAME", &self.connector_slot_name)
            .env(
                "CDC_CONNECTOR_PUBLICATION_NAME",
                &self.connector_publication_name,
            );
    }
}

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_millis();

    format!("{}_{}", std::process::id(), millis)
}

fn bootstrap(config: &TestConfig) -> Result<()> {
    run_cdc(config, &["bootstrap"])?;
    Ok(())
}

fn cleanup(config: &TestConfig) -> Result<()> {
    let mut cleanup_errors = Vec::new();

    if let Err(error) = reset_read_model(config) {
        cleanup_errors.push(format!("reset failed: {error:#}"));
    }

    if let Err(error) = delete_connector(config) {
        cleanup_errors.push(format!("delete connector failed: {error:#}"));
    }

    if cleanup_errors.is_empty() {
        Ok(())
    } else {
        bail!("cleanup failed:\n{}", cleanup_errors.join("\n"));
    }
}

fn delete_connector(config: &TestConfig) -> Result<()> {
    run_cdc(config, &["delete-connector"])?;
    Ok(())
}

fn reset_read_model(config: &TestConfig) -> Result<()> {
    run_cdc(config, &["reset"])?;
    Ok(())
}

fn seed_user(config: &TestConfig) -> Result<Value> {
    let user_id = config.user_id.to_string();

    run_cdc_json(
        config,
        &[
            "seed",
            "--user-id",
            &user_id,
            "--name",
            "Ada",
            "--plan",
            "free",
        ],
    )
}

fn query_user(config: &TestConfig) -> Result<Value> {
    let user_id = config.user_id.to_string();

    run_cdc_json(config, &["query", "--user-id", &user_id])
}

fn query_source_user(config: &TestConfig) -> Result<Value> {
    let user_id = config.user_id.to_string();

    run_cdc_json(config, &["source-query", "--user-id", &user_id])
}

fn produce_upgrade_event(config: &TestConfig) -> Result<Value> {
    let user_id = config.user_id.to_string();

    run_cdc_json(
        config,
        &[
            "produce",
            "--user-id",
            &user_id,
            "--name",
            "Ada",
            "--from",
            "free",
            "--to",
            "pro",
        ],
    )
}

fn index_once(config: &TestConfig) -> Result<()> {
    run_indexer(config, &["once"])?;
    Ok(())
}

fn run_cdc(config: &TestConfig, args: &[&str]) -> Result<String> {
    run_binary(env!("CARGO_BIN_EXE_cdc"), config, args)
}

fn run_indexer(config: &TestConfig, args: &[&str]) -> Result<String> {
    run_binary(env!("CARGO_BIN_EXE_indexer"), config, args)
}

fn run_binary(binary_path: &str, config: &TestConfig, args: &[&str]) -> Result<String> {
    let mut command = Command::new(binary_path);
    command.args(args);
    config.apply_to(&mut command);

    let output = command
        .output()
        .with_context(|| format!("failed to run `{binary_path}`"))?;

    ensure_success(binary_path, &output)
}

fn run_cdc_json(config: &TestConfig, args: &[&str]) -> Result<Value> {
    let stdout = run_cdc(config, args)?;
    json_from_stdout(&stdout)
}

fn ensure_success(binary_path: &str, output: &Output) -> Result<String> {
    if output.status.success() {
        return String::from_utf8(output.stdout.clone()).context("stdout was not UTF-8");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    bail!(
        "`{binary_path}` failed with status {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
}

fn json_from_stdout(stdout: &str) -> Result<Value> {
    let json_start = stdout
        .find('{')
        .ok_or_else(|| anyhow!("command output did not include a JSON object:\n{stdout}"))?;

    serde_json::from_str(&stdout[json_start..])
        .with_context(|| format!("failed to decode JSON from command output:\n{stdout}"))
}

fn assert_plan(user: &Value, expected_plan: &str) -> Result<()> {
    let actual_plan = user
        .get("plan")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("user JSON did not include a string `plan`: {user}"))?;

    if actual_plan != expected_plan {
        bail!("expected plan `{expected_plan}`, got `{actual_plan}` in {user}");
    }

    Ok(())
}
