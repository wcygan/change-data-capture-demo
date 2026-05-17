use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time;

use crate::config::DemoConfig;

pub struct ConnectClient {
    http: reqwest::Client,
    base_url: String,
}

impl ConnectClient {
    #[must_use]
    pub fn from_config(config: &DemoConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: config.connect_url.trim_end_matches('/').to_owned(),
        }
    }

    pub async fn put_postgres_connector(&self, config: &DemoConfig) -> Result<()> {
        let response = self
            .http
            .put(self.connector_config_url(&config.connector_name))
            .json(&postgres_connector_config(config))
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to send Kafka Connect connector request to `{}`",
                    config.connect_url
                )
            })?;

        ensure_success(response, "register Debezium Postgres connector")
            .await
            .map(|_| ())
    }

    pub async fn delete_connector(&self, name: &str) -> Result<()> {
        let response = self
            .http
            .delete(self.connector_url(name))
            .send()
            .await
            .with_context(|| format!("failed to delete Kafka Connect connector `{name}`"))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        ensure_success(response, "delete Debezium Postgres connector")
            .await
            .map(|_| ())
    }

    pub async fn status(&self, name: &str) -> Result<ConnectorStatus> {
        let response = self
            .http
            .get(format!("{}/status", self.connector_url(name)))
            .send()
            .await
            .with_context(|| format!("failed to query Kafka Connect connector `{name}` status"))?;

        if response.status() == StatusCode::NOT_FOUND {
            bail!("Kafka Connect connector `{name}` does not exist");
        }

        let response = ensure_success(response, "query Debezium Postgres connector status").await?;

        response
            .json::<ConnectorStatus>()
            .await
            .context("failed to decode Kafka Connect connector status")
    }

    pub async fn wait_until_running(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<ConnectorStatus> {
        let deadline = Instant::now() + timeout;

        loop {
            let last_error = match self.status(name).await {
                Ok(status) if status.is_running() => return Ok(status),
                Ok(status) => format!("connector status was `{}`", status.summary()),
                Err(error) => error.to_string(),
            };

            if Instant::now() >= deadline {
                bail!("timed out waiting for connector `{name}` to run: {last_error}");
            }

            time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn connector_url(&self, name: &str) -> String {
        format!("{}/connectors/{name}", self.base_url)
    }

    fn connector_config_url(&self, name: &str) -> String {
        format!("{}/config", self.connector_url(name))
    }
}

fn postgres_connector_config(config: &DemoConfig) -> Value {
    json!({
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": &config.connect_postgres_host,
        "database.port": config.connect_postgres_port.to_string(),
        "database.user": &config.postgres_user,
        "database.password": &config.postgres_password,
        "database.dbname": &config.postgres_db,
        "topic.prefix": &config.topic_prefix,
        "schema.include.list": "public",
        "table.include.list": "public.users",
        "plugin.name": "pgoutput",
        "slot.name": &config.connector_slot_name,
        "slot.drop.on.stop": "true",
        "publication.name": &config.connector_publication_name,
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "no_data",
        "skipped.operations": "c,d,t",
        "tombstones.on.delete": "false",
        "include.schema.changes": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    })
}

async fn ensure_success(response: reqwest::Response, action: &str) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|error| format!("<failed to read response body: {error}>"));

    bail!("{action} failed with {status}: {body}");
}

#[derive(Debug, Deserialize)]
pub struct ConnectorStatus {
    pub name: String,
    pub connector: ConnectorState,
    #[serde(default)]
    pub tasks: Vec<ConnectorState>,
}

impl ConnectorStatus {
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.connector.state == "RUNNING"
            && !self.tasks.is_empty()
            && self.tasks.iter().all(|task| task.state == "RUNNING")
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let task_states = self
            .tasks
            .iter()
            .map(|task| task.state.as_str())
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "{} connector={}, tasks=[{}]",
            self.name, self.connector.state, task_states
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct ConnectorState {
    pub state: String,
}
