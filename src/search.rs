use anyhow::{Context, Result, bail};
use opensearch::http::response::Response;
use opensearch::http::transport::Transport;
use opensearch::indices::{
    IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesRefreshParts,
};
use opensearch::{GetParts, IndexParts, OpenSearch};
use serde_json::{Value, json};

use crate::config::DemoConfig;
use crate::event::{DebeziumUpdateEvent, EventValidationError, UserDocument};

pub fn document_from_event(
    event: &DebeziumUpdateEvent,
) -> Result<UserDocument, EventValidationError> {
    event.after_document()
}

#[derive(Clone)]
pub struct SearchClient {
    client: OpenSearch,
    index: String,
}

impl SearchClient {
    pub fn from_config(config: &DemoConfig) -> Result<Self> {
        Self::new(&config.opensearch_url, &config.index)
    }

    pub fn new(opensearch_url: &str, index: impl Into<String>) -> Result<Self> {
        let transport = Transport::single_node(opensearch_url)
            .with_context(|| format!("invalid OpenSearch URL `{opensearch_url}`"))?;

        Ok(Self {
            client: OpenSearch::new(transport),
            index: index.into(),
        })
    }

    pub async fn seed_user(&self, user: &UserDocument) -> Result<()> {
        self.index_user(user).await
    }

    pub async fn index_user(&self, user: &UserDocument) -> Result<()> {
        self.ensure_index().await?;

        let document_id = user.id.to_string();
        let response = self
            .client
            .index(IndexParts::IndexId(&self.index, &document_id))
            .body(json!(user))
            .send()
            .await
            .context("failed to send OpenSearch index request")?;

        ensure_success(response, "index user document").await?;
        self.refresh().await?;

        Ok(())
    }

    pub async fn query_user(&self, user_id: i64) -> Result<Option<UserDocument>> {
        let document_id = user_id.to_string();
        let response = self
            .client
            .get(GetParts::IndexId(&self.index, &document_id))
            .send()
            .await
            .context("failed to send OpenSearch get request")?;

        if response.status_code().as_u16() == 404 {
            return Ok(None);
        }

        let status = response.status_code();
        if !status.is_success() {
            let body = response_text(response).await;
            bail!("query user document failed with {status}: {body}");
        }

        let body = response
            .json::<Value>()
            .await
            .context("failed to decode OpenSearch get response")?;

        if body.get("found").and_then(Value::as_bool) == Some(false) {
            return Ok(None);
        }

        let source = body
            .get("_source")
            .cloned()
            .context("OpenSearch response did not include `_source`")?;
        let user = serde_json::from_value(source).context("failed to decode user document")?;

        Ok(Some(user))
    }

    pub async fn reset(&self) -> Result<()> {
        let response = self
            .client
            .indices()
            .delete(IndicesDeleteParts::Index(&[&self.index]))
            .send()
            .await
            .context("failed to send OpenSearch delete-index request")?;

        if response.status_code().as_u16() == 404 {
            return Ok(());
        }

        ensure_success(response, "delete OpenSearch index").await
    }

    async fn ensure_index(&self) -> Result<()> {
        let response = self
            .client
            .indices()
            .exists(IndicesExistsParts::Index(&[&self.index]))
            .send()
            .await
            .context("failed to send OpenSearch index-exists request")?;

        match response.status_code().as_u16() {
            200 => Ok(()),
            404 => self.create_index().await,
            _ => ensure_success(response, "check OpenSearch index").await,
        }
    }

    async fn create_index(&self) -> Result<()> {
        let response = self
            .client
            .indices()
            .create(IndicesCreateParts::Index(&self.index))
            .body(json!({
                "mappings": {
                    "properties": {
                        "id": { "type": "long" },
                        "name": { "type": "keyword" },
                        "plan": { "type": "keyword" }
                    }
                }
            }))
            .send()
            .await
            .context("failed to send OpenSearch create-index request")?;

        ensure_success(response, "create OpenSearch index").await
    }

    async fn refresh(&self) -> Result<()> {
        let response = self
            .client
            .indices()
            .refresh(IndicesRefreshParts::Index(&[&self.index]))
            .send()
            .await
            .context("failed to send OpenSearch refresh request")?;

        ensure_success(response, "refresh OpenSearch index").await
    }
}

async fn ensure_success(response: Response, action: &str) -> Result<()> {
    let status = response.status_code();

    if status.is_success() {
        return Ok(());
    }

    let body = response_text(response).await;
    bail!("{action} failed with {status}: {body}");
}

async fn response_text(response: Response) -> String {
    response
        .text()
        .await
        .unwrap_or_else(|error| format!("<failed to read response body: {error}>"))
}
