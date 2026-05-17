use std::env;

pub const DEFAULT_TOPIC: &str = "app.public.users";
pub const DEFAULT_INDEX: &str = "users";
pub const DEFAULT_KAFKA_BROKERS: &str = "localhost:19092";
pub const DEFAULT_CONSUMER_GROUP: &str = "cdc-demo-indexer";
pub const DEFAULT_OPENSEARCH_URL: &str = "http://localhost:9200";
pub const DEFAULT_CONSUME_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoConfig {
    pub kafka_brokers: String,
    pub topic: String,
    pub consumer_group: String,
    pub opensearch_url: String,
    pub index: String,
    pub consume_timeout_ms: u64,
}

impl DemoConfig {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: env_or("CDC_KAFKA_BROKERS", DEFAULT_KAFKA_BROKERS),
            topic: env_or("CDC_TOPIC", DEFAULT_TOPIC),
            consumer_group: env_or("CDC_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            opensearch_url: env_or("CDC_OPENSEARCH_URL", DEFAULT_OPENSEARCH_URL),
            index: env_or("CDC_OPENSEARCH_INDEX", DEFAULT_INDEX),
            consume_timeout_ms: env::var("CDC_CONSUME_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_CONSUME_TIMEOUT_MS),
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}
