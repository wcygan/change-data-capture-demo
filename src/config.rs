use std::env;

pub const DEFAULT_TOPIC_PREFIX: &str = "app";
pub const DEFAULT_TOPIC: &str = "app.public.users";
pub const DEFAULT_INDEX: &str = "users";
pub const DEFAULT_KAFKA_BROKERS: &str = "localhost:19092";
pub const DEFAULT_CONSUMER_GROUP: &str = "cdc-demo-indexer";
pub const DEFAULT_OPENSEARCH_URL: &str = "http://localhost:9200";
pub const DEFAULT_CONSUME_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_POSTGRES_HOST: &str = "localhost";
pub const DEFAULT_POSTGRES_PORT: u16 = 15_432;
pub const DEFAULT_POSTGRES_USER: &str = "cdc";
pub const DEFAULT_POSTGRES_PASSWORD: &str = "cdc";
pub const DEFAULT_POSTGRES_DB: &str = "app";
pub const DEFAULT_CONNECT_URL: &str = "http://localhost:8083";
pub const DEFAULT_CONNECTOR_NAME: &str = "cdc-demo-postgres-users";
pub const DEFAULT_CONNECT_POSTGRES_HOST: &str = "postgres";
pub const DEFAULT_CONNECT_POSTGRES_PORT: u16 = 5432;
pub const DEFAULT_CONNECTOR_SLOT_NAME: &str = "cdc_demo_users_slot";
pub const DEFAULT_CONNECTOR_PUBLICATION_NAME: &str = "cdc_demo_users_publication";
pub const DEFAULT_CONNECTOR_WAIT_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoConfig {
    pub kafka_brokers: String,
    pub topic_prefix: String,
    pub topic: String,
    pub consumer_group: String,
    pub opensearch_url: String,
    pub index: String,
    pub consume_timeout_ms: u64,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_db: String,
    pub connect_url: String,
    pub connector_name: String,
    pub connect_postgres_host: String,
    pub connect_postgres_port: u16,
    pub connector_slot_name: String,
    pub connector_publication_name: String,
    pub connector_wait_timeout_ms: u64,
}

impl DemoConfig {
    #[must_use]
    pub fn from_env() -> Self {
        let topic_prefix = env_or("CDC_TOPIC_PREFIX", DEFAULT_TOPIC_PREFIX);
        let default_topic = format!("{topic_prefix}.public.users");

        Self {
            kafka_brokers: env_or("CDC_KAFKA_BROKERS", DEFAULT_KAFKA_BROKERS),
            topic: env::var("CDC_TOPIC").unwrap_or(default_topic),
            topic_prefix,
            consumer_group: env_or("CDC_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
            opensearch_url: env_or("CDC_OPENSEARCH_URL", DEFAULT_OPENSEARCH_URL),
            index: env_or("CDC_OPENSEARCH_INDEX", DEFAULT_INDEX),
            consume_timeout_ms: env::var("CDC_CONSUME_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_CONSUME_TIMEOUT_MS),
            postgres_host: env_or("CDC_POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
            postgres_port: env_u16_or("CDC_POSTGRES_PORT", DEFAULT_POSTGRES_PORT),
            postgres_user: env_or("CDC_POSTGRES_USER", DEFAULT_POSTGRES_USER),
            postgres_password: env_or("CDC_POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
            postgres_db: env_or("CDC_POSTGRES_DB", DEFAULT_POSTGRES_DB),
            connect_url: env_or("CDC_CONNECT_URL", DEFAULT_CONNECT_URL),
            connector_name: env_or("CDC_CONNECTOR_NAME", DEFAULT_CONNECTOR_NAME),
            connect_postgres_host: env_or(
                "CDC_CONNECT_POSTGRES_HOST",
                DEFAULT_CONNECT_POSTGRES_HOST,
            ),
            connect_postgres_port: env_u16_or(
                "CDC_CONNECT_POSTGRES_PORT",
                DEFAULT_CONNECT_POSTGRES_PORT,
            ),
            connector_slot_name: env_or("CDC_CONNECTOR_SLOT_NAME", DEFAULT_CONNECTOR_SLOT_NAME),
            connector_publication_name: env_or(
                "CDC_CONNECTOR_PUBLICATION_NAME",
                DEFAULT_CONNECTOR_PUBLICATION_NAME,
            ),
            connector_wait_timeout_ms: env::var("CDC_CONNECTOR_WAIT_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_CONNECTOR_WAIT_TIMEOUT_MS),
        }
    }

    #[must_use]
    pub fn postgres_connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.postgres_host,
            self.postgres_port,
            self.postgres_user,
            self.postgres_password,
            self.postgres_db
        )
    }
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_u16_or(name: &str, default: u16) -> u16 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
