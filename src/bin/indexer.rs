use std::time::Duration;

use anyhow::{Context, Result};
use change_data_capture_demo::cli::{IndexerArgs, IndexerCommand};
use change_data_capture_demo::config::DemoConfig;
use change_data_capture_demo::event::{DebeziumUpdateEvent, UserDocument};
use change_data_capture_demo::kafka::CdcConsumer;
use change_data_capture_demo::search::{SearchClient, document_from_event};
use clap::Parser;
use rdkafka::Message;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = IndexerArgs::parse();
    let config = DemoConfig::from_env();

    match args.command {
        IndexerCommand::Once => run_once(config).await,
        IndexerCommand::Run => run_forever(config).await,
    }
}

async fn run_once(config: DemoConfig) -> Result<()> {
    let search = SearchClient::from_config(&config)?;
    let consumer = CdcConsumer::connect(&config)?;
    let timeout = Duration::from_millis(config.consume_timeout_ms);

    let (message, event) = consumer.next_event_with_timeout(timeout).await?;
    let document = apply_event(&search, &event).await?;

    consumer.commit(&message)?;

    println!(
        "Indexed {}/_doc/{} with plan `{}` from Kafka offset {}.",
        config.index,
        document.id,
        document.plan,
        message.offset()
    );

    Ok(())
}

async fn run_forever(config: DemoConfig) -> Result<()> {
    let search = SearchClient::from_config(&config)?;
    let consumer = CdcConsumer::connect(&config)?;

    loop {
        let (message, event) = consumer.next_event().await?;
        let document = apply_event(&search, &event).await?;

        consumer.commit(&message)?;

        info!(
            user_id = document.id,
            plan = %document.plan,
            offset = message.offset(),
            "committed Kafka offset after OpenSearch update"
        );
    }
}

async fn apply_event(search: &SearchClient, event: &DebeziumUpdateEvent) -> Result<UserDocument> {
    info!(
        topic = %event.topic,
        key_id = event.key.id,
        before_plan = event.before_plan().unwrap_or("<unavailable>"),
        after_plan = event.after_plan().unwrap_or("<missing>"),
        "received CDC update event"
    );

    let document = document_from_event(event).context("CDC event did not target public.users")?;

    search.index_user(&document).await?;

    info!(
        user_id = document.id,
        plan = %document.plan,
        "updated OpenSearch document from value.after"
    );

    Ok(document)
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "change_data_capture_demo=info,indexer=info".into());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
