use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::time;

use crate::config::DemoConfig;
use crate::event::DebeziumUpdateEvent;

pub async fn produce_event(config: &DemoConfig, event: &DebeziumUpdateEvent) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create Kafka producer")?;

    let key = serde_json::to_string(&event.key).context("failed to encode Kafka key")?;
    let payload =
        serde_json::to_string(&event.value).context("failed to encode CDC event value")?;

    producer
        .send(
            FutureRecord::to(&config.topic).key(&key).payload(&payload),
            Timeout::After(Duration::from_secs(5)),
        )
        .await
        .map_err(|(error, _message)| anyhow!("failed to deliver CDC event: {error}"))?;

    Ok(())
}

pub struct CdcConsumer {
    consumer: StreamConsumer,
}

impl CdcConsumer {
    pub fn connect(config: &DemoConfig) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .create()
            .context("failed to create Kafka consumer")?;

        consumer
            .subscribe(&[&config.topic])
            .with_context(|| format!("failed to subscribe to Kafka topic `{}`", config.topic))?;

        Ok(Self { consumer })
    }

    pub async fn next_event(&self) -> Result<(BorrowedMessage<'_>, DebeziumUpdateEvent)> {
        loop {
            let message = match self.consumer.recv().await {
                Ok(message) => message,
                Err(error) if should_retry_consume_error(&error) => {
                    time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                Err(error) => return Err(error).context("failed to receive Kafka message"),
            };

            return decode_message(message);
        }
    }

    pub async fn next_event_with_timeout(
        &self,
        duration: Duration,
    ) -> Result<(BorrowedMessage<'_>, DebeziumUpdateEvent)> {
        time::timeout(duration, self.next_event())
            .await
            .with_context(|| format!("timed out after {duration:?} waiting for a CDC event"))?
    }

    pub fn commit(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        self.consumer
            .commit_message(message, CommitMode::Sync)
            .context("failed to commit Kafka offset")
    }
}

fn should_retry_consume_error(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)
    )
}

fn decode_message(
    message: BorrowedMessage<'_>,
) -> Result<(BorrowedMessage<'_>, DebeziumUpdateEvent)> {
    let key = match message.key_view::<str>() {
        None => None,
        Some(Ok(key)) => Some(key),
        Some(Err(error)) => bail!("Kafka message key was not UTF-8: {error}"),
    };

    let payload = match message.payload_view::<str>() {
        None => bail!("Kafka message did not include a payload"),
        Some(Ok(payload)) => payload,
        Some(Err(error)) => bail!("Kafka message payload was not UTF-8: {error}"),
    };

    let event = DebeziumUpdateEvent::from_kafka_json(message.topic(), key, Some(payload))
        .context("failed to decode CDC event payload")?;

    Ok((message, event))
}
