use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const EXPECTED_SCHEMA: &str = "public";
pub const EXPECTED_TABLE: &str = "users";
pub const UPDATE_OPERATION: &str = "u";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserDocument {
    pub id: i64,
    pub name: String,
    pub plan: String,
}

impl UserDocument {
    #[must_use]
    pub fn new(id: i64, name: impl Into<String>, plan: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            plan: plan.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeKey {
    pub id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceMetadata {
    pub schema: String,
    pub table: String,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebeziumUpdateValue {
    pub before: Option<UserDocumentBefore>,
    pub after: Option<UserDocument>,
    pub source: SourceMetadata,
    pub op: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebeziumUpdateEvent {
    pub topic: String,
    pub key: ChangeKey,
    pub value: DebeziumUpdateValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserDocumentBefore {
    pub id: Option<i64>,
    pub name: Option<String>,
    pub plan: Option<String>,
}

impl UserDocumentBefore {
    #[must_use]
    pub fn from_document(document: &UserDocument) -> Self {
        Self {
            id: Some(document.id),
            name: Some(document.name.clone()),
            plan: Some(document.plan.clone()),
        }
    }
}

impl DebeziumUpdateEvent {
    #[must_use]
    pub fn user_plan_update(
        topic: impl Into<String>,
        user_id: i64,
        name: impl Into<String>,
        from_plan: impl Into<String>,
        to_plan: impl Into<String>,
    ) -> Self {
        Self::user_plan_update_with_lsn(topic, user_id, name, from_plan, to_plan, current_lsn())
    }

    #[must_use]
    pub fn user_plan_update_with_lsn(
        topic: impl Into<String>,
        user_id: i64,
        name: impl Into<String>,
        from_plan: impl Into<String>,
        to_plan: impl Into<String>,
        lsn: u64,
    ) -> Self {
        let name = name.into();

        Self {
            topic: topic.into(),
            key: ChangeKey { id: user_id },
            value: DebeziumUpdateValue {
                before: Some(UserDocumentBefore::from_document(&UserDocument::new(
                    user_id,
                    name.clone(),
                    from_plan,
                ))),
                after: Some(UserDocument::new(user_id, name, to_plan)),
                source: SourceMetadata {
                    schema: EXPECTED_SCHEMA.to_owned(),
                    table: EXPECTED_TABLE.to_owned(),
                    lsn,
                },
                op: UPDATE_OPERATION.to_owned(),
            },
        }
    }

    pub fn after_document(&self) -> Result<UserDocument, EventValidationError> {
        self.validate_expected_users_update()?;
        Ok(self
            .value
            .after
            .clone()
            .expect("validated update events include an after document"))
    }

    pub fn from_kafka_json(
        topic: impl Into<String>,
        key_payload: Option<&str>,
        value_payload: Option<&str>,
    ) -> Result<Self, EventDecodeError> {
        let value_payload = value_payload.ok_or(EventDecodeError::MissingValue)?;
        let value = value_from_json("value", value_payload)?;

        if value.get("key").is_some() && value.get("value").is_some() {
            return serde_json::from_value(value)
                .map_err(|error| EventDecodeError::InvalidValueJson(error.to_string()));
        }

        let key_payload = key_payload.ok_or(EventDecodeError::MissingKey)?;

        Ok(Self {
            topic: topic.into(),
            key: decode_json_converter_payload("key", key_payload)?,
            value: decode_json_converter_payload_from_value("value", value)?,
        })
    }

    #[must_use]
    pub fn before_plan(&self) -> Option<&str> {
        self.value.before.as_ref()?.plan.as_deref()
    }

    #[must_use]
    pub fn after_plan(&self) -> Option<&str> {
        self.value.after.as_ref().map(|after| after.plan.as_str())
    }

    pub fn validate_expected_users_update(&self) -> Result<(), EventValidationError> {
        if self.value.source.schema != EXPECTED_SCHEMA {
            return Err(EventValidationError::UnexpectedSchema {
                expected: EXPECTED_SCHEMA.to_owned(),
                actual: self.value.source.schema.clone(),
            });
        }

        if self.value.source.table != EXPECTED_TABLE {
            return Err(EventValidationError::UnexpectedTable {
                expected: EXPECTED_TABLE.to_owned(),
                actual: self.value.source.table.clone(),
            });
        }

        if self.value.op != UPDATE_OPERATION {
            return Err(EventValidationError::UnexpectedOperation {
                expected: UPDATE_OPERATION.to_owned(),
                actual: self.value.op.clone(),
            });
        }

        let after = self
            .value
            .after
            .as_ref()
            .ok_or(EventValidationError::MissingAfter)?;

        if self.key.id != after.id {
            return Err(EventValidationError::KeyAfterMismatch {
                key_id: self.key.id,
                after_id: after.id,
            });
        }

        Ok(())
    }
}

fn decode_json_converter_payload<T: DeserializeOwned>(
    part: &'static str,
    json: &str,
) -> Result<T, EventDecodeError> {
    let value = value_from_json(part, json)?;
    decode_json_converter_payload_from_value(part, value)
}

fn decode_json_converter_payload_from_value<T: DeserializeOwned>(
    part: &'static str,
    value: Value,
) -> Result<T, EventDecodeError> {
    if value.is_null() {
        return Err(EventDecodeError::null_payload(part));
    }

    let payload = if value.get("schema").is_some() && value.get("payload").is_some() {
        value.get("payload").cloned().unwrap_or(Value::Null)
    } else {
        value
    };

    if payload.is_null() {
        return Err(EventDecodeError::null_payload(part));
    }

    serde_json::from_value(payload).map_err(|error| EventDecodeError::invalid_json(part, error))
}

fn value_from_json(part: &'static str, json: &str) -> Result<Value, EventDecodeError> {
    serde_json::from_str(json).map_err(|error| EventDecodeError::invalid_json(part, error))
}

fn current_lsn() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    elapsed.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventValidationError {
    UnexpectedSchema { expected: String, actual: String },
    UnexpectedTable { expected: String, actual: String },
    UnexpectedOperation { expected: String, actual: String },
    MissingAfter,
    KeyAfterMismatch { key_id: i64, after_id: i64 },
}

impl fmt::Display for EventValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedSchema { expected, actual } => {
                write!(formatter, "expected schema `{expected}`, got `{actual}`")
            }
            Self::UnexpectedTable { expected, actual } => {
                write!(formatter, "expected table `{expected}`, got `{actual}`")
            }
            Self::UnexpectedOperation { expected, actual } => {
                write!(formatter, "expected operation `{expected}`, got `{actual}`")
            }
            Self::MissingAfter => write!(formatter, "update event did not include `after`"),
            Self::KeyAfterMismatch { key_id, after_id } => {
                write!(
                    formatter,
                    "event key id `{key_id}` does not match after id `{after_id}`"
                )
            }
        }
    }
}

impl Error for EventValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDecodeError {
    MissingKey,
    MissingValue,
    InvalidKeyJson(String),
    InvalidValueJson(String),
}

impl EventDecodeError {
    fn invalid_json(part: &'static str, error: serde_json::Error) -> Self {
        match part {
            "key" => Self::InvalidKeyJson(error.to_string()),
            "value" => Self::InvalidValueJson(error.to_string()),
            _ => Self::InvalidValueJson(error.to_string()),
        }
    }

    fn null_payload(part: &'static str) -> Self {
        match part {
            "key" => Self::InvalidKeyJson("Kafka key payload was null".to_owned()),
            "value" => Self::InvalidValueJson("Kafka value payload was null".to_owned()),
            _ => Self::InvalidValueJson("Kafka payload was null".to_owned()),
        }
    }
}

impl fmt::Display for EventDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKey => write!(formatter, "Kafka message did not include a key"),
            Self::MissingValue => write!(formatter, "Kafka message did not include a value"),
            Self::InvalidKeyJson(error) => write!(formatter, "invalid Kafka key JSON: {error}"),
            Self::InvalidValueJson(error) => {
                write!(formatter, "invalid Kafka value JSON: {error}")
            }
        }
    }
}

impl Error for EventDecodeError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn serializes_debezium_style_update_event() {
        let event = DebeziumUpdateEvent::user_plan_update_with_lsn(
            "app.public.users",
            42,
            "Ada",
            "free",
            "pro",
            24_023_128,
        );

        let json = serde_json::to_value(&event).expect("event should serialize");

        assert_eq!(
            json,
            json!({
                "topic": "app.public.users",
                "key": { "id": 42 },
                "value": {
                    "before": { "id": 42, "name": "Ada", "plan": "free" },
                    "after": { "id": 42, "name": "Ada", "plan": "pro" },
                    "source": {
                        "schema": "public",
                        "table": "users",
                        "lsn": 24023128
                    },
                    "op": "u"
                }
            })
        );
    }

    #[test]
    fn deserializes_debezium_style_update_event() {
        let json = json!({
            "topic": "app.public.users",
            "key": { "id": 42 },
            "value": {
                "before": { "id": 42, "name": "Ada", "plan": "free" },
                "after": { "id": 42, "name": "Ada", "plan": "pro" },
                "source": {
                    "schema": "public",
                    "table": "users",
                    "lsn": 24023128
                },
                "op": "u"
            }
        });

        let event: DebeziumUpdateEvent =
            serde_json::from_value(json).expect("event should deserialize");

        assert_eq!(event.key.id, 42);
        assert_eq!(event.before_plan(), Some("free"));
        assert_eq!(event.after_plan(), Some("pro"));
    }

    #[test]
    fn decodes_real_debezium_postgres_update_from_kafka_key_and_value() {
        let key = r#"{"id":42}"#;
        let value = r#"{
            "before": { "id": 42, "name": "Ada", "plan": "free" },
            "after": { "id": 42, "name": "Ada", "plan": "pro" },
            "source": {
                "version": "3.4.0.Final",
                "connector": "postgresql",
                "name": "app",
                "ts_ms": 1778980000000,
                "snapshot": "false",
                "db": "app",
                "sequence": "[null,\"24023128\"]",
                "schema": "public",
                "table": "users",
                "txId": 771,
                "lsn": 24023128,
                "xmin": null
            },
            "transaction": null,
            "op": "u",
            "ts_ms": 1778980001000
        }"#;

        let event =
            DebeziumUpdateEvent::from_kafka_json("app.public.users", Some(key), Some(value))
                .expect("real Debezium JSON should decode");

        assert_eq!(event.topic, "app.public.users");
        assert_eq!(event.key.id, 42);
        assert_eq!(event.before_plan(), Some("free"));
        assert_eq!(event.after_plan(), Some("pro"));
        assert_eq!(event.value.source.lsn, 24_023_128);
    }

    #[test]
    fn decodes_schema_enabled_json_converter_payloads() {
        let key = r#"{
            "schema": { "type": "struct" },
            "payload": { "id": 42 }
        }"#;
        let value = r#"{
            "schema": { "type": "struct" },
            "payload": {
                "before": { "id": 42 },
                "after": { "id": 42, "name": "Ada", "plan": "pro" },
                "source": { "schema": "public", "table": "users", "lsn": 24023128 },
                "op": "u"
            }
        }"#;

        let event =
            DebeziumUpdateEvent::from_kafka_json("app.public.users", Some(key), Some(value))
                .expect("schema-enabled JSON converter payload should decode");

        assert_eq!(event.key.id, 42);
        assert_eq!(event.before_plan(), None);
        assert_eq!(
            event.after_document().expect("update should include after"),
            UserDocument::new(42, "Ada", "pro")
        );
    }

    #[test]
    fn rejects_tombstone_value_payloads() {
        let error =
            DebeziumUpdateEvent::from_kafka_json("app.public.users", Some(r#"{"id":42}"#), None)
                .expect_err("tombstone should not decode as a user update");

        assert!(matches!(error, EventDecodeError::MissingValue));
    }

    #[test]
    fn rejects_missing_kafka_key() {
        let value = r#"{
            "before": { "id": 42, "name": "Ada", "plan": "free" },
            "after": { "id": 42, "name": "Ada", "plan": "pro" },
            "source": { "schema": "public", "table": "users", "lsn": 24023128 },
            "op": "u"
        }"#;

        let error = DebeziumUpdateEvent::from_kafka_json("app.public.users", None, Some(value))
            .expect_err("keyless user update should fail");

        assert!(matches!(error, EventDecodeError::MissingKey));
    }

    #[test]
    fn rejects_create_events_for_indexer_updates() {
        let event = DebeziumUpdateEvent {
            topic: "app.public.users".to_owned(),
            key: ChangeKey { id: 42 },
            value: DebeziumUpdateValue {
                before: None,
                after: Some(UserDocument::new(42, "Ada", "free")),
                source: SourceMetadata {
                    schema: EXPECTED_SCHEMA.to_owned(),
                    table: EXPECTED_TABLE.to_owned(),
                    lsn: 24_023_128,
                },
                op: "c".to_owned(),
            },
        };

        let error = event
            .validate_expected_users_update()
            .expect_err("create events are not update events");

        assert!(matches!(
            error,
            EventValidationError::UnexpectedOperation { actual, .. } if actual == "c"
        ));
    }

    #[test]
    fn rejects_delete_events_without_after_document() {
        let event = DebeziumUpdateEvent {
            topic: "app.public.users".to_owned(),
            key: ChangeKey { id: 42 },
            value: DebeziumUpdateValue {
                before: Some(UserDocumentBefore::from_document(&UserDocument::new(
                    42, "Ada", "pro",
                ))),
                after: None,
                source: SourceMetadata {
                    schema: EXPECTED_SCHEMA.to_owned(),
                    table: EXPECTED_TABLE.to_owned(),
                    lsn: 24_023_128,
                },
                op: UPDATE_OPERATION.to_owned(),
            },
        };

        let error = event
            .validate_expected_users_update()
            .expect_err("updates require an after document");

        assert!(matches!(error, EventValidationError::MissingAfter));
    }

    #[test]
    fn rejects_key_after_mismatches() {
        let event = DebeziumUpdateEvent {
            topic: "app.public.users".to_owned(),
            key: ChangeKey { id: 42 },
            value: DebeziumUpdateValue {
                before: None,
                after: Some(UserDocument::new(7, "Ada", "pro")),
                source: SourceMetadata {
                    schema: EXPECTED_SCHEMA.to_owned(),
                    table: EXPECTED_TABLE.to_owned(),
                    lsn: 24_023_128,
                },
                op: UPDATE_OPERATION.to_owned(),
            },
        };

        let error = event
            .validate_expected_users_update()
            .expect_err("key and after id must match");

        assert!(matches!(
            error,
            EventValidationError::KeyAfterMismatch {
                key_id: 42,
                after_id: 7
            }
        ));
    }

    #[test]
    fn maps_after_value_to_user_document() {
        let event = DebeziumUpdateEvent::user_plan_update_with_lsn(
            "app.public.users",
            42,
            "Ada",
            "free",
            "pro",
            24_023_128,
        );

        assert_eq!(
            event.after_document().expect("event should target users"),
            UserDocument::new(42, "Ada", "pro")
        );
    }

    #[test]
    fn rejects_events_for_unexpected_tables() {
        let mut event = DebeziumUpdateEvent::user_plan_update_with_lsn(
            "app.public.users",
            42,
            "Ada",
            "free",
            "pro",
            24_023_128,
        );
        event.value.source.table = "orders".to_owned();

        let error = event
            .validate_expected_users_update()
            .expect_err("orders should not be accepted as users");

        assert!(matches!(
            error,
            EventValidationError::UnexpectedTable {
                expected,
                actual
            } if expected == "users" && actual == "orders"
        ));
    }
}
