use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    pub before: UserDocument,
    pub after: UserDocument,
    pub source: SourceMetadata,
    pub op: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebeziumUpdateEvent {
    pub topic: String,
    pub key: ChangeKey,
    pub value: DebeziumUpdateValue,
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
                before: UserDocument::new(user_id, name.clone(), from_plan),
                after: UserDocument::new(user_id, name, to_plan),
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
        Ok(self.value.after.clone())
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

        if self.key.id != self.value.after.id {
            return Err(EventValidationError::KeyAfterMismatch {
                key_id: self.key.id,
                after_id: self.value.after.id,
            });
        }

        Ok(())
    }
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
        assert_eq!(event.value.before.plan, "free");
        assert_eq!(event.value.after.plan, "pro");
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
