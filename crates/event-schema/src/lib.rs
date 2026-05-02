use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

/// Log severity level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "DEBUG" => Ok(Level::Debug),
            "INFO" => Ok(Level::Info),
            "WARN" => Ok(Level::Warn),
            "ERROR" => Ok(Level::Error),
            other => anyhow::bail!("unknown level: {}", other),
        }
    }
}

/// Typed attribute value. JSON integers → Int, floats → Float.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Core log event envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEvent {
    /// Nanosecond epoch timestamp.
    pub timestamp: i64,
    pub level: Level,
    pub service: String,
    pub message: String,
    pub kafka_partition: i32,
    pub kafka_offset: i64,
    pub attributes: HashMap<String, AttributeValue>,
}

impl LogEvent {
    /// Estimate the in-memory byte size of this event. Used by BatchBuffer to
    /// track how full the buffer is without serializing each event.
    pub fn estimated_byte_size(&self) -> usize {
        8 // timestamp
        + self.level.as_str().len()
        + self.service.len()
        + self.message.len()
        + 4 // kafka_partition
        + 8 // kafka_offset
        + self.attributes.iter().map(|(k, v)| {
            k.len() + match v {
                AttributeValue::String(s) => s.len(),
                AttributeValue::Int(_) | AttributeValue::Float(_) => 8,
                AttributeValue::Bool(_) => 1,
            }
        }).sum::<usize>()
    }
}

/// Arrow schema for log events.
pub fn log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("kafka_partition", DataType::Int32, false),
        Field::new("kafka_offset", DataType::Int64, false),
        Field::new("attributes", DataType::Utf8, false),
    ]))
}

/// Encode a slice of LogEvents into a RecordBatch.
pub fn encode_batch(events: &[LogEvent]) -> Result<RecordBatch> {
    let schema = log_schema();

    let timestamps: Int64Array = events.iter().map(|e| e.timestamp).collect();
    let levels: StringArray = events.iter().map(|e| Some(e.level.as_str())).collect();
    let services: StringArray = events.iter().map(|e| Some(e.service.as_str())).collect();
    let messages: StringArray = events.iter().map(|e| Some(e.message.as_str())).collect();
    let partitions: Int32Array = events.iter().map(|e| e.kafka_partition).collect();
    let offsets: Int64Array = events.iter().map(|e| e.kafka_offset).collect();

    let attrs_json: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(&e.attributes))
        .collect::<std::result::Result<_, _>>()
        .context("failed to serialize attributes")?;
    let attributes: StringArray = attrs_json.iter().map(|s| Some(s.as_str())).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps),
        Arc::new(levels),
        Arc::new(services),
        Arc::new(messages),
        Arc::new(partitions),
        Arc::new(offsets),
        Arc::new(attributes),
    ];

    RecordBatch::try_new(schema, columns).context("failed to create RecordBatch")
}

/// Decode a RecordBatch back into LogEvents.
pub fn decode_batch(batch: &RecordBatch) -> Result<Vec<LogEvent>> {
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("timestamp column")?;
    let levels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("level column")?;
    let services = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("service column")?;
    let messages = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("message column")?;
    let partitions = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("kafka_partition column")?;
    let offsets = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("kafka_offset column")?;
    let attrs_col = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("attributes column")?;

    let mut events = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let attributes: HashMap<String, AttributeValue> =
            serde_json::from_str(attrs_col.value(i)).context("failed to deserialize attributes")?;
        events.push(LogEvent {
            timestamp: timestamps.value(i),
            level: Level::from_str(levels.value(i))?,
            service: services.value(i).to_string(),
            message: messages.value(i).to_string(),
            kafka_partition: partitions.value(i),
            kafka_offset: offsets.value(i),
            attributes,
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> LogEvent {
        let mut attrs = HashMap::new();
        attrs.insert("request_id".to_string(), AttributeValue::String("abc-123".to_string()));
        attrs.insert("status_code".to_string(), AttributeValue::Int(200));
        attrs.insert("latency_ms".to_string(), AttributeValue::Float(42.5));
        attrs.insert("cached".to_string(), AttributeValue::Bool(false));

        LogEvent {
            timestamp: 1_700_000_000_000_000_000,
            level: Level::Info,
            service: "api-gateway".to_string(),
            message: "request handled".to_string(),
            kafka_partition: 3,
            kafka_offset: 9999,
            attributes: attrs,
        }
    }

    #[test]
    fn round_trip() {
        let original = sample_event();
        let batch = encode_batch(&[original.clone()]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let decoded = decode_batch(&batch).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], original);
    }
}
