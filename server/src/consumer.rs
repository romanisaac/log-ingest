use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use batch_buffer::BatchBuffer;
use event_schema::LogEvent;
use manifest::Manifest;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio::sync::{broadcast, Mutex};
use tokio::time::interval;

use crate::flush_events;

pub struct ConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic: String,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            group_id: std::env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "log-ingest".to_string()),
            topic: std::env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "logs".to_string()),
        }
    }
}

/// Run the Kafka consumer loop until a shutdown signal is received.
///
/// Events are deserialized from JSON, fed into a BatchBuffer, and flushed to
/// Parquet when any trigger fires (size / record count / time). On shutdown the
/// buffer is drained so in-flight events are not lost.
///
/// Note: auto.commit is enabled here for simplicity. Issue #9 changes this to
/// manual offset commit strictly after a successful Parquet flush.
pub async fn run_consumer(
    config: ConsumerConfig,
    data_dir: PathBuf,
    manifest: Arc<Mutex<Manifest>>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&[&config.topic])?;
    tracing::info!("consumer subscribed to topic '{}'", config.topic);

    let mut buffer = BatchBuffer::with_defaults();
    let mut tick = interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            msg = consumer.recv() => {
                match msg {
                    Err(e) => tracing::warn!("kafka error: {e}"),
                    Ok(m) => {
                        let Some(payload) = m.payload() else { continue };
                        match serde_json::from_slice::<LogEvent>(payload) {
                            Err(e) => tracing::warn!("failed to deserialize message: {e}"),
                            Ok(mut event) => {
                                // Override with actual Kafka metadata — the producer's
                                // embedded values may not match the assigned offset.
                                event.kafka_partition = m.partition();
                                event.kafka_offset = m.offset();

                                if let Some(batch) = buffer.push(event) {
                                    do_flush(&batch, &data_dir, &manifest).await;
                                }
                            }
                        }
                    }
                }
            }
            _ = tick.tick() => {
                if let Some(batch) = buffer.poll() {
                    do_flush(&batch, &data_dir, &manifest).await;
                }
            }
            _ = shutdown.recv() => {
                tracing::info!(
                    "consumer shutting down — draining {} buffered events",
                    buffer.len()
                );
                if !buffer.is_empty() {
                    do_flush(&buffer.drain(), &data_dir, &manifest).await;
                }
                break;
            }
        }
    }

    Ok(())
}

async fn do_flush(batch: &[LogEvent], data_dir: &PathBuf, manifest: &Arc<Mutex<Manifest>>) {
    let mut guard = manifest.lock().await;
    match flush_events(batch, data_dir, &mut guard) {
        Ok(path) => tracing::info!("flushed {} events → {}", batch.len(), path.display()),
        Err(e) => tracing::error!("flush failed: {e:#}"),
    }
}
