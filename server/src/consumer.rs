use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use batch_buffer::BatchBuffer;
use event_schema::LogEvent;
use manifest::Manifest;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
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
/// Offsets are committed manually, strictly after a successful Parquet flush
/// and manifest commit (at-least-once delivery). If the process crashes before
/// a flush completes, uncommitted events are re-consumed from Kafka on restart.
/// Duplicates in the re-consumed window are removed at query time via
/// (kafka_partition, kafka_offset) deduplication (issue #14).
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
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    consumer.subscribe(&[&config.topic])?;
    tracing::info!("consumer subscribed to topic '{}'", config.topic);

    let mut buffer = BatchBuffer::with_defaults();
    // Tracks the next-to-read offset (last consumed + 1) per (topic, partition).
    // Reset after each successful commit so stale offsets are never re-committed.
    let mut pending: HashMap<(String, i32), i64> = HashMap::new();
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
                                event.kafka_partition = m.partition();
                                event.kafka_offset = m.offset();

                                // Record next-to-read offset for this partition.
                                pending.insert(
                                    (m.topic().to_string(), m.partition()),
                                    m.offset() + 1,
                                );

                                if let Some(batch) = buffer.push(event) {
                                    if do_flush(&batch, &data_dir, &manifest).await {
                                        commit_offsets(&consumer, &pending);
                                        pending.clear();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ = tick.tick() => {
                if let Some(batch) = buffer.poll() {
                    if do_flush(&batch, &data_dir, &manifest).await {
                        commit_offsets(&consumer, &pending);
                        pending.clear();
                    }
                }
            }
            _ = shutdown.recv() => {
                tracing::info!(
                    "consumer shutting down — draining {} buffered events",
                    buffer.len()
                );
                if !buffer.is_empty() {
                    if do_flush(&buffer.drain(), &data_dir, &manifest).await {
                        commit_offsets(&consumer, &pending);
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

/// Flush a batch to Parquet + manifest. Returns true on success.
async fn do_flush(batch: &[LogEvent], data_dir: &PathBuf, manifest: &Arc<Mutex<Manifest>>) -> bool {
    let mut guard = manifest.lock().await;
    match flush_events(batch, data_dir, &mut guard) {
        Ok(path) => {
            tracing::info!("flushed {} events → {}", batch.len(), path.display());
            true
        }
        Err(e) => {
            tracing::error!("flush failed: {e:#}");
            false
        }
    }
}

/// Commit the pending offsets to Kafka synchronously.
/// Called only after a confirmed successful flush.
fn commit_offsets(consumer: &StreamConsumer, pending: &HashMap<(String, i32), i64>) {
    if pending.is_empty() {
        return;
    }
    let mut tpl = TopicPartitionList::new();
    for ((topic, partition), offset) in pending {
        if let Err(e) = tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset)) {
            tracing::error!("failed to build offset list: {e}");
            return;
        }
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
        tracing::error!("offset commit failed: {e}");
    } else {
        tracing::debug!("committed offsets for {} partition(s)", pending.len());
    }
}
