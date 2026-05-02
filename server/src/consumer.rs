use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use batch_buffer::{BatchBuffer, SystemClock, DEFAULT_MAX_AGE_MS, DEFAULT_MAX_RECORDS};
use event_schema::LogEvent;
use manifest::Manifest;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::ClientConfig;
use tokio::sync::{broadcast, Mutex};
use tokio::time::interval;

use crate::flush_events;

const HIGH_WATER_MARK: f32 = 0.8;
const LOW_WATER_MARK: f32 = 0.4;

pub struct ConsumerConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topic: String,
    /// Max batch size in bytes before a flush is triggered. Override in tests to
    /// produce backpressure without requiring 64 MB of data.
    pub batch_max_bytes: usize,
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
            batch_max_bytes: batch_buffer::DEFAULT_MAX_BYTES,
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
///
/// `backpressure_pauses` is incremented each time the consumer pauses its
/// Kafka partitions due to buffer occupancy exceeding the high-water mark.
pub async fn run_consumer(
    config: ConsumerConfig,
    data_dir: PathBuf,
    manifest: Arc<Mutex<Manifest>>,
    backpressure_pauses: Arc<AtomicU64>,
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

    let mut buffer = BatchBuffer::new(
        config.batch_max_bytes,
        DEFAULT_MAX_RECORDS,
        DEFAULT_MAX_AGE_MS,
        Box::new(SystemClock),
    );
    // Tracks the next-to-read offset (last consumed + 1) per (topic, partition).
    // Reset after each successful commit so stale offsets are never re-committed.
    let mut pending: HashMap<(String, i32), i64> = HashMap::new();
    let mut tick = interval(Duration::from_millis(100));
    let mut is_paused = false;

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
                                        if is_paused && buffer.occupancy() < LOW_WATER_MARK {
                                            resume_partitions(&consumer);
                                            is_paused = false;
                                        }
                                    }
                                } else if !is_paused && buffer.occupancy() > HIGH_WATER_MARK {
                                    pause_partitions(&consumer, &backpressure_pauses);
                                    is_paused = true;
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
                        if is_paused && buffer.occupancy() < LOW_WATER_MARK {
                            resume_partitions(&consumer);
                            is_paused = false;
                        }
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

/// Pause all currently assigned partitions. Called when buffer occupancy exceeds HIGH_WATER_MARK.
fn pause_partitions(consumer: &StreamConsumer, pauses_total: &AtomicU64) {
    match consumer.assignment() {
        Ok(tpl) if !tpl.elements().is_empty() => {
            if let Err(e) = consumer.pause(&tpl) {
                tracing::error!("failed to pause partitions: {e}");
            } else {
                let count = pauses_total.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::info!(
                    "backpressure: paused {} partition(s) (total pauses: {count})",
                    tpl.count()
                );
            }
        }
        Ok(_) => {}
        Err(e) => tracing::error!("failed to get assignment for pause: {e}"),
    }
}

/// Resume all currently assigned partitions. Called after a flush drops occupancy below LOW_WATER_MARK.
fn resume_partitions(consumer: &StreamConsumer) {
    match consumer.assignment() {
        Ok(tpl) if !tpl.elements().is_empty() => {
            if let Err(e) = consumer.resume(&tpl) {
                tracing::error!("failed to resume partitions: {e}");
            } else {
                tracing::info!("backpressure: resumed {} partition(s)", tpl.count());
            }
        }
        Ok(_) => {}
        Err(e) => tracing::error!("failed to get assignment for resume: {e}"),
    }
}
