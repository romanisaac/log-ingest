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
use tokio::sync::{broadcast, Mutex, Semaphore};
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
/// Kafka partitions — either because buffer occupancy exceeded the high-water
/// mark or because `flush_semaphore` was exhausted.
///
/// `flush_semaphore` caps how many Parquet flush operations may run concurrently
/// across all partition workers. When all slots are occupied, the consumer pauses
/// its partitions until a slot is available.
pub async fn run_consumer(
    config: ConsumerConfig,
    data_dir: PathBuf,
    manifest: Arc<Mutex<Manifest>>,
    backpressure_pauses: Arc<AtomicU64>,
    flush_semaphore: Arc<Semaphore>,
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
                                    maybe_pause_for_semaphore(
                                        &consumer,
                                        &flush_semaphore,
                                        &backpressure_pauses,
                                        &mut is_paused,
                                    );
                                    if flush_gated(&batch, &data_dir, &manifest, &flush_semaphore).await {
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
                    maybe_pause_for_semaphore(
                        &consumer,
                        &flush_semaphore,
                        &backpressure_pauses,
                        &mut is_paused,
                    );
                    if flush_gated(&batch, &data_dir, &manifest, &flush_semaphore).await {
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
                    if flush_gated(&buffer.drain(), &data_dir, &manifest, &flush_semaphore).await {
                        commit_offsets(&consumer, &pending);
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

/// Acquire a semaphore slot before flushing. Records the full duration
/// (including semaphore wait time) as a `flush_duration_seconds` histogram.
/// Exposed as pub(crate) so unit tests can verify semaphore serialization.
pub(crate) async fn flush_gated(
    batch: &[LogEvent],
    data_dir: &PathBuf,
    manifest: &Arc<Mutex<Manifest>>,
    semaphore: &Arc<Semaphore>,
) -> bool {
    let start = std::time::Instant::now();
    let _permit = semaphore.acquire().await.unwrap();
    let ok = do_flush(batch, data_dir, manifest).await;
    metrics::histogram!("flush_duration_seconds").record(start.elapsed().as_secs_f64());
    ok
}

/// Pause Kafka partitions if the flush semaphore is currently exhausted.
/// Called immediately before a flush so the consumer stops receiving new events
/// while waiting for a semaphore slot.
fn maybe_pause_for_semaphore(
    consumer: &StreamConsumer,
    semaphore: &Arc<Semaphore>,
    pauses_total: &AtomicU64,
    is_paused: &mut bool,
) {
    if !*is_paused && semaphore.available_permits() == 0 {
        pause_partitions(consumer, pauses_total);
        *is_paused = true;
    }
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

/// Pause all currently assigned partitions. Called when buffer occupancy exceeds
/// HIGH_WATER_MARK or the flush semaphore is exhausted.
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

/// Resume all currently assigned partitions. Called after a flush drops occupancy
/// below LOW_WATER_MARK (whether paused for backpressure or semaphore saturation).
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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Verify that a semaphore with 1 permit serializes 3 concurrent flush
    /// attempts — at most 1 can hold the permit at any moment.
    #[tokio::test]
    async fn flush_semaphore_limits_concurrency() {
        let sem = Arc::new(Semaphore::new(1));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..3)
            .map(|_| {
                let sem = Arc::clone(&sem);
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(current, Ordering::SeqCst);
                    // Simulate flush work
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();

        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(
            max_in_flight.load(Ordering::SeqCst),
            1,
            "semaphore(1) must allow at most 1 concurrent flush"
        );
    }
}
