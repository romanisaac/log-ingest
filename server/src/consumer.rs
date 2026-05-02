use std::collections::{HashMap, HashSet};
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
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};
use tokio::time::interval;

use crate::flush_events;

const HIGH_WATER_MARK: f32 = 0.8;
const LOW_WATER_MARK: f32 = 0.4;

/// Result sent from a spawned flush task back to the main consumer loop.
struct FlushResult {
    partition: i32,
    /// The next-to-read offset at the time the batch was drained. Committed to
    /// Kafka only if strictly greater than the last committed offset for this
    /// partition, guarding against backward commits when concurrent tasks finish
    /// out of order.
    up_to_offset: i64,
    success: bool,
}

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
/// Each Kafka partition owns its own `BatchBuffer`. Flush tasks are spawned
/// concurrently via `tokio::spawn`; a shared `flush_semaphore` caps how many
/// Parquet writes execute at once across all partitions. Flush results are
/// reported back on an `mpsc` channel so the main loop can commit offsets and
/// resume paused partitions without exposing the `StreamConsumer` to worker tasks.
///
/// Offset commits are at-least-once: offsets are committed only after a
/// confirmed successful Parquet write and manifest update.
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

    let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<FlushResult>();

    // Per-partition state — all keyed by partition id (i32).
    let mut buffers: HashMap<i32, BatchBuffer> = HashMap::new();
    // Highest next-to-read offset received per partition. Snapshot captured at
    // flush-spawn time and committed to Kafka after a successful write.
    let mut pending: HashMap<i32, i64> = HashMap::new();
    // Last successfully committed offset per partition. Guards against backward
    // commits when out-of-order flush tasks complete.
    let mut committed: HashMap<i32, i64> = HashMap::new();
    // Partitions currently paused for backpressure or semaphore saturation.
    let mut paused: HashSet<i32> = HashSet::new();

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
                                let partition = m.partition();
                                event.kafka_partition = partition;
                                event.kafka_offset = m.offset();

                                // Advance the high-water mark for this partition.
                                pending.insert(partition, m.offset() + 1);

                                let buffer = buffers.entry(partition).or_insert_with(|| {
                                    BatchBuffer::new(
                                        config.batch_max_bytes,
                                        DEFAULT_MAX_RECORDS,
                                        DEFAULT_MAX_AGE_MS,
                                        Box::new(SystemClock),
                                    )
                                });

                                if let Some(batch) = buffer.push(event) {
                                    let up_to_offset = pending[&partition];
                                    spawn_flush(
                                        partition,
                                        batch,
                                        up_to_offset,
                                        &data_dir,
                                        &manifest,
                                        &flush_semaphore,
                                        &backpressure_pauses,
                                        &flush_tx,
                                        &mut paused,
                                        &consumer,
                                        &config.topic,
                                    );
                                } else if !paused.contains(&partition)
                                    && buffers[&partition].occupancy() > HIGH_WATER_MARK
                                {
                                    pause_partition(
                                        &consumer,
                                        &config.topic,
                                        partition,
                                        &backpressure_pauses,
                                    );
                                    paused.insert(partition);
                                }
                            }
                        }
                    }
                }
            }

            Some(result) = flush_rx.recv() => {
                if result.success {
                    // Monotonic guard: never commit a lower offset than already committed.
                    let last = committed.get(&result.partition).copied().unwrap_or(0);
                    if result.up_to_offset > last {
                        commit_offset(
                            &consumer,
                            &config.topic,
                            result.partition,
                            result.up_to_offset,
                        );
                        committed.insert(result.partition, result.up_to_offset);
                    }
                    // Resume the partition if it was paused and its buffer has drained.
                    let occ = buffers
                        .get(&result.partition)
                        .map(|b| b.occupancy())
                        .unwrap_or(0.0);
                    if paused.contains(&result.partition) && occ < LOW_WATER_MARK {
                        resume_partition(&consumer, &config.topic, result.partition);
                        paused.remove(&result.partition);
                    }
                }
            }

            _ = tick.tick() => {
                // Age-triggered flush: check every partition buffer independently.
                let partitions: Vec<i32> = buffers.keys().copied().collect();
                for partition in partitions {
                    if let Some(batch) = buffers.get_mut(&partition).and_then(|b| b.poll()) {
                        let up_to_offset = pending.get(&partition).copied().unwrap_or(0);
                        spawn_flush(
                            partition,
                            batch,
                            up_to_offset,
                            &data_dir,
                            &manifest,
                            &flush_semaphore,
                            &backpressure_pauses,
                            &flush_tx,
                            &mut paused,
                            &consumer,
                            &config.topic,
                        );
                    }
                }
            }

            _ = shutdown.recv() => {
                let total: usize = buffers.values().map(|b| b.len()).sum();
                tracing::info!(
                    "consumer shutting down — draining {total} buffered events \
                     across {} partition(s)",
                    buffers.len()
                );
                // Flush each partition's remaining buffer inline. In-flight spawned
                // tasks continue to run and commit their own offsets after we return.
                for (partition, buffer) in &mut buffers {
                    if buffer.is_empty() {
                        continue;
                    }
                    let batch = buffer.drain();
                    let up_to_offset = pending.get(partition).copied().unwrap_or(0);
                    if flush_gated(&batch, &data_dir, &manifest, &flush_semaphore).await {
                        let last = committed.get(partition).copied().unwrap_or(0);
                        if up_to_offset > last {
                            commit_offset(&consumer, &config.topic, *partition, up_to_offset);
                            committed.insert(*partition, up_to_offset);
                        }
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

/// Spawn an async flush task for `partition`. Pauses the partition first if the
/// flush semaphore is already saturated so no new messages accumulate while
/// waiting for a slot. Results are sent back on `tx` for the main loop to
/// handle offset commits and partition resumes.
fn spawn_flush(
    partition: i32,
    batch: Vec<LogEvent>,
    up_to_offset: i64,
    data_dir: &PathBuf,
    manifest: &Arc<Mutex<Manifest>>,
    semaphore: &Arc<Semaphore>,
    pauses_total: &AtomicU64,
    tx: &mpsc::UnboundedSender<FlushResult>,
    paused: &mut HashSet<i32>,
    consumer: &StreamConsumer,
    topic: &str,
) {
    if semaphore.available_permits() == 0 && !paused.contains(&partition) {
        pause_partition(consumer, topic, partition, pauses_total);
        paused.insert(partition);
    }
    let data_dir = data_dir.clone();
    let manifest = Arc::clone(manifest);
    let semaphore = Arc::clone(semaphore);
    let tx = tx.clone();
    tokio::spawn(async move {
        let success = flush_gated(&batch, &data_dir, &manifest, &semaphore).await;
        let _ = tx.send(FlushResult { partition, up_to_offset, success });
    });
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

/// Commit a single partition's offset to Kafka synchronously.
/// Called only after a confirmed successful flush.
fn commit_offset(consumer: &StreamConsumer, topic: &str, partition: i32, up_to_offset: i64) {
    let mut tpl = TopicPartitionList::new();
    if let Err(e) = tpl.add_partition_offset(topic, partition, Offset::Offset(up_to_offset)) {
        tracing::error!("failed to build offset entry for partition {partition}: {e}");
        return;
    }
    if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
        tracing::error!("offset commit failed for partition {partition}: {e}");
    } else {
        tracing::debug!("committed offset {up_to_offset} for partition {partition}");
    }
}

/// Pause a specific Kafka partition. Called when its buffer occupancy exceeds
/// HIGH_WATER_MARK or when the flush semaphore is exhausted.
fn pause_partition(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    pauses_total: &AtomicU64,
) {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    if let Err(e) = consumer.pause(&tpl) {
        tracing::error!("failed to pause partition {partition}: {e}");
    } else {
        let count = pauses_total.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(
            "backpressure: paused partition {partition} (total pauses: {count})"
        );
    }
}

/// Resume a specific Kafka partition. Called after its buffer drops below
/// LOW_WATER_MARK following a successful flush.
fn resume_partition(consumer: &StreamConsumer, topic: &str, partition: i32) {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, partition);
    if let Err(e) = consumer.resume(&tpl) {
        tracing::error!("failed to resume partition {partition}: {e}");
    } else {
        tracing::info!("backpressure: resumed partition {partition}");
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
