use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use batch_buffer::{BatchBuffer, SystemClock, DEFAULT_MAX_AGE_MS, DEFAULT_MAX_RECORDS};
use event_schema::LogEvent;
use manifest::Manifest;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, ClientContext};
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

/// Per-partition mutable state shared between the main consumer loop and the
/// rebalance callback. Uses `std::sync::Mutex` (not tokio) so the callback —
/// which runs on librdkafka's native C thread — can lock without a tokio context.
struct PartitionStore {
    buffers: HashMap<i32, BatchBuffer>,
    pending: HashMap<i32, i64>,
    committed: HashMap<i32, i64>,
    paused: HashSet<i32>,
}

impl PartitionStore {
    fn new() -> Self {
        Self {
            buffers: HashMap::new(),
            pending: HashMap::new(),
            committed: HashMap::new(),
            paused: HashSet::new(),
        }
    }
}

/// rdkafka `ClientContext` that flushes buffered events to Parquet before any
/// partition is revoked.
///
/// `pre_rebalance(Revoke)` runs on librdkafka's native C thread (not a tokio
/// thread), so async flushes are driven inline via `rt_handle.block_on`. Flush
/// results are forwarded on the same channel the main loop uses, keeping offset
/// commits consistent with the normal flush path.
struct RebalanceContext {
    store: Arc<std::sync::Mutex<PartitionStore>>,
    data_dir: PathBuf,
    manifest: Arc<Mutex<Manifest>>,
    flush_semaphore: Arc<Semaphore>,
    flush_tx: mpsc::UnboundedSender<FlushResult>,
    rt_handle: tokio::runtime::Handle,
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(tpl) = rebalance else {
            return;
        };

        let revoked: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
        if revoked.is_empty() {
            return;
        }

        // Extract and remove all revoked partition state under the std Mutex.
        // The lock is held only for the in-memory operation; never across the flush.
        let to_flush: Vec<(i32, Vec<LogEvent>, i64)> = {
            let mut store = self.store.lock().unwrap();
            revoked
                .iter()
                .filter_map(|&p| {
                    let batch = store
                        .buffers
                        .remove(&p)
                        .map(|mut b| b.drain())
                        .unwrap_or_default();
                    let up_to_offset = store.pending.remove(&p).unwrap_or(0);
                    store.committed.remove(&p);
                    store.paused.remove(&p);
                    (!batch.is_empty()).then_some((p, batch, up_to_offset))
                })
                .collect()
        };

        // Flush each revoked partition's batch synchronously on the tokio runtime.
        // block_on is safe here because this callback runs on librdkafka's native
        // C thread, not on a tokio worker thread.
        for (partition, batch, up_to_offset) in to_flush {
            let success = self.rt_handle.block_on(flush_gated(
                &batch,
                &self.data_dir,
                &self.manifest,
                &self.flush_semaphore,
            ));
            let _ = self.flush_tx.send(FlushResult { partition, up_to_offset, success });
        }
    }

    fn post_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let partitions: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                tracing::info!("rebalance: assigned partitions {partitions:?}");
                // BatchBuffers are initialized lazily on first message; nothing to do here.
            }
            Rebalance::Revoke(tpl) => {
                let partitions: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                tracing::info!("rebalance: revocation complete for partitions {partitions:?}");
            }
            Rebalance::Error(e) => {
                tracing::error!("rebalance error: {e}");
            }
        }
    }
}

/// Run the Kafka consumer loop until a shutdown signal is received.
///
/// Each Kafka partition owns its own `BatchBuffer` inside a shared
/// `PartitionStore`. Flush tasks are spawned concurrently via `tokio::spawn`;
/// a shared `flush_semaphore` caps how many Parquet writes execute at once.
/// Flush results are reported back on an `mpsc` channel so the main loop can
/// commit offsets and resume paused partitions.
///
/// A `RebalanceContext` drains and flushes buffered events for any partition
/// about to be revoked, ensuring no buffered-but-unflushed events are lost
/// during consumer group rebalancing.
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
    let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<FlushResult>();

    let store = Arc::new(std::sync::Mutex::new(PartitionStore::new()));

    let ctx = RebalanceContext {
        store: Arc::clone(&store),
        data_dir: data_dir.clone(),
        manifest: Arc::clone(&manifest),
        flush_semaphore: Arc::clone(&flush_semaphore),
        flush_tx: flush_tx.clone(),
        rt_handle: tokio::runtime::Handle::current(),
    };

    let consumer: StreamConsumer<RebalanceContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create_with_context(ctx)?;

    consumer.subscribe(&[&config.topic])?;
    tracing::info!("consumer subscribed to topic '{}'", config.topic);

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

                                let (batch, up_to_offset, pause_semaphore, pause_highwater) = {
                                    let mut s = store.lock().unwrap();
                                    s.pending.insert(partition, m.offset() + 1);

                                    let batch = {
                                        let buf = s.buffers.entry(partition).or_insert_with(|| {
                                            BatchBuffer::new(
                                                config.batch_max_bytes,
                                                DEFAULT_MAX_RECORDS,
                                                DEFAULT_MAX_AGE_MS,
                                                Box::new(SystemClock),
                                            )
                                        });
                                        buf.push(event)
                                    };

                                    let up_to_offset = s.pending[&partition];
                                    let is_paused = s.paused.contains(&partition);

                                    if batch.is_some() {
                                        // Flush triggered — pause if semaphore saturated.
                                        let sem = flush_semaphore.available_permits() == 0 && !is_paused;
                                        if sem { s.paused.insert(partition); }
                                        (batch, up_to_offset, sem, false)
                                    } else {
                                        // No flush — check high-water mark.
                                        let occ = s.buffers[&partition].occupancy();
                                        let hwm = !is_paused && occ > HIGH_WATER_MARK;
                                        if hwm { s.paused.insert(partition); }
                                        (None, up_to_offset, false, hwm)
                                    }
                                };

                                if pause_semaphore || pause_highwater {
                                    pause_partition(
                                        &consumer,
                                        &config.topic,
                                        partition,
                                        &backpressure_pauses,
                                    );
                                }

                                if let Some(batch) = batch {
                                    spawn_flush(
                                        partition,
                                        batch,
                                        up_to_offset,
                                        &data_dir,
                                        &manifest,
                                        &flush_semaphore,
                                        &flush_tx,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            Some(result) = flush_rx.recv() => {
                if result.success {
                    let (do_commit, do_resume) = {
                        let s = store.lock().unwrap();
                        let last = s.committed.get(&result.partition).copied().unwrap_or(0);
                        let occ = s
                            .buffers
                            .get(&result.partition)
                            .map(|b| b.occupancy())
                            .unwrap_or(0.0);
                        (
                            result.up_to_offset > last,
                            s.paused.contains(&result.partition) && occ < LOW_WATER_MARK,
                        )
                    };
                    if do_commit {
                        commit_offset(
                            &consumer,
                            &config.topic,
                            result.partition,
                            result.up_to_offset,
                        );
                        store.lock().unwrap().committed.insert(result.partition, result.up_to_offset);
                    }
                    if do_resume {
                        resume_partition(&consumer, &config.topic, result.partition);
                        store.lock().unwrap().paused.remove(&result.partition);
                    }
                }
            }

            _ = tick.tick() => {
                // Age-triggered flush: check every partition buffer independently.
                let partitions: Vec<i32> =
                    store.lock().unwrap().buffers.keys().copied().collect();
                for partition in partitions {
                    let batch_info = {
                        let mut s = store.lock().unwrap();
                        let batch = match s.buffers.get_mut(&partition) {
                            Some(b) => b.poll(),
                            None => None,
                        };
                        if let Some(batch) = batch {
                            let up_to_offset = s.pending.get(&partition).copied().unwrap_or(0);
                            let is_paused = s.paused.contains(&partition);
                            let sem =
                                flush_semaphore.available_permits() == 0 && !is_paused;
                            if sem { s.paused.insert(partition); }
                            Some((batch, up_to_offset, sem))
                        } else {
                            None
                        }
                    };
                    if let Some((batch, up_to_offset, pause_semaphore)) = batch_info {
                        if pause_semaphore {
                            pause_partition(
                                &consumer,
                                &config.topic,
                                partition,
                                &backpressure_pauses,
                            );
                        }
                        spawn_flush(
                            partition,
                            batch,
                            up_to_offset,
                            &data_dir,
                            &manifest,
                            &flush_semaphore,
                            &flush_tx,
                        );
                    }
                }
            }

            _ = shutdown.recv() => {
                let (total, n_partitions, partition_batches) = {
                    let mut s = store.lock().unwrap();
                    let total: usize = s.buffers.values().map(|b| b.len()).sum();
                    let n_partitions = s.buffers.len();
                    let keys: Vec<i32> = s.buffers.keys().copied().collect();
                    let mut batches: Vec<(i32, Vec<LogEvent>, i64)> = Vec::new();
                    for p in keys {
                        if s.buffers.get(&p).map_or(true, |b| b.is_empty()) {
                            continue;
                        }
                        let batch = s.buffers.get_mut(&p).unwrap().drain();
                        let up_to_offset = s.pending.get(&p).copied().unwrap_or(0);
                        batches.push((p, batch, up_to_offset));
                    }
                    (total, n_partitions, batches)
                };

                tracing::info!(
                    "consumer shutting down — draining {total} buffered events \
                     across {n_partitions} partition(s)",
                );

                // Flush each partition's remaining buffer inline. In-flight spawned
                // tasks continue to run and commit their own offsets after we return.
                for (partition, batch, up_to_offset) in partition_batches {
                    if flush_gated(&batch, &data_dir, &manifest, &flush_semaphore).await {
                        let last = store
                            .lock()
                            .unwrap()
                            .committed
                            .get(&partition)
                            .copied()
                            .unwrap_or(0);
                        if up_to_offset > last {
                            commit_offset(&consumer, &config.topic, partition, up_to_offset);
                            store.lock().unwrap().committed.insert(partition, up_to_offset);
                        }
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

/// Spawn an async flush task. Pause/semaphore checks are handled by the caller
/// before invoking this function.
fn spawn_flush(
    partition: i32,
    batch: Vec<LogEvent>,
    up_to_offset: i64,
    data_dir: &PathBuf,
    manifest: &Arc<Mutex<Manifest>>,
    semaphore: &Arc<Semaphore>,
    tx: &mpsc::UnboundedSender<FlushResult>,
) {
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
fn commit_offset<C: ConsumerContext>(
    consumer: &StreamConsumer<C>,
    topic: &str,
    partition: i32,
    up_to_offset: i64,
) {
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
fn pause_partition<C: ConsumerContext>(
    consumer: &StreamConsumer<C>,
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
fn resume_partition<C: ConsumerContext>(consumer: &StreamConsumer<C>, topic: &str, partition: i32) {
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
