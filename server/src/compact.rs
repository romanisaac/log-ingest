use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use manifest::{FlushMeta, Manifest};
use parquet::arrow::ArrowWriter;
use tokio::sync::{broadcast, Mutex, Semaphore};
use uuid::Uuid;

pub struct CompactionConfig {
    pub data_dir: PathBuf,
    /// Minimum file count per (service, time_bucket) for the scheduled compaction pass.
    pub min_files_per_bucket: usize,
    /// Target IPC byte size per output file. Default: 64 MiB.
    pub target_file_bytes: u64,
    /// How often the scheduled compaction pass runs.
    pub interval_secs: u64,
    /// Trigger compaction immediately when a bucket has at least this many files.
    pub small_file_threshold: usize,
    /// Trigger compaction immediately when estimated duplicate density reaches this ratio.
    pub max_duplicate_ratio: f64,
    /// How often the threshold check runs (should be much shorter than `interval_secs`).
    pub threshold_check_secs: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            min_files_per_bucket: 2,
            target_file_bytes: 64 * 1024 * 1024,
            interval_secs: 1800,
            small_file_threshold: 10,
            max_duplicate_ratio: 0.5,
            threshold_check_secs: 60,
        }
    }
}

pub async fn run_compaction(
    config: CompactionConfig,
    manifest: Arc<Mutex<Manifest>>,
    semaphore: Arc<Semaphore>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut threshold_tick =
        tokio::time::interval(Duration::from_secs(config.threshold_check_secs));
    let mut schedule_tick = tokio::time::interval(Duration::from_secs(config.interval_secs));
    // Consume the immediate first tick so neither timer fires on startup.
    threshold_tick.tick().await;
    schedule_tick.tick().await;

    loop {
        let buckets: Vec<(String, String)> = tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            _ = threshold_tick.tick() => {
                let stats = manifest.lock().await.hot_bucket_stats()?;
                stats.into_iter()
                    .filter(|s| {
                        s.file_count >= config.small_file_threshold
                            || s.duplicate_density >= config.max_duplicate_ratio
                    })
                    .map(|s| (s.service, s.time_bucket))
                    .collect()
            },
            _ = schedule_tick.tick() => {
                manifest.lock().await.compactable_buckets(config.min_files_per_bucket)?
            },
        };

        for (service, time_bucket) in buckets {
            let permit = Arc::clone(&semaphore)
                .acquire_owned()
                .await
                .context("acquire semaphore for compaction")?;
            metrics::gauge!("files_compacting").increment(1.0);
            if let Err(e) = compact_bucket(&service, &time_bucket, &config, &manifest).await {
                tracing::warn!(service = %service, bucket = %time_bucket, "compaction failed: {e:#}");
            }
            metrics::gauge!("files_compacting").decrement(1.0);
            drop(permit);
        }
    }
}

/// Compact all hot active files for one (service, time_bucket).
/// Public so it can be called directly in tests.
pub async fn compact_bucket(
    service: &str,
    time_bucket: &str,
    config: &CompactionConfig,
    manifest: &Arc<Mutex<Manifest>>,
) -> Result<()> {
    let files = {
        let m = manifest.lock().await;
        m.active_hot_files_for_bucket(service, time_bucket)?
    };

    if files.len() < 2 {
        return Ok(());
    }

    let old_ids: Vec<i64> = files.iter().map(|f| f.id).collect();

    // Sort all rows by timestamp and deduplicate by (kafka_partition, kafka_offset),
    // keeping the earliest copy of each unique key — same window function used at
    // query time, now applied once permanently.
    let ctx = SessionContext::new();
    for (i, entry) in files.iter().enumerate() {
        ctx.register_parquet(&format!("_f{i}"), &entry.path, ParquetReadOptions::default())
            .await
            .with_context(|| format!("register {}", entry.path))?;
    }

    let union_sql = (0..files.len())
        .map(|i| format!("SELECT * FROM _f{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let compact_sql = format!(
        "SELECT * EXCEPT (_rn) FROM (\
            SELECT *, ROW_NUMBER() OVER \
                (PARTITION BY kafka_partition, kafka_offset ORDER BY timestamp) AS _rn \
            FROM ({union_sql})\
        ) WHERE _rn = 1 \
        ORDER BY timestamp"
    );

    let batches: Vec<RecordBatch> = ctx
        .sql(&compact_sql)
        .await
        .context("plan compaction query")?
        .collect()
        .await
        .context("execute compaction query")?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(());
    }

    // Split into target-sized chunks and write each as a Parquet file.
    let chunks = split_by_ipc_size(batches, config.target_file_bytes);
    let mut new_files: Vec<FlushMeta> = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        let meta = write_chunk(chunk, service, time_bucket, &config.data_dir)
            .context("write compacted chunk")?;
        new_files.push(meta);
    }

    // Atomic swap: new files become active, old files become superseded in one transaction.
    {
        let mut m = manifest.lock().await;
        m.swap_compacted(&old_ids, &new_files)
            .context("swap_compacted")?;
    }

    // Best-effort deletion of the now-superseded local files.
    for file in &files {
        if let Err(e) = tokio::fs::remove_file(&file.path).await {
            tracing::warn!(path = %file.path, "delete superseded file failed: {e}");
        }
    }

    tracing::info!(
        service = %service,
        bucket = %time_bucket,
        old_files = old_ids.len(),
        new_files = new_files.len(),
        "compaction complete"
    );
    Ok(())
}

/// Split a flat batch list into chunks where each chunk's IPC size stays under `target_bytes`.
/// A single batch that exceeds `target_bytes` is kept as its own chunk.
fn split_by_ipc_size(batches: Vec<RecordBatch>, target_bytes: u64) -> Vec<Vec<RecordBatch>> {
    if batches.is_empty() {
        return vec![];
    }
    let mut chunks: Vec<Vec<RecordBatch>> = Vec::new();
    let mut current: Vec<RecordBatch> = Vec::new();
    let mut current_bytes: u64 = 0;

    for batch in batches {
        let size = ipc_size(&batch);
        if current_bytes + size > target_bytes && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += size;
        current.push(batch);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// IPC stream byte size of a single RecordBatch (used as a proxy for Parquet file size).
fn ipc_size(batch: &RecordBatch) -> u64 {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    if let Ok(mut w) = StreamWriter::try_new(&mut buf, &batch.schema()) {
        w.write(batch).ok();
        w.finish().ok();
    }
    buf.len() as u64
}

/// Write a chunk of sorted, deduplicated batches to a Parquet file. Returns the FlushMeta.
fn write_chunk(
    batches: &[RecordBatch],
    service: &str,
    time_bucket: &str,
    data_dir: &Path,
) -> Result<FlushMeta> {
    let schema = batches[0].schema();
    let dir = data_dir.join(service).join(time_bucket);
    std::fs::create_dir_all(&dir).context("create compaction output dir")?;
    let path = dir.join(format!("{}.parquet", Uuid::new_v4()));

    let file = std::fs::File::create(&path).context("create compaction output file")?;
    // Explicitly enable page-level statistics so DataFusion can prune row groups using min/max.
    let props = parquet::file::properties::WriterProperties::builder()
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(props)).context("create ArrowWriter")?;
    for batch in batches {
        writer.write(batch).context("write batch to compaction file")?;
    }
    writer.close().context("close compaction writer")?;

    let size_bytes = std::fs::metadata(&path)
        .context("stat compaction file")?
        .len() as i64;
    let record_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let (min_ts, max_ts) = col_min_max_i64(batches, "timestamp")?;
    let (min_kafka_offset, max_kafka_offset) = col_min_max_i64(batches, "kafka_offset")?;

    Ok(FlushMeta {
        path: path.to_string_lossy().into_owned(),
        service: service.to_string(),
        time_bucket: time_bucket.to_string(),
        min_ts,
        max_ts,
        size_bytes,
        record_count,
        min_kafka_offset,
        max_kafka_offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(n_rows: usize) -> RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr = Int64Array::from_iter_values(0..n_rows as i64);
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(arr)]).unwrap()
    }

    #[test]
    fn split_by_ipc_size_splits_when_target_exceeded() {
        let batch = make_batch(100);
        let single_size = ipc_size(&batch);
        assert!(single_size > 0);

        // target less than one batch → each batch must be its own chunk
        let chunks = split_by_ipc_size(
            vec![batch.clone(), batch.clone(), batch.clone()],
            single_size - 1,
        );
        assert_eq!(chunks.len(), 3, "each batch should be its own chunk when target < single batch size");
        for chunk in &chunks {
            assert_eq!(chunk.len(), 1);
        }
    }

    #[test]
    fn split_by_ipc_size_no_split_when_target_large() {
        let batch = make_batch(100);
        let single_size = ipc_size(&batch);

        // target larger than 3 batches combined → all in one chunk
        let chunks = split_by_ipc_size(
            vec![batch.clone(), batch.clone(), batch.clone()],
            single_size * 10,
        );
        assert_eq!(chunks.len(), 1, "all batches should be in one chunk when target is large");
        assert_eq!(chunks[0].len(), 3);
    }

    #[test]
    fn split_by_ipc_size_empty_input() {
        let chunks = split_by_ipc_size(vec![], 1024);
        assert!(chunks.is_empty());
    }
}

fn col_min_max_i64(batches: &[RecordBatch], column: &str) -> Result<(i64, i64)> {
    let mut min_val = i64::MAX;
    let mut max_val = i64::MIN;
    for batch in batches {
        let idx = batch
            .schema()
            .index_of(column)
            .with_context(|| format!("column '{column}' not found in compaction output"))?;
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .with_context(|| format!("column '{column}' is not Int64"))?;
        if let Some(v) = arrow::compute::min(arr) {
            min_val = min_val.min(v);
        }
        if let Some(v) = arrow::compute::max(arr) {
            max_val = max_val.max(v);
        }
    }
    Ok((min_val, max_val))
}
