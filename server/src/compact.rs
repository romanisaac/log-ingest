use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use manifest::{FlushMeta, Manifest};
use parquet::arrow::ArrowWriter;
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;

pub struct CompactionConfig {
    pub data_dir: PathBuf,
    /// Minimum number of hot active files per (service, time_bucket) to trigger compaction.
    pub min_files_per_bucket: usize,
    /// Target IPC byte size per output file used to decide when to split. Default: 64 MiB.
    /// Parquet compression means the on-disk file is typically 30–60 % of this value,
    /// landing in the 32–128 MiB target range for typical log schemas.
    pub target_file_bytes: u64,
    /// How often the compaction task wakes to scan for eligible buckets.
    pub interval_secs: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            min_files_per_bucket: 2,
            target_file_bytes: 64 * 1024 * 1024,
            interval_secs: 1800,
        }
    }
}

pub async fn run_compaction(
    config: CompactionConfig,
    manifest: Arc<Mutex<Manifest>>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(config.interval_secs)) => {}
        }

        let buckets = {
            let m = manifest.lock().await;
            m.compactable_buckets(config.min_files_per_bucket)?
        };

        for (service, time_bucket) in buckets {
            if let Err(e) = compact_bucket(&service, &time_bucket, &config, &manifest).await {
                tracing::warn!(service = %service, bucket = %time_bucket, "compaction failed: {e:#}");
            }
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
    let mut writer =
        ArrowWriter::try_new(file, schema, None).context("create ArrowWriter")?;
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
