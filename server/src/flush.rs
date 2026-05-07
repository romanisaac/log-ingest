use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use event_schema::{encode_batch, LogEvent};
use manifest::{FlushMeta, Manifest};
use parquet::arrow::ArrowWriter;
use uuid::Uuid;

/// Write a batch of events to Parquet under `data_dir`, register in manifest.
/// Returns the path of the written file.
pub fn flush_events(events: &[LogEvent], data_dir: &Path, manifest: &mut Manifest) -> Result<PathBuf> {
    assert!(!events.is_empty(), "flush_events called with empty batch");

    let batch = encode_batch(events).context("encode batch")?;

    let min_ts = events.iter().map(|e| e.timestamp).min().unwrap();
    let max_ts = events.iter().map(|e| e.timestamp).max().unwrap();
    let min_kafka_offset = events.iter().map(|e| e.kafka_offset).min().unwrap();
    let max_kafka_offset = events.iter().map(|e| e.kafka_offset).max().unwrap();
    let service = &events[0].service;

    let dt = Utc.timestamp_nanos(min_ts);
    let time_bucket = dt.format("%Y-%m-%d-%H").to_string();

    let dir = data_dir.join(service).join(&time_bucket);
    std::fs::create_dir_all(&dir).context("create parquet directory")?;
    let file_path = dir.join(format!("{}.parquet", Uuid::new_v4()));

    let file = std::fs::File::create(&file_path).context("create parquet file")?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).context("create ArrowWriter")?;
    writer.write(&batch).context("write batch")?;
    writer.close().context("close writer")?;

    let size_bytes = std::fs::metadata(&file_path).context("stat parquet file")?.len() as i64;

    manifest
        .commit_flush(&FlushMeta {
            path: file_path.to_string_lossy().into_owned(),
            service: service.clone(),
            time_bucket,
            min_ts,
            max_ts,
            size_bytes,
            record_count: events.len() as i64,
            min_kafka_offset,
            max_kafka_offset,
        })
        .context("commit flush")?;

    metrics::counter!(
        "events_ingested_total",
        "service" => service.clone(),
        "partition" => events[0].kafka_partition.to_string()
    )
    .increment(events.len() as u64);

    Ok(file_path)
}
