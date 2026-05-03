use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use storage::object_store::path::Path as ObjPath;
use tokio::sync::{broadcast, Mutex};

use manifest::Manifest;
use storage::{minio_store, MinioConfig};

pub struct TieringConfig {
    pub minio: MinioConfig,
    /// Files with max_ts older than this many days are moved to cold storage.
    pub threshold_days: u64,
    /// How often the tiering task wakes to scan for eligible files.
    pub interval_secs: u64,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            minio: MinioConfig::default(),
            threshold_days: 7,
            interval_secs: 60,
        }
    }
}

pub async fn run_tiering(
    config: TieringConfig,
    manifest: Arc<Mutex<Manifest>>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let store = minio_store(&config.minio)?;
    let threshold_ns = config.threshold_days as i64 * 24 * 3600 * 1_000_000_000i64;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(config.interval_secs)) => {}
        }

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let cutoff_ns = now_ns.saturating_sub(threshold_ns);

        let eligible = {
            let m = manifest.lock().await;
            m.hot_files_older_than(cutoff_ns)?
        };

        for file in eligible {
            if let Err(e) = tier_file(&file, &store, &config.minio.bucket, &manifest).await {
                tracing::warn!(path = %file.path, "tiering failed: {e:#}");
            }
        }

        // Refresh files_active gauge after each pass.
        let (hot, cold) = {
            let m = manifest.lock().await;
            m.count_active_by_tier()?
        };
        metrics::gauge!("files_active", "tier" => "hot").set(hot as f64);
        metrics::gauge!("files_active", "tier" => "cold").set(cold as f64);
    }
}

async fn tier_file(
    file: &manifest::FileEntry,
    store: &Arc<dyn storage::object_store::ObjectStore>,
    bucket: &str,
    manifest: &Arc<Mutex<Manifest>>,
) -> Result<()> {
    let local_path = std::path::Path::new(&file.path);
    let filename = local_path
        .file_name()
        .and_then(|n| n.to_str())
        .with_context(|| format!("invalid local path: {}", file.path))?;

    let cold_key = format!("{}/{}/{}", file.service, file.time_bucket, filename);
    let cold_uri = format!("s3://{bucket}/{cold_key}");

    // 1. Read and upload — file still exists locally.
    let bytes = tokio::fs::read(local_path)
        .await
        .with_context(|| format!("read {}", file.path))?;
    store
        .put(&ObjPath::from(cold_key.as_str()), Bytes::from(bytes).into())
        .await
        .with_context(|| format!("upload to MinIO: {cold_key}"))?;

    // 2. Update manifest atomically (tier + path in one UPDATE).
    //    File is now recorded as cold; it still exists locally as a redundant copy.
    {
        let mut m = manifest.lock().await;
        m.set_cold(file.id, &cold_uri)
            .with_context(|| format!("set_cold id={}", file.id))?;
    }

    // 3. Delete the local copy now that the manifest points to cold storage.
    tokio::fs::remove_file(local_path)
        .await
        .with_context(|| format!("delete local {}", file.path))?;

    tracing::info!(id = file.id, local = %file.path, cold = %cold_uri, "tiered to cold");
    Ok(())
}
