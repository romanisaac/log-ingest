use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use manifest::Manifest;
use storage::{minio_store, MinioConfig};
use tokio::sync::{broadcast, Mutex};

pub struct SweeperConfig {
    /// Retention window for hot-tier files in seconds. Default: 14 days.
    pub hot_retention_secs: u64,
    /// Retention window for cold-tier files in seconds. Default: 90 days.
    pub cold_retention_secs: u64,
    /// Grace period before superseded files are deleted in seconds. Default: 10 minutes.
    pub superseded_grace_secs: u64,
    /// How often the sweeper runs in seconds. Default: 5 minutes.
    pub interval_secs: u64,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            hot_retention_secs: 14 * 86_400,
            cold_retention_secs: 90 * 86_400,
            superseded_grace_secs: 600,
            interval_secs: 300,
        }
    }
}

pub async fn run_sweeper(
    config: SweeperConfig,
    manifest: Arc<Mutex<Manifest>>,
    minio: MinioConfig,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut tick = tokio::time::interval(Duration::from_secs(config.interval_secs));
    tick.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            _ = tick.tick() => {}
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let now_ns = now_secs * 1_000_000_000;
        let hot_cutoff_ns = now_ns - config.hot_retention_secs as i64 * 1_000_000_000;
        let cold_cutoff_ns = now_ns - config.cold_retention_secs as i64 * 1_000_000_000;
        let grace_cutoff_secs = now_secs - config.superseded_grace_secs as i64;

        sweep_expired(&manifest, &minio, hot_cutoff_ns, cold_cutoff_ns).await;
        sweep_superseded(&manifest, grace_cutoff_secs).await;
    }
}

async fn sweep_expired(
    manifest: &Arc<Mutex<Manifest>>,
    minio: &MinioConfig,
    hot_cutoff_ns: i64,
    cold_cutoff_ns: i64,
) {
    let files = {
        match manifest.lock().await.expired_active_files(hot_cutoff_ns, cold_cutoff_ns) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("sweeper: failed to query expired files: {e:#}");
                return;
            }
        }
    };

    if files.is_empty() {
        return;
    }

    // Build the MinIO store once, lazily, only if cold files need deletion.
    let cold_store = if files.iter().any(|f| f.tier == "cold") {
        match minio_store(minio) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!("sweeper: cannot build MinIO store for cold deletion: {e:#}");
                None
            }
        }
    } else {
        None
    };

    for file in files {
        // Delete the physical file first; only remove the manifest row on success
        // so a crash between the two doesn't orphan the manifest entry while the
        // file is still readable.
        let deleted = if file.tier == "cold" {
            delete_cold(&file.path, minio, cold_store.as_deref()).await
        } else {
            delete_local(&file.path).await
        };

        if !deleted {
            continue;
        }

        if let Err(e) = manifest.lock().await.delete_file(file.id) {
            tracing::warn!(id = file.id, path = %file.path, "sweeper: failed to remove expired file from manifest: {e:#}");
        } else {
            tracing::info!(
                id = file.id, tier = %file.tier, path = %file.path,
                "sweeper: deleted expired file"
            );
        }
    }
}

async fn sweep_superseded(manifest: &Arc<Mutex<Manifest>>, grace_cutoff_secs: i64) {
    let files = {
        match manifest.lock().await.superseded_files_past_grace(grace_cutoff_secs) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("sweeper: failed to query superseded files: {e:#}");
                return;
            }
        }
    };

    for file in files {
        // Superseded files are hot-tier only (compaction never touches cold files),
        // so we always delete locally.
        let deleted = delete_local(&file.path).await;

        // For superseded files the manifest row is the source of truth; even if
        // the physical file is already gone we still clean up the row.
        if let Err(e) = manifest.lock().await.delete_file(file.id) {
            tracing::warn!(id = file.id, "sweeper: failed to remove superseded file from manifest: {e:#}");
        } else if deleted {
            tracing::info!(id = file.id, path = %file.path, "sweeper: deleted superseded file");
        } else {
            tracing::info!(id = file.id, path = %file.path, "sweeper: removed already-missing superseded file from manifest");
        }
    }
}

async fn delete_local(path: &str) -> bool {
    match tokio::fs::remove_file(path).await {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => {
            tracing::warn!(path, "sweeper: failed to delete local file: {e}");
            false
        }
    }
}

async fn delete_cold(
    cold_uri: &str,
    minio: &MinioConfig,
    store: Option<&dyn storage::object_store::ObjectStore>,
) -> bool {
    // cold_uri format: "s3://{bucket}/{key}"
    let key = cold_uri
        .strip_prefix(&format!("s3://{}/", minio.bucket))
        .unwrap_or(cold_uri);

    let Some(store) = store else { return false };
    use storage::object_store::ObjectStore as _;
    match store.delete(&storage::object_store::path::Path::from(key)).await {
        Ok(()) => true,
        Err(storage::object_store::Error::NotFound { .. }) => true,
        Err(e) => {
            tracing::warn!(uri = cold_uri, "sweeper: failed to delete cold file: {e}");
            false
        }
    }
}
