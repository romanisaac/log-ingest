use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::{Context, Result};
use lru::LruCache;
use storage::{minio_store, object_store::path::Path as ObjPath, MinioConfig};

/// On-disk LRU cache for cold Parquet segments.
///
/// On a cache miss the file is downloaded from MinIO and written to `cache_dir`.
/// LRU entries are evicted when `current_bytes + new_file_bytes > max_bytes`.
/// The cache is in-memory only (the LRU ordering is not persisted across restarts),
/// but cached files survive restarts — they will simply be counted as cache misses
/// on the first access after a restart and then re-added to the in-memory index.
pub struct ColdCache {
    cache_dir: PathBuf,
    max_bytes: u64,
    // Maps cold URI → (local path, size_bytes) in LRU order.
    entries: LruCache<String, (PathBuf, u64)>,
    current_bytes: u64,
}

impl ColdCache {
    pub fn new(cache_dir: PathBuf, max_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(&cache_dir).context("create cold cache dir")?;
        Ok(Self {
            cache_dir,
            max_bytes,
            // Count cap is very high; size-based eviction is the binding constraint.
            entries: LruCache::new(NonZeroUsize::new(65_536).unwrap()),
            current_bytes: 0,
        })
    }

    /// Return the local path for `cold_uri`, downloading from MinIO on a miss.
    pub async fn get_or_fetch(&mut self, cold_uri: &str, minio: &MinioConfig) -> Result<PathBuf> {
        if let Some((path, _)) = self.entries.get(cold_uri) {
            metrics::counter!("cache_hits_total").increment(1);
            return Ok(path.clone());
        }

        metrics::counter!("cache_misses_total").increment(1);

        let key = cold_uri
            .strip_prefix(&format!("s3://{}/", minio.bucket))
            .with_context(|| format!("invalid cold URI: {cold_uri}"))?;

        let store = minio_store(minio).context("build MinIO store for cache fetch")?;
        let bytes = store
            .get(&ObjPath::from(key))
            .await
            .with_context(|| format!("fetch {cold_uri} from MinIO"))?
            .bytes()
            .await
            .context("read MinIO response bytes")?;

        let size = bytes.len() as u64;

        // Evict LRU entries until there's room for the incoming file.
        while self.current_bytes + size > self.max_bytes {
            match self.entries.pop_lru() {
                None => break,
                Some((_, (evicted_path, evicted_size))) => {
                    if let Err(e) = std::fs::remove_file(&evicted_path) {
                        tracing::warn!(path = %evicted_path.display(), "eviction delete failed: {e}");
                    }
                    self.current_bytes = self.current_bytes.saturating_sub(evicted_size);
                }
            }
        }

        let local_path = self.cache_dir.join(uri_to_cache_filename(cold_uri));
        tokio::fs::write(&local_path, bytes)
            .await
            .with_context(|| format!("write cache file {}", local_path.display()))?;

        self.current_bytes += size;
        self.entries.put(cold_uri.to_string(), (local_path.clone(), size));

        tracing::debug!(uri = %cold_uri, path = %local_path.display(), bytes = size, "cold cache miss — fetched from MinIO");
        Ok(local_path)
    }
}

/// Map a cold URI to a safe filename inside the cache dir.
/// e.g. `s3://logs/svc/2024-01-01-00/uuid.parquet` → `logs_svc_2024-01-01-00_uuid.parquet`
fn uri_to_cache_filename(uri: &str) -> String {
    uri.strip_prefix("s3://").unwrap_or(uri).replace('/', "_")
}
