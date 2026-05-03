use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

pub use object_store;

/// Configuration for a MinIO (S3-compatible) backend.
#[derive(Clone)]
pub struct MinioConfig {
    /// HTTP endpoint, e.g. "http://localhost:9000"
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            bucket: "logs".to_string(),
        }
    }
}

/// Object store backed by the local filesystem, rooted at `root`.
pub fn local_store(root: &Path) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        LocalFileSystem::new_with_prefix(root).context("create local object store")?,
    ))
}

/// Object store backed by a MinIO (S3-compatible) bucket.
pub fn minio_store(config: &MinioConfig) -> Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_access_key_id(&config.access_key)
        .with_secret_access_key(&config.secret_key)
        .with_bucket_name(&config.bucket)
        .with_allow_http(true)
        .build()
        .context("build MinIO object store")?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::path::Path as ObjPath;

    #[tokio::test]
    async fn local_store_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path()).unwrap();

        let path = ObjPath::from("subdir/hello.txt");
        let payload = Bytes::from_static(b"hello from local store");

        store.put(&path, payload.clone().into()).await.unwrap();
        let result = store.get(&path).await.unwrap().bytes().await.unwrap();

        assert_eq!(result, payload);
    }

    /// Requires MinIO running: `make up`
    /// Run with: cargo test -p storage -- --ignored
    #[tokio::test]
    #[ignore]
    async fn minio_store_write_read_roundtrip() {
        let store = minio_store(&MinioConfig::default()).unwrap();

        let path = ObjPath::from("test/hello.txt");
        let payload = Bytes::from_static(b"hello from minio store");

        store.put(&path, payload.clone().into()).await.unwrap();
        let result = store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();

        assert_eq!(result, payload);
    }
}
