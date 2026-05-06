use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use datafusion::prelude::*;
use futures::{channel::mpsc, StreamExt};
use manifest::Manifest;
use serde::Deserialize;
use cold_cache::ColdCache;
use server::{run_consumer, run_tiering, ConsumerConfig, TieringConfig};
use storage::MinioConfig;

mod assets;
mod cold_cache;
use tokio::sync::{broadcast, Mutex, Semaphore};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ─── HTTP API ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
    time_from: Option<i64>,
    time_to: Option<i64>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize { 1000 }

#[derive(Clone)]
struct AppState {
    manifest: Arc<Mutex<Manifest>>,
    minio: MinioConfig,
    cold_semaphore: Arc<Semaphore>,
    cold_cache: Arc<Mutex<ColdCache>>,
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Response {
    if req.time_from.is_none() || req.time_to.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "time_from and time_to are required",
                "code": "missing_time_predicate"
            })),
        )
        .into_response();
    }

    match stream_query(state, req).await {
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("{:#}", e) })),
        )
        .into_response(),
        Ok(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .body(Body::from_stream(stream))
            .unwrap(),
    }
}

async fn stream_query(
    state: AppState,
    req: QueryRequest,
) -> Result<impl futures::Stream<Item = Result<Bytes, BoxError>> + Send + 'static> {
    let files = {
        let guard = state.manifest.lock().await;
        guard.active_files(None)?
    };

    let time_from = req.time_from.unwrap();
    let time_to = req.time_to.unwrap();

    // Compute file-level stats from manifest metadata before touching DataFusion.
    let is_pruned = |f: &&manifest::FileEntry| f.max_ts < time_from || f.min_ts > time_to;
    let files_pruned = files.iter().filter(is_pruned).count() as u64;
    let bytes_scanned: u64 = files.iter().filter(|f| !is_pruned(f)).map(|f| f.size_bytes as u64).sum();
    // Pre-dedup row count: total records in all non-pruned files per manifest.
    let rows_scanned: u64 = files.iter().filter(|f| !is_pruned(f)).map(|f| f.record_count as u64).sum();

    if files.is_empty() {
        return Ok(futures::stream::once(futures::future::ready(Ok(
            make_stats_bytes(0, 0, 0, 0),
        )))
        .boxed());
    }

    let ctx = SessionContext::new();

    // Acquire a cold semaphore permit if any cold files are present.
    // The permit is held for the full query duration to cap concurrent cold-tier I/O.
    let has_cold = files.iter().any(|f| f.tier == "cold");
    let cold_permit = if has_cold {
        Some(
            Arc::clone(&state.cold_semaphore)
                .acquire_owned()
                .await
                .context("acquire cold semaphore")?,
        )
    } else {
        None
    };

    // Register each file with DataFusion. Cold files are resolved through the local
    // LRU cache — a miss downloads from MinIO and writes to disk; a hit returns the
    // cached path immediately. Either way, DataFusion always reads a local file.
    let mut table_names: Vec<String> = Vec::new();
    for (i, entry) in files.iter().enumerate() {
        let tname = format!("_file_{i}");
        let path = if entry.tier == "cold" {
            let mut cache = state.cold_cache.lock().await;
            let local = cache.get_or_fetch(&entry.path, &state.minio).await?;
            drop(cache);
            local.to_string_lossy().into_owned()
        } else {
            entry.path.clone()
        };
        ctx.register_parquet(&tname, &path, ParquetReadOptions::default())
            .await
            .with_context(|| format!("register parquet {}", entry.path))?;
        table_names.push(tname);
    }

    let union_sql = table_names
        .iter()
        .map(|t| format!("SELECT * FROM {t}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    // Deduplicate at the view level so any user projection gets duplicate-free data.
    let view_sql = format!(
        "SELECT * EXCEPT (_rn) FROM (\
            SELECT *, ROW_NUMBER() OVER \
                (PARTITION BY kafka_partition, kafka_offset ORDER BY timestamp) AS _rn \
            FROM ({union_sql})\
        ) WHERE _rn = 1"
    );
    ctx.sql(&format!("CREATE OR REPLACE VIEW logs AS {view_sql}"))
        .await
        .context("create logs view")?
        .collect()
        .await
        .context("materialize view creation")?;

    let final_sql = format!("{} LIMIT {}", req.sql.trim_end_matches(';'), req.limit);
    let batch_stream = ctx
        .sql(&final_sql)
        .await
        .context("parse user sql")?
        .execute_stream()
        .await
        .context("execute query")?;

    let (tx, rx) = mpsc::unbounded::<Result<Bytes, BoxError>>();
    let started_at = std::time::Instant::now();

    tokio::spawn(async move {
        // Hold the cold permit for the full duration of the scan so that at most
        // `cold_semaphore.available_permits()` queries read from cold storage at once.
        let _cold_permit = cold_permit;
        let mut stream = batch_stream;

        while let Some(result) = stream.next().await {
            match result {
                Err(e) => {
                    tx.unbounded_send(Err(Box::new(e) as BoxError)).ok();
                    return;
                }
                Ok(batch) => {
                    let schema = batch.schema();
                    for row_idx in 0..batch.num_rows() {
                        let mut obj = serde_json::Map::new();
                        for col_idx in 0..batch.num_columns() {
                            let val = arrow_value_to_json(batch.column(col_idx), row_idx)
                                .unwrap_or(serde_json::Value::Null);
                            obj.insert(schema.field(col_idx).name().clone(), val);
                        }
                        match serde_json::to_string(&serde_json::Value::Object(obj)) {
                            Err(e) => {
                                tx.unbounded_send(Err(Box::new(e) as BoxError)).ok();
                                return;
                            }
                            Ok(mut line) => {
                                line.push('\n');
                                tx.unbounded_send(Ok(Bytes::from(line))).ok();
                            }
                        }
                    }
                }
            }
        }

        let duration_ms = started_at.elapsed().as_millis() as u64;
        tx.unbounded_send(Ok(make_stats_bytes(
            rows_scanned,
            files_pruned,
            bytes_scanned,
            duration_ms,
        )))
        .ok();
    });

    Ok(rx.boxed())
}

fn make_stats_bytes(rows_scanned: u64, files_pruned: u64, bytes_scanned: u64, duration_ms: u64) -> Bytes {
    let mut s = serde_json::json!({
        "rows_scanned": rows_scanned,
        "files_pruned": files_pruned,
        "bytes_scanned": bytes_scanned,
        "duration_ms": duration_ms,
    })
    .to_string();
    s.push('\n');
    Bytes::from(s)
}

fn arrow_value_to_json(col: &dyn Array, row: usize) -> Result<serde_json::Value> {
    if col.is_null(row) {
        return Ok(serde_json::Value::Null);
    }
    match col.data_type() {
        DataType::Int64 => Ok(col.as_any().downcast_ref::<Int64Array>().unwrap().value(row).into()),
        DataType::Int32 => Ok(col.as_any().downcast_ref::<Int32Array>().unwrap().value(row).into()),
        DataType::Utf8 => Ok(col.as_any().downcast_ref::<StringArray>().unwrap().value(row).into()),
        DataType::LargeUtf8 => {
            use arrow::array::LargeStringArray;
            Ok(col.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row).into())
        }
        DataType::Utf8View => Ok(col.as_any().downcast_ref::<StringViewArray>().unwrap().value(row).into()),
        DataType::Boolean => Ok(col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row).into()),
        dt => anyhow::bail!("unsupported column type: {dt:?}"),
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("server=info".parse().unwrap()),
        )
        .init();

    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string()),
    );
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let db_path = std::env::var("MANIFEST_PATH").unwrap_or_else(|_| "manifest.db".to_string());
    let manifest = Arc::new(Mutex::new(
        Manifest::open(Path::new(&db_path)).expect("open manifest"),
    ));

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let max_concurrent_flushes = std::env::var("MAX_CONCURRENT_FLUSHES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4usize);
    let flush_semaphore = Arc::new(Semaphore::new(max_concurrent_flushes));

    // Start Kafka consumer as a background task.
    let consumer_manifest = Arc::clone(&manifest);
    let consumer_semaphore = Arc::clone(&flush_semaphore);
    let consumer_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        if let Err(e) = run_consumer(
            ConsumerConfig::default(),
            data_dir,
            consumer_manifest,
            Arc::new(AtomicU64::new(0)),
            consumer_semaphore,
            consumer_shutdown,
        )
        .await
        {
            tracing::error!("consumer exited with error: {e:#}");
        }
    });

    let minio_config = MinioConfig {
        endpoint: std::env::var("MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:9000".to_string()),
        access_key: std::env::var("MINIO_ACCESS_KEY")
            .unwrap_or_else(|_| "minioadmin".to_string()),
        secret_key: std::env::var("MINIO_SECRET_KEY")
            .unwrap_or_else(|_| "minioadmin".to_string()),
        bucket: std::env::var("MINIO_BUCKET")
            .unwrap_or_else(|_| "logs".to_string()),
    };

    // Start background tiering task.
    let tiering_manifest = Arc::clone(&manifest);
    let tiering_shutdown = shutdown_tx.subscribe();
    let tiering_config = TieringConfig {
        minio: minio_config.clone(),
        threshold_days: std::env::var("TIER_THRESHOLD_DAYS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7),
        interval_secs: std::env::var("TIER_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60),
    };
    tokio::spawn(async move {
        if let Err(e) = run_tiering(tiering_config, tiering_manifest, tiering_shutdown).await {
            tracing::error!("tiering task exited with error: {e:#}");
        }
    });

    let cold_concurrency = std::env::var("COLD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4usize);
    let cold_cache_dir = PathBuf::from(
        std::env::var("COLD_CACHE_DIR").unwrap_or_else(|_| "cold_cache".to_string()),
    );
    let cold_cache_max_bytes = std::env::var("COLD_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10u64 * 1024 * 1024 * 1024); // 10 GB default
    let cold_cache = Arc::new(Mutex::new(
        ColdCache::new(cold_cache_dir, cold_cache_max_bytes).expect("create cold cache"),
    ));
    let state = AppState {
        manifest,
        minio: minio_config,
        cold_semaphore: Arc::new(Semaphore::new(cold_concurrency)),
        cold_cache,
    };
    let app = Router::new()
        .route("/query", post(query_handler))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route("/metrics", axum::routing::get({
            let handle = prometheus_handle.clone();
            move || { let h = handle.clone(); async move { h.render() } }
        }))
        .with_state(state)
        .fallback(assets::handler);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());

    tokio::select! {
        _ = axum::serve(listener, app) => {}
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received, shutting down");
            let _ = shutdown_tx.send(());
            // Give the consumer time to drain its buffer before exiting.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use event_schema::{AttributeValue, Level, LogEvent};
    use server::flush_events;
    use std::sync::atomic::Ordering;
    use std::collections::HashMap;
    use storage::MinioConfig;
    use tower::util::ServiceExt;

    fn make_state(manifest: Manifest) -> AppState {
        AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(
                    std::env::temp_dir().join("log-ingest-test-cache"),
                    10 * 1024 * 1024 * 1024,
                )
                .expect("create test cold cache"),
            )),
        }
    }

    // Returns (data_rows, stats) from a query response.
    async fn query(state: AppState, req: QueryRequest) -> (Vec<serde_json::Value>, serde_json::Value) {
        let app = Router::new()
            .route("/query", post(query_handler))
            .with_state(state);
        let body = serde_json::json!({
            "sql": req.sql,
            "time_from": req.time_from,
            "time_to": req.time_to,
            "limit": req.limit
        });
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let mut lines: Vec<serde_json::Value> = bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let stats = lines.pop().expect("response must include a trailing stats line");
        (lines, stats)
    }

    async fn query_rows(state: AppState, req: QueryRequest) -> Vec<serde_json::Value> {
        query(state, req).await.0
    }

    fn make_event() -> LogEvent {
        let mut attrs = HashMap::new();
        attrs.insert("request_id".to_string(), AttributeValue::String("test-req-1".to_string()));
        attrs.insert("status_code".to_string(), AttributeValue::Int(200));
        attrs.insert("latency_ms".to_string(), AttributeValue::Float(12.3));
        attrs.insert("cached".to_string(), AttributeValue::Bool(true));
        LogEvent {
            timestamp: 1_700_000_000_000_000_000,
            level: Level::Info,
            service: "test-svc".to_string(),
            message: "integration test event".to_string(),
            kafka_partition: 0,
            kafka_offset: 42,
            attributes: attrs,
        }
    }

    #[tokio::test]
    async fn write_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");

        let event = make_event();
        let mut manifest = Manifest::open(&db_path).unwrap();
        flush_events(&[event.clone()], &data_dir, &mut manifest).unwrap();

        let files = manifest.active_files(None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 1);
        assert_eq!(files[0].service, "test-svc");

        let state = make_state(manifest);
        let app = Router::new()
            .route("/query", post(query_handler))
            .with_state(state.clone());

        let body = serde_json::json!({
            "sql": "SELECT * FROM logs",
            "time_from": 0,
            "time_to": i64::MAX,
            "limit": 100
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );

        let (rows, stats) = query(
            state,
            QueryRequest {
                sql: "SELECT * FROM logs".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 100,
            },
        )
        .await;

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row["timestamp"], serde_json::json!(1_700_000_000_000_000_000i64));
        assert_eq!(row["level"], serde_json::json!("INFO"));
        assert_eq!(row["service"], serde_json::json!("test-svc"));
        assert_eq!(row["message"], serde_json::json!("integration test event"));
        assert_eq!(row["kafka_partition"], serde_json::json!(0));
        assert_eq!(row["kafka_offset"], serde_json::json!(42));

        let attrs: HashMap<String, AttributeValue> =
            serde_json::from_str(row["attributes"].as_str().unwrap()).unwrap();
        assert_eq!(attrs["request_id"], AttributeValue::String("test-req-1".to_string()));
        assert_eq!(attrs["status_code"], AttributeValue::Int(200));
        assert_eq!(attrs["cached"], AttributeValue::Bool(true));

        assert_eq!(stats["rows_scanned"], serde_json::json!(1u64));
        assert_eq!(stats["files_pruned"], serde_json::json!(0u64));
        assert!(stats["bytes_scanned"].as_u64().unwrap() > 0);
        assert!(stats["duration_ms"].as_u64().is_some());
    }

    #[test]
    fn buffer_flush_writes_parquet_and_registers_in_manifest() {
        use batch_buffer::BatchBuffer;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // Use a threshold small enough that a single make_event() push triggers a flush.
        // make_event() estimated size is well over 100 bytes.
        let mut buf = BatchBuffer::new(100, batch_buffer::DEFAULT_MAX_RECORDS, batch_buffer::DEFAULT_MAX_AGE_MS, Box::new(batch_buffer::SystemClock));
        let batch = buf.push(make_event()).expect("event should trigger flush");
        assert_eq!(batch.len(), 1);
        assert!(buf.is_empty());

        let parquet_path = flush_events(&batch, &data_dir, &mut manifest).unwrap();

        // Parquet file exists on disk.
        assert!(parquet_path.exists(), "parquet file should exist at {parquet_path:?}");

        // Manifest has exactly one entry pointing to that file.
        let files = manifest.active_files(None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, parquet_path.to_string_lossy());
        assert_eq!(files[0].record_count, 1);
    }

    #[tokio::test]
    async fn missing_time_bounds_returns_400() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::open(&dir.path().join("manifest.db")).unwrap();
        let state = make_state(manifest);

        let app = Router::new()
            .route("/query", post(query_handler))
            .with_state(state);

        let cases = [
            serde_json::json!({"sql": "SELECT 1"}),
            serde_json::json!({"sql": "SELECT 1", "time_from": 0}),
            serde_json::json!({"sql": "SELECT 1", "time_to": 9999}),
        ];

        for body in &cases {
            let response = axum::body::to_bytes(
                app.clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .method("POST")
                            .uri("/query")
                            .header("content-type", "application/json")
                            .body(axum::body::Body::from(body.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .into_body(),
                usize::MAX,
            )
            .await
            .unwrap();

            let json: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(json["code"], "missing_time_predicate", "body={body}");
        }
    }

    #[tokio::test]
    async fn valid_time_bounds_returns_200() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::open(&dir.path().join("manifest.db")).unwrap();
        let state = make_state(manifest);

        let app = Router::new()
            .route("/query", post(query_handler))
            .with_state(state);

        let body = serde_json::json!({
            "sql": "SELECT * FROM logs",
            "time_from": 0,
            "time_to": i64::MAX
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn files_pruned_reflects_out_of_range_files() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // 4 files: 2 inside [0, 3_000], 2 outside. Each flush produces one file,
        // so files_pruned must count exactly 2 — not just "non-zero" or "total minus one".
        let make = |ts: i64, offset: i64| LogEvent {
            timestamp: ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "msg".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };
        flush_events(&[make(1_000, 0)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(2_000, 1)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(5_000, 2)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(6_000, 3)], &data_dir, &mut manifest).unwrap();

        let all_files = manifest.active_files(None).unwrap();
        assert_eq!(all_files.len(), 4);

        // Expected bytes: sum of size_bytes for the 2 in-range files only.
        let expected_bytes: u64 = all_files
            .iter()
            .filter(|f| !(f.max_ts < 0 || f.min_ts > 3_000))
            .map(|f| f.size_bytes as u64)
            .sum();

        let state = make_state(manifest);
        // SQL WHERE clause matches time_to so rows_scanned reflects only in-range rows.
        let (_, stats) = query(
            state,
            QueryRequest {
                sql: "SELECT * FROM logs WHERE timestamp <= 3000".to_string(),
                time_from: Some(0),
                time_to: Some(3_000),
                limit: 100,
            },
        )
        .await;

        assert_eq!(stats["files_pruned"], serde_json::json!(2u64));
        assert_eq!(stats["rows_scanned"], serde_json::json!(2u64));
        assert_eq!(stats["bytes_scanned"], serde_json::json!(expected_bytes));
    }

    #[tokio::test]
    async fn dedup_removes_duplicate_partition_offset_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // Two files containing the same (kafka_partition=0, kafka_offset=1) event —
        // simulates Kafka replay after a crash where the flush committed but the
        // offset commit did not.
        let duplicate = LogEvent {
            timestamp: 1_000,
            level: Level::Info,
            service: "svc".to_string(),
            message: "replayed event".to_string(),
            kafka_partition: 0,
            kafka_offset: 1,
            attributes: HashMap::new(),
        };
        let unique = LogEvent {
            timestamp: 2_000,
            level: Level::Info,
            service: "svc".to_string(),
            message: "unique event".to_string(),
            kafka_partition: 0,
            kafka_offset: 2,
            attributes: HashMap::new(),
        };

        // File 1: contains the duplicate and the unique event.
        flush_events(&[duplicate.clone(), unique.clone()], &data_dir, &mut manifest).unwrap();
        // File 2: contains only the duplicate (the replayed copy).
        flush_events(&[duplicate.clone()], &data_dir, &mut manifest).unwrap();

        let state = make_state(manifest);
        let (rows, stats) = query(
            state,
            QueryRequest {
                sql: "SELECT * FROM logs ORDER BY kafka_offset".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 100,
            },
        )
        .await;

        // 3 rows from DataFusion (2 from file 1, 1 from file 2), 2 after dedup.
        assert_eq!(stats["rows_scanned"], serde_json::json!(3u64), "rows_scanned is pre-dedup");
        assert_eq!(rows.len(), 2, "result contains each event exactly once");

        // Verify the two distinct events are present and in order.
        assert_eq!(rows[0]["kafka_offset"], serde_json::json!(1i64));
        assert_eq!(rows[1]["kafka_offset"], serde_json::json!(2i64));

        // No duplicate (partition, offset) pairs in the result.
        let keys: std::collections::HashSet<(i64, i64)> = rows
            .iter()
            .map(|r| (r["kafka_partition"].as_i64().unwrap(), r["kafka_offset"].as_i64().unwrap()))
            .collect();
        assert_eq!(keys.len(), rows.len(), "no duplicate (partition, offset) pairs");
    }

    #[tokio::test]
    async fn dedup_works_with_partial_projection() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // Same (partition, offset) pair in two files — genuine duplicates.
        let event = LogEvent {
            timestamp: 1_000,
            level: Level::Info,
            service: "svc".to_string(),
            message: "dup".to_string(),
            kafka_partition: 0,
            kafka_offset: 1,
            attributes: HashMap::new(),
        };
        flush_events(&[event.clone()], &data_dir, &mut manifest).unwrap();
        flush_events(&[event.clone()], &data_dir, &mut manifest).unwrap();

        let state = make_state(manifest);
        // Projection omits kafka_partition and kafka_offset — dedup still fires
        // because it happens in the view before the user's projection.
        let (rows, stats) = query(
            state,
            QueryRequest {
                sql: "SELECT message, timestamp FROM logs".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 100,
            },
        )
        .await;

        // rows_scanned is total records across non-pruned files (pre-dedup).
        assert_eq!(stats["rows_scanned"], serde_json::json!(2u64));
        // Duplicate removed even though key columns are not in the projection.
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get("message").is_some());
        assert!(rows[0].get("kafka_partition").is_none());
    }

    /// Requires MinIO running: `make up`
    /// Run with: cargo test -p server -- --ignored
    #[tokio::test]
    #[ignore]
    async fn datafusion_reads_parquet_from_minio() {
        use event_schema::encode_batch;
        use object_store::path::Path as ObjPath;
        use parquet::arrow::ArrowWriter;
        use storage::{minio_store, MinioConfig};
        use url::Url;

        let config = MinioConfig::default();
        let store = minio_store(&config).unwrap();

        // Write a small Parquet file to MinIO.
        let event = make_event();
        let batch = encode_batch(&[event]).unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let obj_path = ObjPath::from("test/datafusion_test.parquet");
        store.put(&obj_path, bytes::Bytes::from(buf).into()).await.unwrap();

        // Register the MinIO store with DataFusion and query the file.
        let ctx = SessionContext::new();
        let minio_url = Url::parse(&format!("s3://{}", config.bucket)).unwrap();
        ctx.register_object_store(&minio_url, store.clone());
        ctx.register_parquet(
            "logs",
            &format!("s3://{}/test/datafusion_test.parquet", config.bucket),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let rows = ctx.sql("SELECT service, level FROM logs").await.unwrap().collect().await.unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        store.delete(&obj_path).await.unwrap();
    }

    /// Full ingest → store → query via real Kafka.
    /// Requires the Docker Compose stack: `make up`
    /// Run with: cargo test -p server -- --ignored
    #[tokio::test]
    #[ignore]
    async fn kafka_ingest_and_query() {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        use std::time::Duration;

        const TOPIC: &str = "logs-test-integration";
        const N: usize = 10;

        // Publish N synthetic events to a dedicated test topic.
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        for i in 0..N {
            let event = serde_json::json!({
                "timestamp": now_ns + i as i64,
                "level": "INFO",
                "service": "integration-test",
                "message": "kafka ingest test",
                "kafka_partition": 0,
                "kafka_offset": i,
                "attributes": { "seq": i }
            });
            producer
                .send(BaseRecord::to(TOPIC).payload(&event.to_string()).key(&format!("{i}")))
                .expect("send failed");
        }
        producer.flush(Duration::from_secs(5)).unwrap();

        // Start the consumer and let it run long enough to flush.
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let manifest = Arc::new(Mutex::new(
            Manifest::open(&dir.path().join("manifest.db")).unwrap(),
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let consumer_manifest = Arc::clone(&manifest);
        let consumer_data_dir = data_dir.clone();
        tokio::spawn(async move {
            let _ = run_consumer(
                ConsumerConfig {
                    bootstrap_servers: "localhost:9092".to_string(),
                    group_id: format!("test-{}", uuid::Uuid::new_v4()),
                    topic: TOPIC.to_string(),
                    ..ConsumerConfig::default()
                },
                consumer_data_dir,
                consumer_manifest,
                Arc::new(AtomicU64::new(0)),
                Arc::new(Semaphore::new(4)),
                shutdown_rx,
            )
            .await;
        });

        // Wait for the time trigger (1s) plus buffer.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = shutdown_tx.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Query and assert all N events are present.
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())) };
        let rows = query_rows(state, QueryRequest {
            sql: "SELECT * FROM logs".to_string(),
            time_from: Some(0),
            time_to: Some(i64::MAX),
            limit: 1000,
        }).await;
        assert_eq!(rows.len(), N, "expected {N} events, got {}", rows.len());
    }

    /// After a successful flush, a second consumer with the same group ID must not
    /// re-consume events — proves offsets were committed.
    /// Requires: `make up`
    #[tokio::test]
    #[ignore]
    async fn committed_offsets_not_reprocessed_after_flush() {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        use std::time::Duration;

        let topic = format!("test-committed-{}", uuid::Uuid::new_v4());
        let group_id = format!("grp-{}", uuid::Uuid::new_v4());
        const N: usize = 5;

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create().unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;

        for i in 0..N {
            let event = serde_json::json!({
                "timestamp": now_ns + i as i64, "level": "INFO",
                "service": "commit-test", "message": "commit test",
                "kafka_partition": 0, "kafka_offset": i, "attributes": {}
            });
            producer.send(BaseRecord::to(&topic).payload(&event.to_string()).key(&format!("{i}"))).unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();

        // First consumer: flushes events and commits offsets.
        let dir = tempfile::tempdir().unwrap();
        let manifest = Arc::new(Mutex::new(Manifest::open(&dir.path().join("manifest.db")).unwrap()));
        let (tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let m2 = Arc::clone(&manifest);
        let data_dir = dir.path().join("data");
        let dd = data_dir.clone();
        let gid = group_id.clone();
        let top = topic.clone();
        tokio::spawn(async move {
            let _ = run_consumer(ConsumerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                group_id: gid, topic: top,
                ..ConsumerConfig::default()
            }, dd, m2, Arc::new(AtomicU64::new(0)), Arc::new(Semaphore::new(4)), rx).await;
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Second consumer with the same group ID: should see no messages because
        // offsets were committed to the end of the topic.
        let second: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create().unwrap();
        second.subscribe(&[topic.as_str()]).unwrap();

        // Allow time for partition assignment, then assert no messages arrive.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let result = tokio::time::timeout(Duration::from_secs(2), second.recv()).await;
        assert!(result.is_err(), "second consumer received a message — offsets were not committed");
    }

    /// A consumer that crashes without committing offsets must cause the next
    /// consumer (same group) to re-consume and re-flush those events — proves
    /// at-least-once delivery.
    /// Requires: `make up`
    #[tokio::test]
    #[ignore]
    async fn uncommitted_offsets_reprocessed_on_restart() {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        use std::time::Duration;

        let topic = format!("test-reprocess-{}", uuid::Uuid::new_v4());
        let group_id = format!("grp-{}", uuid::Uuid::new_v4());
        const N: usize = 5;

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create().unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;

        for i in 0..N {
            let event = serde_json::json!({
                "timestamp": now_ns + i as i64, "level": "INFO",
                "service": "reprocess-test", "message": "reprocess test",
                "kafka_partition": 0, "kafka_offset": i, "attributes": {}
            });
            producer.send(BaseRecord::to(&topic).payload(&event.to_string()).key(&format!("{i}"))).unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();

        // Simulate crash: consume all N events but do NOT commit offsets.
        {
            let crasher: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", "localhost:9092")
                .set("group.id", &group_id)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .create().unwrap();
            crasher.subscribe(&[topic.as_str()]).unwrap();
            for _ in 0..N {
                tokio::time::timeout(Duration::from_secs(5), crasher.recv())
                    .await.expect("timed out waiting for message").unwrap();
            }
            // crasher dropped here without committing — simulates kill -9
        }

        // Recovery: run_consumer with the same group ID. Because no offsets were
        // committed, it re-consumes all N events from the beginning.
        let dir = tempfile::tempdir().unwrap();
        let manifest = Arc::new(Mutex::new(Manifest::open(&dir.path().join("manifest.db")).unwrap()));
        let (tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let m2 = Arc::clone(&manifest);
        let data_dir = dir.path().join("data");
        let dd = data_dir.clone();
        let gid = group_id.clone();
        let top = topic.clone();
        tokio::spawn(async move {
            let _ = run_consumer(ConsumerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                group_id: gid, topic: top,
                ..ConsumerConfig::default()
            }, dd, m2, Arc::new(AtomicU64::new(0)), Arc::new(Semaphore::new(4)), rx).await;
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // All N events must be present — none were permanently lost.
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())) };
        let rows = query_rows(state, QueryRequest {
            sql: "SELECT * FROM logs".to_string(),
            time_from: Some(0), time_to: Some(i64::MAX), limit: 1000,
        }).await;
        assert_eq!(rows.len(), N, "expected {N} events after re-consumption, got {}", rows.len());
    }

    /// Verify that no events are lost when a second consumer joins the same group,
    /// triggering a Kafka rebalance that forces the first consumer to flush its
    /// buffered-but-not-yet-threshold events via the `pre_rebalance` callback.
    ///
    /// Requires the Docker Compose stack: `make up`
    /// Run with: cargo test -p server -- --ignored rebalance
    #[tokio::test]
    #[ignore]
    async fn rebalance_flushes_without_data_loss() {
        use rdkafka::config::ClientConfig as RdkConfig;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        use std::collections::HashSet;

        const N: usize = 6;
        let topic = format!("test-rebalance-{}", uuid::Uuid::new_v4());
        let group_id = format!("grp-rebalance-{}", uuid::Uuid::new_v4());

        // Use a large batch size so events won't flush by size threshold.
        // We rely on the rebalance callback (or the 1 s age trigger) to flush.
        const BATCH_MAX_BYTES: usize = 64 * 1024 * 1024;

        let producer: BaseProducer = RdkConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        for i in 0..N {
            let event = serde_json::json!({
                "timestamp": now_ns + i as i64,
                "level": "INFO",
                "service": "rebalance-test",
                "message": "rebalance test event",
                "kafka_partition": 0,
                "kafka_offset": i,
                "attributes": {}
            });
            producer
                .send(
                    BaseRecord::to(&topic)
                        .payload(&event.to_string())
                        .key(&format!("{i}")),
                )
                .expect("send failed");
        }
        producer.flush(Duration::from_secs(5)).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let manifest = Arc::new(Mutex::new(
            Manifest::open(&dir.path().join("manifest.db")).unwrap(),
        ));

        // Start consumer 1.
        let (tx1, rx1) = tokio::sync::broadcast::channel::<()>(1);
        let m1 = Arc::clone(&manifest);
        let dd1 = data_dir.clone();
        let gid1 = group_id.clone();
        let top1 = topic.clone();
        tokio::spawn(async move {
            let _ = run_consumer(
                ConsumerConfig {
                    bootstrap_servers: "localhost:9092".to_string(),
                    group_id: gid1,
                    topic: top1,
                    batch_max_bytes: BATCH_MAX_BYTES,
                },
                dd1,
                m1,
                Arc::new(AtomicU64::new(0)),
                Arc::new(Semaphore::new(4)),
                rx1,
            )
            .await;
        });

        // Give consumer 1 time to receive and buffer the events (but less than the
        // 1 s age trigger so they stay in the buffer when consumer 2 joins).
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start consumer 2 with the same group — triggers a rebalance.
        // Consumer 1's pre_rebalance callback will flush its buffer before yielding.
        let (tx2, rx2) = tokio::sync::broadcast::channel::<()>(1);
        let m2 = Arc::clone(&manifest);
        let dd2 = data_dir.clone();
        let gid2 = group_id.clone();
        let top2 = topic.clone();
        tokio::spawn(async move {
            let _ = run_consumer(
                ConsumerConfig {
                    bootstrap_servers: "localhost:9092".to_string(),
                    group_id: gid2,
                    topic: top2,
                    batch_max_bytes: BATCH_MAX_BYTES,
                },
                dd2,
                m2,
                Arc::new(AtomicU64::new(0)),
                Arc::new(Semaphore::new(4)),
                rx2,
            )
            .await;
        });

        // Allow time for the rebalance and any in-flight flushes to complete.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = tx1.send(());
        let _ = tx2.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // All N events must be present with no duplicates (unique by kafka_offset).
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())) };
        let rows = query_rows(
            state,
            QueryRequest {
                sql: "SELECT kafka_offset FROM logs ORDER BY kafka_offset".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 1000,
            },
        )
        .await;

        assert_eq!(
            rows.len(),
            N,
            "expected {N} events after rebalance, got {} (data loss or duplication)",
            rows.len()
        );

        let offsets: HashSet<i64> = rows
            .iter()
            .map(|r| r["kafka_offset"].as_i64().unwrap())
            .collect();
        assert_eq!(offsets.len(), N, "duplicate events detected after rebalance");
    }

    /// Flush events, then run tiering with threshold_days=0 so all hot files are immediately
    /// eligible. Assert the file appears in MinIO and is removed locally.
    /// Requires MinIO running: `make up`
    /// Run with: cargo test -p server -- --ignored
    #[tokio::test]
    #[ignore]
    async fn tiering_moves_hot_files_to_minio() {
        use object_store::path::Path as ObjPath;
        use server::{run_tiering, TieringConfig};
        use storage::{minio_store, MinioConfig};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");

        let event = make_event();
        let mut manifest = Manifest::open(&db_path).unwrap();
        flush_events(&[event], &data_dir, &mut manifest).unwrap();

        let hot_files = manifest.active_files(None).unwrap();
        assert_eq!(hot_files.len(), 1);
        let hot_path = hot_files[0].path.clone();
        assert!(
            std::path::Path::new(&hot_path).exists(),
            "hot file must exist before tiering"
        );

        let manifest = Arc::new(Mutex::new(manifest));
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        // threshold_days=0 → cutoff = now, so any file with max_ts < now is eligible.
        // interval_secs=0 → sleep resolves immediately; first pass fires right away.
        let tiering_config = TieringConfig {
            minio: MinioConfig::default(),
            threshold_days: 0,
            interval_secs: 0,
        };

        let tiering_manifest = Arc::clone(&manifest);
        let handle = tokio::spawn(async move {
            run_tiering(tiering_config, tiering_manifest, shutdown_rx).await
        });

        // Allow at least one tiering pass to complete, then shut down.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = shutdown_tx.send(());
        handle.await.unwrap().unwrap();

        // Local file must be gone.
        assert!(
            !std::path::Path::new(&hot_path).exists(),
            "local file must be removed after tiering"
        );

        // Manifest must reflect the file as cold with an S3 URI.
        let files = manifest.lock().await.active_files(None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].tier, "cold");
        assert!(files[0].path.starts_with("s3://"), "cold path must be an S3 URI");

        // File must be readable from MinIO.
        let minio_config = MinioConfig::default();
        let store = minio_store(&minio_config).unwrap();
        let cold_key = files[0].path
            .strip_prefix(&format!("s3://{}/", minio_config.bucket))
            .expect("cold path must start with bucket prefix");
        let obj_path = ObjPath::from(cold_key);
        store.get(&obj_path).await.expect("file must be readable from MinIO");

        // Cleanup MinIO object so repeated test runs don't accumulate objects.
        store.delete(&obj_path).await.unwrap();
    }

    /// Ingest 4 events → tier 2 to MinIO → query spanning both tiers → assert all 4 returned.
    /// Requires MinIO running: `make up`
    /// Run with: cargo test -p server -- --ignored cross_tier
    #[tokio::test]
    #[ignore]
    async fn cross_tier_query_returns_rows_from_both_tiers() {
        use object_store::path::Path as ObjPath;
        use server::{run_tiering, TieringConfig};
        use storage::{minio_store, MinioConfig};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // Flush 4 events into 4 separate files so tiering can move a known subset.
        let make = |ts: i64, offset: i64| LogEvent {
            timestamp: ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "cross-tier test".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };
        flush_events(&[make(1_000, 10)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(2_000, 11)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(3_000, 12)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(4_000, 13)], &data_dir, &mut manifest).unwrap();
        assert_eq!(manifest.active_files(None).unwrap().len(), 4);

        // Tier all 4 files to MinIO (threshold_days=0 → all files eligible).
        let manifest_arc = Arc::new(Mutex::new(manifest));
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
        let tiering_config = TieringConfig {
            minio: MinioConfig::default(),
            threshold_days: 0,
            interval_secs: 0,
        };
        let tier_manifest = Arc::clone(&manifest_arc);
        let handle = tokio::spawn(async move {
            run_tiering(tiering_config, tier_manifest, shutdown_rx).await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = shutdown_tx.send(());
        handle.await.unwrap().unwrap();

        // Verify 2 files are cold; manually flip 2 back to hot so we have a mixed state.
        {
            let m = manifest_arc.lock().await;
            let all = m.active_files(None).unwrap();
            assert_eq!(all.len(), 4, "all 4 files must still be active after tiering");
            let cold_count = all.iter().filter(|f| f.tier == "cold").count();
            assert_eq!(cold_count, 4, "all 4 should have been tiered");

            // Flip 2 files back to hot using the local path that no longer exists — instead
            // we re-flush 2 fresh events so we have genuine local files for the hot tier.
        }

        // Re-flush 2 new hot events to create a genuine mixed state.
        {
            let mut m = manifest_arc.lock().await;
            flush_events(&[make(5_000, 20)], &data_dir, &mut *m).unwrap();
            flush_events(&[make(6_000, 21)], &data_dir, &mut *m).unwrap();
        }

        // 4 cold + 2 hot = 6 active files, 6 distinct events.
        let cache_dir = dir.path().join("cache");
        let state = AppState {
            manifest: Arc::clone(&manifest_arc),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(ColdCache::new(cache_dir, 10 * 1024 * 1024 * 1024).unwrap())),
        };
        let (rows, _stats) = query(
            state,
            QueryRequest {
                sql: "SELECT kafka_offset FROM logs ORDER BY kafka_offset".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 100,
            },
        )
        .await;

        assert_eq!(rows.len(), 6, "expected 6 rows from both tiers, got {}", rows.len());

        let offsets: Vec<i64> = rows.iter().map(|r| r["kafka_offset"].as_i64().unwrap()).collect();
        assert_eq!(offsets, vec![10, 11, 12, 13, 20, 21], "rows must span both tiers in order");

        // Cleanup cold objects from MinIO.
        let minio_config = MinioConfig::default();
        let store = minio_store(&minio_config).unwrap();
        let cold_files = manifest_arc.lock().await.active_files(None).unwrap();
        for f in cold_files.iter().filter(|f| f.tier == "cold") {
            let key = f.path
                .strip_prefix(&format!("s3://{}/", minio_config.bucket))
                .unwrap();
            store.delete(&ObjPath::from(key)).await.ok();
        }
    }

    /// First access to a cold file downloads it from MinIO (cache miss); the second
    /// access is served from the local cache even after the MinIO object is deleted,
    /// proving that the cache shields subsequent queries from object store I/O.
    /// Requires MinIO running: `make up`
    /// Run with: cargo test -p server -- --ignored cold_cache
    #[tokio::test]
    #[ignore]
    async fn cold_cache_serves_subsequent_reads_without_minio() {
        use object_store::path::Path as ObjPath;
        use server::{run_tiering, TieringConfig};
        use storage::{minio_store, MinioConfig};

        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        flush_events(&[make_event()], &data_dir, &mut manifest).unwrap();

        // Tier the hot file to MinIO.
        let manifest_arc = Arc::new(Mutex::new(manifest));
        let (tx, rx) = broadcast::channel::<()>(1);
        let tier_manifest = Arc::clone(&manifest_arc);
        let handle = tokio::spawn(async move {
            run_tiering(
                TieringConfig { minio: MinioConfig::default(), threshold_days: 0, interval_secs: 0 },
                tier_manifest,
                rx,
            ).await
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = tx.send(());
        handle.await.unwrap().unwrap();

        let files = manifest_arc.lock().await.active_files(None).unwrap();
        assert_eq!(files[0].tier, "cold", "file must be cold before cache test");
        let cold_uri = files[0].path.clone();

        let state = AppState {
            manifest: Arc::clone(&manifest_arc),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(cache_dir.clone(), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
        };

        // First query — cache miss: file is downloaded from MinIO.
        let (rows1, _) = query(
            state.clone(),
            QueryRequest {
                sql: "SELECT kafka_offset FROM logs".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 10,
            },
        ).await;
        assert_eq!(rows1.len(), 1, "first query must return the event");

        // Cache file must now exist on disk.
        let cache_files: Vec<_> = std::fs::read_dir(&cache_dir).unwrap().collect();
        assert_eq!(cache_files.len(), 1, "one file should be in the cache dir");

        // Delete the object from MinIO so the second query cannot re-download it.
        let minio_config = MinioConfig::default();
        let store = minio_store(&minio_config).unwrap();
        let key = cold_uri
            .strip_prefix(&format!("s3://{}/", minio_config.bucket))
            .unwrap();
        store.delete(&ObjPath::from(key)).await.expect("delete from MinIO");

        // Second query — cache hit: succeeds even though MinIO object is gone.
        let (rows2, _) = query(
            state,
            QueryRequest {
                sql: "SELECT kafka_offset FROM logs".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 10,
            },
        ).await;
        assert_eq!(rows2.len(), 1, "second query must return the same event from cache");
        assert_eq!(rows1[0]["kafka_offset"], rows2[0]["kafka_offset"]);
    }

    /// Verify that buffer backpressure pauses and resumes the Kafka consumer without
    /// dropping events. Uses a tiny batch buffer (500 bytes) so that two events
    /// (~240 bytes each) push occupancy above the 0.8 high-water mark, triggering
    /// a pause before the third event is polled.
    ///
    /// Requires the Docker Compose stack: `make up`
    /// Run with: cargo test -p server -- --ignored backpressure
    #[tokio::test]
    #[ignore]
    async fn backpressure_pauses_and_resumes_without_dropping_events() {
        use rdkafka::config::ClientConfig as RdkConfig;
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

        // 450 bytes/event estimated (31 fixed + 419 message); 1000-byte limit means
        // 2 events reach 90% occupancy (> 0.8 high-water mark) without triggering a
        // size flush (900 < 1000), so the pause check fires in the else branch.
        const BATCH_MAX_BYTES: usize = 1000;
        const N: usize = 6;

        let topic = format!("test-bp-{}", uuid::Uuid::new_v4());
        let group_id = format!("test-bp-grp-{}", uuid::Uuid::new_v4());

        let producer: BaseProducer = RdkConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        for i in 0..N {
            let event = serde_json::json!({
                "timestamp": now_ns + i as i64,
                "level": "INFO",
                "service": "bp-test",
                // 31 fixed bytes + 419 message = 450 bytes estimated per event
                "message": "x".repeat(419),
                "kafka_partition": 0,
                "kafka_offset": i,
                "attributes": {}
            });
            producer
                .send(BaseRecord::to(&topic).payload(&event.to_string()).key(&format!("{i}")))
                .expect("send failed");
        }
        producer.flush(Duration::from_secs(5)).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let manifest = Arc::new(Mutex::new(
            Manifest::open(&dir.path().join("manifest.db")).unwrap(),
        ));
        let (tx, rx) = tokio::sync::broadcast::channel::<()>(1);
        let m2 = Arc::clone(&manifest);
        let dd = data_dir.clone();
        let pauses = Arc::new(AtomicU64::new(0));
        let pauses_clone = Arc::clone(&pauses);

        tokio::spawn(async move {
            let _ = run_consumer(
                ConsumerConfig {
                    bootstrap_servers: "localhost:9092".to_string(),
                    group_id,
                    topic,
                    batch_max_bytes: BATCH_MAX_BYTES,
                },
                dd,
                m2,
                pauses_clone,
                Arc::new(Semaphore::new(4)),
                rx,
            )
            .await;
        });

        // Each pause/resume cycle takes ~1 s (the age trigger). With N=6 events and
        // 2 events per cycle, we expect ~3 cycles. Wait 10 s to be safe on slow CI.
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            pauses.load(Ordering::Relaxed) > 0,
            "expected at least one backpressure pause"
        );

        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())) };
        let rows = query_rows(
            state,
            QueryRequest {
                sql: "SELECT * FROM logs".to_string(),
                time_from: Some(0),
                time_to: Some(i64::MAX),
                limit: 1000,
            },
        )
        .await;
        assert_eq!(
            rows.len(),
            N,
            "expected {N} events after backpressure cycles, got {}",
            rows.len()
        );
    }
}
