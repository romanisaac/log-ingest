use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use axum::{
    body::Body,
    extract::{Path as AxumPath, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use datafusion::prelude::*;
use futures::{channel::mpsc, StreamExt};
use manifest::Manifest;
use serde::Deserialize;
use cold_cache::ColdCache;
use compact::{run_compaction, CompactionConfig};
use server::{flush_events, run_consumer, run_sweeper, run_tiering, ConsumerConfig, SweeperConfig, TieringConfig};
use storage::MinioConfig;

mod assets;
mod cold_cache;
mod compact;
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
enum JobState {
    Queued,
    Running,
    Complete { result_count: usize, result_path: PathBuf },
    Failed { error: String },
    Cancelled,
}

#[derive(Clone)]
struct JobEntry {
    state: JobState,
    cancel: tokio_util::sync::CancellationToken,
    client_ip: std::net::IpAddr,
}

type JobStore = Arc<Mutex<HashMap<uuid::Uuid, JobEntry>>>;

#[derive(Debug, Deserialize)]
struct IngestRequest {
    service: String,
    timestamp: i64,
    kafka_partition: i32,
    kafka_offset: i64,
    level: String,
    message: String,
    #[serde(default)]
    attributes: HashMap<String, event_schema::AttributeValue>,
}

#[derive(Clone)]
struct AppState {
    manifest: Arc<Mutex<Manifest>>,
    minio: MinioConfig,
    cold_semaphore: Arc<Semaphore>,
    cold_cache: Arc<Mutex<ColdCache>>,
    job_store: JobStore,
    jobs_dir: PathBuf,
    job_timeout_secs: u64,
    job_semaphore: Arc<Semaphore>,
    per_client_queue_cap: usize,
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

    // Partition files into scan set (overlaps the requested window) and pruned set.
    let is_pruned = |f: &&manifest::FileEntry| f.max_ts < time_from || f.min_ts > time_to;
    let files_pruned = files.iter().filter(is_pruned).count() as u64;
    let scan_files: Vec<&manifest::FileEntry> = files.iter().filter(|f| !is_pruned(f)).collect();
    let bytes_scanned: u64 = scan_files.iter().map(|f| f.size_bytes as u64).sum();
    // Pre-dedup row count across files that will actually be scanned.
    let rows_scanned: u64 = scan_files.iter().map(|f| f.record_count as u64).sum();

    if scan_files.is_empty() {
        return Ok(futures::stream::once(futures::future::ready(Ok(
            make_stats_bytes(0, files_pruned, 0, 0),
        )))
        .boxed());
    }

    let ctx = SessionContext::new();

    // Acquire a cold semaphore permit if any cold files are present.
    // The permit is held for the full query duration to cap concurrent cold-tier I/O.
    let has_cold = scan_files.iter().any(|f| f.tier == "cold");
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
    for (i, entry) in scan_files.iter().enumerate() {
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

    // Apply the server-side cap via the DataFrame API so it sits on top of the
    // logical plan without discarding any ORDER BY the user included.
    let inner = req.sql.trim_end_matches(';').trim();
    let batch_stream = ctx
        .sql(inner)
        .await
        .context("parse user sql")?
        .limit(0, Some(req.limit as usize))
        .context("apply server limit")?
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

// ─── Async job queue ─────────────────────────────────────────────────────────

async fn run_job(state: AppState, req: QueryRequest, job_id: uuid::Uuid) -> Result<usize> {
    let result_path = state.jobs_dir.join(format!("{job_id}.ndjson"));
    let stream = stream_query(state, req).await?;

    let mut all_rows: Vec<serde_json::Value> = Vec::new();
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.map_err(|e| anyhow::anyhow!("{e}"))?;
        for line in bytes.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) {
                all_rows.push(v);
            }
        }
    }

    // Last element is always the stats object emitted by stream_query — drop it.
    if !all_rows.is_empty() {
        all_rows.pop();
    }

    let result_count = all_rows.len();

    let content: String = all_rows
        .iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    tokio::fs::write(&result_path, content).await.context("write job results")?;

    Ok(result_count)
}

async fn submit_job_handler(
    State(state): State<AppState>,
    maybe_addr: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
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

    let client_ip: std::net::IpAddr = maybe_addr
        .map(|c| c.0.ip())
        .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

    {
        let store = state.job_store.lock().await;
        let queued = store.values()
            .filter(|e| matches!(e.state, JobState::Queued) && e.client_ip == client_ip)
            .count();
        if queued >= state.per_client_queue_cap {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "5")],
                Json(serde_json::json!({
                    "error": "per-client queue cap exceeded",
                    "code": "queue_cap_exceeded",
                    "cap": state.per_client_queue_cap,
                })),
            ).into_response();
        }
    }

    let job_id = uuid::Uuid::new_v4();
    let cancel = tokio_util::sync::CancellationToken::new();
    {
        state.job_store.lock().await.insert(job_id, JobEntry {
            state: JobState::Queued,
            cancel: cancel.clone(),
            client_ip,
        });
    }

    let state_bg = state.clone();
    tokio::spawn(async move {
        // Acquire a semaphore permit before running; this bounds concurrency and
        // ensures the permit is released on completion, failure, timeout, or cancel.
        let _permit = Arc::clone(&state_bg.job_semaphore)
            .acquire_owned()
            .await
            .expect("job semaphore closed");

        {
            let mut store = state_bg.job_store.lock().await;
            if let Some(entry) = store.get_mut(&job_id) {
                entry.state = JobState::Running;
            }
        }

        let timeout = Duration::from_secs(state_bg.job_timeout_secs);
        let new_state = tokio::select! {
            biased;
            _ = cancel.cancelled() => JobState::Cancelled,
            result = tokio::time::timeout(timeout, run_job(state_bg.clone(), req, job_id)) => {
                match result {
                    Err(_) => JobState::Failed {
                        error: format!("job timed out after {} seconds", state_bg.job_timeout_secs),
                    },
                    Ok(Err(e)) => JobState::Failed { error: format!("{e:#}") },
                    Ok(Ok(result_count)) => JobState::Complete {
                        result_count,
                        result_path: state_bg.jobs_dir.join(format!("{job_id}.ndjson")),
                    },
                }
            }
        };

        // Only write back if DELETE hasn't already set Cancelled.
        let mut store = state_bg.job_store.lock().await;
        if let Some(entry) = store.get_mut(&job_id) {
            if !matches!(entry.state, JobState::Cancelled) {
                entry.state = new_state;
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "job_id": job_id.to_string() })),
    )
    .into_response()
}

fn parse_job_id(id_str: &str) -> Option<uuid::Uuid> {
    id_str.parse().ok()
}

async fn poll_job_handler(
    State(state): State<AppState>,
    AxumPath(id_str): AxumPath<String>,
) -> Response {
    let job_id = match parse_job_id(&id_str) {
        Some(id) => id,
        None => return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid job id" })),
        ).into_response(),
    };
    let entry = state.job_store.lock().await.get(&job_id).cloned();

    match entry.map(|e| e.state) {
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        )
        .into_response(),
        Some(JobState::Queued) => Json(serde_json::json!({ "state": "queued" })).into_response(),
        Some(JobState::Running) => Json(serde_json::json!({ "state": "running" })).into_response(),
        Some(JobState::Cancelled) => Json(serde_json::json!({ "state": "cancelled" })).into_response(),
        Some(JobState::Failed { error }) => {
            Json(serde_json::json!({ "state": "failed", "error": error })).into_response()
        }
        Some(JobState::Complete { result_count, result_path }) => {
            let rows: Vec<serde_json::Value> = match tokio::fs::read_to_string(&result_path).await {
                Err(_) => vec![],
                Ok(content) => content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .filter_map(|l| serde_json::from_str(l).ok())
                    .collect(),
            };
            Json(serde_json::json!({
                "state": "complete",
                "result_count": result_count,
                "rows": rows,
            }))
            .into_response()
        }
    }
}

async fn delete_job_handler(
    State(state): State<AppState>,
    AxumPath(id_str): AxumPath<String>,
) -> Response {
    let job_id = match parse_job_id(&id_str) {
        Some(id) => id,
        None => return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid job id" })),
        ).into_response(),
    };

    let mut store = state.job_store.lock().await;
    match store.get_mut(&job_id) {
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        )
        .into_response(),
        Some(entry) => match &entry.state {
            JobState::Queued | JobState::Running => {
                entry.cancel.cancel();
                entry.state = JobState::Cancelled;
                StatusCode::NO_CONTENT.into_response()
            }
            JobState::Complete { .. } => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "job already complete" })),
            )
            .into_response(),
            JobState::Failed { .. } => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "job already failed" })),
            )
            .into_response(),
            JobState::Cancelled => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "job already cancelled" })),
            )
            .into_response(),
        },
    }
}

#[derive(serde::Deserialize)]
struct ResultsQuery {
    #[serde(default)]
    page: usize,
    #[serde(default = "default_page_size")]
    page_size: usize,
}

fn default_page_size() -> usize { 100 }

async fn results_job_handler(
    State(state): State<AppState>,
    AxumPath(id_str): AxumPath<String>,
    axum::extract::Query(params): axum::extract::Query<ResultsQuery>,
) -> Response {
    let job_id = match parse_job_id(&id_str) {
        Some(id) => id,
        None => return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid job id" })),
        ).into_response(),
    };

    let entry = state.job_store.lock().await.get(&job_id).cloned();
    let result_path = match entry.map(|e| e.state) {
        None => return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "job not found" })),
        )
        .into_response(),
        Some(JobState::Complete { result_path, .. }) => result_path,
        Some(_) => return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "job results not yet available" })),
        )
        .into_response(),
    };

    let all_rows: Vec<serde_json::Value> = match tokio::fs::read_to_string(&result_path).await {
        Err(_) => vec![],
        Ok(content) => content
            .lines()
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect(),
    };

    let total = all_rows.len();
    let page_size = params.page_size.max(1);
    let rows: Vec<serde_json::Value> = all_rows
        .into_iter()
        .skip(params.page * page_size)
        .take(page_size)
        .collect();

    Json(serde_json::json!({
        "page": params.page,
        "page_size": page_size,
        "total": total,
        "rows": rows,
    }))
    .into_response()
}

async fn ingest_handler(
    State(state): State<AppState>,
    Json(req): Json<IngestRequest>,
) -> Response {
    let level = match event_schema::Level::from_str(&req.level.to_uppercase()) {
        Ok(l) => l,
        Err(_) => return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("unknown level '{}'", req.level)})),
        ).into_response(),
    };
    let event = event_schema::LogEvent {
        service: req.service,
        timestamp: req.timestamp,
        kafka_partition: req.kafka_partition,
        kafka_offset: req.kafka_offset,
        level,
        message: req.message,
        attributes: req.attributes,
    };
    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string()),
    );
    let mut m = state.manifest.lock().await;
    match flush_events(&[event], &data_dir, &mut m) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{e:#}")})),
        ).into_response(),
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
    let data_dir = data_dir; // keep owned before cloning into spawned tasks

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
    let consumer_data_dir = data_dir.clone();
    tokio::spawn(async move {
        if let Err(e) = run_consumer(
            ConsumerConfig::default(),
            consumer_data_dir,
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

    // Start background compaction task.
    let compaction_manifest = Arc::clone(&manifest);
    let compaction_semaphore = Arc::clone(&flush_semaphore);
    let compaction_shutdown = shutdown_tx.subscribe();
    let compaction_config = CompactionConfig {
        data_dir: data_dir.clone(),
        min_files_per_bucket: std::env::var("COMPACT_MIN_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2),
        target_file_bytes: std::env::var("COMPACT_TARGET_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64 * 1024 * 1024),
        interval_secs: std::env::var("COMPACT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1800),
        small_file_threshold: std::env::var("COMPACT_SMALL_FILE_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10),
        max_duplicate_ratio: std::env::var("COMPACT_MAX_DUPLICATE_RATIO")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.5),
        threshold_check_secs: std::env::var("COMPACT_THRESHOLD_CHECK_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60),
    };
    tokio::spawn(async move {
        if let Err(e) = run_compaction(
            compaction_config,
            compaction_manifest,
            compaction_semaphore,
            compaction_shutdown,
        )
        .await
        {
            tracing::error!("compaction task exited with error: {e:#}");
        }
    });

    // Start background retention sweeper.
    let sweeper_manifest = Arc::clone(&manifest);
    let sweeper_minio = minio_config.clone();
    let sweeper_shutdown = shutdown_tx.subscribe();
    let sweeper_config = SweeperConfig {
        hot_retention_secs: std::env::var("HOT_RETENTION_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(14)
            * 86_400,
        cold_retention_secs: std::env::var("COLD_RETENTION_DAYS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(90)
            * 86_400,
        superseded_grace_secs: std::env::var("SUPERSEDED_GRACE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(600),
        interval_secs: std::env::var("SWEEP_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
    };
    tokio::spawn(async move {
        if let Err(e) =
            run_sweeper(sweeper_config, sweeper_manifest, sweeper_minio, sweeper_shutdown).await
        {
            tracing::error!("sweeper task exited with error: {e:#}");
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
    let jobs_dir = PathBuf::from(
        std::env::var("JOBS_DIR").unwrap_or_else(|_| "jobs".to_string()),
    );
    std::fs::create_dir_all(&jobs_dir).expect("create jobs dir");

    let job_timeout_secs = std::env::var("JOB_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300u64);

    let max_concurrent_jobs = std::env::var("MAX_CONCURRENT_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8usize);

    let per_client_queue_cap = std::env::var("PER_CLIENT_QUEUE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5usize);

    let state = AppState {
        manifest,
        minio: minio_config,
        cold_semaphore: Arc::new(Semaphore::new(cold_concurrency)),
        cold_cache,
        job_store: Arc::new(Mutex::new(HashMap::new())),
        jobs_dir,
        job_timeout_secs,
        job_semaphore: Arc::new(Semaphore::new(max_concurrent_jobs)),
        per_client_queue_cap,
    };
    let app = Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/query", post(query_handler))
        .route("/jobs", post(submit_job_handler))
        .route("/jobs/:id", get(poll_job_handler).delete(delete_job_handler))
        .route("/jobs/:id/results", get(results_job_handler))
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
        _ = axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()) => {}
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
        let jobs_dir = std::env::temp_dir().join("log-ingest-test-jobs");
        std::fs::create_dir_all(&jobs_dir).ok();
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
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 300,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
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
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())), job_store: Arc::new(Mutex::new(HashMap::new())), jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"), job_timeout_secs: 300, job_semaphore: Arc::new(Semaphore::new(8)), per_client_queue_cap: 5 };
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
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())), job_store: Arc::new(Mutex::new(HashMap::new())), jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"), job_timeout_secs: 300, job_semaphore: Arc::new(Semaphore::new(8)), per_client_queue_cap: 5 };
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
        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())), job_store: Arc::new(Mutex::new(HashMap::new())), jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"), job_timeout_secs: 300, job_semaphore: Arc::new(Semaphore::new(8)), per_client_queue_cap: 5 };
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
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"),
            job_timeout_secs: 300,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
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
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"),
            job_timeout_secs: 300,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
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

    /// Compact 3 small, out-of-order, duplicate-containing files into a single
    /// sorted, deduplicated output; assert the manifest reflects the swap atomically.
    #[tokio::test]
    async fn compaction_sorts_deduplicates_and_swaps_manifest() {
        use crate::compact::{compact_bucket, CompactionConfig};
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        let make = |ts: i64, offset: i64| LogEvent {
            timestamp: ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "msg".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };

        // File 1: ts=3000 (out of order), unique offset=3.
        flush_events(&[make(3_000, 3)], &data_dir, &mut manifest).unwrap();
        // File 2: ts=1000, offset=1.
        flush_events(&[make(1_000, 1)], &data_dir, &mut manifest).unwrap();
        // File 3: ts=1000 offset=1 (duplicate of file 2) + ts=2000 offset=2.
        flush_events(&[make(1_000, 1), make(2_000, 2)], &data_dir, &mut manifest).unwrap();

        assert_eq!(manifest.active_files(None).unwrap().len(), 3);

        // Determine the time_bucket so compact_bucket can find the right files.
        // All events have ts in the nanosecond range → 1970-01-01-00.
        let buckets = manifest.compactable_buckets(2).unwrap();
        assert_eq!(buckets.len(), 1);
        let (service, time_bucket) = &buckets[0];

        let manifest_arc = Arc::new(Mutex::new(manifest));
        let config = CompactionConfig {
            data_dir: data_dir.clone(),
            ..CompactionConfig::default()
        };

        compact_bucket(service, time_bucket, &config, &manifest_arc)
            .await
            .unwrap();

        let active = manifest_arc.lock().await.active_files(None).unwrap();

        // Old 3 files superseded → 1 new compacted file active.
        assert_eq!(active.len(), 1, "exactly one compacted file should be active");
        assert_eq!(active[0].service, "svc");
        assert_eq!(active[0].tier, "hot");
        assert_eq!(active[0].record_count, 3, "3 unique events after dedup");

        // Read the compacted output file directly with DataFusion to verify content.
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_parquet("out", &active[0].path, ParquetReadOptions::default())
            .await
            .unwrap();
        let batches = ctx
            .sql("SELECT timestamp, kafka_offset FROM out ORDER BY timestamp")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let ts_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let off_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "3 unique rows after dedup");

        // Verify ascending timestamp sort.
        assert!(ts_col.value(0) <= ts_col.value(1) && ts_col.value(1) <= ts_col.value(2),
            "rows must be sorted by timestamp");

        // Verify deduplication: no two rows share (partition, offset).
        let offsets: Vec<i64> = (0..off_col.len()).map(|i| off_col.value(i)).collect();
        let unique: std::collections::HashSet<i64> = offsets.iter().cloned().collect();
        assert_eq!(unique.len(), offsets.len(), "no duplicate offsets");
    }

    /// Compacted Parquet files must carry row-group statistics on the timestamp column so
    /// DataFusion can prune row groups at query time without reading page data.
    #[tokio::test]
    async fn compacted_file_has_row_group_statistics() {
        use crate::compact::{compact_bucket, CompactionConfig};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        let make = |ts: i64, offset: i64| LogEvent {
            timestamp: ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "msg".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };

        flush_events(&[make(1_000, 1)], &data_dir, &mut manifest).unwrap();
        flush_events(&[make(2_000, 2)], &data_dir, &mut manifest).unwrap();

        let buckets = manifest.compactable_buckets(2).unwrap();
        assert_eq!(buckets.len(), 1);
        let (service, time_bucket) = &buckets[0];

        let manifest_arc = Arc::new(Mutex::new(manifest));
        let config = CompactionConfig {
            data_dir: data_dir.clone(),
            ..CompactionConfig::default()
        };
        compact_bucket(service, time_bucket, &config, &manifest_arc)
            .await
            .unwrap();

        let active = manifest_arc.lock().await.active_files(None).unwrap();
        assert_eq!(active.len(), 1, "one compacted file expected");

        let file = std::fs::File::open(&active[0].path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        assert!(metadata.num_row_groups() > 0);

        let file_schema = metadata.file_metadata().schema_descr();
        let ts_col_idx = (0..file_schema.num_columns())
            .find(|&i| file_schema.column(i).name() == "timestamp")
            .expect("timestamp column must exist in compacted file");

        let rg = metadata.row_group(0);
        assert!(
            rg.column(ts_col_idx).statistics().is_some(),
            "timestamp column must carry row-group statistics for min/max pruning"
        );
    }

    /// Ingest enough single-event files to exceed the small-file threshold, then run
    /// the compaction scheduler with a short check interval and assert the file count
    /// drops once the threshold trigger fires.
    #[tokio::test]
    async fn threshold_trigger_fires_when_small_file_count_exceeded() {
        use crate::compact::{run_compaction, CompactionConfig};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        let make = |offset: i64| LogEvent {
            timestamp: 1_000_000 + offset,
            level: Level::Info,
            service: "svc".to_string(),
            message: "msg".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };

        // Flush 5 single-event files — one per call produces one small file per call.
        for i in 0..5 {
            flush_events(&[make(i)], &data_dir, &mut manifest).unwrap();
        }
        assert_eq!(manifest.active_files(None).unwrap().len(), 5);

        let manifest_arc = Arc::new(Mutex::new(manifest));
        let semaphore = Arc::new(Semaphore::new(4));
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

        let config = CompactionConfig {
            data_dir: data_dir.clone(),
            // High schedule threshold so only the threshold check can fire.
            min_files_per_bucket: 100,
            // Trigger when a bucket reaches 3 files.
            small_file_threshold: 3,
            // Disable duplicate-density trigger.
            max_duplicate_ratio: 1.0,
            // Check every second so the test completes quickly.
            threshold_check_secs: 1,
            interval_secs: 3600,
            ..CompactionConfig::default()
        };

        let m = Arc::clone(&manifest_arc);
        let s = Arc::clone(&semaphore);
        tokio::spawn(async move {
            let _ = run_compaction(config, m, s, shutdown_rx).await;
        });

        // Give the threshold check time to fire and compaction to complete.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = shutdown_tx.send(());

        let active = manifest_arc.lock().await.active_files(None).unwrap();
        assert!(
            active.len() < 5,
            "threshold trigger should have compacted 5 small files; still {} active",
            active.len()
        );
    }

    #[tokio::test]
    async fn sweeper_removes_expired_hot_files() {
        use server::{run_sweeper, SweeperConfig};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let mut manifest = Manifest::open(&db_path).unwrap();

        // Flush a file whose timestamp is ancient (1 000 ns — epoch + 1 µs).
        let ancient_ts: i64 = 1_000;
        let expired_event = LogEvent {
            timestamp: ancient_ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "ancient event".to_string(),
            kafka_partition: 0,
            kafka_offset: 0,
            attributes: HashMap::new(),
        };
        flush_events(&[expired_event], &data_dir, &mut manifest).unwrap();

        let files = manifest.active_files(None).unwrap();
        assert_eq!(files.len(), 1);
        let expired_path = files[0].path.clone();
        assert!(std::path::Path::new(&expired_path).exists(), "file should be on disk before sweep");

        let manifest_arc = Arc::new(Mutex::new(manifest));
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
        let sweeper_manifest = Arc::clone(&manifest_arc);

        tokio::spawn(async move {
            let _ = run_sweeper(
                SweeperConfig {
                    // 1-second retention so the ancient file is immediately eligible.
                    hot_retention_secs: 1,
                    cold_retention_secs: 1,
                    superseded_grace_secs: 600,
                    interval_secs: 1,
                },
                sweeper_manifest,
                MinioConfig::default(),
                shutdown_rx,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = shutdown_tx.send(());

        // File must be gone from disk.
        assert!(
            !std::path::Path::new(&expired_path).exists(),
            "expired file should be deleted from disk by sweeper"
        );
        // File must be gone from manifest.
        let remaining = manifest_arc.lock().await.active_files(None).unwrap();
        assert_eq!(remaining.len(), 0, "expired file should be removed from manifest by sweeper");
    }

    #[tokio::test]
    async fn submit_and_poll_job_matches_direct_query() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let jobs_dir = dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();

        let mut manifest = Manifest::open(&db_path).unwrap();

        let make = |ts: i64, offset: i64| LogEvent {
            timestamp: ts,
            level: Level::Info,
            service: "svc".to_string(),
            message: "job test".to_string(),
            kafka_partition: 0,
            kafka_offset: offset,
            attributes: HashMap::new(),
        };
        flush_events(&[make(1_000, 0), make(2_000, 1), make(3_000, 2)], &data_dir, &mut manifest).unwrap();

        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(dir.path().join("cache"), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 60,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
        };

        let app = Router::new()
            .route("/query", post(query_handler))
            .route("/jobs", post(submit_job_handler))
            .route("/jobs/:id", get(poll_job_handler).delete(delete_job_handler))
            .route("/jobs/:id/results", get(results_job_handler))
            .with_state(state);

        let query_body = serde_json::json!({
            "sql": "SELECT kafka_offset FROM logs ORDER BY kafka_offset",
            "time_from": 0,
            "time_to": i64::MAX,
            "limit": 100
        });

        // Reference result via direct query.
        let direct_bytes = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("POST").uri("/query")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(query_body.to_string()))
                    .unwrap(),
            ).await.unwrap().into_body(),
            usize::MAX,
        ).await.unwrap();
        let mut direct_rows: Vec<serde_json::Value> = direct_bytes
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_slice(l).ok())
            .collect();
        direct_rows.pop(); // remove trailing stats line

        // Submit job — must return immediately with a job_id.
        let t0 = std::time::Instant::now();
        let submit_bytes = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("POST").uri("/jobs")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(query_body.to_string()))
                    .unwrap(),
            ).await.unwrap().into_body(),
            usize::MAX,
        ).await.unwrap();
        assert!(t0.elapsed().as_millis() < 50, "POST /jobs must return in < 50ms");

        let submit_json: serde_json::Value = serde_json::from_slice(&submit_bytes).unwrap();
        let job_id = submit_json["job_id"].as_str().expect("job_id missing").to_string();

        // Poll until complete (up to 5 s).
        let mut job_result = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let poll_bytes = axum::body::to_bytes(
                app.clone().oneshot(
                    axum::http::Request::builder()
                        .method("GET").uri(format!("/jobs/{job_id}"))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                ).await.unwrap().into_body(),
                usize::MAX,
            ).await.unwrap();
            let poll_json: serde_json::Value = serde_json::from_slice(&poll_bytes).unwrap();
            match poll_json["state"].as_str().unwrap() {
                "complete" => { job_result = Some(poll_json); break; }
                "failed" => panic!("job failed: {}", poll_json["error"]),
                _ => {}
            }
        }

        let job_result = job_result.expect("job did not complete within 5 s");
        assert_eq!(job_result["result_count"].as_u64().unwrap(), 3);

        let job_rows = job_result["rows"].as_array().unwrap();
        assert_eq!(job_rows.len(), direct_rows.len(), "job row count must match direct query");
        for (job_row, direct_row) in job_rows.iter().zip(direct_rows.iter()) {
            assert_eq!(job_row["kafka_offset"], direct_row["kafka_offset"]);
        }
    }

    // Hold all permits on the job semaphore so submitted jobs stay Queued.
    // Returns the permits; drop them to unblock.
    async fn exhaust_semaphore(sem: &Arc<Semaphore>, n: usize) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let mut permits = Vec::with_capacity(n);
        for _ in 0..n {
            permits.push(Arc::clone(sem).acquire_owned().await.unwrap());
        }
        permits
    }

    fn make_job_app(state: AppState) -> axum::Router {
        Router::new()
            .route("/jobs", post(submit_job_handler))
            .route("/jobs/:id", get(poll_job_handler).delete(delete_job_handler))
            .route("/jobs/:id/results", get(results_job_handler))
            .with_state(state)
    }

    async fn submit_job(app: &axum::Router, body: &serde_json::Value) -> String {
        let bytes = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("POST").uri("/jobs")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            ).await.unwrap().into_body(),
            usize::MAX,
        ).await.unwrap();
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
            ["job_id"].as_str().unwrap().to_string()
    }

    async fn get_job(app: &axum::Router, job_id: &str) -> (StatusCode, serde_json::Value) {
        let resp = app.clone().oneshot(
            axum::http::Request::builder()
                .method("GET").uri(format!("/jobs/{job_id}"))
                .body(axum::body::Body::empty()).unwrap(),
        ).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn delete_job(app: &axum::Router, job_id: &str) -> StatusCode {
        app.clone().oneshot(
            axum::http::Request::builder()
                .method("DELETE").uri(format!("/jobs/{job_id}"))
                .body(axum::body::Body::empty()).unwrap(),
        ).await.unwrap().status()
    }

    #[tokio::test]
    async fn cancel_queued_job_transitions_to_cancelled_and_releases_semaphore() {
        let dir = tempfile::tempdir().unwrap();
        let jobs_dir = dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();
        let manifest = Manifest::open(&dir.path().join("manifest.db")).unwrap();

        let job_semaphore = Arc::new(Semaphore::new(1));
        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(dir.path().join("cache"), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 60,
            job_semaphore: Arc::clone(&job_semaphore),
            per_client_queue_cap: 5,
        };

        let app = make_job_app(state);

        // Hold the only semaphore permit so any submitted job stays Queued.
        let _held = exhaust_semaphore(&job_semaphore, 1).await;

        let body = serde_json::json!({
            "sql": "SELECT 1", "time_from": 0, "time_to": i64::MAX, "limit": 1
        });
        let job_id = submit_job(&app, &body).await;

        // Job must be Queued (semaphore exhausted).
        let (_, poll) = get_job(&app, &job_id).await;
        assert_eq!(poll["state"], "queued", "job should be queued while semaphore is held");

        // Cancel it.
        let status = delete_job(&app, &job_id).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (_, poll) = get_job(&app, &job_id).await;
        assert_eq!(poll["state"], "cancelled");

        // Release the held permit — the cancelled job must NOT grab it and run.
        drop(_held);
        tokio::task::yield_now().await;

        // Semaphore permit must be available again (job never acquired it).
        assert_eq!(job_semaphore.available_permits(), 1, "semaphore permit should be free");
    }

    #[tokio::test]
    async fn delete_complete_job_returns_409() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let jobs_dir = dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();

        let mut manifest = Manifest::open(&db_path).unwrap();
        flush_events(&[LogEvent {
            timestamp: 1_000, level: Level::Info, service: "svc".to_string(),
            message: "msg".to_string(), kafka_partition: 0, kafka_offset: 0,
            attributes: HashMap::new(),
        }], &data_dir, &mut manifest).unwrap();

        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(dir.path().join("cache"), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 60,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
        };

        let app = make_job_app(state);
        let body = serde_json::json!({
            "sql": "SELECT * FROM logs", "time_from": 0, "time_to": i64::MAX, "limit": 10
        });

        let job_id = submit_job(&app, &body).await;

        // Poll until complete.
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (_, poll) = get_job(&app, &job_id).await;
            if poll["state"] == "complete" { break; }
        }
        let (_, poll) = get_job(&app, &job_id).await;
        assert_eq!(poll["state"], "complete", "job should be complete before testing 409");

        let status = delete_job(&app, &job_id).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn results_endpoint_paginates_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("manifest.db");
        let data_dir = dir.path().join("data");
        let jobs_dir = dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();

        let mut manifest = Manifest::open(&db_path).unwrap();
        // Flush 5 events so we can paginate with page_size=2.
        for i in 0..5i64 {
            flush_events(&[LogEvent {
                timestamp: 1_000 + i, level: Level::Info, service: "svc".to_string(),
                message: "msg".to_string(), kafka_partition: 0, kafka_offset: i,
                attributes: HashMap::new(),
            }], &data_dir, &mut manifest).unwrap();
        }

        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(dir.path().join("cache"), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 60,
            job_semaphore: Arc::new(Semaphore::new(8)),
            per_client_queue_cap: 5,
        };

        let app = Router::new()
            .route("/jobs", post(submit_job_handler))
            .route("/jobs/:id", get(poll_job_handler).delete(delete_job_handler))
            .route("/jobs/:id/results", get(results_job_handler))
            .with_state(state);

        let body = serde_json::json!({
            "sql": "SELECT kafka_offset FROM logs ORDER BY kafka_offset",
            "time_from": 0, "time_to": i64::MAX, "limit": 100
        });
        let job_id = submit_job(&app, &body).await;

        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let (_, poll) = get_job(&app, &job_id).await;
            if poll["state"] == "complete" { break; }
        }

        // Page 0: rows 0-1
        let resp = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("GET").uri(format!("/jobs/{job_id}/results?page=0&page_size=2"))
                    .body(axum::body::Body::empty()).unwrap(),
            ).await.unwrap().into_body(), usize::MAX,
        ).await.unwrap();
        let page0: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(page0["total"], 5);
        assert_eq!(page0["page"], 0);
        assert_eq!(page0["rows"].as_array().unwrap().len(), 2);
        assert_eq!(page0["rows"][0]["kafka_offset"], 0);

        // Page 1: rows 2-3
        let resp = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("GET").uri(format!("/jobs/{job_id}/results?page=1&page_size=2"))
                    .body(axum::body::Body::empty()).unwrap(),
            ).await.unwrap().into_body(), usize::MAX,
        ).await.unwrap();
        let page1: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(page1["rows"].as_array().unwrap().len(), 2);
        assert_eq!(page1["rows"][0]["kafka_offset"], 2);

        // Page 2: row 4 only (last page, partial)
        let resp = axum::body::to_bytes(
            app.clone().oneshot(
                axum::http::Request::builder()
                    .method("GET").uri(format!("/jobs/{job_id}/results?page=2&page_size=2"))
                    .body(axum::body::Body::empty()).unwrap(),
            ).await.unwrap().into_body(), usize::MAX,
        ).await.unwrap();
        let page2: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(page2["rows"].as_array().unwrap().len(), 1);
        assert_eq!(page2["rows"][0]["kafka_offset"], 4);
    }

    #[tokio::test]
    async fn per_client_queue_cap_returns_429_on_sixth_job() {
        let dir = tempfile::tempdir().unwrap();
        let jobs_dir = dir.path().join("jobs");
        std::fs::create_dir_all(&jobs_dir).unwrap();
        let manifest = Manifest::open(&dir.path().join("manifest.db")).unwrap();

        // Cap of 5; semaphore of 0 keeps every submitted job in Queued state.
        let job_semaphore = Arc::new(Semaphore::new(0));
        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
            minio: MinioConfig::default(),
            cold_semaphore: Arc::new(Semaphore::new(4)),
            cold_cache: Arc::new(Mutex::new(
                ColdCache::new(dir.path().join("cache"), 10 * 1024 * 1024 * 1024).unwrap(),
            )),
            job_store: Arc::new(Mutex::new(HashMap::new())),
            jobs_dir,
            job_timeout_secs: 60,
            job_semaphore: Arc::clone(&job_semaphore),
            per_client_queue_cap: 5,
        };

        let app = make_job_app(state);

        let body = serde_json::json!({
            "sql": "SELECT 1", "time_from": 0, "time_to": i64::MAX, "limit": 1
        });

        // First 5 submissions must be accepted (202).
        for i in 1..=5 {
            let resp = app.clone().oneshot(
                axum::http::Request::builder()
                    .method("POST").uri("/jobs")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            ).await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED, "job {i} should be accepted");
        }

        // 6th submission must be rejected with 429 and Retry-After.
        let resp = app.clone().oneshot(
            axum::http::Request::builder()
                .method("POST").uri("/jobs")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        ).await.unwrap();

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            resp.headers().contains_key(header::RETRY_AFTER),
            "429 response must include Retry-After header"
        );

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "queue_cap_exceeded");
        assert_eq!(body["cap"], 5);

        // Release the semaphore so the 5 queued jobs can eventually finish.
        drop(job_semaphore);
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

        let state = AppState { manifest, minio: MinioConfig::default(), cold_semaphore: Arc::new(Semaphore::new(4)), cold_cache: Arc::new(Mutex::new(ColdCache::new(std::env::temp_dir().join("log-ingest-test-cache"), 10 * 1024 * 1024 * 1024).unwrap())), job_store: Arc::new(Mutex::new(HashMap::new())), jobs_dir: std::env::temp_dir().join("log-ingest-test-jobs"), job_timeout_secs: 300, job_semaphore: Arc::new(Semaphore::new(8)), per_client_queue_cap: 5 };
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
