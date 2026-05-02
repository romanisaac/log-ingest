use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use datafusion::prelude::*;
use manifest::Manifest;
use serde::Deserialize;
use server::{run_consumer, ConsumerConfig};
use tokio::sync::{broadcast, Mutex};

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
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    if req.time_from.is_none() || req.time_to.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "time_from and time_to are required",
                "code": "missing_time_predicate"
            })),
        ).into_response();
    }

    match run_query(state, req).await {
        Ok(rows) => (StatusCode::OK, Json(rows)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("{:#}", e) })),
        )
            .into_response(),
    }
}

async fn run_query(state: AppState, req: QueryRequest) -> Result<Vec<serde_json::Value>> {
    let files = {
        let guard = state.manifest.lock().await;
        guard.active_files(None)?
    };

    if files.is_empty() {
        return Ok(vec![]);
    }

    let ctx = SessionContext::new();

    let mut table_names: Vec<String> = Vec::new();
    for (i, entry) in files.iter().enumerate() {
        let tname = format!("_file_{i}");
        ctx.register_parquet(&tname, &entry.path, ParquetReadOptions::default())
            .await
            .with_context(|| format!("register parquet {}", entry.path))?;
        table_names.push(tname);
    }

    let union_sql = table_names
        .iter()
        .map(|t| format!("SELECT * FROM {t}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    ctx.sql(&format!("CREATE OR REPLACE VIEW logs AS {union_sql}"))
        .await
        .context("create logs view")?
        .collect()
        .await
        .context("materialize view creation")?;

    let final_sql = format!("{} LIMIT {}", req.sql.trim_end_matches(';'), req.limit);
    let batches = ctx
        .sql(&final_sql)
        .await
        .context("parse user sql")?
        .collect()
        .await
        .context("execute query")?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let val = arrow_value_to_json(batch.column(col_idx), row_idx)?;
                obj.insert(schema.field(col_idx).name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }
    Ok(rows)
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

    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string()),
    );
    std::fs::create_dir_all(&data_dir).expect("create data dir");

    let db_path = std::env::var("MANIFEST_PATH").unwrap_or_else(|_| "manifest.db".to_string());
    let manifest = Arc::new(Mutex::new(
        Manifest::open(Path::new(&db_path)).expect("open manifest"),
    ));

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Start Kafka consumer as a background task.
    let consumer_manifest = Arc::clone(&manifest);
    tokio::spawn(async move {
        if let Err(e) = run_consumer(
            ConsumerConfig::default(),
            data_dir,
            consumer_manifest,
            shutdown_rx,
        )
        .await
        {
            tracing::error!("consumer exited with error: {e:#}");
        }
    });

    let state = AppState { manifest };
    let app = Router::new()
        .route("/query", post(query_handler))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(state);

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
    use std::collections::HashMap;
    use tower::util::ServiceExt;

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

        let state = AppState { manifest: Arc::new(Mutex::new(manifest)) };
        let req = QueryRequest {
            sql: "SELECT * FROM logs".to_string(),
            time_from: Some(0),
            time_to: Some(i64::MAX),
            limit: 100,
        };
        let rows = run_query(state, req).await.unwrap();
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
        let state = AppState { manifest: Arc::new(Mutex::new(manifest)) };

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
        let state = AppState { manifest: Arc::new(Mutex::new(manifest)) };

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
                },
                consumer_data_dir,
                consumer_manifest,
                shutdown_rx,
            )
            .await;
        });

        // Wait for the time trigger (1s) plus buffer.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = shutdown_tx.send(());
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Query and assert all N events are present.
        let state = AppState { manifest };
        let req = QueryRequest {
            sql: "SELECT * FROM logs".to_string(),
            time_from: Some(0),
            time_to: Some(i64::MAX),
            limit: 1000,
        };
        let rows = run_query(state, req).await.unwrap();
        assert_eq!(rows.len(), N, "expected {N} events, got {}", rows.len());
    }
}
