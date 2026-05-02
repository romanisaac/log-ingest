use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use chrono::{TimeZone, Utc};
use datafusion::prelude::*;
use event_schema::{encode_batch, LogEvent};
use manifest::{FlushMeta, Manifest};
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use tokio::sync::Mutex;
use uuid::Uuid;

// ─── Flush logic ─────────────────────────────────────────────────────────────

/// Write a batch of events to Parquet under `data_dir`, register in manifest.
/// Returns the path of the written file.
pub fn flush_events(
    events: &[LogEvent],
    data_dir: &Path,
    manifest: &mut Manifest,
) -> Result<PathBuf> {
    assert!(!events.is_empty(), "flush_events called with empty batch");

    let batch = encode_batch(events).context("encode batch")?;

    // Derive metadata from the batch.
    let min_ts = events.iter().map(|e| e.timestamp).min().unwrap();
    let max_ts = events.iter().map(|e| e.timestamp).max().unwrap();
    let min_kafka_offset = events.iter().map(|e| e.kafka_offset).min().unwrap();
    let max_kafka_offset = events.iter().map(|e| e.kafka_offset).max().unwrap();
    // Use the service from the first event (single-service batches in this slice).
    let service = &events[0].service;

    // time_bucket = YYYY-MM-DD-HH derived from min_ts (nanoseconds).
    let dt = Utc.timestamp_nanos(min_ts);
    let time_bucket = dt.format("%Y-%m-%d-%H").to_string();

    // Build file path: data/{service}/{time_bucket}/{uuid}.parquet
    let dir = data_dir.join(service).join(&time_bucket);
    std::fs::create_dir_all(&dir).context("create parquet directory")?;
    let file_name = format!("{}.parquet", Uuid::new_v4());
    let file_path = dir.join(&file_name);

    // Write Parquet file.
    let file = std::fs::File::create(&file_path).context("create parquet file")?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .context("create ArrowWriter")?;
    writer.write(&batch).context("write batch")?;
    writer.close().context("close writer")?;

    let size_bytes = std::fs::metadata(&file_path)
        .context("stat parquet file")?
        .len() as i64;

    let meta = FlushMeta {
        path: file_path.to_string_lossy().into_owned(),
        service: service.clone(),
        time_bucket,
        min_ts,
        max_ts,
        size_bytes,
        record_count: events.len() as i64,
        min_kafka_offset,
        max_kafka_offset,
    };
    manifest.commit_flush(&meta).context("commit flush")?;

    Ok(file_path)
}

// ─── HTTP API ─────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
    #[allow(dead_code)] // enforced in issue #4
    time_from: i64,
    #[allow(dead_code)] // enforced in issue #4
    time_to: i64,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    1000
}

#[derive(Clone)]
struct AppState {
    manifest: Arc<Mutex<Manifest>>,
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
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
    // Collect active files while holding the lock briefly.
    let files = {
        let guard = state.manifest.lock().await;
        guard.active_files(None)?
    };

    if files.is_empty() {
        return Ok(vec![]);
    }

    let ctx = SessionContext::new();

    // Register each parquet file as its own table, then UNION ALL into "logs".
    let mut table_names: Vec<String> = Vec::new();
    for (i, entry) in files.iter().enumerate() {
        let tname = format!("_file_{}", i);
        ctx.register_parquet(&tname, &entry.path, ParquetReadOptions::default())
            .await
            .with_context(|| format!("register parquet {}", entry.path))?;
        table_names.push(tname);
    }

    let union_sql = table_names
        .iter()
        .map(|t| format!("SELECT * FROM {}", t))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    ctx.sql(&format!("CREATE OR REPLACE VIEW logs AS {}", union_sql))
        .await
        .context("create logs view")?
        .collect()
        .await
        .context("materialize view creation")?;

    // Apply user limit to the SQL.
    let final_sql = format!("{} LIMIT {}", req.sql.trim_end_matches(';'), req.limit);
    let df = ctx.sql(&final_sql).await.context("parse user sql")?;
    let batches = df.collect().await.context("execute query")?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let field = schema.field(col_idx);
                let val = arrow_value_to_json(col, row_idx)?;
                obj.insert(field.name().clone(), val);
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
        DataType::Int64 => {
            let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            Ok(serde_json::Value::Number(v.into()))
        }
        DataType::Int32 => {
            let v = col.as_any().downcast_ref::<Int32Array>().unwrap().value(row);
            Ok(serde_json::Value::Number(v.into()))
        }
        DataType::Utf8 => {
            let v = col.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            Ok(serde_json::Value::String(v.to_string()))
        }
        DataType::LargeUtf8 => {
            use arrow::array::LargeStringArray;
            let v = col.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row);
            Ok(serde_json::Value::String(v.to_string()))
        }
        DataType::Utf8View => {
            let v = col.as_any().downcast_ref::<StringViewArray>().unwrap().value(row);
            Ok(serde_json::Value::String(v.to_string()))
        }
        DataType::Boolean => {
            let v = col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            Ok(serde_json::Value::Bool(v))
        }
        dt => anyhow::bail!("unsupported column type: {:?}", dt),
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

    let db_path = std::env::var("MANIFEST_PATH").unwrap_or_else(|_| "manifest.db".to_string());
    let manifest = Manifest::open(Path::new(&db_path)).expect("open manifest");
    let state = AppState {
        manifest: Arc::new(Mutex::new(manifest)),
    };

    let app = Router::new()
        .route("/query", post(query_handler))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

// ─── Integration test ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use event_schema::{AttributeValue, Level};
    use std::collections::HashMap;

    fn make_event() -> LogEvent {
        let mut attrs = HashMap::new();
        attrs.insert(
            "request_id".to_string(),
            AttributeValue::String("test-req-1".to_string()),
        );
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

        // Write event to Parquet + manifest.
        let mut manifest = Manifest::open(&db_path).unwrap();
        flush_events(&[event.clone()], &data_dir, &mut manifest).unwrap();

        // Verify manifest has one active file.
        let files = manifest.active_files(None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 1);
        assert_eq!(files[0].service, "test-svc");

        // Query via DataFusion.
        let state = AppState {
            manifest: Arc::new(Mutex::new(manifest)),
        };
        let req = QueryRequest {
            sql: "SELECT * FROM logs".to_string(),
            time_from: 0,
            time_to: i64::MAX,
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

        // Verify attributes round-trip through the JSON column.
        let attrs: HashMap<String, AttributeValue> =
            serde_json::from_str(row["attributes"].as_str().unwrap()).unwrap();
        assert_eq!(attrs["request_id"], AttributeValue::String("test-req-1".to_string()));
        assert_eq!(attrs["status_code"], AttributeValue::Int(200));
        assert_eq!(attrs["cached"], AttributeValue::Bool(true));
    }
}
