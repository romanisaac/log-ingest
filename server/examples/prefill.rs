// Dev-only helper for manually testing the query path before the Kafka consumer
// (issue #6) is wired up. Writes a small batch of synthetic events directly to
// the local Parquet + manifest store so the server has data to query.
//
// Usage:
//   cargo run --example prefill   # populate data/ and manifest.db
//   cargo run --bin server        # start server
//   curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
//        -d '{"sql":"SELECT * FROM logs"}'
//
// Remove this file once issue #6 (Kafka ingestion) lands and `make seed` works.

use event_schema::{AttributeValue, Level, LogEvent};
use manifest::Manifest;
use server::flush_events;
use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn main() {
    std::fs::create_dir_all("data").unwrap();
    let mut manifest = Manifest::open(Path::new("manifest.db")).unwrap();

    let services = ["web", "auth", "billing"];
    let levels = [Level::Info, Level::Warn, Level::Error];
    let messages = ["request handled", "auth failed", "payment declined"];

    let events: Vec<LogEvent> = (0..20)
        .map(|i| {
            let mut attrs = HashMap::new();
            attrs.insert("request_id".to_string(), AttributeValue::String(format!("req-{i:04}")));
            attrs.insert("duration_ms".to_string(), AttributeValue::Float((i * 7 % 300) as f64));
            attrs.insert("retries".to_string(), AttributeValue::Int(i % 3));

            LogEvent {
                timestamp: now_nanos() - (i as i64 * 1_000_000_000),
                level: levels[i as usize % levels.len()].clone(),
                service: services[i as usize % services.len()].to_string(),
                message: messages[i as usize % messages.len()].to_string(),
                kafka_partition: (i % 4) as i32,
                kafka_offset: i,
                attributes: attrs,
            }
        })
        .collect();

    flush_events(&events, Path::new("data"), &mut manifest).unwrap();
    println!("wrote {} events to data/ and manifest.db", events.len());
}
