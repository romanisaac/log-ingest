use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BOOTSTRAP: &str = "localhost:9092";
const TOPIC: &str = "logs";
const EVENT_COUNT: usize = 1_000;

fn main() {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create Kafka producer — is Redpanda running?");

    let services = ["web", "auth", "billing", "worker"];
    let levels = ["INFO", "INFO", "INFO", "WARN", "ERROR", "DEBUG"];
    let messages = [
        "request handled",
        "user authenticated",
        "payment processed",
        "job completed",
        "connection timeout",
        "cache miss",
    ];

    for i in 0..EVENT_COUNT {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        let event = json!({
            "timestamp": ts,
            "level": levels[i % levels.len()],
            "service": services[i % services.len()],
            "message": messages[i % messages.len()],
            "kafka_partition": 0,
            "kafka_offset": i,
            "attributes": {
                "request_id": format!("req-{i:06}"),
                "duration_ms": (i % 500) as f64,
                "user_id": format!("user-{}", i % 100),
            }
        });

        let payload = event.to_string();
        producer
            .send(BaseRecord::to(TOPIC).payload(&payload).key(&format!("k{i}")))
            .unwrap_or_else(|_| panic!("failed to enqueue event {i}"));
    }

    producer
        .flush(Duration::from_secs(10))
        .expect("failed to flush producer");

    println!("seeded {EVENT_COUNT} events to topic '{TOPIC}' on {BOOTSTRAP}");
}
