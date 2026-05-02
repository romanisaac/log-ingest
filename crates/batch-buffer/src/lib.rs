use event_schema::LogEvent;

pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RECORDS: usize = 50_000;
pub const DEFAULT_MAX_AGE_MS: u64 = 1_000;

// ─── Clock abstraction ───────────────────────────────────────────────────────

pub trait Clock: Send {
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

// ─── BatchBuffer ─────────────────────────────────────────────────────────────

pub struct BatchBuffer {
    events: Vec<LogEvent>,
    current_bytes: usize,
    max_bytes: usize,
    max_records: usize,
    max_age_ms: u64,
    batch_started_ms: Option<u64>,
    clock: Box<dyn Clock>,
}

impl BatchBuffer {
    pub fn new(
        max_bytes: usize,
        max_records: usize,
        max_age_ms: u64,
        clock: Box<dyn Clock>,
    ) -> Self {
        Self {
            events: Vec::new(),
            current_bytes: 0,
            max_bytes,
            max_records,
            max_age_ms,
            batch_started_ms: None,
            clock,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            DEFAULT_MAX_BYTES,
            DEFAULT_MAX_RECORDS,
            DEFAULT_MAX_AGE_MS,
            Box::new(SystemClock),
        )
    }

    /// Push an event. Returns the flushed batch if any threshold is reached.
    pub fn push(&mut self, event: LogEvent) -> Option<Vec<LogEvent>> {
        if self.batch_started_ms.is_none() {
            self.batch_started_ms = Some(self.clock.now_ms());
        }
        self.current_bytes += event.estimated_byte_size();
        self.events.push(event);
        if self.over_any_limit() {
            Some(self.drain())
        } else {
            None
        }
    }

    /// Check the time trigger. Call this periodically when push() isn't being
    /// called frequently enough to rely on it catching the age threshold.
    pub fn poll(&mut self) -> Option<Vec<LogEvent>> {
        if !self.is_empty() && self.age_exceeded() {
            Some(self.drain())
        } else {
            None
        }
    }

    /// Drain all buffered events, resetting the buffer.
    pub fn drain(&mut self) -> Vec<LogEvent> {
        self.current_bytes = 0;
        self.batch_started_ms = None;
        std::mem::take(&mut self.events)
    }

    /// Fraction of max_bytes currently buffered, clamped to [0.0, 1.0].
    pub fn occupancy(&self) -> f32 {
        (self.current_bytes as f32 / self.max_bytes as f32).min(1.0)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn age_exceeded(&self) -> bool {
        self.batch_started_ms
            .map(|start| self.clock.now_ms().saturating_sub(start) >= self.max_age_ms)
            .unwrap_or(false)
    }

    fn over_any_limit(&self) -> bool {
        self.current_bytes >= self.max_bytes
            || self.events.len() >= self.max_records
            || self.age_exceeded()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use event_schema::{AttributeValue, Level};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn make_event(message_size: usize) -> LogEvent {
        LogEvent {
            timestamp: 1_700_000_000_000_000_000,
            level: Level::Info,
            service: "svc".to_string(),
            message: "x".repeat(message_size),
            kafka_partition: 0,
            kafka_offset: 0,
            attributes: HashMap::new(),
        }
    }

    struct MockClock(Arc<Mutex<u64>>);

    impl Clock for MockClock {
        fn now_ms(&self) -> u64 {
            *self.0.lock().unwrap()
        }
    }

    fn mock_clock() -> (Box<dyn Clock>, Arc<Mutex<u64>>) {
        let inner = Arc::new(Mutex::new(0u64));
        (Box::new(MockClock(Arc::clone(&inner))), inner)
    }

    fn buf_with_mock_clock(max_age_ms: u64) -> (BatchBuffer, Arc<Mutex<u64>>) {
        let (clock, time) = mock_clock();
        let buf = BatchBuffer::new(DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS, max_age_ms, clock);
        (buf, time)
    }

    #[test]
    fn time_trigger_fires_via_poll() {
        let (mut buf, time) = buf_with_mock_clock(1_000);

        buf.push(make_event(0));
        assert!(buf.poll().is_none(), "should not flush before age threshold");

        *time.lock().unwrap() = 1_001;
        let batch = buf.poll().expect("should flush after age threshold");
        assert_eq!(batch.len(), 1);
        assert!(buf.is_empty());
    }

    #[test]
    fn time_trigger_fires_via_push() {
        let (mut buf, time) = buf_with_mock_clock(1_000);

        buf.push(make_event(0));
        *time.lock().unwrap() = 1_001;

        // Next push sees the age exceeded and flushes both events.
        let batch = buf.push(make_event(0)).expect("push should flush when age exceeded");
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn time_trigger_resets_after_flush() {
        let (mut buf, time) = buf_with_mock_clock(1_000);

        buf.push(make_event(0));
        *time.lock().unwrap() = 1_001;
        buf.poll().unwrap(); // flush

        // New batch starts; advancing time slightly should not flush.
        buf.push(make_event(0));
        *time.lock().unwrap() = 1_500; // only 499ms since new batch started
        assert!(buf.poll().is_none(), "new batch should not flush yet");

        *time.lock().unwrap() = 2_002; // now 1001ms since new batch
        assert!(buf.poll().is_some(), "new batch should flush now");
    }

    #[test]
    fn size_trigger_fires_exactly_once() {
        let mut buf = BatchBuffer::new(100, DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));

        let result = buf.push(make_event(40));
        assert!(result.is_none());
        assert_eq!(buf.len(), 1);

        let batch = buf.push(make_event(40)).expect("expected flush on second push");
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn record_count_trigger_fires_before_size_limit() {
        let mut buf = BatchBuffer::new(10_000, 5, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));

        for _ in 0..4 {
            assert!(buf.push(make_event(0)).is_none(), "should not flush before limit");
        }

        let batch = buf.push(make_event(0)).expect("5th event should trigger flush");
        assert_eq!(batch.len(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn size_trigger_fires_before_record_count_limit() {
        let mut buf = BatchBuffer::new(1_500, 1_000, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));

        assert!(buf.push(make_event(1000)).is_none());
        let batch = buf.push(make_event(1000)).expect("size trigger should fire on 2nd event");
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn occupancy_tracks_fill_level() {
        let mut buf = BatchBuffer::new(100, DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));
        assert_eq!(buf.occupancy(), 0.0);

        buf.push(make_event(23));
        let occ = buf.occupancy();
        assert!(occ > 0.0 && occ <= 1.0, "occupancy={occ}");
    }

    #[test]
    fn buffer_empty_after_drain() {
        let mut buf = BatchBuffer::with_defaults();
        buf.push(make_event(10));
        buf.push(make_event(10));
        assert!(!buf.is_empty());

        let batch = buf.drain();
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
        assert_eq!(buf.occupancy(), 0.0);
    }

    #[test]
    fn oversized_event_flushes_immediately() {
        let mut buf = BatchBuffer::new(10, DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));
        let batch = buf.push(make_event(1000)).expect("oversized event should trigger flush");
        assert_eq!(batch.len(), 1);
        assert_eq!(buf.occupancy(), 0.0);
        assert!(buf.is_empty());
    }

    #[test]
    fn attributes_contribute_to_byte_count() {
        let make = |attrs: HashMap<String, AttributeValue>| LogEvent {
            timestamp: 0,
            level: Level::Debug,
            service: "x".to_string(),
            message: "y".to_string(),
            kafka_partition: 1,
            kafka_offset: 2,
            attributes: attrs,
        };

        let mut bare_buf = BatchBuffer::new(40, DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));
        assert!(bare_buf.push(make(HashMap::new())).is_none(), "bare event should not flush");

        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), AttributeValue::String("value".to_string()));
        attrs.insert("count".to_string(), AttributeValue::Int(42));
        let mut attr_buf = BatchBuffer::new(40, DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGE_MS, Box::new(SystemClock));
        assert!(attr_buf.push(make(attrs)).is_some(), "attributed event should flush");
    }
}
