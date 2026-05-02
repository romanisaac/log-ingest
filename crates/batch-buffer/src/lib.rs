use event_schema::LogEvent;

pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RECORDS: usize = 50_000;

pub struct BatchBuffer {
    events: Vec<LogEvent>,
    current_bytes: usize,
    max_bytes: usize,
    max_records: usize,
}

impl BatchBuffer {
    pub fn new(max_bytes: usize, max_records: usize) -> Self {
        Self {
            events: Vec::new(),
            current_bytes: 0,
            max_bytes,
            max_records,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS)
    }

    /// Push an event. Returns the flushed batch if either threshold is reached.
    pub fn push(&mut self, event: LogEvent) -> Option<Vec<LogEvent>> {
        self.current_bytes += event.estimated_byte_size();
        self.events.push(event);
        if self.current_bytes >= self.max_bytes || self.events.len() >= self.max_records {
            Some(self.drain())
        } else {
            None
        }
    }

    /// Drain all buffered events, resetting the buffer.
    pub fn drain(&mut self) -> Vec<LogEvent> {
        self.current_bytes = 0;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_schema::{AttributeValue, Level};
    use std::collections::HashMap;

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

    #[test]
    fn size_trigger_fires_exactly_once() {
        let mut buf = BatchBuffer::new(100, DEFAULT_MAX_RECORDS);

        let result = buf.push(make_event(40));
        assert!(result.is_none());
        assert_eq!(buf.len(), 1);

        let result = buf.push(make_event(40));
        let batch = result.expect("expected flush on second push");
        assert_eq!(batch.len(), 2);

        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn record_count_trigger_fires_before_size_limit() {
        // Small events — 27 bytes each — so 5 events = 135 bytes, well under a 10 KB size limit.
        // Set max_records = 5 to trigger on count first.
        let mut buf = BatchBuffer::new(10_000, 5);

        for _ in 0..4 {
            assert!(buf.push(make_event(0)).is_none(), "should not flush before limit");
        }

        let batch = buf.push(make_event(0)).expect("5th event should trigger flush");
        assert_eq!(batch.len(), 5);
        assert!(buf.is_empty());
    }

    #[test]
    fn size_trigger_fires_before_record_count_limit() {
        // Large events — each one is 27 + 1000 = 1027 bytes.
        // max_bytes = 1500 → flush after 2 events (2054 bytes).
        // max_records = 1000 → would never fire before size.
        let mut buf = BatchBuffer::new(1_500, 1_000);

        assert!(buf.push(make_event(1000)).is_none());
        let batch = buf.push(make_event(1000)).expect("size trigger should fire on 2nd event");
        assert_eq!(batch.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn occupancy_tracks_fill_level() {
        let mut buf = BatchBuffer::new(100, DEFAULT_MAX_RECORDS);
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
        let mut buf = BatchBuffer::new(10, DEFAULT_MAX_RECORDS);
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

        let mut bare_buf = BatchBuffer::new(40, DEFAULT_MAX_RECORDS);
        assert!(bare_buf.push(make(HashMap::new())).is_none(), "bare event should not flush");

        let mut attrs = HashMap::new();
        attrs.insert("key".to_string(), AttributeValue::String("value".to_string()));
        attrs.insert("count".to_string(), AttributeValue::Int(42));
        let mut attr_buf = BatchBuffer::new(40, DEFAULT_MAX_RECORDS);
        assert!(attr_buf.push(make(attrs)).is_some(), "attributed event should flush");
    }
}
